"""日结账本：从已应用 CDC 事件里拣出当日订单与等级变更，逐单选等级。

跨批次合并是刻意的：主文件、旧列名补数、迟到补丁是分别到达的导出，日结必须把它们
合成一份账本，否则只按最新批次算会丢掉先到的订单。合并键 = 规范化事件签名
（与 M3 去重同一口径），所以同一补数重复到达只算一次，不会重复累计。

截止点：

- `boundary_utc`：交班边界 = 次日 09:00（本地时区），固定值，与数据无关；
- `data_horizon_utc`：本次实际纳入的已应用事件最大 `event_time`，由已应用事件决定；
  冲突变更不参与计算，因此**不能推进** `data_horizon_utc`。

边界是「哪些事件属于这一天」的硬闸，horizon 是「这一天实际用到了多新的数据」的证据。
"""
from __future__ import annotations

import datetime as dt
import hashlib
import json
from dataclasses import dataclass, field
from decimal import Decimal

from ... import config
from ..m2_schema.coerce import iso_utc, parse_iso
from . import policy


@dataclass(frozen=True)
class LedgerEvent:
    table: str
    pk: str
    version: int
    op: str
    event_time: dt.datetime
    payload: dict
    amount: Decimal | None
    qty: int | None
    row_ref: str
    reason_code: str | None = None

    @property
    def key(self) -> tuple[str, str]:
        return (self.table, self.pk)

    def signature(self) -> str:
        """与 M3 去重同一口径：金额用规范化 Decimal，时区写法先归 UTC。"""
        body = "|".join([
            self.table, self.pk, str(self.version), self.op,
            iso_utc(self.event_time) or "",
            json.dumps(self._canonical_payload(), ensure_ascii=False,
                       separators=(",", ":")),
        ])
        return hashlib.sha256(body.encode("utf-8")).hexdigest()

    def _canonical_payload(self) -> dict:
        out = {}
        for key in sorted(self.payload):
            if key == "amount":
                out[key] = None if self.amount is None else str(self.amount)
            else:
                value = self.payload[key]
                out[key] = "" if value is None else str(value).strip()
        if self.amount is not None and "amount" not in out:
            out["amount"] = str(self.amount)
        return out


@dataclass
class OrderLine:
    order_pk: str
    customer_pk: str | None
    order_time: dt.datetime
    amount: Decimal | None
    commission: Decimal
    tier: str
    tier_source: str
    tier_event_ref: str | None
    status: str
    reason_code: str | None
    row_ref: str


@dataclass
class Anomaly:
    kind: str
    key: str
    reason_code: str
    detail: str = ""
    refs: list[str] = field(default_factory=list)


@dataclass
class Ledger:
    settle_date: str
    boundary_utc: dt.datetime
    data_horizon_utc: dt.datetime | None
    orders: list[OrderLine] = field(default_factory=list)
    anomalies: list[Anomaly] = field(default_factory=list)
    conflict_events: int = 0
    input_digest: str = ""

    @property
    def commission_total(self) -> Decimal:
        total = Decimal("0.00")
        for row in self.orders:
            if row.status == "settled":
                total += row.commission
        return total

    @property
    def order_total(self) -> Decimal:
        total = Decimal("0.00")
        for row in self.orders:
            if row.status == "settled" and row.amount is not None:
                total += row.amount
        return total

    def counts(self) -> dict:
        settled = [r for r in self.orders if r.status == "settled"]
        return {
            "orders": len(self.orders),
            "settled": len(settled),
            "held": len(self.orders) - len(settled),
            "anomalies": len(self.anomalies),
            "conflict_events": self.conflict_events,
        }


def from_rows(rows: list[dict]) -> list[LedgerEvent]:
    out = []
    for row in rows:
        when = parse_iso(row.get("event_time_utc"))
        if when is None:
            continue
        out.append(LedgerEvent(
            table=row["table_name"], pk=str(row["pk"]),
            version=int(row.get("version") or 0), op=row.get("op") or "upsert",
            event_time=when, payload=dict(row.get("payload") or {}),
            amount=row.get("amount"), qty=row.get("qty"),
            row_ref=row.get("row_ref") or "",
            reason_code=row.get("reason_code")))
    return out


def merge(events: list[LedgerEvent]) -> list[LedgerEvent]:
    """跨批次合并：同签名只留一条（保留最早到达的证据行引用）。

    同签名在一批里是已应用、另一批里是冲突时，按保守口径取冲突：
    宁可挂起等国税对账，也不拿一条「可能被否掉」的金额入账。
    """
    seen: dict[str, LedgerEvent] = {}
    for event in events:
        sig = event.signature()
        prior = seen.get(sig)
        if prior is None:
            seen[sig] = event
        elif _is_conflict(event) and not _is_conflict(prior):
            seen[sig] = event
    return list(seen.values())


def _is_conflict(event: LedgerEvent) -> bool:
    return (event.reason_code or "").startswith("C_")


def build(events: list[LedgerEvent], settle_date: str) -> Ledger:
    boundary = policy.cutoff_local(settle_date)
    merged = merge(events)
    ledger = Ledger(settle_date=settle_date, boundary_utc=boundary,
                    data_horizon_utc=None)

    used: list[LedgerEvent] = []
    tier_events: list[policy.TierEvent] = []
    # 客户 → 冲突等级变更的最早时刻：该时刻及之后的订单无法定级，挂起。
    conflict_since: dict[str, dt.datetime] = {}
    conflict_refs: dict[str, list[str]] = {}
    order_conflicts: dict[str, list[LedgerEvent]] = {}

    for event in merged:
        if event.event_time > boundary:
            continue
        if _is_conflict(event):
            # 冲突事件不参与计算，因此不推进 data_horizon_utc；
            # 它只把「依赖它的订单」挂起，不改写更早的订单。
            if event.table == "customers" and "tier" in event.payload:
                prior = conflict_since.get(event.pk)
                if prior is None or event.event_time < prior:
                    conflict_since[event.pk] = event.event_time
                conflict_refs.setdefault(event.pk, []).append(event.row_ref)
            if (event.table == "orders"
                    and policy.local_date_of(event.event_time) == settle_date):
                order_conflicts.setdefault(event.pk, []).append(event)
            continue
        used.append(event)

    for event in used:
        if event.table == "customers" and event.op == "upsert":
            tier = str(event.payload.get("tier") or "").strip()
            if not tier:
                continue
            tier_events.append(policy.TierEvent(
                customer_pk=event.pk, tier=tier, event_time=event.event_time,
                version=event.version, seq=0, row_ref=event.row_ref))

    by_customer: dict[str, list[policy.TierEvent]] = {}
    for item in tier_events:
        by_customer.setdefault(item.customer_pk, []).append(item)

    day_orders = _orders_of_day(used, settle_date)
    known = {row.pk for row in day_orders}
    for pk, items in sorted(order_conflicts.items()):
        if pk in known:
            continue
        # 整单只有冲突版本：仍列成一行挂起，避免「订单消失、无人对账」。
        first = min(items, key=lambda e: (e.event_time, e.version))
        day_orders.append(LedgerEvent(
            table="orders", pk=pk, version=first.version, op="upsert",
            event_time=first.event_time, payload={}, amount=None, qty=None,
            row_ref=first.row_ref))
    day_orders.sort(key=lambda e: (e.event_time, e.pk))

    # 只登记「会影响当日订单」的冲突：当日订单冲突 + 当日订单所属客户的等级冲突。
    day_customers = {str(o.payload.get("customer") or "").strip()
                     for o in day_orders} - {""}
    relevant_customers = set(conflict_since) & day_customers
    ledger.conflict_events = (sum(len(v) for v in order_conflicts.values())
                              + sum(len(conflict_refs[c]) for c in relevant_customers))
    for customer in sorted(relevant_customers):
        ledger.anomalies.append(Anomaly(
            kind="tier_conflict", key=f"customers/{customer}",
            reason_code="C_TIER_CONFLICT",
            detail=(f"该客户在 {iso_utc(conflict_since[customer])} 出现冲突的等级变更；"
                    "该时刻及之后的订单挂起，更早订单不受影响"),
            refs=conflict_refs[customer]))

    for order in day_orders:
        customer = str(order.payload.get("customer") or "").strip() or None
        if customer is None and order.pk not in order_conflicts:
            # 佣金只针对可归属会员的订单；没有客户号的订单不在结算范围。
            continue
        if order.pk in order_conflicts:
            # 订单自身的同版本冲突：金额无法确定，整单挂起，不猜哪个版本对。
            ledger.anomalies.append(Anomaly(
                kind="order_conflict", key=f"orders/{order.pk}",
                reason_code="C_VERSION_CONFLICT",
                detail="订单同版本内容冲突，金额无法确定，挂起不结",
                refs=[e.row_ref for e in order_conflicts[order.pk]]))
            ledger.orders.append(OrderLine(
                order_pk=order.pk, customer_pk=customer,
                order_time=order.event_time, amount=order.amount,
                commission=Decimal("0.00"), tier="", tier_source="conflict",
                tier_event_ref=None, status="held",
                reason_code="C_VERSION_CONFLICT", row_ref=order.row_ref))
            continue
        since = conflict_since.get(customer)
        if since is not None and order.event_time >= since:
            resolution = policy.TierResolution(tier="", source="conflict",
                                               reason_code="C_TIER_CONFLICT")
        else:
            resolution = policy.effective_tier(
                by_customer.get(customer, []), order.event_time)

        commission = policy.commission_for(
            order.amount, resolution.tier) if resolution.tier else None
        status = "settled"
        reason = resolution.reason_code
        if resolution.source == "conflict":
            status = "held"
            reason = "C_TIER_CONFLICT"
        elif resolution.source == "unknown":
            status = "held"
            reason = "T_TIER_UNKNOWN"
            ledger.anomalies.append(Anomaly(
                kind="unknown_tier", key=f"orders/{order.pk}",
                reason_code="T_TIER_UNKNOWN",
                detail=f"等级 {resolution.tier!r} 未在费率表登记，佣金无法计价",
                refs=[resolution.event.row_ref if resolution.event else order.row_ref]))
        elif commission is None:
            status = "held"
            reason = reason or "T_TIER_DEFAULT"
        ledger.orders.append(OrderLine(
            order_pk=order.pk, customer_pk=customer,
            order_time=order.event_time, amount=order.amount,
            commission=commission if commission is not None else Decimal("0.00"),
            tier=resolution.tier, tier_source=resolution.source,
            tier_event_ref=resolution.event.row_ref if resolution.event else None,
            status=status, reason_code=reason, row_ref=order.row_ref))

    ledger.orders.sort(key=lambda r: (r.order_time, r.order_pk))
    ledger.anomalies.sort(key=lambda a: (a.kind, a.key))
    horizon = [e.event_time for e in used]
    ledger.data_horizon_utc = max(horizon) if horizon else None
    ledger.input_digest = _digest(ledger)
    return ledger


def _orders_of_day(events: list[LedgerEvent], settle_date: str) -> list[LedgerEvent]:
    """当日发生的订单：最早 upsert 定「订单发生时间」，截至边界的最新 upsert 定金额。

    订单发生时间落在结算日当天才算当日订单；之后（含次日交班前）的版本只用来
    更新金额，不会把订单挪到另一天。删除（墓碑）意味着订单不再存在，整单不计佣金。
    """
    upserts: dict[str, list[LedgerEvent]] = {}
    deleted: set[str] = set()
    for event in events:
        if event.table != "orders":
            continue
        if event.op == "delete":
            deleted.add(event.pk)
            continue
        upserts.setdefault(event.pk, []).append(event)
    out: list[LedgerEvent] = []
    for pk, items in upserts.items():
        if pk in deleted:
            continue
        first = min(items, key=lambda e: (e.event_time, e.version))
        if policy.local_date_of(first.event_time) != settle_date:
            continue
        last = max(items, key=lambda e: (e.event_time, e.version))
        out.append(LedgerEvent(
            table=last.table, pk=pk, version=last.version, op="upsert",
            event_time=first.event_time, payload=last.payload,
            amount=last.amount, qty=last.qty, row_ref=last.row_ref))
    return sorted(out, key=lambda e: (e.event_time, e.pk))


def _digest(ledger: Ledger) -> str:
    parts = []
    for row in ledger.orders:
        parts.append("|".join([
            row.order_pk, row.customer_pk or "", iso_utc(row.order_time) or "",
            "" if row.amount is None else str(row.amount),
            str(row.commission), row.tier, row.tier_source, row.status,
            row.reason_code or "",
        ]))
    for item in ledger.anomalies:
        parts.append(f"{item.kind}|{item.key}|{item.reason_code}")
    return hashlib.sha256("\n".join(parts).encode("utf-8")).hexdigest()


def diff(old_orders: list[dict], new_orders: list[dict]) -> list[dict]:
    """逐订单对比：added / removed / changed，delta = 新佣金 − 旧佣金。"""
    old = {row["order_pk"]: row for row in old_orders}
    new = {row["order_pk"]: row for row in new_orders}
    out = []
    for pk in sorted(set(old) | set(new)):
        before = old.get(pk)
        after = new.get(pk)
        if before is None:
            out.append({
                "order_pk": pk, "change": "added",
                "old_commission": None, "new_commission": after["commission"],
                "old_tier": None, "new_tier": after["tier"],
                "delta": after["commission"], "row_ref": after.get("row_ref")})
            continue
        if after is None:
            out.append({
                "order_pk": pk, "change": "removed",
                "old_commission": before["commission"], "new_commission": None,
                "old_tier": before["tier"], "new_tier": None,
                "delta": -before["commission"], "row_ref": before.get("row_ref")})
            continue
        delta = after["commission"] - before["commission"]
        if (delta == 0 and before["tier"] == after["tier"]
                and before["status"] == after["status"]):
            continue
        out.append({
            "order_pk": pk, "change": "changed",
            "old_commission": before["commission"],
            "new_commission": after["commission"],
            "old_tier": before["tier"], "new_tier": after["tier"],
            "delta": delta, "row_ref": after.get("row_ref")})
    return out
