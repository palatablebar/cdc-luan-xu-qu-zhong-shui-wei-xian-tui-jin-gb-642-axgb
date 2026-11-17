"""发布与修正：计算结果 → 待发布预览 → 原子发布，失败保留旧版。

三条不变量：

1. **一次结果只落一次**：同一个 `input_digest` 重跑直接复用已算好的 run，
   不新增订单行、不改变佣金合计；
2. **发布是原子的**：新版本置 published、旧版本置 superseded、写影响面，
   同一事务提交；任何一步失败整体回滚，旧版与它的订单原样可读；
3. **改写已发布订单必须可解释**：发布前给出影响面（哪些订单、佣金差多少），
   没预览或预览后输入又变了就拒绝发布，旧版继续对外。
"""
from __future__ import annotations

import datetime as dt
import hashlib

from ... import config, errors, reasons
from ..m2_schema.coerce import iso_utc
from . import ledger as ledger_mod
from . import policy


def compute(store, *, settle_date: str, batch_id: str | None = None,
            rule_version: str | None = None) -> dict:
    """从 CDC 已应用事件算一份日结账本（不落库）。"""
    rule_version = rule_version or config.RULE_VERSION
    batches = _source_batches(store, batch_id)
    events: list[ledger_mod.LedgerEvent] = []
    for batch in batches:
        events.extend(ledger_mod.from_rows(store.load_events(batch["batch_id"], "applied")))
        events.extend(ledger_mod.from_rows(store.load_events(batch["batch_id"], "conflict")))
    book = ledger_mod.build(events, settle_date)
    digest = hashlib.sha256("|".join([
        book.input_digest, settle_date, rule_version,
        ",".join(b["batch_id"] for b in batches),
    ]).encode("utf-8")).hexdigest()
    book.input_digest = digest
    return {
        "ledger": book,
        "batches": batches,
        "rule_version": rule_version,
        "cutoff_utc": iso_utc(book.boundary_utc),
    }


def stage(store, *, settle_date: str, batch_id: str | None = None) -> dict:
    """算一次并落成 computed 状态的 run；同一 digest 已存在则原样复用。"""
    store_settle = store
    computed = compute(store, settle_date=settle_date, batch_id=batch_id)
    book = computed["ledger"]
    existing = _find_by_digest(store_settle, settle_date, book.input_digest)
    if existing is not None:
        return {"run": existing, "reused": True, "ledger": book,
                "batches": computed["batches"]}
    revision = store.next_settlement_revision(settle_date)
    run_id = f"S-{settle_date.replace('-', '')}-{revision:02d}-{book.input_digest[:6]}"
    header = {
        "run_id": run_id, "settle_date": settle_date, "revision": revision,
        "status": "computed", "computed_at": _now_iso(), "published_at": None,
        "cutoff_utc": computed["cutoff_utc"],
        "data_horizon_utc": iso_utc(book.data_horizon_utc),
        "rule_version": computed["rule_version"],
        "tier_policy": policy.policy_view()["tier_policy"],
        "input_digest": book.input_digest,
        "asof_batch_id": computed["batches"][-1]["batch_id"] if computed["batches"] else None,
        "supersedes": None, "blocked_reason": "",
        "note": _note_for(book),
    }
    store.save_settlement(header=header, orders=_order_rows(book),
                          anomalies=_anomaly_rows(book))
    return {"run": store.settlement_run(run_id), "reused": False, "ledger": book,
            "batches": computed["batches"]}


def preview(store, run_id: str) -> dict:
    """影响面：与当前已发布版本逐订单对比。"""
    run = _require_run(store, run_id)
    book_rows = _as_diff_rows(store.load_settlement_orders(run_id))
    published = store.published_settlement(run["settle_date"])
    if published is not None and published["run_id"] == run_id:
        face = {"run_id": run_id, "from_run_id": None, "rows": [],
                "added": 0, "removed": 0, "changed": 0,
                "delta_total": _sum([]), "published": None,
                "current": True}
        face["digest"] = _face_digest(face)
        return face
    if published is None:
        rows = [{"order_pk": r["order_pk"], "change": "added",
                 "old_commission": None, "new_commission": r["commission"],
                 "old_tier": None, "new_tier": r["tier"],
                 "delta": r["commission"], "row_ref": r.get("row_ref")}
                for r in book_rows]
        face = {"run_id": run_id, "from_run_id": None, "rows": rows,
                "added": len(rows), "removed": 0, "changed": 0,
                "delta_total": _sum(r["delta"] for r in rows),
                "published": None}
        face["digest"] = _face_digest(face)
        return face
    old_rows = _as_diff_rows(store.load_settlement_orders(published["run_id"]))
    rows = ledger_mod.diff(old_rows, book_rows)
    face = {
        "run_id": run_id, "from_run_id": published["run_id"], "rows": rows,
        "added": sum(1 for r in rows if r["change"] == "added"),
        "removed": sum(1 for r in rows if r["change"] == "removed"),
        "changed": sum(1 for r in rows if r["change"] == "changed"),
        "delta_total": _sum(r["delta"] for r in rows),
        "published": {"run_id": published["run_id"],
                      "revision": published["revision"],
                      "published_at": published.get("published_at")},
    }
    face["digest"] = _face_digest(face)
    return face


def publish(store, run_id: str, *, confirm_digest: str | None = None) -> dict:
    """发布一份预览过的 run。`confirm_digest` 必须等于本次影响面的摘要。

    未预览（无 confirm_digest）或预览后输入变了 → 409，旧版不动。
    """
    run = _require_run(store, run_id)
    if run["status"] == "published":
        return {"run_id": run_id, "status": "published", "noop": True,
                "reused": True, "supersedes": run.get("supersedes"),
                "diff": preview(store, run_id), "note": "重复发布是幂等的"}
    if run["status"] == "superseded":
        raise errors.Conflict(
            "该版本已被更新的版本替换，不能重新发布；请发布最新一版",
            code="B_SETTLE_PUBLISH_BLOCKED",
            detail={"run_id": run_id, "status": "superseded"})
    face = preview(store, run_id)
    if config.RESTATEMENT_PREVIEW_REQUIRED:
        if not confirm_digest:
            raise errors.Conflict(
                "发布前必须先预览影响面并带上确认摘要，旧版本保持对外",
                code="B_SETTLE_PUBLISH_BLOCKED",
                detail={"run_id": run_id, "impact_digest": face["digest"]})
        if confirm_digest != face["digest"]:
            raise errors.Conflict(
                "影响面已变化或确认摘要不匹配，拒绝发布，旧版本保持对外",
                code="B_SETTLE_PUBLISH_BLOCKED",
                detail={"run_id": run_id, "expected_digest": face["digest"],
                        "got": confirm_digest})
    if not face["rows"] and face["from_run_id"]:
        store.supersede_settlement(run_id)
        return {"run_id": run_id, "status": "noop", "noop": True,
                "reused": True, "supersedes": None, "diff": face,
                "note": reasons.label("B_SETTLE_NOTHING_TO_PUBLISH")}
    if not face["rows"] and face.get("current"):
        return {"run_id": run_id, "status": "published", "noop": True,
                "reused": True, "supersedes": run.get("supersedes"),
                "diff": face, "note": "当前版本已是最新"}
    diff_rows = [{**row, "old_commission": row["old_commission"],
                  "new_commission": row["new_commission"]}
                 for row in face["rows"]]
    result = store.publish_settlement(run_id, diff_rows=diff_rows,
                                      note=_note_for_face(face))
    return {"run_id": run_id, "status": "published", "noop": False,
            "reused": False, "supersedes": result["supersedes"],
            "diff": face,
            "note": reasons.label("S_SETTLE_RESTATED") if face["from_run_id"]
            else "首次发布"}


def publish_with_impact(store, *, settle_date: str, batch_id: str | None = None,
                        confirm_digest: str | None = None) -> dict:
    """一步式：算 → 预览 → 发布。未带 confirm_digest 时只返回预览（不发布）。"""
    staged = stage(store, settle_date=settle_date, batch_id=batch_id)
    run_id = staged["run"]["run_id"]
    face = preview(store, run_id)
    if not confirm_digest:
        return {"run": staged["run"], "reused": staged["reused"],
                "preview": face, "published": False,
                "impact_digest": _face_digest(face),
                "note": "已计算并给出影响面；带同一 impact_digest 再提交即发布"}
    published = publish(store, run_id, confirm_digest=confirm_digest)
    return {"run": staged["run"], "reused": staged["reused"],
            "preview": face, "published": True, "result": published,
            "impact_digest": _face_digest(face)}


def published_book(store, settle_date: str) -> dict | None:
    run = store.published_settlement(settle_date)
    if run is None:
        return None
    return {"run": run, "orders": store.load_settlement_orders(run["run_id"]),
            "anomalies": store.load_settlement_anomalies(run["run_id"]),
            "diffs": store.load_settlement_diffs(run["run_id"])}


def _source_batches(store, batch_id: str | None) -> list[dict]:
    """默认读所有已完成批次（跨批次合并当天事件）；指定 batch_id 时只读该批。"""
    if batch_id:
        batch = store.get_batch(batch_id)
        if batch is None:
            raise errors.NotFound(f"批次 {batch_id} 不存在", code="E_NOT_FOUND")
        return [batch]
    done = [b for b in store.list_batches(limit=500) if b["status"] == "done"]
    return list(reversed(done))


def _find_by_digest(store, settle_date: str, digest: str) -> dict | None:
    for run in store.list_settlement_runs(settle_date, limit=200):
        if run["input_digest"] == digest and run["status"] != "failed":
            return run
    return None


def _order_rows(book: ledger_mod.Ledger) -> list[dict]:
    out = []
    for row in book.orders:
        out.append({
            "order_pk": row.order_pk, "customer_pk": row.customer_pk,
            "order_time_utc": iso_utc(row.order_time), "amount": row.amount,
            "commission": row.commission, "tier": row.tier,
            "tier_source": row.tier_source, "tier_event_ref": row.tier_event_ref,
            "status": row.status, "reason_code": row.reason_code,
            "row_ref": row.row_ref,
        })
    return out


def _anomaly_rows(book: ledger_mod.Ledger) -> list[dict]:
    return [{"kind": a.kind, "key": a.key, "reason_code": a.reason_code,
             "detail": a.detail, "refs": a.refs} for a in book.anomalies]


def _as_diff_rows(rows: list[dict]) -> list[dict]:
    return [{"order_pk": r["order_pk"], "commission": r["commission"],
             "tier": r["tier"], "status": r["status"], "row_ref": r.get("row_ref")}
            for r in rows]


def _face_digest(face: dict) -> str:
    parts = [face.get("from_run_id") or "-"]
    for row in face["rows"]:
        parts.append("|".join([
            row["order_pk"], row["change"],
            "" if row.get("old_commission") is None else str(row["old_commission"]),
            "" if row.get("new_commission") is None else str(row["new_commission"]),
            row.get("old_tier") or "", row.get("new_tier") or "",
            str(row["delta"]),
        ]))
    return hashlib.sha256("\n".join(parts).encode("utf-8")).hexdigest()[:16]


def _sum(values) -> object:
    from decimal import Decimal
    total = Decimal("0.00")
    for value in values:
        total += value if isinstance(value, Decimal) else Decimal(str(value or 0))
    return total


def _note_for(book: ledger_mod.Ledger) -> str:
    c = book.counts()
    parts = [f"订单 {c['orders']}"]
    if c["held"]:
        parts.append(f"挂起 {c['held']}")
    if c["conflict_events"]:
        parts.append(f"冲突事件 {c['conflict_events']}")
    return " · ".join(parts)


def _note_for_face(face: dict) -> str:
    return (f"改写 {face['added']} 增 / {face['removed']} 删 / "
            f"{face['changed']} 变，佣金差 {face['delta_total']}")


def _require_run(store, run_id: str) -> dict:
    run = store.settlement_run(run_id)
    if run is None:
        raise errors.NotFound(f"结算运行 {run_id} 不存在", code="E_NOT_FOUND")
    return run


def _now_iso() -> str:
    return dt.datetime.now(config.UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
