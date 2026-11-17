"""M8 佣金日结：等级口径、乱序、冲突不推进、幂等、发布失败保留旧版、影响面。

这些测试把「结算日按订单发生时的等级」这个裁决写成可执行的反例：
朴素实现（按当前/最新等级、按到达顺序、按最新批次）会挂在哪一行。
"""
from __future__ import annotations

import datetime as dt
import pathlib
from dataclasses import replace
from decimal import Decimal

import pytest

from app import config, errors
from app.modules.m6_batches import service as batch_service
from app.modules.m8_settlement import ledger, policy, service
from fixtures import expected as X


def _settings(settings, directory):
    return replace(settings, sample_dir=str(directory))


def _event(table, pk, version, event_time, payload, *, op="upsert",
           row_ref="unit:1", reason_code=None):
    return ledger.LedgerEvent(
        table=table, pk=pk, version=version, op=op,
        event_time=policy.cutoff_local("2026-11-16") - dt.timedelta(hours=20)
        if event_time is None else _utc(event_time),
        payload=payload, amount=_dec(payload.get("amount")), qty=None,
        row_ref=row_ref, reason_code=reason_code)


def _utc(text: str) -> dt.datetime:
    return dt.datetime.fromisoformat(text.replace("Z", "+00:00")).astimezone(
        dt.timezone.utc)


def _dec(value):
    return None if value is None else Decimal(str(value))


_HORIZON = "2026-11-16T10:00:00Z"


def _run(settings, store, directory, name="events.jsonl", body="", horizon=None):
    """跑一个采信批次。默认补一条 10:00 的「现状」事件，避免补数被判陈旧。"""
    directory.mkdir(exist_ok=True)
    text = body
    mark = horizon or _HORIZON
    text += _line("products", f"H{abs(hash(name)) % 100000}", 1, mark,
                  {"status": "在售"})
    (directory / name).write_text(text, encoding="utf-8")
    return batch_service.run(settings=_settings(settings, directory), store=store,
                             source=name)


def _line(table, pk, version, when, payload, op="upsert"):
    import json
    return json.dumps({"table": table, "pk": pk, "version": version, "op": op,
                       "event_time": when, "schema": "v2", "payload": payload},
                      ensure_ascii=False) + "\n"


# ---------------------------------------------------------------- 口径裁决

def test_tier_is_as_of_order_not_current():
    """核心裁决：10:00 的订单用当时等级（银卡），不被 18:00 的升级改写。"""
    events = [
        _event("customers", "C1", 1, "2026-11-16T09:00:00Z", {"tier": "银卡", "status": "活跃"}),
        _event("orders", "A1", 1, "2026-11-16T10:00:00Z",
               {"status": "已支付", "amount": "1000.00", "customer": "C1"}),
        _event("customers", "C1", 2, "2026-11-16T18:00:00Z", {"tier": "金卡", "status": "活跃"}),
    ]
    book = ledger.build(events, "2026-11-16")
    row = book.orders[0]
    assert row.tier == "银卡"
    assert row.commission == Decimal("30.00")   # 1000 × 3%
    assert row.tier_event_ref == "unit:1"
    assert book.commission_total == Decimal("30.00")

    # 反例：如果按「当前/最新等级」，同一订单会算成 50.00 —— 台账和导出对不上。
    assert row.commission != Decimal("50.00")


def test_late_tier_change_rewrites_only_after_its_time():
    """迟到等级变更按时间回溯：只改它之后发生的订单，之前的订单不动。"""
    events = [
        _event("customers", "C1", 1, "2026-11-16T08:00:00Z", {"tier": "普通", "status": "活跃"}),
        _event("orders", "B1", 1, "2026-11-16T09:00:00Z",
               {"status": "已支付", "amount": "100.00", "customer": "C1"}),
        _event("orders", "B2", 1, "2026-11-16T11:00:00Z",
               {"status": "已支付", "amount": "100.00", "customer": "C1"}),
        # 迟到到达，但 event_time 在 B1 与 B2 之间
        _event("customers", "C1", 2, "2026-11-16T10:00:00Z", {"tier": "金卡", "status": "活跃"}),
    ]
    book = ledger.build(events, "2026-11-16")
    by_pk = {row.order_pk: row for row in book.orders}
    assert by_pk["B1"].tier == "普通"
    assert by_pk["B1"].commission == Decimal("1.00")
    assert by_pk["B2"].tier == "金卡"
    assert by_pk["B2"].commission == Decimal("5.00")


def test_order_time_is_first_upsert_not_version_time():
    """先建单后改金额：发生时间取最早 upsert，金额取最新 upsert。"""
    events = [
        _event("customers", "C1", 1, "2026-11-16T08:00:00Z", {"tier": "银卡", "status": "活跃"}),
        _event("orders", "D1", 1, "2026-11-16T10:00:00Z",
               {"status": "新建", "amount": "100.00", "customer": "C1"}),
        _event("orders", "D1", 4, "2026-11-16T20:00:00Z",
               {"status": "已支付", "amount": "300.00", "customer": "C1"}),
    ]
    row = ledger.build(events, "2026-11-16").orders[0]
    assert row.order_time == _utc("2026-11-16T10:00:00Z")
    assert row.amount == Decimal("300.00")
    assert row.commission == Decimal("9.00")


def test_tie_on_same_instant_same_tier_is_stable():
    tiers = [
        policy.TierEvent("C1", "银卡", _utc("2026-11-15T00:00:00Z"), 1, 1, "a"),
        policy.TierEvent("C1", "银卡", _utc("2026-11-15T00:00:00Z"), 2, 2, "b"),
    ]
    resolved = policy.effective_tier(tiers, _utc("2026-11-16T00:00:00Z"))
    assert resolved.tier == "银卡"
    assert resolved.reason_code == "T_TIER_ASOF_ORDER"


def test_tier_conflict_holds_orders_without_advancing_boundary():
    """同一客户同一时刻两个等级 → 挂起，不猜；冲突事件不进数据边界。"""
    events = [
        _event("customers", "C1", 1, "2026-11-16T09:00:00Z", {"tier": "银卡", "status": "活跃"}),
        _event("customers", "C1", 1, "2026-11-16T09:00:00Z", {"tier": "金卡", "status": "活跃"},
               reason_code="C_VERSION_CONFLICT"),
        _event("orders", "E1", 1, "2026-11-16T10:00:00Z",
               {"status": "已支付", "amount": "100.00", "customer": "C1"}),
        _event("orders", "E2", 1, "2026-11-16T11:00:00Z",
               {"status": "已支付", "amount": "200.00", "customer": "C2"}),
    ]
    book = ledger.build(events, "2026-11-16")
    by_pk = {row.order_pk: row for row in book.orders}
    assert by_pk["E1"].status == "held"
    assert by_pk["E1"].reason_code == "C_TIER_CONFLICT"
    assert by_pk["E1"].commission == Decimal("0.00")
    assert by_pk["E2"].status == "settled"
    assert book.commission_total == Decimal("2.00")     # 只有 E2 的 200×1%
    # 冲突事件不推进数据边界：边界只看 E2 的 11:00，不看冲突等级事件的 09:00
    assert book.data_horizon_utc == _utc("2026-11-16T11:00:00Z")


def test_earlier_orders_survive_later_tier_conflict():
    """冲突只影响它时刻之后的订单，更早的已定订单不回溯改写。"""
    events = [
        _event("customers", "C1", 1, "2026-11-16T08:00:00Z", {"tier": "银卡", "status": "活跃"}),
        _event("orders", "F1", 1, "2026-11-16T09:00:00Z",
               {"status": "已支付", "amount": "100.00", "customer": "C1"}),
        _event("customers", "C1", 2, "2026-11-16T10:00:00Z", {"tier": "金卡", "status": "活跃"},
               reason_code="C_VERSION_CONFLICT"),
        _event("orders", "F2", 1, "2026-11-16T11:00:00Z",
               {"status": "已支付", "amount": "100.00", "customer": "C1"}),
    ]
    by_pk = {row.order_pk: row for row in ledger.build(events, "2026-11-16").orders}
    assert by_pk["F1"].status == "settled"
    assert by_pk["F1"].commission == Decimal("3.00")
    assert by_pk["F2"].status == "held"


def test_unknown_tier_held_and_reported():
    events = [
        _event("customers", "C1", 1, "2026-11-16T09:00:00Z", {"tier": "黑卡", "status": "活跃"}),
        _event("orders", "G1", 1, "2026-11-16T10:00:00Z",
               {"status": "已支付", "amount": "100.00", "customer": "C1"}),
    ]
    book = ledger.build(events, "2026-11-16")
    assert book.orders[0].status == "held"
    assert book.orders[0].reason_code == "T_TIER_UNKNOWN"
    assert {a.kind for a in book.anomalies} == {"unknown_tier"}


def test_order_without_customer_is_out_of_scope():
    events = [_event("orders", "H1", 1, "2026-11-16T10:00:00Z",
                     {"status": "已支付", "amount": "100.00"})]
    book = ledger.build(events, "2026-11-16")
    assert book.orders == []
    assert book.commission_total == Decimal("0.00")


def test_deleted_order_is_not_billed():
    events = [
        _event("customers", "C1", 1, "2026-11-16T08:00:00Z", {"tier": "银卡", "status": "活跃"}),
        _event("orders", "I1", 1, "2026-11-16T10:00:00Z",
               {"status": "新建", "amount": "100.00", "customer": "C1"}),
        _event("orders", "I1", 2, "2026-11-16T12:00:00Z", {}, op="delete"),
    ]
    book = ledger.build(events, "2026-11-16")
    assert book.orders == []


def test_boundary_splits_by_local_midnight():
    """结算日按本地自然日切分：本地 23:30 与次日 00:30 分属两天。"""
    events = [
        _event("customers", "C1", 1, "2026-11-16T09:00:00Z", {"tier": "银卡", "status": "活跃"}),
        _event("orders", "J1", 1, "2026-11-16T15:30:00Z",          # 本地 23:30
               {"status": "已支付", "amount": "100.00", "customer": "C1"}),
        _event("orders", "J2", 1, "2026-11-16T16:30:00Z",          # 本地次日 00:30
               {"status": "已支付", "amount": "100.00", "customer": "C1"}),
    ]
    day16 = ledger.build(events, "2026-11-16")
    day17 = ledger.build(events, "2026-11-17")
    assert [r.order_pk for r in day16.orders] == ["J1"]
    assert [r.order_pk for r in day17.orders] == ["J2"]


# ---------------------------------------------------------------- 幂等与重跑

def test_repeated_backfill_is_idempotent(settings, store, tmp_path):
    """同一补数重跑：命中同一 digest/run，不新增订单行、不重复累计佣金。"""
    directory = tmp_path / "s1"
    body = (_line("customers", "C1", 1, "2026-11-16T09:00:00Z",
                  {"tier": "金卡", "status": "活跃"})
            + _line("orders", "K1", 1, "2026-11-16T10:00:00Z",
                    {"status": "已支付", "amount": "1000.00", "customer": "C1"}))
    _run(settings, store, directory, "events.jsonl", body)
    first = service.stage(store, settle_date="2026-11-16")
    before = store.count_rows  # sanity import
    first_rows = store.load_settlement_orders(first["run"]["run_id"])
    again = service.stage(store, settle_date="2026-11-16")
    assert again["reused"] is True
    assert again["run"]["run_id"] == first["run"]["run_id"]
    assert len(store.load_settlement_orders(first["run"]["run_id"])) == len(first_rows) == 1
    assert sum(r["commission"] for r in first_rows) == Decimal("50.00")
    assert len(store.list_settlement_runs("2026-11-16")) == 1


def test_merging_batches_does_not_double_count(settings, store, tmp_path):
    """同一订单在两次导出里各来一次：跨批次合并只算一次。"""
    row = _line("orders", "L1", 1, "2026-11-16T10:00:00Z",
                {"status": "已支付", "amount": "100.00", "customer": "C1"})
    tier = _line("customers", "C1", 1, "2026-11-16T09:00:00Z",
                 {"tier": "金卡", "status": "活跃"})
    _run(settings, store, tmp_path / "a", "main.jsonl", tier + row,
         horizon="2026-11-16T10:00:00Z")
    _run(settings, store, tmp_path / "b", "late.jsonl", row,
         horizon="2026-11-16T10:00:00Z")
    staged = service.stage(store, settle_date="2026-11-16")
    assert len(staged["ledger"].orders) == 1
    assert staged["ledger"].commission_total == Decimal("5.00")


# ---------------------------------------------------------------- 发布与修正

def _prepare(tmp_path, settings, store, extra=""):
    directory = tmp_path / "pub"
    body = (_line("customers", "C1", 1, "2026-11-16T09:00:00Z",
                  {"tier": "银卡", "status": "活跃"})
            + _line("orders", "P1", 1, "2026-11-16T10:00:00Z",
                    {"status": "已支付", "amount": "1000.00", "customer": "C1"}))
    _run(settings, store, directory, "events.jsonl", body + extra)
    return directory


def test_publish_requires_preview_digest(settings, store, tmp_path):
    _prepare(tmp_path, settings, store)
    staged = service.stage(store, settle_date="2026-11-16")
    with pytest.raises(errors.Conflict) as info:
        service.publish(store, staged["run"]["run_id"])
    assert info.value.code == "B_SETTLE_PUBLISH_BLOCKED"
    assert store.published_settlement("2026-11-16") is None


def test_publish_atomic_and_repeat_is_idempotent(settings, store, tmp_path):
    _prepare(tmp_path, settings, store)
    staged = service.stage(store, settle_date="2026-11-16")
    face = service.preview(store, staged["run"]["run_id"])
    result = service.publish(store, staged["run"]["run_id"], confirm_digest=face["digest"])
    assert result["status"] == "published"
    published = store.published_settlement("2026-11-16")
    assert published["run_id"] == staged["run"]["run_id"]
    again = service.publish(store, staged["run"]["run_id"], confirm_digest=face["digest"])
    assert again["noop"] is True
    assert len(store.load_settlement_diffs(published["run_id"])) == 1


def test_publish_failure_keeps_old_version(settings, store, tmp_path, monkeypatch):
    """发布中途失败：整体回滚，旧版仍对外、旧版订单行不变。"""
    _prepare(tmp_path, settings, store)
    first = service.stage(store, settle_date="2026-11-16")
    face = service.preview(store, first["run"]["run_id"])
    service.publish(store, first["run"]["run_id"], confirm_digest=face["digest"])
    old = store.published_settlement("2026-11-16")
    old_rows = store.load_settlement_orders(old["run_id"])

    _run(settings, store, tmp_path / "pub", "more.jsonl",
         _line("orders", "P2", 1, "2026-11-16T11:00:00Z",
               {"status": "已支付", "amount": "500.00", "customer": "C1"}))
    second = service.stage(store, settle_date="2026-11-16")
    assert second["run"]["run_id"] != first["run"]["run_id"]
    face2 = service.preview(store, second["run"]["run_id"])

    real = store.publish_settlement

    def boom(run_id, *, diff_rows, note=""):
        raise RuntimeError("publish failed mid-flight")

    monkeypatch.setattr(store, "publish_settlement", boom)
    with pytest.raises(RuntimeError):
        service.publish(store, second["run"]["run_id"], confirm_digest=face2["digest"])
    monkeypatch.setattr(store, "publish_settlement", real)

    still = store.published_settlement("2026-11-16")
    assert still["run_id"] == old["run_id"]
    assert store.load_settlement_orders(old["run_id"]) == old_rows
    assert service.preview(store, second["run"]["run_id"])["from_run_id"] == old["run_id"]


def test_late_tier_change_shows_impact_before_publish(settings, store, tmp_path):
    """迟到修正改写已发布订单：影响面列出订单号与佣金差，发布后旧版被替换。"""
    directory = _prepare(tmp_path, settings, store)
    first = service.stage(store, settle_date="2026-11-16")
    face = service.preview(store, first["run"]["run_id"])
    service.publish(store, first["run"]["run_id"], confirm_digest=face["digest"])

    # 迟到的等级补数：10:30 升级为金卡（P1 在 10:00，不受影响）
    late_tier = _line("customers", "C1", 2, "2026-11-16T10:30:00Z",
                      {"tier": "金卡", "status": "活跃"})
    _run(settings, store, directory, "late.jsonl", late_tier,
         horizon="2026-11-16T10:30:00Z")
    staged = service.stage(store, settle_date="2026-11-16")
    assert staged["run"]["run_id"] != first["run"]["run_id"]
    impact = service.preview(store, staged["run"]["run_id"])
    assert impact["from_run_id"] == first["run"]["run_id"]
    assert impact["rows"] == []          # P1 在 10:00，等级 10:30 才变，无影响

    # 再补一笔 10:45 的订单：它按下单时（10:45）的等级即金卡计价
    late_order = _line("orders", "P2", 1, "2026-11-16T10:45:00Z",
                       {"status": "已支付", "amount": "1000.00", "customer": "C1"})
    _run(settings, store, directory, "late2.jsonl", late_order,
         horizon="2026-11-16T10:45:00Z")
    staged2 = service.stage(store, settle_date="2026-11-16")
    impact2 = service.preview(store, staged2["run"]["run_id"])
    assert impact2["added"] == 1
    assert impact2["rows"][0]["order_pk"] == "P2"
    assert impact2["rows"][0]["new_tier"] == "金卡"
    assert impact2["delta_total"] == Decimal("50.00")


def test_changed_order_reports_commission_delta(settings, store, tmp_path):
    """已发布订单被迟到修正改等级：影响面给出原/新佣金与差额。"""
    directory = _prepare(tmp_path, settings, store)
    first = service.stage(store, settle_date="2026-11-16")
    face = service.preview(store, first["run"]["run_id"])
    service.publish(store, first["run"]["run_id"], confirm_digest=face["digest"])
    orders_before = store.load_settlement_orders(first["run"]["run_id"])
    assert orders_before[0]["commission"] == Decimal("30.00")

    # 迟到修正：补一条「09:30 已升金卡」的等级事件（版本更大、时间更早地回溯）
    correction = _line("customers", "C1", 2, "2026-11-16T09:30:00Z",
                       {"tier": "金卡", "status": "活跃"})
    _run(settings, store, directory, "fix.jsonl", correction,
         horizon="2026-11-16T12:00:00Z")
    staged = service.stage(store, settle_date="2026-11-16")
    impact = service.preview(store, staged["run"]["run_id"])
    assert impact["changed"] == 1
    row = impact["rows"][0]
    assert row["order_pk"] == "P1"
    assert row["old_commission"] == Decimal("30.00")
    assert row["new_commission"] == Decimal("50.00")
    assert row["delta"] == Decimal("20.00")
    assert row["old_tier"] == "银卡" and row["new_tier"] == "金卡"

    result = service.publish(store, staged["run"]["run_id"],
                             confirm_digest=impact["digest"])
    assert result["supersedes"] == first["run"]["run_id"]
    assert store.published_settlement("2026-11-16")["run_id"] == staged["run"]["run_id"]
    diffs = store.load_settlement_diffs(staged["run"]["run_id"])
    assert len(diffs) == 1 and diffs[0]["delta"] == Decimal("20.00")


def test_stale_preview_digest_is_rejected(settings, store, tmp_path):
    """预览后输入又变：旧摘要失效，拒绝发布，旧版保持对外。"""
    _prepare(tmp_path, settings, store)
    first = service.stage(store, settle_date="2026-11-16")
    face = service.preview(store, first["run"]["run_id"])
    service.publish(store, first["run"]["run_id"], confirm_digest=face["digest"])
    old = store.published_settlement("2026-11-16")

    _run(settings, store, tmp_path / "pub", "more.jsonl",
         _line("orders", "P2", 1, "2026-11-16T11:00:00Z",
               {"status": "已支付", "amount": "500.00", "customer": "C1"}))
    staged = service.stage(store, settle_date="2026-11-16")
    with pytest.raises(errors.Conflict) as info:
        service.publish(store, staged["run"]["run_id"], confirm_digest="deadbeef")
    assert info.value.code == "B_SETTLE_PUBLISH_BLOCKED"
    assert store.published_settlement("2026-11-16")["run_id"] == old["run_id"]


def test_superseded_run_is_not_published_twice(settings, store, tmp_path):
    """旧版本一旦被替换，不能再被重复发布而覆盖新版。"""
    _prepare(tmp_path, settings, store)
    first = service.stage(store, settle_date="2026-11-16")
    face = service.preview(store, first["run"]["run_id"])
    service.publish(store, first["run"]["run_id"], confirm_digest=face["digest"])
    _run(settings, store, tmp_path / "pub", "more.jsonl",
         _line("orders", "P2", 1, "2026-11-16T11:00:00Z",
               {"status": "已支付", "amount": "500.00", "customer": "C1"}))
    second = service.stage(store, settle_date="2026-11-16")
    assert second["run"]["run_id"] != first["run"]["run_id"]
    face2 = service.preview(store, second["run"]["run_id"])
    service.publish(store, second["run"]["run_id"], confirm_digest=face2["digest"])
    with pytest.raises(errors.Conflict) as info:
        service.publish(store, first["run"]["run_id"], confirm_digest=face["digest"])
    assert info.value.code == "B_SETTLE_PUBLISH_BLOCKED"
    assert store.published_settlement("2026-11-16")["run_id"] == second["run"]["run_id"]


# ---------------------------------------------------------------- 与主流程衔接

def test_settlement_sample_matches_expected(settings, store):
    """自带日结样例：16 日 4 单，17 日 3 单，等级全部按订单时刻。"""
    batch_service.run(settings=settings, store=store, source="settlement")
    day16 = service.stage(store, settle_date="2026-11-16")["ledger"]
    by_pk = {row.order_pk: row for row in day16.orders}
    assert set(by_pk) == {"A100", "A101", "A102", "A103"}
    assert by_pk["A100"].tier == "金卡"          # C01 在 15 日 18:00 已升金卡
    assert by_pk["A100"].commission == Decimal("50.00")
    assert by_pk["A101"].tier == "钻石"
    assert by_pk["A102"].tier_source == "default"   # C99 无等级 → 默认普通
    assert day16.commission_total == Decimal("132.00")

    day17 = service.stage(store, settle_date="2026-11-17")["ledger"]
    by_pk17 = {row.order_pk: row for row in day17.orders}
    # A107 在 01:30（本地 09:30 前），C02 的降级 03:00 还没发生 → 仍按钻石
    assert by_pk17["A107"].tier == "钻石"
    assert by_pk17["A107"].commission == Decimal("96.00")


def test_policy_view_documents_decision():
    view = policy.policy_view()
    assert view["tier_policy"] == "asof_order"
    assert view["label"] == "订单发生时的会员等级"
    assert "current_tier" in view["alternative"]
    assert set(view["tiers"][0]) == {"tier", "rate"}


def test_api_compute_preview_publish(client):
    """HTTP 面：算一次 → 拿影响面摘要 → 带摘要发布；不带摘要只预览。"""
    assert client.post("/api/events/import",
                       json={"source": "settlement"}).status_code == 200
    computed = client.post("/api/settlement/compute",
                           json={"settle_date": "2026-11-16"}).json()
    assert computed["run"]["status"] == "computed"
    assert computed["run"]["tier_policy"] == "asof_order"
    digest = computed["impact_digest"]
    blocked = client.post(f"/api/settlement/{computed['run']['run_id']}/publish",
                          json={})
    assert blocked.status_code == 409
    assert blocked.json()["error"] == "B_SETTLE_PUBLISH_BLOCKED"
    published = client.post(f"/api/settlement/{computed['run']['run_id']}/publish",
                            json={"impact_digest": digest})
    assert published.status_code == 200
    assert published.json()["run"]["status"] == "published"
    policy_view = client.get("/api/settlement/policy").json()
    assert policy_view["tier_policy"] == "asof_order"
    page = client.get("/settlement?date=2026-11-16")
    assert page.status_code == 200
    assert "佣金日结" in page.text
    assert "订单发生时的会员等级" in page.text


def test_api_settlement_export(client):
    imported = client.post("/api/events/import", json={"source": "settlement"})
    assert imported.status_code == 200
    computed = client.post("/api/settlement/compute",
                           json={"settle_date": "2026-11-16"}).json()
    run_id = computed["run"]["run_id"]
    face = client.get(f"/api/settlement/{run_id}/impact").json()
    client.post(f"/api/settlement/{run_id}/publish", json={"impact_digest": face["digest"]})
    csv = client.get(f"/api/export/settlement.csv?run_id={run_id}")
    assert csv.status_code == 200
    assert csv.content.startswith(b"\xef\xbb\xbf")
    text = csv.content.decode("utf-8-sig")
    assert "订单号" in text and "佣金(元)" in text
    assert "A100" in text
    diff_csv = client.get(f"/api/export/settlement-diff.csv?run_id={run_id}")
    assert diff_csv.status_code == 200
    assert "佣金差(元)" in diff_csv.content.decode("utf-8-sig")


# ---------------------------------------------------------------- 边界与回归

def test_conflict_event_outside_day_does_not_hold(settings, store, tmp_path):
    """次日（17 日）才到的冲突等级变更，不应影响 16 日的订单。"""
    directory = _prepare(tmp_path, settings, store)
    conflict = _line("customers", "C1", 2, "2026-11-17T10:00:00Z",
                     {"tier": "金卡", "status": "活跃"})
    _run(settings, store, directory, "next.jsonl", conflict,
         horizon="2026-11-17T11:00:00Z")
    book = service.stage(store, settle_date="2026-11-16")["ledger"]
    assert book.orders[0].status == "settled"


def test_publish_never_supersedes_other_dates(settings, store, tmp_path):
    """发布 17 日不应改写 16 日的已发布版本。"""
    directory = _prepare(tmp_path, settings, store)
    d16 = service.stage(store, settle_date="2026-11-16")
    f16 = service.preview(store, d16["run"]["run_id"])
    service.publish(store, d16["run"]["run_id"], confirm_digest=f16["digest"])
    day17 = _line("orders", "Q1", 1, "2026-11-17T10:00:00Z",
                  {"status": "已支付", "amount": "100.00", "customer": "C1"})
    _run(settings, store, directory, "day17.jsonl", day17,
         horizon="2026-11-17T10:00:00Z")
    d17 = service.stage(store, settle_date="2026-11-17")
    f17 = service.preview(store, d17["run"]["run_id"])
    service.publish(store, d17["run"]["run_id"], confirm_digest=f17["digest"])
    assert store.published_settlement("2026-11-16")["run_id"] == d16["run"]["run_id"]
    assert store.published_settlement("2026-11-17")["run_id"] == d17["run"]["run_id"]


def test_recompute_after_publish_marks_superseded_without_changing_published(
        settings, store, tmp_path):
    """同一结果重算：不新增版本；已发布版本与订单行保持不变。"""
    _prepare(tmp_path, settings, store)
    first = service.stage(store, settle_date="2026-11-16")
    face = service.preview(store, first["run"]["run_id"])
    service.publish(store, first["run"]["run_id"], confirm_digest=face["digest"])
    before = store.load_settlement_orders(first["run"]["run_id"])
    again = service.stage(store, settle_date="2026-11-16")
    assert again["reused"] is True
    assert again["run"]["run_id"] == first["run"]["run_id"]
    assert len(store.list_settlement_runs("2026-11-16")) == 1
    assert store.load_settlement_orders(first["run"]["run_id"]) == before



def test_unrelated_conflicts_do_not_pollute_the_day():
    """别的客户/别的日期的冲突事件，不算进当日，也不进数据边界。"""
    events = [
        _event("customers", "C1", 1, "2026-11-16T08:00:00Z", {"tier": "银卡", "status": "活跃"}),
        _event("customers", "C9", 1, "2026-11-16T08:00:00Z", {"tier": "普通", "status": "活跃"}),
        _event("customers", "C9", 1, "2026-11-16T08:00:00Z", {"tier": "金卡", "status": "活跃"},
               reason_code="C_VERSION_CONFLICT"),
        _event("orders", "M1", 1, "2026-11-16T10:00:00Z",
               {"status": "已支付", "amount": "100.00", "customer": "C1"}),
    ]
    book = ledger.build(events, "2026-11-16")
    assert book.conflict_events == 0
    assert book.anomalies == []
    assert book.orders[0].status == "settled"
    assert book.data_horizon_utc == _utc("2026-11-16T10:00:00Z")


def test_conflicted_later_version_holds_otherwise_applied_order():
    """订单有已应用旧版本、但更高版本冲突：按冲突挂起，不拿旧金额入账。"""
    events = [
        _event("customers", "C1", 1, "2026-11-16T08:00:00Z", {"tier": "银卡", "status": "活跃"}),
        _event("orders", "N1", 1, "2026-11-16T10:00:00Z",
               {"status": "新建", "amount": "100.00", "customer": "C1"}),
        _event("orders", "N1", 2, "2026-11-16T11:00:00Z",
               {"status": "已支付", "amount": "900.00", "customer": "C1"},
               reason_code="C_VERSION_CONFLICT"),
    ]
    book = ledger.build(events, "2026-11-16")
    assert len(book.orders) == 1
    assert book.orders[0].status == "held"
    assert book.orders[0].reason_code == "C_VERSION_CONFLICT"
    assert book.commission_total == Decimal("0.00")
