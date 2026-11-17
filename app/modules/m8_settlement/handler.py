"""M8 入口：算一次日结、看影响面、发布/修正、导出结算台账。"""
from __future__ import annotations

import datetime as dt
from decimal import Decimal

from fastapi import APIRouter, Depends, Query, Request, Response

from ... import config, errors
from ...web import Ctx, get_ctx, render
from . import policy, service
from . import ledger as ledger_mod

router = APIRouter(tags=["M8 佣金日结"])

CSV_MEDIA = "text/csv; charset=utf-8"


@router.get("/api/settlement/policy")
def api_policy() -> dict:
    return policy.policy_view()


@router.get("/api/settlement")
def api_settlements(date: str | None = Query(default=None),
                    ctx: Ctx = Depends(get_ctx)) -> dict:
    runs = ctx.store.list_settlement_runs(date)
    dates = ctx.store.settlement_dates()
    return {"settle_date": date, "dates": dates,
            "runs": [_run_json(row) for row in runs]}


@router.post("/api/settlement/compute")
def api_compute(payload: dict | None = None, ctx: Ctx = Depends(get_ctx)) -> dict:
    body = payload or {}
    settle_date = _settle_date(body.get("settle_date"))
    batch_id = body.get("batch_id") or None
    staged = service.stage(ctx.store, settle_date=settle_date, batch_id=batch_id)
    face = service.preview(ctx.store, staged["run"]["run_id"])
    return {"run": _run_json(staged["run"]), "reused": staged["reused"],
            "orders": len(staged["ledger"].orders),
            "counts": staged["ledger"].counts(),
            "totals": _totals(staged["ledger"]),
            "preview": _face_json(face),
            "impact_digest": face["digest"]}


@router.get("/api/settlement/{run_id}")
def api_settlement(run_id: str, ctx: Ctx = Depends(get_ctx)) -> dict:
    run = ctx.store.settlement_run(run_id)
    if run is None:
        raise errors.NotFound(f"结算运行 {run_id} 不存在", code="E_NOT_FOUND")
    orders = ctx.store.load_settlement_orders(run_id)
    anomalies = ctx.store.load_settlement_anomalies(run_id)
    diffs = ctx.store.load_settlement_diffs(run_id)
    published = ctx.store.published_settlement(run["settle_date"])
    return {
        "run": _run_json(run),
        "orders": [_order_json(r) for r in orders],
        "anomalies": [_anomaly_json(r) for r in anomalies],
        "diffs": [_diff_json(r) for r in diffs],
        "counts": _counts(orders, anomalies),
        "totals": _totals_rows(run, orders),
        "published_run_id": published["run_id"] if published else None,
        "preview": _face_json(service.preview(ctx.store, run_id)),
    }


@router.get("/api/settlement/{run_id}/impact")
def api_impact(run_id: str, ctx: Ctx = Depends(get_ctx)) -> dict:
    return _face_json(service.preview(ctx.store, run_id))


@router.post("/api/settlement/{run_id}/publish")
def api_publish(run_id: str, payload: dict | None = None,
                ctx: Ctx = Depends(get_ctx)) -> dict:
    body = payload or {}
    result = service.publish(ctx.store, run_id,
                             confirm_digest=body.get("impact_digest"))
    return {"run": _run_json(ctx.store.settlement_run(run_id)),
            "result": {**result,
                       "diff": _face_json(result["diff"])}}


@router.post("/api/settlement/run")
def api_run(payload: dict | None = None, ctx: Ctx = Depends(get_ctx)) -> dict:
    """一步式：算 → 预览；带 impact_digest 时同事务发布。"""
    body = payload or {}
    settle_date = _settle_date(body.get("settle_date"))
    result = service.publish_with_impact(
        ctx.store, settle_date=settle_date, batch_id=body.get("batch_id") or None,
        confirm_digest=body.get("impact_digest"))
    out = {"run": _run_json(result["run"]), "reused": result["reused"],
           "preview": _face_json(result["preview"]),
           "impact_digest": result["impact_digest"],
           "published": result["published"], "note": result.get("note")}
    if result["published"]:
        out["result"] = {**result["result"],
                         "diff": _face_json(result["result"]["diff"])}
    return out


@router.get("/api/export/settlement.csv")
def export_settlement(run_id: str | None = None,
                      ctx: Ctx = Depends(get_ctx)) -> Response:
    run = _resolve_run(ctx.store, run_id)
    orders = ctx.store.load_settlement_orders(run["run_id"])
    data = _orders_csv(run, orders)
    return Response(content=data, media_type=CSV_MEDIA, headers={
        "Content-Disposition": f'attachment; filename="{run["run_id"]}-settlement.csv"'})


@router.get("/api/export/settlement-diff.csv")
def export_diff(run_id: str | None = None, ctx: Ctx = Depends(get_ctx)) -> Response:
    run = _resolve_run(ctx.store, run_id)
    rows = ctx.store.load_settlement_diffs(run["run_id"])
    data = _diff_csv(run, rows)
    return Response(content=data, media_type=CSV_MEDIA, headers={
        "Content-Disposition": f'attachment; filename="{run["run_id"]}-diff.csv"'})


@router.get("/settlement")
def page_settlement(request: Request, date: str | None = None,
                    run_id: str | None = None, ctx: Ctx = Depends(get_ctx)):
    settle_date = date or _default_date(ctx.store)
    runs = ctx.store.list_settlement_runs(settle_date) if settle_date else []
    published = ctx.store.published_settlement(settle_date) if settle_date else None
    selected = ctx.store.settlement_run(run_id) if run_id else (
        runs[0] if runs else None)
    orders = ctx.store.load_settlement_orders(selected["run_id"]) if selected else []
    anomalies = (ctx.store.load_settlement_anomalies(selected["run_id"])
                 if selected else [])
    face = None
    if selected and selected["status"] != "published":
        face = service.preview(ctx.store, selected["run_id"])
    published_orders = (ctx.store.load_settlement_orders(published["run_id"])
                        if published else [])
    return render(
        request, "settlement.html", page="settlement",
        page_title="佣金日结",
        policy=policy.policy_view(),
        dates=ctx.store.settlement_dates(),
        settle_date=settle_date,
        runs=[_run_view(row) for row in runs],
        selected=_run_view(selected) if selected else None,
        published=_run_view(published) if published else None,
        counts=_counts(orders, anomalies),
        totals=_totals_rows(selected, orders) if selected else None,
        published_totals=_totals_rows(published, published_orders) if published else None,
        orders=[_order_view(r) for r in orders],
        anomalies=[_anomaly_view(r) for r in anomalies],
        diffs=[_diff_view(r) for r in (ctx.store.load_settlement_diffs(
            selected["run_id"]) if selected else [])],
        impact=_face_view(face) if face else None,
        cutoff=config.SETTLEMENT_CUTOFF,
        default_date=_default_date(ctx.store))


def _settle_date(value) -> str:
    if value:
        try:
            return dt.date.fromisoformat(str(value)).isoformat()
        except ValueError as exc:
            raise errors.BadRequest(f"结算日期 {value!r} 不是 YYYY-MM-DD",
                                    code="E_BAD_DATE") from exc
    return _default_date(None)


def _default_date(store) -> str:
    if store is not None:
        dates = store.settlement_dates(limit=1)
        if dates:
            return dates[0]
    latest = store.latest_batch() if store is not None else None
    if latest and latest.get("max_event_time"):
        when = dt.datetime.fromisoformat(latest["max_event_time"].replace("Z", "+00:00"))
        return when.astimezone(config.CHINA_TZ).date().isoformat()
    return dt.datetime.now(config.CHINA_TZ).date().isoformat()


def _resolve_run(store, run_id: str | None) -> dict:
    if run_id:
        run = store.settlement_run(run_id)
        if run is None:
            raise errors.NotFound(f"结算运行 {run_id} 不存在", code="E_NOT_FOUND")
        return run
    run = store.latest_settlement(_default_date(store))
    if run is None:
        raise errors.NotFound("还没有日结运行，先算一次", code="E_NO_SETTLEMENT")
    return run


def _run_json(run: dict) -> dict:
    return {
        "run_id": run["run_id"], "settle_date": run["settle_date"],
        "revision": run["revision"], "status": run["status"],
        "computed_at": run["computed_at"], "published_at": run.get("published_at"),
        "cutoff_utc": run["cutoff_utc"],
        "data_horizon_utc": run.get("data_horizon_utc"),
        "rule_version": run["rule_version"],
        "tier_policy": run["tier_policy"], "input_digest": run["input_digest"],
        "asof_batch_id": run.get("asof_batch_id"),
        "supersedes": run.get("supersedes"),
        "blocked_reason": run.get("blocked_reason") or "",
        "note": run.get("note") or "",
    }


def _order_json(row: dict) -> dict:
    return {
        "order_pk": row["order_pk"], "customer_pk": row["customer_pk"],
        "order_time_utc": row["order_time_utc"],
        "amount": None if row["amount"] is None else str(row["amount"]),
        "commission": str(row["commission"]), "tier": row["tier"],
        "tier_source": row["tier_source"], "tier_event_ref": row["tier_event_ref"],
        "status": row["status"], "reason_code": row["reason_code"],
        "row_ref": row["row_ref"],
    }


def _anomaly_json(row: dict) -> dict:
    return {"kind": row["kind"], "key": row["key"],
            "reason_code": row["reason_code"], "detail": row["detail"],
            "refs": row["refs"]}


def _diff_json(row: dict) -> dict:
    return {
        "order_pk": row["order_pk"], "change": row["change"],
        "old_commission": _str_or_none(row["old_commission"]),
        "new_commission": _str_or_none(row["new_commission"]),
        "old_tier": row["old_tier"], "new_tier": row["new_tier"],
        "delta": str(row["delta"]), "row_ref": row["row_ref"],
    }


def _str_or_none(value):
    return None if value is None else str(value)


def _face_json(face: dict) -> dict:
    return {
        "run_id": face["run_id"], "from_run_id": face["from_run_id"],
        "digest": face["digest"], "added": face["added"],
        "removed": face["removed"], "changed": face["changed"],
        "delta_total": str(face["delta_total"]),
        "published": face["published"],
        "rows": [_diff_json(row) for row in face["rows"]],
    }


def _face_view(face: dict) -> dict:
    return {"digest": face["digest"], "from_run_id": face["from_run_id"],
            "added": face["added"], "removed": face["removed"],
            "changed": face["changed"], "delta_total": face["delta_total"],
            "rows": [_diff_view(row) for row in face["rows"]],
            "published": face["published"]}


def _counts(orders: list[dict], anomalies: list[dict]) -> dict:
    settled = [r for r in orders if r["status"] == "settled"]
    return {"orders": len(orders), "settled": len(settled),
            "held": len(orders) - len(settled),
            "anomalies": len(anomalies)}


def _totals(book: ledger_mod.Ledger) -> dict:
    return {"order_amount": str(book.order_total),
            "commission": str(book.commission_total)}


def _totals_rows(run: dict | None, orders: list[dict]) -> dict:
    if run is None:
        return {"orders": 0, "settled": 0, "held": 0,
                "order_amount": Decimal("0.00"), "commission": Decimal("0.00")}
    settled = [r for r in orders if r["status"] == "settled"]
    amount = sum((r["amount"] for r in settled if r["amount"] is not None),
                 Decimal("0.00"))
    commission = sum((r["commission"] for r in settled), Decimal("0.00"))
    return {"orders": len(orders), "settled": len(settled),
            "held": len(orders) - len(settled),
            "order_amount": amount, "commission": commission}


def _run_view(run: dict | None) -> dict | None:
    if run is None:
        return None
    return _run_json(run)


def _order_view(row: dict) -> dict:
    return {
        "order_pk": row["order_pk"], "customer_pk": row["customer_pk"],
        "order_time": row["order_time_utc"], "amount": row["amount"],
        "commission": row["commission"], "tier": row["tier"],
        "tier_source": row["tier_source"], "tier_event_ref": row["tier_event_ref"],
        "status": row["status"], "reason_code": row["reason_code"],
        "row_ref": row["row_ref"],
    }


def _anomaly_view(row: dict) -> dict:
    return {"kind": row["kind"], "key": row["key"],
            "reason_code": row["reason_code"], "detail": row["detail"],
            "refs": row["refs"]}


def _diff_view(row: dict) -> dict:
    return {"order_pk": row["order_pk"], "change": row["change"],
            "old_commission": row["old_commission"],
            "new_commission": row["new_commission"],
            "old_tier": row["old_tier"], "new_tier": row["new_tier"],
            "delta": row["delta"], "row_ref": row["row_ref"]}


ORDER_COLUMNS = ("订单号", "客户号", "订单时间(UTC)", "状态", "等级", "等级来源",
                 "等级依据", "金额(元)", "佣金(元)", "原因码", "行号引用")
DIFF_COLUMNS = ("订单号", "变化", "原佣金(元)", "新佣金(元)", "原等级", "新等级",
                "佣金差(元)", "行号引用")


def _orders_csv(run: dict, orders: list[dict]) -> bytes:
    from ..m7_export import csv_out
    rows = [[
        r["order_pk"], r["customer_pk"] or "", r["order_time_utc"], r["status"],
        r["tier"], r["tier_source"], r["tier_event_ref"] or "",
        "" if r["amount"] is None else str(r["amount"]), str(r["commission"]),
        r["reason_code"] or "", r["row_ref"] or "",
    ] for r in orders]
    text = csv_out.to_csv(ORDER_COLUMNS, rows)
    return csv_out.encode_csv(text)


def _diff_csv(run: dict, rows: list[dict]) -> bytes:
    from ..m7_export import csv_out
    body = [[
        r["order_pk"], r["change"], "" if r["old_commission"] is None
        else str(r["old_commission"]),
        "" if r["new_commission"] is None else str(r["new_commission"]),
        r["old_tier"] or "", r["new_tier"] or "", str(r["delta"]),
        r["row_ref"] or "",
    ] for r in rows]
    text = csv_out.to_csv(DIFF_COLUMNS, body)
    return csv_out.encode_csv(text)
