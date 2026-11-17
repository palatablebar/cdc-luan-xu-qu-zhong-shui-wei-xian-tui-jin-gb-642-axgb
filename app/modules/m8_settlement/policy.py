"""等级口径：订单发生时的会员等级（as-of order），不是结账时刻的最新等级。

裁决（写进 docs/DATA-DICTIONARY.md「佣金日结」一节）：

- 一笔订单的佣金 = 订单金额 × 费率(该订单 `event_time` 当时有效的会员等级)；
- 「当时有效」= 该客户所有 `tier` 变更事件里 `event_time ≤ 订单 event_time` 的最大
  `(event_time, version, seq)` 那条；同一时刻多条取版本最大、再取到达序最大；
- 等级事件按 **时间** 回溯，不按到达顺序：迟到的等级变更会自动改写它之后、
  发布之前的订单；已发布订单的改写必须走「影响面预览 → 发布修正版」；
- 订单发生时尚无任何等级事件 → 用默认等级并记 `T_TIER_DEFAULT`；
- 同一客户同一时刻出现两个不同等级 → 判 `C_TIER_CONFLICT`，该客户当日订单
  不参与计价（不进合计），也不推进结账截止点。

选择「订单发生时」而不是「当前/最新等级」的理由：

1. 同一订单无论补数何时到达，佣金只由订单自身时刻决定，重跑结果稳定；
2. 台账与导出用同一口径，不会出现「台账按旧等级、导出按新等级」的两套数字；
3. 迟到等级变更的影响面是显式的（哪些订单的佣金变了），可以审计；
   而「当前等级」口径会让历史订单随等级漂移，无法对账。
"""
from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from decimal import Decimal, ROUND_HALF_UP

from ... import config, reasons


@dataclass(frozen=True)
class TierEvent:
    customer_pk: str
    tier: str
    event_time: dt.datetime
    version: int
    seq: int
    row_ref: str

    @property
    def order_key(self) -> tuple[dt.datetime, int, int]:
        return (self.event_time, self.version, self.seq)


@dataclass
class TierResolution:
    tier: str
    source: str                 # asof / default / unknown / conflict
    event: TierEvent | None = None
    reason_code: str | None = None


def rate_of(tier: str) -> Decimal | None:
    return config.TIER_RATES.get(tier)


def is_known_tier(tier: str) -> bool:
    return tier in config.TIER_RATES


def effective_tier(events: list[TierEvent], at: dt.datetime,
                   *, default: str = config.TIER_DEFAULT) -> TierResolution:
    """取 `event_time ≤ at` 中最大的一条等级事件。

    同一时刻的多条由 (version, seq) 决出唯一一条；若同一时刻存在取值不同的
    两条，返回 conflict（调用方据此隔离该客户，不猜哪条对）。
    """
    inside = [e for e in events if e.event_time <= at]
    if not inside:
        return TierResolution(tier=default, source="default",
                              reason_code="T_TIER_DEFAULT")
    latest = max(inside, key=lambda e: e.order_key)
    peers = [e for e in inside if e.event_time == latest.event_time]
    if len({p.tier for p in peers}) > 1:
        return TierResolution(tier="", source="conflict",
                              reason_code="C_TIER_CONFLICT")
    if not is_known_tier(latest.tier):
        return TierResolution(tier=latest.tier, source="unknown", event=latest,
                              reason_code="T_TIER_UNKNOWN")
    return TierResolution(tier=latest.tier, source="asof", event=latest,
                          reason_code="T_TIER_ASOF_ORDER")


def commission_for(amount: Decimal | None, tier: str) -> Decimal | None:
    """佣金 = 金额 × 费率，量化到 0.01（ROUND_HALF_UP）；金额为空则无佣金。"""
    if amount is None:
        return None
    rate = rate_of(tier)
    if rate is None:
        return None
    return (amount * rate).quantize(config.COMMISSION_DECIMAL,
                                    rounding=ROUND_HALF_UP)


def policy_view() -> dict:
    return {
        "tier_policy": "asof_order",
        "label": "订单发生时的会员等级",
        "alternative": "current_tier（结账时最新等级）——未采用",
        "tiers": [{"tier": name, "rate": str(rate)}
                  for name, rate in config.TIER_RATES.items()],
        "default_tier": config.TIER_DEFAULT,
        "ties": "同一客户同一时刻多条等级：取版本最大、再取到达序最大；取值不同判冲突",
        "commission": "金额 × 费率，Decimal 量化 0.01，ROUND_HALF_UP",
        "reason": [
            "同一订单只由订单时刻决定，重跑/补数结果稳定",
            "台账与导出同一口径，不出现两套数字",
            "迟到等级变更的影响面显式可审计",
        ],
        "note": reasons.label("T_TIER_ASOF_ORDER"),
    }


def cutoff_local(settle_date: str) -> dt.datetime:
    """交班截止点：结算日「次日」本地 09:00，转成 UTC 参与比较。

    结算日按本地时区自然日切分（00:00–24:00），09:00 是次日早上的交班时刻；
    当日订单在次日 09:00 前必须结完，所以截止点是次日 09:00 而不是当日 09:00。
    """
    hour, minute = (int(part) for part in config.SETTLEMENT_CUTOFF.split(":"))
    day = dt.date.fromisoformat(settle_date) + dt.timedelta(days=1)
    local = dt.datetime(day.year, day.month, day.day, hour, minute,
                        tzinfo=config.CHINA_TZ)
    return local.astimezone(config.UTC)


def local_date_of(moment: dt.datetime) -> str:
    return moment.astimezone(config.CHINA_TZ).date().isoformat()
