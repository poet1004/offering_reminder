
from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any

import pandas as pd

from src.utils import safe_float


@dataclass
class ProportionalBreakEvenResult:
    deposit_amount: float
    offer_price: float
    target_sell_price: float
    competition_ratio: float
    deposit_rate: float
    fee: float
    requested_shares: float
    expected_allocated_shares: float
    expected_profit_per_share: float
    expected_pnl: float
    break_even_allocated_shares: int | None
    break_even_competition_ratio: float | None


def proportional_subscription_model(
    deposit_amount: float,
    offer_price: float,
    target_sell_price: float,
    competition_ratio: float,
    fee: float = 2000.0,
    deposit_rate: float = 0.5,
) -> ProportionalBreakEvenResult:
    """
    비례청약 손익분기 추정기.
    실제 배정은 균등/비례 풀 비중, 증권사 규칙, 반올림, 청약수수료 차이로 달라질 수 있으므로
    '대략적인 의사결정용' 계산기로 사용한다.
    """
    deposit_amount = float(max(deposit_amount, 0))
    offer_price = float(max(offer_price, 1))
    target_sell_price = float(max(target_sell_price, 0))
    competition_ratio = float(max(competition_ratio, 0.0001))
    fee = float(max(fee, 0))
    deposit_rate = float(max(min(deposit_rate, 1), 0.0001))

    requested_shares = deposit_amount / (offer_price * deposit_rate)
    expected_allocated_shares = requested_shares / competition_ratio
    expected_profit_per_share = target_sell_price - offer_price
    expected_pnl = expected_allocated_shares * expected_profit_per_share - fee

    break_even_allocated_shares = None
    break_even_competition_ratio = None
    if expected_profit_per_share > 0:
        break_even_allocated_shares = max(1, math.ceil(fee / expected_profit_per_share))
        if break_even_allocated_shares > 0:
            break_even_competition_ratio = requested_shares / break_even_allocated_shares

    return ProportionalBreakEvenResult(
        deposit_amount=deposit_amount,
        offer_price=offer_price,
        target_sell_price=target_sell_price,
        competition_ratio=competition_ratio,
        deposit_rate=deposit_rate,
        fee=fee,
        requested_shares=requested_shares,
        expected_allocated_shares=expected_allocated_shares,
        expected_profit_per_share=expected_profit_per_share,
        expected_pnl=expected_pnl,
        break_even_allocated_shares=break_even_allocated_shares,
        break_even_competition_ratio=break_even_competition_ratio,
    )


def compute_technical_indicators(history: pd.DataFrame) -> pd.DataFrame:
    if history.empty:
        return history.copy()
    df = history.copy()
    if "close" not in df.columns:
        raise ValueError("history DataFrame must include a 'close' column")
    df = df.sort_values("date").reset_index(drop=True)
    close = pd.to_numeric(df["close"], errors="coerce")
    df["ma20"] = close.rolling(20).mean()
    df["ma60"] = close.rolling(60).mean()

    delta = close.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.rolling(14).mean()
    avg_loss = loss.rolling(14).mean()
    rs = avg_gain / avg_loss.replace(0, pd.NA)
    df["rsi14"] = 100 - (100 / (1 + rs))
    return df


def signal_from_values(current_price: Any, ma20: Any, ma60: Any, rsi14: Any) -> str:
    current_price = safe_float(current_price)
    ma20 = safe_float(ma20)
    ma60 = safe_float(ma60)
    rsi14 = safe_float(rsi14)
    if None in (current_price, ma20, ma60, rsi14):
        return "데이터부족"
    if current_price > ma20 > ma60 and 50 <= rsi14 <= 70:
        return "상승추세"
    if current_price < ma20 < ma60 and rsi14 < 45:
        return "약세추세"
    if rsi14 >= 70:
        return "과열권"
    if rsi14 <= 30:
        return "과매도권"
    return "중립"


def latest_signal_from_history(history: pd.DataFrame) -> dict[str, float | str | None]:
    if history.empty:
        return {"current_price": None, "ma20": None, "ma60": None, "rsi14": None, "signal": "데이터부족"}
    df = compute_technical_indicators(history)
    row = df.iloc[-1]
    signal = signal_from_values(row.get("close"), row.get("ma20"), row.get("ma60"), row.get("rsi14"))
    return {
        "current_price": safe_float(row.get("close")),
        "ma20": safe_float(row.get("ma20")),
        "ma60": safe_float(row.get("ma60")),
        "rsi14": safe_float(row.get("rsi14")),
        "signal": signal,
    }
