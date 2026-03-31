from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd

from src.services.backtest_repository import BacktestRepository
from src.services.calculations import signal_from_values
from src.utils import normalize_name_key, parse_date_columns, safe_float


class StrategyBridge:
    def __init__(self, base_dir: Path | None = None) -> None:
        self.repo = BacktestRepository(base_dir=base_dir)

    def term_edge_table(self, version: str) -> pd.DataFrame:
        grouped = self.repo.best_term_edge(version)
        if grouped.empty:
            return grouped
        grouped = grouped.copy()
        if "compound_ret" in grouped.columns:
            grouped["edge_rank"] = grouped["compound_ret"].rank(ascending=False, method="dense")
        else:
            grouped["edge_rank"] = range(1, len(grouped) + 1)
        return grouped

    def rank_upcoming_unlock_candidates(
        self,
        unlocks: pd.DataFrame,
        issues: pd.DataFrame,
        today: pd.Timestamp,
        version: str,
        horizon_days: int = 45,
    ) -> pd.DataFrame:
        if unlocks.empty:
            return pd.DataFrame()
        upcoming = unlocks[
            unlocks["unlock_date"].between(today, today + pd.Timedelta(days=horizon_days), inclusive="both")
        ].copy()
        if upcoming.empty:
            return upcoming

        issue_map = {}
        if not issues.empty:
            tmp = issues.copy()
            tmp["name_key"] = tmp["name"].map(normalize_name_key)
            issue_map = tmp.set_index("name_key").to_dict(orient="index")

        term_edge = self.term_edge_table(version)
        edge_map = term_edge.set_index("term").to_dict(orient="index") if not term_edge.empty else {}

        rows: list[dict[str, Any]] = []
        for _, row in upcoming.iterrows():
            key = normalize_name_key(row.get("name"))
            issue = issue_map.get(key, {})
            edge = edge_map.get(row.get("term"), {})
            current_price = safe_float(issue.get("current_price"))
            offer_price = safe_float(issue.get("offer_price") or row.get("offer_price"))
            current_vs_offer = None
            if current_price and offer_price:
                current_vs_offer = round((current_price / offer_price - 1.0) * 100, 2)
            technical_signal = signal_from_values(
                issue.get("current_price"),
                issue.get("ma20"),
                issue.get("ma60"),
                issue.get("rsi14"),
            )
            historical_edge = safe_float(edge.get("compound_ret"), 0.0) or 0.0
            avg_ret = safe_float(edge.get("avg_ret"), 0.0) or 0.0
            win_rate = safe_float(edge.get("win_rate"), 0.0) or 0.0
            float_ratio = safe_float(issue.get("circulating_shares_ratio_on_listing"), 0.0) or 0.0
            existing_ratio = safe_float(issue.get("existing_shareholder_ratio"), 0.0) or 0.0
            pressure = float_ratio * 0.6 + existing_ratio * 0.4
            signal_bonus = {
                "상승추세": 8.0,
                "중립": 3.0,
                "데이터부족": 0.0,
                "약세추세": -6.0,
                "과열권": -3.0,
                "과매도권": 4.0,
            }.get(technical_signal, 0.0)
            combined_score = historical_edge * 0.35 + avg_ret * 0.25 + win_rate * 0.20 - pressure * 0.15 + signal_bonus
            rows.append(
                {
                    "name": row.get("name"),
                    "symbol": row.get("symbol") or issue.get("symbol"),
                    "market": issue.get("market"),
                    "unlock_date": row.get("unlock_date"),
                    "term": row.get("term"),
                    "days_left": int((pd.Timestamp(row["unlock_date"]) - today).days) if pd.notna(row.get("unlock_date")) else pd.NA,
                    "historical_edge": round(historical_edge, 2),
                    "avg_ret": round(avg_ret, 2),
                    "win_rate": round(win_rate, 2),
                    "pressure_score": round(pressure, 2),
                    "current_vs_offer_pct": current_vs_offer,
                    "technical_signal": technical_signal,
                    "combined_score": round(combined_score, 2),
                    "source": row.get("source"),
                }
            )
        out = pd.DataFrame(rows)
        if out.empty:
            return out
        out = parse_date_columns(out, ["unlock_date"])
        return out.sort_values(["combined_score", "days_left"], ascending=[False, True]).reset_index(drop=True)

    def monthly_unlock_heatmap(self, unlocks: pd.DataFrame) -> pd.DataFrame:
        if unlocks.empty:
            return pd.DataFrame()
        out = unlocks.copy()
        out["year_month"] = pd.to_datetime(out["unlock_date"], errors="coerce").dt.strftime("%Y-%m")
        result = out.groupby(["year_month", "term"], as_index=False).size().rename(columns={"size": "count"})
        pivot = result.pivot(index="year_month", columns="term", values="count").fillna(0).astype(int)
        return pivot.reset_index()
