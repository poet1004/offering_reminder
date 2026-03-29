from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pandas as pd

from src.services.calculations import signal_from_values
from src.utils import coalesce, normalize_name_key, parse_date_columns, safe_bool, safe_float


@dataclass
class AlertSettings:
    unlock_alert_days: int = 7
    move_threshold_pct: float = 5.0
    volume_spike_ratio: float = 3.0
    include_technical: bool = True
    unlock_window_days: int = 45


class AlertEngine:
    def generate(
        self,
        issues: pd.DataFrame,
        unlocks: pd.DataFrame,
        today: pd.Timestamp,
        settings: AlertSettings | None = None,
    ) -> pd.DataFrame:
        settings = settings or AlertSettings()
        rows: list[dict[str, Any]] = []

        issue_map: dict[str, dict[str, Any]] = {}
        if not issues.empty:
            for _, issue in issues.iterrows():
                key = normalize_name_key(issue.get("name"))
                if key:
                    issue_map[key] = issue.to_dict()

        if not unlocks.empty:
            target_unlocks = unlocks[
                unlocks["unlock_date"].between(
                    today,
                    today + pd.Timedelta(days=settings.unlock_window_days),
                    inclusive="both",
                )
            ].copy()
            for _, row in target_unlocks.iterrows():
                name_key = normalize_name_key(row.get("name"))
                issue = issue_map.get(name_key, {})
                days_left = None
                if pd.notna(row.get("unlock_date")):
                    days_left = int((pd.Timestamp(row["unlock_date"]) - today).days)
                risk_score = self.unlock_pressure_score(issue)
                detail = f"{row.get('term')} 해제"
                if days_left is not None:
                    detail += f" · D-{days_left}"
                rows.append(
                    {
                        "alert_type": "보호예수 해제",
                        "severity": self._severity_from_unlock(days_left, risk_score),
                        "name": row.get("name"),
                        "symbol": coalesce(row.get("symbol"), issue.get("symbol")),
                        "when": row.get("unlock_date"),
                        "detail": detail,
                        "metric_1": days_left,
                        "metric_2": risk_score,
                    }
                )

        if not issues.empty:
            for _, row in issues.iterrows():
                day_change = abs(safe_float(row.get("day_change_pct"), 0.0) or 0.0)
                spike = safe_float(row.get("volume_spike_ratio"), 0.0) or 0.0
                if day_change >= settings.move_threshold_pct or spike >= settings.volume_spike_ratio or safe_bool(row.get("unusual_move_flag"), False):
                    rows.append(
                        {
                            "alert_type": "이례적 가격변동",
                            "severity": self._severity_from_move(day_change, spike),
                            "name": row.get("name"),
                            "symbol": row.get("symbol"),
                            "when": today,
                            "detail": f"등락률 {day_change:.2f}% · 거래량 {spike:.2f}배",
                            "metric_1": day_change,
                            "metric_2": spike,
                        }
                    )
                if settings.include_technical:
                    signal = signal_from_values(row.get("current_price"), row.get("ma20"), row.get("ma60"), row.get("rsi14"))
                    if signal in {"과열권", "과매도권"}:
                        rows.append(
                            {
                                "alert_type": "기술신호",
                                "severity": "중간",
                                "name": row.get("name"),
                                "symbol": row.get("symbol"),
                                "when": today,
                                "detail": signal,
                                "metric_1": row.get("rsi14"),
                                "metric_2": row.get("current_price"),
                            }
                        )
        out = pd.DataFrame(rows)
        if out.empty:
            return out
        out = parse_date_columns(out, ["when"])
        order = {"높음": 0, "중간": 1, "낮음": 2}
        out["severity_order"] = out["severity"].map(order).fillna(9)
        out = out.sort_values(["severity_order", "when", "alert_type", "name"]).drop(columns=["severity_order"])
        return out.reset_index(drop=True)

    def unlock_pressure_score(self, issue: dict[str, Any]) -> float:
        float_ratio = safe_float(issue.get("circulating_shares_ratio_on_listing"), 0.0) or 0.0
        existing_ratio = safe_float(issue.get("existing_shareholder_ratio"), 0.0) or 0.0
        lockup_ratio = safe_float(issue.get("lockup_commitment_ratio"), 0.0) or 0.0
        employee_forfeit = safe_float(issue.get("employee_forfeit_ratio"), 0.0) or 0.0
        score = float_ratio * 0.45 + existing_ratio * 0.35 + employee_forfeit * 3.0 - lockup_ratio * 0.15
        return round(max(0.0, min(100.0, score)), 2)

    @staticmethod
    def _severity_from_unlock(days_left: int | None, pressure_score: float) -> str:
        if days_left is not None and days_left <= 3:
            return "높음"
        if pressure_score >= 45:
            return "높음"
        if pressure_score >= 25:
            return "중간"
        return "낮음"

    @staticmethod
    def _severity_from_move(day_change: float, spike: float) -> str:
        if day_change >= 10 or spike >= 5:
            return "높음"
        if day_change >= 6 or spike >= 3:
            return "중간"
        return "낮음"
