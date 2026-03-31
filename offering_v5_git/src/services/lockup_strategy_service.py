from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd
from pandas.tseries.offsets import BDay

from src.services.backtest_repository import BacktestRepository
from src.services.strategy_bridge import StrategyBridge
from src.utils import clip_score, coalesce, data_dir, normalize_name_key, parse_date_columns, safe_float


ENTRY_MODE_LABELS = {
    "close": "해제일 종가",
    "open": "해제일 시가",
    "next_day_open": "익일 시가",
    "next_day_close": "익일 종가",
}

SKIP_REASON_LABELS = {
    "prev_close_vs_ipo_below_min": "공모가 대비 전일종가 배수 미달",
    "ipo_price_missing_for_prev_filter": "공모가 데이터 없음",
    "daily_empty_symbol": "일봉 조회 실패",
    "daily_empty_window": "대상 구간 일봉 없음",
    "entry_not_found": "진입 시점 탐색 실패",
    "exit_out_of_range": "청산 구간 부족",
}


def _business_day_on_or_after(value: Any) -> pd.Timestamp | pd.NaT:
    ts = pd.to_datetime(value, errors="coerce")
    if pd.isna(ts):
        return pd.NaT
    return BDay().rollforward(pd.Timestamp(ts).normalize())


class LockupStrategyService:
    def __init__(self, base_dir: Path | str | None = None) -> None:
        self.base_dir = Path(base_dir) if base_dir is not None else data_dir()
        self.backtest_repo = BacktestRepository(self.base_dir)
        self.strategy_bridge = StrategyBridge(self.base_dir)

    def term_rules(self, version: str) -> pd.DataFrame:
        summary = self.backtest_repo.load_summary(version, pretty=True)
        trades = self.backtest_repo.load_trades(version)
        skips = self.backtest_repo.load_skip_reasons(version)
        if summary.empty:
            return pd.DataFrame()

        rows: list[dict[str, Any]] = []
        for _, row in summary.sort_values(["compound_ret", "win_rate"], ascending=[False, False]).iterrows():
            term = str(row.get("term") or "")
            threshold = None
            if not skips.empty and "term" in skips.columns:
                mask = (skips["term"].astype(str) == term) & (skips["reason"].astype(str) == "prev_close_vs_ipo_below_min")
                threshold = safe_float(skips.loc[mask, "threshold"].median()) if mask.any() else None
            median_lag = None
            if not trades.empty and {"term", "unlock_date", "entry_dt"}.issubset(trades.columns):
                term_trades = trades[trades["term"].astype(str) == term].copy()
                if not term_trades.empty:
                    calendar_lag = (pd.to_datetime(term_trades["entry_dt"], errors="coerce") - pd.to_datetime(term_trades["unlock_date"], errors="coerce")).dt.days
                    if not calendar_lag.dropna().empty:
                        median_lag = int(calendar_lag.dropna().median())
            rows.append(
                {
                    "term": term,
                    "strategy_name": row.get("strategy_name"),
                    "entry_mode": row.get("entry_mode"),
                    "entry_rule": ENTRY_MODE_LABELS.get(str(row.get("entry_mode") or ""), str(row.get("entry_mode") or "-")),
                    "hold_days_after_entry": safe_float(row.get("hold_days_after_entry"), 0.0) or 0.0,
                    "trades": safe_float(row.get("trades"), 0.0) or 0.0,
                    "win_rate": safe_float(row.get("win_rate"), 0.0) or 0.0,
                    "avg_ret": safe_float(row.get("avg_ret"), 0.0) or 0.0,
                    "median_ret": safe_float(row.get("median_ret"), 0.0) or 0.0,
                    "compound_ret": safe_float(row.get("compound_ret"), 0.0) or 0.0,
                    "min_prev_close_vs_ipo": threshold,
                    "min_prev_close_vs_ipo_pct": None if threshold is None else round((float(threshold) - 1.0) * 100, 2),
                    "median_calendar_lag": median_lag,
                }
            )
        out = pd.DataFrame(rows)
        if out.empty:
            return out
        return out.sort_values(["compound_ret", "win_rate", "term"], ascending=[False, False, True]).reset_index(drop=True)

    def build_strategy_board(
        self,
        unlocks: pd.DataFrame,
        issues: pd.DataFrame,
        today: pd.Timestamp,
        version: str,
        horizon_days: int = 60,
    ) -> pd.DataFrame:
        candidates = self.strategy_bridge.rank_upcoming_unlock_candidates(
            unlocks=unlocks,
            issues=issues,
            today=today,
            version=version,
            horizon_days=horizon_days,
        )
        if candidates.empty:
            return pd.DataFrame()

        issue_map: dict[str, dict[str, Any]] = {}
        if not issues.empty:
            issue_work = issues.copy()
            issue_work["name_key"] = issue_work.get("name_key", pd.Series(dtype="object")).fillna(issue_work["name"].map(normalize_name_key))
            issue_work = issue_work.drop_duplicates(subset=["name_key"], keep="first")
            issue_map = issue_work.set_index("name_key").to_dict(orient="index")

        term_rules = self.term_rules(version)
        term_rule_map = term_rules.set_index("term").to_dict(orient="index") if not term_rules.empty else {}

        rows: list[dict[str, Any]] = []
        for _, candidate in candidates.iterrows():
            cand = candidate.to_dict()
            unlock_source = cand.pop("source", None)
            issue = issue_map.get(normalize_name_key(cand.get("name")), {})
            term_rule = term_rule_map.get(str(cand.get("term") or ""), {})

            record: dict[str, Any] = dict(issue)
            record.update(cand)
            record["unlock_source"] = unlock_source
            for key, value in term_rule.items():
                record[key] = coalesce(record.get(key), value)

            current_price = safe_float(record.get("current_price"))
            offer_price = safe_float(record.get("offer_price"))
            if current_price is not None and offer_price not in {None, 0}:
                current_vs_offer_ratio = current_price / float(offer_price)
                current_vs_offer_pct = round((current_vs_offer_ratio - 1.0) * 100, 2)
            else:
                current_vs_offer_ratio = safe_float(record.get("current_vs_offer_ratio"))
                current_vs_offer_pct = safe_float(record.get("current_vs_offer_pct"))
            record["current_vs_offer_ratio"] = current_vs_offer_ratio
            record["current_vs_offer_pct"] = current_vs_offer_pct

            threshold = safe_float(record.get("min_prev_close_vs_ipo"))
            if current_vs_offer_ratio is None or threshold is None:
                entry_filter_pass = pd.NA
            else:
                entry_filter_pass = bool(current_vs_offer_ratio >= threshold)
            record["entry_filter_pass"] = entry_filter_pass

            unlock_date = _business_day_on_or_after(record.get("unlock_date"))
            check_date = pd.NaT if pd.isna(unlock_date) else BDay(5).rollback(unlock_date - pd.Timedelta(days=1))
            if pd.isna(check_date):
                check_date = pd.NaT if pd.isna(unlock_date) else unlock_date
            entry_mode = str(record.get("entry_mode") or "close")
            planned_entry_date = unlock_date
            if not pd.isna(unlock_date) and entry_mode.startswith("next_day"):
                planned_entry_date = unlock_date + BDay(1)
            hold_days = int(safe_float(record.get("hold_days_after_entry"), 0.0) or 0)
            planned_exit_date = pd.NaT if pd.isna(planned_entry_date) else planned_entry_date + BDay(max(hold_days, 0))
            record["planned_check_date"] = check_date
            record["planned_entry_date"] = planned_entry_date
            record["planned_exit_date"] = planned_exit_date
            record["entry_rule"] = ENTRY_MODE_LABELS.get(entry_mode, entry_mode)

            conviction = self._conviction_score(record)
            record["conviction_score"] = round(conviction, 1)
            record["decision"] = self._decision_label(record)
            record["decision_rank"] = {"우선검토": 1, "관찰강화": 2, "관찰": 3, "보류": 4}[record["decision"]]
            record["priority_tier"] = self._priority_tier(conviction)
            record["suggested_weight_pct_of_base"] = {"A": 100, "B": 70, "C": 40, "D": 0}[record["priority_tier"]]
            risk_flags = self._risk_flags(record)
            positives = self._positive_flags(record)
            record["risk_flags"] = " · ".join(risk_flags) if risk_flags else "-"
            record["positive_flags"] = " · ".join(positives) if positives else "-"
            record["rationale"] = self._build_rationale(record, positives, risk_flags)
            rows.append(record)

        out = pd.DataFrame(rows)
        if out.empty:
            return out
        out = parse_date_columns(out, ["unlock_date", "listing_date", "planned_check_date", "planned_entry_date", "planned_exit_date"])
        return out.sort_values(["decision_rank", "conviction_score", "combined_score", "days_left"], ascending=[True, False, False, True]).reset_index(drop=True)

    def decision_summary(self, board: pd.DataFrame) -> pd.DataFrame:
        if board.empty:
            return pd.DataFrame()
        grouped = board.groupby("decision", as_index=False).agg(
            candidates=("name", "count"),
            avg_conviction=("conviction_score", "mean"),
            avg_combined=("combined_score", "mean"),
        )
        grouped["decision_rank"] = grouped["decision"].map({"우선검토": 1, "관찰강화": 2, "관찰": 3, "보류": 4})
        return grouped.sort_values(["decision_rank", "avg_conviction"], ascending=[True, False]).drop(columns=["decision_rank"]).reset_index(drop=True)

    def build_order_sheet(self, board: pd.DataFrame, *, min_decision_rank: int = 2) -> pd.DataFrame:
        if board.empty:
            return pd.DataFrame()
        work = board.copy()
        if "decision_rank" in work.columns:
            work = work[work["decision_rank"] <= min_decision_rank].copy()
        if work.empty:
            return work
        export = pd.DataFrame(
            {
                "strategy_version": work.get("strategy_version", pd.Series([None] * len(work))),
                "decision": work["decision"],
                "priority_tier": work["priority_tier"],
                "symbol": work.get("symbol"),
                "name": work.get("name"),
                "market": work.get("market"),
                "unlock_date": pd.to_datetime(work.get("unlock_date"), errors="coerce").dt.strftime("%Y-%m-%d"),
                "planned_check_date": pd.to_datetime(work.get("planned_check_date"), errors="coerce").dt.strftime("%Y-%m-%d"),
                "planned_entry_date": pd.to_datetime(work.get("planned_entry_date"), errors="coerce").dt.strftime("%Y-%m-%d"),
                "planned_exit_date": pd.to_datetime(work.get("planned_exit_date"), errors="coerce").dt.strftime("%Y-%m-%d"),
                "entry_rule": work.get("entry_rule"),
                "term": work.get("term"),
                "hold_days_after_entry": work.get("hold_days_after_entry"),
                "suggested_weight_pct_of_base": work.get("suggested_weight_pct_of_base"),
                "current_vs_offer_pct": work.get("current_vs_offer_pct"),
                "technical_signal": work.get("technical_signal"),
                "combined_score": work.get("combined_score"),
                "conviction_score": work.get("conviction_score"),
                "memo": work.get("rationale"),
            }
        )
        return export.reset_index(drop=True)

    def historical_examples(
        self,
        version: str,
        term: str,
        *,
        reference_ratio: float | None = None,
        limit: int = 12,
    ) -> pd.DataFrame:
        trades = self.backtest_repo.load_trades(version)
        if trades.empty:
            return trades
        work = trades[trades["term"].astype(str) == str(term)].copy()
        if work.empty:
            return work
        if reference_ratio is not None and "entry_price_vs_ipo" in work.columns:
            work["distance_vs_candidate"] = (pd.to_numeric(work["entry_price_vs_ipo"], errors="coerce") - float(reference_ratio)).abs()
            work = work.sort_values(["distance_vs_candidate", "unlock_date"], ascending=[True, False])
        else:
            work = work.sort_values(["unlock_date"], ascending=[False])
        work["net_ret_pct"] = pd.to_numeric(work.get("net_ret"), errors="coerce") * 100.0
        work["gross_ret_pct"] = pd.to_numeric(work.get("gross_ret"), errors="coerce") * 100.0
        return work.head(limit).reset_index(drop=True)

    def skip_breakdown(self, version: str, term: str) -> pd.DataFrame:
        skip_summary = self.backtest_repo.load_skip_summary(version)
        if skip_summary.empty:
            return skip_summary
        work = skip_summary[skip_summary["term"].astype(str) == str(term)].copy()
        if work.empty:
            return work
        total = pd.to_numeric(work["count"], errors="coerce").fillna(0).sum()
        work["reason_label"] = work["reason"].map(lambda x: SKIP_REASON_LABELS.get(str(x), str(x)))
        work["share_pct"] = pd.to_numeric(work["count"], errors="coerce").fillna(0).map(lambda x: round(x / total * 100, 2) if total > 0 else 0.0)
        return work.sort_values(["count", "reason_label"], ascending=[False, True]).reset_index(drop=True)

    def recent_skip_examples(self, version: str, term: str, limit: int = 12) -> pd.DataFrame:
        skip_reasons = self.backtest_repo.load_skip_reasons(version)
        if skip_reasons.empty:
            return skip_reasons
        work = skip_reasons[skip_reasons["term"].astype(str) == str(term)].copy()
        if work.empty:
            return work
        work["reason_label"] = work["reason"].map(lambda x: SKIP_REASON_LABELS.get(str(x), str(x)))
        work = work.sort_values(["unlock_date", "name"], ascending=[False, True])
        return work.head(limit).reset_index(drop=True)

    def _conviction_score(self, row: dict[str, Any]) -> float:
        score = safe_float(row.get("combined_score"), 0.0) or 0.0
        historical_edge = safe_float(row.get("historical_edge"), 0.0) or 0.0
        win_rate = safe_float(row.get("win_rate"), 0.0) or 0.0
        lockup = safe_float(row.get("lockup_commitment_ratio"))
        float_ratio = safe_float(row.get("circulating_shares_ratio_on_listing"))
        existing = safe_float(row.get("existing_shareholder_ratio"))
        employee = safe_float(row.get("employee_forfeit_ratio"))
        secondary_sale = safe_float(row.get("secondary_sale_ratio"))
        premium = safe_float(row.get("current_vs_offer_pct"))
        technical_signal = str(row.get("technical_signal") or "")
        days_left = safe_float(row.get("days_left"))
        entry_filter_pass = row.get("entry_filter_pass")

        if historical_edge > 0:
            score += 8
        elif historical_edge < 0:
            score -= 4

        if win_rate >= 55:
            score += 6
        elif win_rate >= 45:
            score += 3
        elif 0 < win_rate < 40:
            score -= 4

        if entry_filter_pass is True:
            score += 6
        elif entry_filter_pass is False:
            score -= 8

        if lockup is not None:
            if lockup >= 15:
                score += 5
            elif lockup >= 8:
                score += 2
            elif lockup < 5:
                score -= 4

        if float_ratio is not None:
            if float_ratio <= 25:
                score += 4
            elif float_ratio <= 35:
                score += 1
            elif float_ratio >= 50:
                score -= 7
            elif float_ratio >= 40:
                score -= 4

        if existing is not None:
            if existing <= 45:
                score += 3
            elif existing >= 70:
                score -= 5
            elif existing >= 60:
                score -= 3

        if employee is not None:
            if employee <= 1:
                score += 1
            elif employee >= 5:
                score -= 4
            elif employee >= 3:
                score -= 2

        if secondary_sale is not None:
            if secondary_sale <= 10:
                score += 2
            elif secondary_sale >= 50:
                score -= 6
            elif secondary_sale >= 30:
                score -= 3

        if premium is not None:
            if premium >= 120:
                score += 4
            elif premium >= 50:
                score += 2
            elif premium <= -20:
                score -= 3

        if technical_signal == "상승추세":
            score += 4
        elif technical_signal == "과매도권":
            score += 2
        elif technical_signal == "약세추세":
            score -= 6
        elif technical_signal == "과열권":
            score -= 3

        if days_left is not None:
            if days_left <= 5:
                score += 1
            elif days_left > 30:
                score -= 1

        return float(round(clip_score(score, -200.0, 200.0, default=0.0), 2))

    def _decision_label(self, row: dict[str, Any]) -> str:
        conviction = safe_float(row.get("conviction_score"), 0.0) or 0.0
        historical_edge = safe_float(row.get("historical_edge"), 0.0) or 0.0
        technical_signal = str(row.get("technical_signal") or "")
        entry_filter_pass = row.get("entry_filter_pass")

        if conviction >= 35 and historical_edge > 0 and entry_filter_pass is not False and technical_signal != "약세추세":
            return "우선검토"
        if conviction >= 22 and historical_edge > 0:
            return "관찰강화"
        if conviction >= 10:
            return "관찰"
        return "보류"

    @staticmethod
    def _priority_tier(conviction: float) -> str:
        if conviction >= 35:
            return "A"
        if conviction >= 22:
            return "B"
        if conviction >= 10:
            return "C"
        return "D"

    def _risk_flags(self, row: dict[str, Any]) -> list[str]:
        risks: list[str] = []
        lockup = safe_float(row.get("lockup_commitment_ratio"))
        float_ratio = safe_float(row.get("circulating_shares_ratio_on_listing"))
        existing = safe_float(row.get("existing_shareholder_ratio"))
        employee = safe_float(row.get("employee_forfeit_ratio"))
        secondary_sale = safe_float(row.get("secondary_sale_ratio"))
        technical_signal = str(row.get("technical_signal") or "")
        premium = safe_float(row.get("current_vs_offer_pct"))
        entry_filter_pass = row.get("entry_filter_pass")

        if entry_filter_pass is False:
            risks.append("백테스트 진입배수 미달")
        if lockup is not None and lockup < 5:
            risks.append("확약 낮음")
        if float_ratio is not None and float_ratio >= 45:
            risks.append("유통가능물량 큼")
        if existing is not None and existing >= 60:
            risks.append("기존주주 비율 높음")
        if employee is not None and employee >= 3:
            risks.append("우리사주 실권 높음")
        if secondary_sale is not None and secondary_sale >= 30:
            risks.append("구주매출 비중 큼")
        if technical_signal == "약세추세":
            risks.append("기술 약세")
        elif technical_signal == "과열권":
            risks.append("과열권")
        if premium is not None and premium <= -20:
            risks.append("공모가 하회 폭 큼")
        return risks

    def _positive_flags(self, row: dict[str, Any]) -> list[str]:
        positives: list[str] = []
        historical_edge = safe_float(row.get("historical_edge"), 0.0) or 0.0
        win_rate = safe_float(row.get("win_rate"), 0.0) or 0.0
        lockup = safe_float(row.get("lockup_commitment_ratio"))
        float_ratio = safe_float(row.get("circulating_shares_ratio_on_listing"))
        existing = safe_float(row.get("existing_shareholder_ratio"))
        secondary_sale = safe_float(row.get("secondary_sale_ratio"))
        technical_signal = str(row.get("technical_signal") or "")
        entry_filter_pass = row.get("entry_filter_pass")
        premium = safe_float(row.get("current_vs_offer_pct"))

        if historical_edge > 0:
            positives.append("해당 term 히스토리컬 우위")
        if win_rate >= 50:
            positives.append("승률 양호")
        if entry_filter_pass is True:
            positives.append("공모가 배수 필터 충족")
        if lockup is not None and lockup >= 10:
            positives.append("확약 비율 양호")
        if float_ratio is not None and float_ratio <= 30:
            positives.append("상장 유통물량 부담 낮음")
        if existing is not None and existing <= 50:
            positives.append("기존주주 비율 양호")
        if secondary_sale is not None and secondary_sale <= 10:
            positives.append("구주매출 부담 낮음")
        if technical_signal == "상승추세":
            positives.append("기술 상승추세")
        if premium is not None and premium >= 80:
            positives.append("공모가 대비 프리미엄 확보")
        return positives

    @staticmethod
    def _build_rationale(row: dict[str, Any], positives: list[str], risks: list[str]) -> str:
        pieces: list[str] = []
        decision = str(row.get("decision") or "-")
        term = str(row.get("term") or "-")
        edge = safe_float(row.get("historical_edge"), 0.0)
        conviction = safe_float(row.get("conviction_score"), 0.0)
        pieces.append(f"{term} 해제 전략 기준 {decision} ({conviction:.1f}점)")
        if edge is not None:
            pieces.append(f"백테스트 compound {edge:.2f}%")
        if positives:
            pieces.append("강점: " + ", ".join(positives[:3]))
        if risks:
            pieces.append("리스크: " + ", ".join(risks[:3]))
        return " · ".join(pieces)
