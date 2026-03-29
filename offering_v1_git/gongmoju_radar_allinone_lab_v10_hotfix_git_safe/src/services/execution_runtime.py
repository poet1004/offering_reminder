from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd

from src.utils import ensure_dir, normalize_name_key, runtime_dir, safe_float, today_kst


@dataclass
class RuntimePlanBundle:
    plan: pd.DataFrame
    warnings: pd.DataFrame
    summary: dict[str, Any]
    payloads: list[dict[str, Any]]


class ExecutionRuntimeService:
    """
    실제 주문 전 단계에서 사용할 실행 계획 / 드라이런 보조 서비스.

    주의:
    - 이 서비스는 기본적으로 CSV/JSON 실행계획과 드라이런 로그만 생성한다.
    - 실주문 API 호출은 포함하지 않는다.
    """

    REQUIRED_COLUMNS = [
        "symbol",
        "name",
        "decision",
        "planned_entry_date",
        "suggested_weight_pct_of_base",
    ]

    def __init__(self, base_dir: Path | str | None = None) -> None:
        self.base_dir = Path(base_dir) if base_dir is not None else runtime_dir().parent

    @staticmethod
    def _as_ts(value: Any) -> pd.Timestamp | pd.NaT:
        ts = pd.to_datetime(value, errors="coerce")
        if pd.isna(ts):
            return pd.NaT
        if getattr(ts, "tzinfo", None) is not None:
            return ts.tz_localize(None)
        return pd.Timestamp(ts)

    @staticmethod
    def _fmt_date(value: Any) -> str:
        ts = pd.to_datetime(value, errors="coerce")
        if pd.isna(ts):
            return ""
        return pd.Timestamp(ts).strftime("%Y-%m-%d")

    @staticmethod
    def _infer_order_mode(entry_rule: Any) -> str:
        text = str(entry_rule or "").strip()
        lowered = text.lower()
        if "종가" in text or "close" in lowered:
            return "AT_CLOSE_REVIEW"
        if "시가" in text or "open" in lowered:
            return "AT_OPEN_REVIEW"
        if "signal" in lowered or "신호" in text:
            return "SIGNAL_REVIEW"
        return "MANUAL_REVIEW"

    @staticmethod
    def _infer_runtime_action(row: pd.Series, today: pd.Timestamp) -> str:
        entry_date = pd.to_datetime(row.get("planned_entry_date"), errors="coerce")
        check_date = pd.to_datetime(row.get("planned_check_date"), errors="coerce")
        if pd.notna(check_date) and check_date.normalize() > today.normalize():
            return "watchlist"
        if pd.notna(entry_date):
            if entry_date.normalize() > today.normalize():
                return "queue_for_entry"
            if entry_date.normalize() == today.normalize():
                return "review_today"
            return "late_review"
        return "manual_review"

    @staticmethod
    def _priority_score(row: pd.Series) -> float:
        decision_rank = safe_float(row.get("decision_rank"), 99.0) or 99.0
        conviction = safe_float(row.get("conviction_score"), 0.0) or 0.0
        combined = safe_float(row.get("combined_score"), 0.0) or 0.0
        bridge_bonus = {
            "신호발생": 6.0,
            "데이터적재": 3.0,
            "수집중": 2.0,
            "수집대기": 1.0,
            "큐미설정": 0.5,
        }.get(str(row.get("bridge_status") or ""), 0.0)
        return round((100.0 - decision_rank * 12.0) + conviction * 0.6 + combined * 0.3 + bridge_bonus, 2)

    @staticmethod
    def _payload_from_row(row: pd.Series) -> dict[str, Any]:
        return {
            "symbol": str(row.get("symbol") or "").zfill(6),
            "name": row.get("name"),
            "decision": row.get("decision"),
            "priority_tier": row.get("priority_tier"),
            "runtime_action": row.get("runtime_action"),
            "order_mode": row.get("order_mode"),
            "planned_check_date": row.get("planned_check_date"),
            "planned_entry_date": row.get("planned_entry_date"),
            "planned_exit_date": row.get("planned_exit_date"),
            "entry_rule": row.get("entry_rule"),
            "term": row.get("term"),
            "budget_krw": row.get("allocated_budget_krw"),
            "reference_price": row.get("reference_price"),
            "planned_qty": row.get("planned_qty"),
            "bridge_status": row.get("bridge_status"),
            "minute_job_status": row.get("minute_job_status"),
            "memo": row.get("memo") or row.get("rationale"),
        }

    def validate_input(self, df: pd.DataFrame) -> pd.DataFrame:
        warnings: list[dict[str, Any]] = []
        for column in self.REQUIRED_COLUMNS:
            if column not in df.columns:
                warnings.append({
                    "severity": "critical",
                    "issue": "missing_column",
                    "column": column,
                    "detail": f"필수 컬럼 누락: {column}",
                })
        if warnings:
            return pd.DataFrame(warnings)
        return pd.DataFrame()

    def build_runtime_plan(
        self,
        board: pd.DataFrame,
        *,
        total_budget_krw: float = 10_000_000,
        cash_reserve_pct: float = 5.0,
        max_single_position_pct: float = 35.0,
        min_decision_rank: int = 2,
        lot_size: int = 1,
        today: pd.Timestamp | None = None,
    ) -> RuntimePlanBundle:
        if board.empty:
            return RuntimePlanBundle(plan=pd.DataFrame(), warnings=pd.DataFrame(), summary={"selected": 0}, payloads=[])

        warnings = self.validate_input(board)
        if not warnings.empty and (warnings["severity"] == "critical").any():
            return RuntimePlanBundle(plan=pd.DataFrame(), warnings=warnings, summary={"selected": 0}, payloads=[])

        today = pd.Timestamp(today or today_kst()).normalize()
        work = board.copy()
        work["symbol"] = work.get("symbol", pd.Series(dtype="object")).astype(str).str.extract(r"(\d+)", expand=False).fillna(work.get("symbol", pd.Series(dtype="object")).astype(str)).str.zfill(6)
        work["name_key"] = work.get("name_key", pd.Series(dtype="object")).fillna(work.get("name", pd.Series(dtype="object")).map(normalize_name_key))
        if "decision_rank" in work.columns:
            work = work[pd.to_numeric(work["decision_rank"], errors="coerce").fillna(99) <= int(min_decision_rank)].copy()
        if work.empty:
            return RuntimePlanBundle(plan=work, warnings=warnings, summary={"selected": 0}, payloads=[])

        work["planned_check_date"] = pd.to_datetime(work.get("planned_check_date"), errors="coerce")
        work["planned_entry_date"] = pd.to_datetime(work.get("planned_entry_date"), errors="coerce")
        work["planned_exit_date"] = pd.to_datetime(work.get("planned_exit_date"), errors="coerce")
        work["weight_base"] = pd.to_numeric(work.get("suggested_weight_pct_of_base"), errors="coerce").fillna(0.0).clip(lower=0.0)
        work["reference_price"] = pd.to_numeric(
            work.get("turnover_first_entry_price", work.get("current_price", pd.Series([pd.NA] * len(work)))),
            errors="coerce",
        )
        work["priority_score"] = work.apply(self._priority_score, axis=1)
        weight_sum = float(work["weight_base"].sum())
        investable_budget = float(total_budget_krw) * max(0.0, 1.0 - float(cash_reserve_pct) / 100.0)
        single_cap = float(total_budget_krw) * max(0.0, float(max_single_position_pct) / 100.0)

        if weight_sum <= 0:
            work["normalized_weight"] = 0.0
        else:
            work["normalized_weight"] = work["weight_base"] / weight_sum
        work["allocated_budget_krw"] = (investable_budget * work["normalized_weight"]).round(0)
        if single_cap > 0:
            work["allocated_budget_krw"] = work["allocated_budget_krw"].clip(upper=single_cap)
        work["allocated_budget_krw"] = work["allocated_budget_krw"].fillna(0.0)
        work["order_mode"] = work.get("entry_rule", pd.Series([""] * len(work))).map(self._infer_order_mode)
        work["runtime_action"] = work.apply(lambda row: self._infer_runtime_action(row, today), axis=1)
        work["days_to_entry"] = (work["planned_entry_date"].dt.normalize() - today).dt.days

        qty_rows: list[int | None] = []
        price_missing_rows = 0
        zero_qty_rows = 0
        for _, row in work.iterrows():
            ref_price = safe_float(row.get("reference_price"))
            alloc = safe_float(row.get("allocated_budget_krw"), 0.0) or 0.0
            if ref_price in {None, 0}:
                qty_rows.append(None)
                price_missing_rows += 1
                continue
            qty = int(alloc // ref_price)
            if lot_size > 1:
                qty = (qty // int(lot_size)) * int(lot_size)
            if qty <= 0:
                zero_qty_rows += 1
                qty_rows.append(0)
            else:
                qty_rows.append(int(qty))
        work["planned_qty"] = qty_rows
        work["estimated_order_value_krw"] = (pd.to_numeric(work["planned_qty"], errors="coerce") * pd.to_numeric(work["reference_price"], errors="coerce")).round(0)
        work["runtime_ready"] = pd.to_numeric(work["planned_qty"], errors="coerce").fillna(0.0) > 0

        row_warnings: list[dict[str, Any]] = []
        for _, row in work.iterrows():
            if not str(row.get("symbol") or "").strip():
                row_warnings.append({"severity": "critical", "issue": "missing_symbol", "symbol": "", "name": row.get("name"), "detail": "종목코드가 비어 있습니다."})
            if pd.isna(row.get("planned_entry_date")):
                row_warnings.append({"severity": "warning", "issue": "missing_entry_date", "symbol": row.get("symbol"), "name": row.get("name"), "detail": "planned_entry_date가 없어 수동 검토가 필요합니다."})
            if pd.isna(row.get("reference_price")):
                row_warnings.append({"severity": "warning", "issue": "missing_reference_price", "symbol": row.get("symbol"), "name": row.get("name"), "detail": "현재가/분봉 진입가가 없어 수량 계산을 생략했습니다."})
            if safe_float(row.get("planned_qty"), 0.0) == 0:
                row_warnings.append({"severity": "warning", "issue": "zero_quantity", "symbol": row.get("symbol"), "name": row.get("name"), "detail": "예산 대비 주문 수량이 0주입니다."})
            if str(row.get("runtime_action") or "") == "late_review":
                row_warnings.append({"severity": "warning", "issue": "entry_date_passed", "symbol": row.get("symbol"), "name": row.get("name"), "detail": "planned_entry_date가 기준일보다 과거입니다."})

        if not warnings.empty:
            warnings = pd.concat([warnings, pd.DataFrame(row_warnings)], ignore_index=True)
        else:
            warnings = pd.DataFrame(row_warnings)

        preferred_cols = [
            "strategy_version",
            "decision",
            "priority_tier",
            "priority_score",
            "symbol",
            "name",
            "market",
            "term",
            "unlock_date",
            "planned_check_date",
            "planned_entry_date",
            "planned_exit_date",
            "entry_rule",
            "order_mode",
            "runtime_action",
            "days_to_entry",
            "allocated_budget_krw",
            "reference_price",
            "planned_qty",
            "estimated_order_value_krw",
            "bridge_status",
            "minute_job_status",
            "turnover_signal_hits",
            "turnover_first_signal_ts",
            "turnover_best_multiple",
            "turnover_best_price_filter",
            "turnover_best_ratio",
            "combined_score",
            "conviction_score",
            "runtime_ready",
            "rationale",
        ]
        plan = work[[col for col in preferred_cols if col in work.columns]].copy()
        for col in ["unlock_date", "planned_check_date", "planned_entry_date", "planned_exit_date"]:
            if col in plan.columns:
                plan[col] = pd.to_datetime(plan[col], errors="coerce").dt.strftime("%Y-%m-%d")
        if "turnover_first_signal_ts" in plan.columns:
            plan["turnover_first_signal_ts"] = pd.to_datetime(plan["turnover_first_signal_ts"], errors="coerce").dt.strftime("%Y-%m-%d %H:%M:%S")

        payloads = [self._payload_from_row(row) for _, row in plan.iterrows()]
        summary = {
            "selected": int(len(plan)),
            "runtime_ready": int(pd.to_numeric(work["runtime_ready"], errors="coerce").fillna(False).astype(bool).sum()),
            "watchlist": int((work["runtime_action"] == "watchlist").sum()),
            "queue_for_entry": int((work["runtime_action"] == "queue_for_entry").sum()),
            "review_today": int((work["runtime_action"] == "review_today").sum()),
            "late_review": int((work["runtime_action"] == "late_review").sum()),
            "price_missing": int(price_missing_rows),
            "zero_quantity": int(zero_qty_rows),
            "investable_budget_krw": round(investable_budget, 0),
            "allocated_budget_krw": round(float(pd.to_numeric(work["allocated_budget_krw"], errors="coerce").sum()), 0),
            "estimated_order_value_krw": round(float(pd.to_numeric(work["estimated_order_value_krw"], errors="coerce").fillna(0).sum()), 0),
            "asof": today.strftime("%Y-%m-%d"),
        }
        return RuntimePlanBundle(plan=plan.reset_index(drop=True), warnings=warnings.reset_index(drop=True), summary=summary, payloads=payloads)

    def dry_run(self, plan: pd.DataFrame, *, today: pd.Timestamp | None = None) -> pd.DataFrame:
        if plan.empty:
            return pd.DataFrame()
        today = pd.Timestamp(today or today_kst()).normalize()
        out = plan.copy()
        out["dry_run_ts"] = pd.Timestamp.now(tz="Asia/Seoul").tz_localize(None).strftime("%Y-%m-%d %H:%M:%S")
        statuses: list[str] = []
        notes: list[str] = []
        for _, row in out.iterrows():
            action = str(row.get("runtime_action") or "")
            qty = safe_float(row.get("planned_qty"), 0.0) or 0.0
            entry_date = self._as_ts(row.get("planned_entry_date"))
            if qty <= 0:
                statuses.append("SKIP")
                notes.append("수량 0주 또는 기준가격 없음")
            elif pd.notna(entry_date) and entry_date.normalize() < today:
                statuses.append("LATE")
                notes.append("planned_entry_date 경과")
            elif action == "watchlist":
                statuses.append("WATCH")
                notes.append("점검일 도래 전")
            elif action == "queue_for_entry":
                statuses.append("READY")
                notes.append("planned_entry_date 대기")
            elif action == "review_today":
                statuses.append("REVIEW")
                notes.append("당일 실행 후보")
            else:
                statuses.append("MANUAL")
                notes.append("수동 검토 필요")
        out["dry_run_status"] = statuses
        out["dry_run_note"] = notes
        return out

    def export_bundle(
        self,
        bundle: RuntimePlanBundle,
        *,
        out_dir: str | Path | None = None,
        prefix: str = "runtime",
        stamp: str | None = None,
        dry_run_df: pd.DataFrame | None = None,
    ) -> dict[str, Path]:
        out_root = ensure_dir(out_dir or runtime_dir())
        stamp = stamp or pd.Timestamp.now(tz="Asia/Seoul").strftime("%Y%m%d")
        paths: dict[str, Path] = {}

        plan_path = out_root / f"{prefix}_plan_{stamp}.csv"
        bundle.plan.to_csv(plan_path, index=False, encoding="utf-8-sig")
        paths["plan_csv"] = plan_path

        warnings_path = out_root / f"{prefix}_warnings_{stamp}.csv"
        bundle.warnings.to_csv(warnings_path, index=False, encoding="utf-8-sig")
        paths["warnings_csv"] = warnings_path

        payload_path = out_root / f"{prefix}_payload_{stamp}.json"
        payload_path.write_text(json.dumps(bundle.payloads, ensure_ascii=False, indent=2), encoding="utf-8")
        paths["payload_json"] = payload_path

        summary_path = out_root / f"{prefix}_summary_{stamp}.json"
        summary_path.write_text(json.dumps(bundle.summary, ensure_ascii=False, indent=2), encoding="utf-8")
        paths["summary_json"] = summary_path

        if dry_run_df is not None:
            dry_run_path = out_root / f"{prefix}_dry_run_{stamp}.csv"
            dry_run_df.to_csv(dry_run_path, index=False, encoding="utf-8-sig")
            paths["dry_run_csv"] = dry_run_path

        latest_manifest_path = out_root / f"{prefix}_latest_manifest.json"
        manifest = {
            "summary": bundle.summary,
            "paths": {key: str(value) for key, value in paths.items()},
        }
        latest_manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
        paths["latest_manifest_json"] = latest_manifest_path
        return paths
