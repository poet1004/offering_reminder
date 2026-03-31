from __future__ import annotations

import calendar
import json
import os
from html import escape
from pathlib import Path
from typing import Any

import pandas as pd
import streamlit as st

st.set_page_config(page_title="공모주 알리미", page_icon="📊", layout="wide")

from src.services.alert_engine import AlertEngine, AlertSettings
from src.services.backtest_repository import BacktestRepository
from src.services.calculations import proportional_subscription_model, signal_from_values
from src.services.dart_client import DartClient
from src.services.dart_ipo_parser import DartIPOParser, snapshot_evidence_frame, snapshot_overlay_frame, snapshot_summary_text
from src.services.ipo_pipeline import IPODataBundle, IPODataHub
from src.services.ipo_repository import IPORepository
from src.services.kis_client import KISClient
from src.services.lockup_strategy_service import LockupStrategyService
from src.services.market_service import MarketService
from src.services.ipo_scrapers import fetch_38_schedule, standardize_38_schedule_table
from src.services.shorts_service import ShortsStudioService
from src.services.scoring import IPOScorer
from src.services.strategy_bridge import StrategyBridge
from src.services.turnover_strategy_service import TurnoverStrategyParams, TurnoverStrategyService
from src.services.unified_lab_bridge import UnifiedLabBridgeService, UnifiedLabBundle, UnifiedLabPaths
from src.utils import detect_project_env_file, fmt_date, fmt_num, fmt_pct, fmt_ratio, fmt_won, humanize_source, issue_recency_sort, load_project_env, mask_secret, normalize_name_key, runtime_dir, safe_float, standardize_issue_frame, to_csv_bytes, today_kst


APP_ROOT = Path(__file__).resolve().parent
DATA_DIR = APP_ROOT / "data"
CACHE_REV = "20260331_v17_stable_path_live_bootstrap"

PAGES_REQUIRING_BUNDLE = {
    "대시보드",
    "딜 탐색기",
    "청약",
    "상장",
    "보호예수",
    "실험실",
    "데이터 / 설정",
}
PAGES_REQUIRING_UNIFIED = {"실험실", "데이터 / 설정"}

SOURCE_MODE_OPTIONS = ["실데이터 우선", "캐시 우선", "샘플만"]


try:
    env_file = detect_project_env_file(APP_ROOT)
    if env_file is not None:
        load_project_env(env_file, override=False)
except Exception:
    pass


def sync_streamlit_secrets_to_env() -> None:
    keys = [
        "KIS_APP_KEY",
        "KIS_APP_SECRET",
        "KIS_ENV",
        "KIS_ACCOUNT_NO",
        "KIS_CANO",
        "KIS_ACNT_PRDT_CD",
        "DART_API_KEY",
    ]
    missing = [key for key in keys if not os.environ.get(key)]
    if not missing:
        return
    try:
        secrets = st.secrets
    except BaseException:
        return
    for key in missing:
        try:
            value = secrets.get(key)
        except BaseException:
            value = None
        if value in (None, ""):
            continue
        os.environ[key] = str(value)


sync_streamlit_secrets_to_env()


@st.cache_data(show_spinner=False, ttl=900)
def load_bundle_cached(
    source_mode: str,
    external_unlock_path: str,
    local_kind_export_path: str,
    allow_sample_fallback: bool,
    allow_packaged_sample_paths: bool,
    cache_rev: str = CACHE_REV,
) -> IPODataBundle:
    _ = cache_rev
    hub = IPODataHub(DATA_DIR, kis_client=KISClient.from_env(), dart_client=DartClient.from_env())
    prefer_live = source_mode == "실데이터 우선"
    use_cache = source_mode != "샘플만"
    return hub.load_bundle(
        prefer_live=prefer_live,
        use_cache=use_cache,
        external_unlock_path=external_unlock_path or None,
        local_kind_export_path=local_kind_export_path or None,
        allow_sample_fallback=allow_sample_fallback,
        allow_packaged_sample_paths=allow_packaged_sample_paths,
    )


@st.cache_data(show_spinner=False)
def load_backtest_version(version: str) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    repo = BacktestRepository(DATA_DIR)
    return (
        repo.load_summary(version),
        repo.load_annual(version),
        repo.load_trades(version),
        repo.load_skip_summary(version),
        repo.load_skip_reasons(version),
    )


@st.cache_data(show_spinner=False)
def load_custom_backtest_view(min_prev_close_vs_ipo: float, base_version: str = "1.0", cache_rev: str = CACHE_REV) -> dict[str, Any]:
    _ = cache_rev
    repo = BacktestRepository(DATA_DIR)
    return repo.custom_threshold_view(float(min_prev_close_vs_ipo), base_version=base_version)


@st.cache_data(show_spinner=False, ttl=900)
def run_turnover_research_cached(workspace_path: str, allow_packaged_sample: bool, params_json: str, cache_rev: str = CACHE_REV) -> dict[str, Any]:
    _ = cache_rev
    payload = json.loads(params_json) if params_json else {}
    params = TurnoverStrategyParams(**payload)
    service = TurnoverStrategyService(DATA_DIR, kis_client=KISClient.from_env())
    return service.run_workspace_research(workspace_path or None, params, allow_packaged_sample=allow_packaged_sample)


@st.cache_data(show_spinner=False, ttl=3600)
def load_company_filings_cached(stock_code: str, corp_name: str, cache_rev: str = CACHE_REV) -> pd.DataFrame:
    _ = cache_rev
    dart_client = DartClient.from_env()
    if dart_client is None:
        return pd.DataFrame()
    hub = IPODataHub(DATA_DIR, dart_client=dart_client)
    return hub.load_company_filings(stock_code=stock_code or None, corp_name=corp_name or None, days=540)


@st.cache_data(show_spinner=False, ttl=3600)
def load_dart_ipo_snapshot_cached(stock_code: str, corp_name: str, force: bool = False, cache_rev: str = CACHE_REV) -> dict[str, Any] | None:
    _ = cache_rev
    dart_client = DartClient.from_env()
    if dart_client is None:
        return None
    parser = DartIPOParser(dart_client, base_dir=DATA_DIR / "cache")
    try:
        return parser.analyze_company(stock_code=stock_code or None, corp_name=corp_name or None, force=force, days=540)
    except Exception as exc:
        return {
            "error": str(exc),
            "company": {"corp_name": corp_name or "", "stock_code": stock_code or ""},
            "filing": {},
            "metrics": {},
            "evidence": [],
            "structured_tables": {},
        }


@st.cache_data(show_spinner=False, ttl=300)
def load_market_snapshot_bundle_cached(prefer_live: bool, allow_sample_fallback: bool, cache_rev: str = CACHE_REV) -> dict[str, Any]:
    _ = cache_rev
    market_service = MarketService(DATA_DIR, kis_client=KISClient.from_env())
    return market_service.get_market_snapshot_bundle(prefer_live=prefer_live, allow_sample_fallback=allow_sample_fallback)


@st.cache_data(show_spinner=False, ttl=300)
def load_market_history_bundle_cached(ticker: str, prefer_live: bool, period: str, allow_sample_fallback: bool, cache_rev: str = CACHE_REV) -> dict[str, Any]:
    _ = cache_rev
    market_service = MarketService(DATA_DIR, kis_client=KISClient.from_env())
    return market_service.get_market_history_bundle(ticker=ticker, prefer_live=prefer_live, period=period, allow_sample_fallback=allow_sample_fallback)


@st.cache_data(show_spinner=False, ttl=1800)
def load_kis_signal_cached(symbol: str, prefer_live: bool, cache_rev: str = CACHE_REV) -> dict[str, Any] | None:
    _ = cache_rev
    if not prefer_live:
        return None
    kis_client = KISClient.from_env()
    if kis_client is None:
        return None
    market_service = MarketService(DATA_DIR, kis_client=kis_client)
    return market_service.get_stock_signal_from_kis(symbol)


@st.cache_data(show_spinner=False, ttl=1800)
def load_unified_lab_bundle_cached(workspace_path: str, allow_packaged_sample: bool, cache_rev: str = CACHE_REV) -> UnifiedLabBundle:
    _ = cache_rev
    service = UnifiedLabBridgeService(DATA_DIR)

    def _attempt(path_text: str | None, allow_sample: bool) -> UnifiedLabBundle:
        try:
            return service.load_bundle(path_text or None, allow_packaged_sample=allow_sample)
        except Exception as exc:
            bundle = empty_unified_bundle()
            bundle.source_status = pd.DataFrame(
                [{"source": "workspace load", "ok": False, "rows": 0, "detail": str(exc)}]
            )
            return bundle

    primary = _attempt(workspace_path or None, allow_packaged_sample)
    explicit_workspace = bool(str(workspace_path or "").strip())
    if explicit_workspace:
        return primary

    sample_candidate = service.auto_detect_workspace(allow_packaged_sample=True)
    if sample_candidate is None:
        return primary
    try:
        primary_workspace = str(primary.paths.workspace or "")
    except Exception:
        primary_workspace = ""
    if primary_workspace and Path(primary_workspace).resolve() == Path(sample_candidate).resolve():
        return primary

    sample_bundle = _attempt(str(sample_candidate), True)
    if unified_bundle_quality_score(sample_bundle) > unified_bundle_quality_score(primary):
        status = sample_bundle.source_status.copy() if isinstance(sample_bundle.source_status, pd.DataFrame) else pd.DataFrame()
        info_row = pd.DataFrame(
            [{
                "source": "workspace fallback",
                "ok": True,
                "rows": 0,
                "detail": f"packaged sample workspace selected: {sample_candidate}",
            }]
        )
        sample_bundle.source_status = pd.concat([info_row, status], ignore_index=True)
        return sample_bundle
    return primary


def empty_unified_bundle() -> UnifiedLabBundle:
    return UnifiedLabBundle(
        paths=UnifiedLabPaths(
            workspace=None,
            unlock_csv=None,
            signals_csv=None,
            misses_csv=None,
            minute_db_path=None,
            turnover_backtest_dir=None,
            turnover_summary_csv=None,
            turnover_summary_pretty_csv=None,
            turnover_annual_csv=None,
            turnover_annual_pretty_csv=None,
            turnover_trades_csv=None,
            turnover_skip_summary_csv=None,
            turnover_skip_reasons_csv=None,
            beta_summary_csv=None,
            beta_trades_csv=None,
        ),
        unlocks=pd.DataFrame(),
        signals=pd.DataFrame(),
        misses=pd.DataFrame(),
        turnover_summary_raw=pd.DataFrame(),
        turnover_summary_pretty=pd.DataFrame(),
        turnover_annual_raw=pd.DataFrame(),
        turnover_annual_pretty=pd.DataFrame(),
        turnover_trades=pd.DataFrame(),
        turnover_skip_summary=pd.DataFrame(),
        turnover_skip_reasons=pd.DataFrame(),
        beta_summary=pd.DataFrame(),
        beta_trades=pd.DataFrame(),
        minute_job_counts=pd.DataFrame(),
        minute_jobs=pd.DataFrame(),
        minute_job_preview=pd.DataFrame(),
        minute_bar_stats=pd.DataFrame(),
        minute_unlock_events=pd.DataFrame(),
        minute_symbol_coverage=pd.DataFrame(),
        source_status=pd.DataFrame(),
    )


def unified_bundle_quality_score(bundle: UnifiedLabBundle | None) -> float:
    if bundle is None:
        return -1.0
    score = 0.0
    data_weights = {
        "unlocks": 3.0,
        "signals": 2.0,
        "turnover_summary_pretty": 2.0,
        "turnover_trades": 2.0,
        "beta_summary": 1.5,
        "minute_jobs": 1.0,
        "minute_bar_stats": 1.0,
    }
    for attr, weight in data_weights.items():
        df = getattr(bundle, attr, pd.DataFrame())
        if isinstance(df, pd.DataFrame) and not df.empty:
            score += weight
    status = getattr(bundle, "source_status", pd.DataFrame())
    if isinstance(status, pd.DataFrame) and not status.empty and "ok" in status.columns:
        ok_count = int(status["ok"].fillna(False).sum())
        fail_count = int((~status["ok"].fillna(False)).sum())
        score += ok_count * 0.15
        score -= fail_count * 0.10
    return score


def experimental_lab_password() -> str:
    for key in ["LAB_ACCESS_PASSWORD", "EXPERIMENTAL_LAB_PASSWORD"]:
        value = str(os.getenv(key, "") or "").strip()
        if value:
            return value
    return ""


def is_experimental_lab_unlocked() -> bool:
    configured = experimental_lab_password()
    if not configured:
        return True
    return bool(st.session_state.get("lab_unlocked"))


def render_experimental_lab_gate() -> bool:
    configured = experimental_lab_password()
    if not configured:
        st.caption("실험실 비밀번호가 설정되어 있지 않아 바로 열립니다. 필요하면 `LAB_ACCESS_PASSWORD`를 설정하세요.")
        return True
    if st.session_state.get("lab_unlocked"):
        lock_cols = st.columns([0.78, 0.22])
        lock_cols[0].caption("실험실 잠금이 해제된 상태입니다.")
        if lock_cols[1].button("다시 잠그기", key="lock_lab_button", use_container_width=True):
            st.session_state.pop("lab_unlocked", None)
            st.rerun()
        return True

    st.subheader("실험실")
    st.info("전략 연구, 백테스트, 쇼츠 생성 기능은 비밀번호로 잠글 수 있습니다.")
    pwd = st.text_input("실험실 비밀번호", type="password", key="lab_password_input")
    if st.button("실험실 열기", key="unlock_lab_button", use_container_width=True):
        if pwd == configured:
            st.session_state["lab_unlocked"] = True
            st.rerun()
        else:
            st.error("비밀번호가 맞지 않습니다.")
    return False


def list_artifact_rows(base_dir: Path, patterns: list[str], limit: int = 20) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    if not base_dir.exists():
        return pd.DataFrame()
    for pattern in patterns:
        matched = sorted(base_dir.glob(pattern), key=lambda p: p.stat().st_mtime, reverse=True)[:limit]
        for path in matched:
            stat = path.stat()
            rows.append(
                {
                    "파일": path.name,
                    "경로": str(path),
                    "수정시각": pd.Timestamp(stat.st_mtime, unit="s").strftime("%Y-%m-%d %H:%M:%S"),
                    "크기(bytes)": int(stat.st_size),
                }
            )
    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(rows).drop_duplicates(subset=["경로"]).reset_index(drop=True)


def load_latest_preflight_report() -> dict[str, Any] | None:
    runtime_path = runtime_dir()
    candidates = sorted(runtime_path.glob("preflight_report*.json"), key=lambda p: p.stat().st_mtime, reverse=True)
    if not candidates:
        return None
    try:
        import json

        return json.loads(candidates[0].read_text(encoding="utf-8"))
    except Exception:
        return None


def status_badge(label: str, ok: bool, detail: str = "") -> None:
    icon = "🟢" if ok else "⚪"
    text = f"{icon} **{label}**"
    if detail:
        text += f" — {detail}"
    st.markdown(text)


def has_value(value: Any) -> bool:
    if value is None:
        return False
    try:
        if pd.isna(value):
            return False
    except Exception:
        pass
    return str(value).strip() != ""


def text_value(value: Any, default: str = "-") -> str:
    return str(value).strip() if has_value(value) else default


def normalized_string_options(values: Any) -> list[str]:
    if isinstance(values, pd.Series):
        iterable = values.tolist()
    else:
        iterable = list(values or [])
    options: list[str] = []
    seen: set[str] = set()
    for value in iterable:
        if not has_value(value):
            continue
        text = str(value).replace("\xa0", " ").strip()
        if not text or text.lower() in {"nan", "none", "null", "nat"}:
            continue
        if text.endswith(".0") and text[:-2].isdigit():
            text = text[:-2]
        if text in seen:
            continue
        seen.add(text)
        options.append(text)
    return sorted(options, key=lambda item: item.casefold())


def issue_missing_detail_count(issue: pd.Series | dict[str, Any]) -> int:
    row = issue if isinstance(issue, dict) else issue.to_dict()
    target_cols = [
        "symbol",
        "market",
        "sector",
        "existing_shareholder_ratio",
        "employee_forfeit_ratio",
        "secondary_sale_ratio",
        "total_offer_shares",
        "post_listing_total_shares",
    ]
    return int(sum(0 if has_value(row.get(col)) else 1 for col in target_cols))


@st.cache_data(show_spinner=False, ttl=3600)
def load_issue_detail_overlay_cached(stock_code: str, corp_name: str, cache_rev: str = CACHE_REV) -> dict[str, Any]:
    _ = cache_rev
    stock_code = str(stock_code or "").strip()
    corp_name = str(corp_name or "").strip()
    overlay: dict[str, Any] = {}

    if corp_name:
        try:
            raw_38 = fetch_38_schedule(timeout=6, include_detail_links=True)
            if not raw_38.empty:
                work = raw_38.copy()
                name_col = next((c for c in ["기업명", "종목명", "회사명"] if c in work.columns), None)
                if name_col is not None:
                    work["name_key"] = work[name_col].map(normalize_name_key)
                    target_key = normalize_name_key(corp_name)
                    subset = work[work["name_key"] == target_key].copy()
                    if subset.empty:
                        compact_target = target_key.replace("구", "")
                        mask = work["name_key"].astype(str).str.contains(target_key, na=False)
                        if compact_target and compact_target != target_key:
                            mask = mask | work["name_key"].astype(str).str.contains(compact_target, na=False)
                        subset = work.loc[mask].copy()
                    if not subset.empty:
                        detail_df = standardize_38_schedule_table(subset.drop(columns=["name_key"], errors="ignore"), fetch_details=True)
                        if not detail_df.empty:
                            first = detail_df.iloc[0].to_dict()
                            for key, value in first.items():
                                if has_value(value):
                                    overlay.setdefault(key, value)
        except Exception:
            pass

    dart_client = DartClient.from_env()
    if dart_client is not None and (stock_code or corp_name):
        try:
            parser = DartIPOParser(dart_client, base_dir=DATA_DIR / "cache")
            snapshot = parser.analyze_company(stock_code=stock_code or None, corp_name=corp_name or None, force=False, days=540)
            if snapshot and not snapshot.get("error"):
                dart_overlay = parser.snapshot_to_issue_overlay(snapshot)
                preferred = {
                    "offer_price",
                    "lockup_commitment_ratio",
                    "employee_subscription_ratio",
                    "employee_forfeit_ratio",
                    "circulating_shares_on_listing",
                    "circulating_shares_ratio_on_listing",
                    "existing_shareholder_ratio",
                    "total_offer_shares",
                    "new_shares",
                    "selling_shares",
                    "secondary_sale_ratio",
                    "post_listing_total_shares",
                    "dart_receipt_no",
                    "dart_viewer_url",
                    "dart_report_nm",
                    "dart_filing_date",
                    "notes",
                }
                for key, value in dart_overlay.items():
                    if not has_value(value):
                        continue
                    if key in preferred or not has_value(overlay.get(key)):
                        overlay[key] = value
        except Exception:
            pass

    return overlay


def hydrate_issue_for_display(issue: pd.Series) -> pd.Series:
    row = issue.copy()
    if issue_missing_detail_count(row) < 4:
        return row
    corp_name = text_value(row.get("name"), "").strip()
    stock_code = text_value(row.get("symbol"), "").strip()
    if not corp_name:
        return row
    cache_key = f"issue_detail_overlay::{normalize_name_key(corp_name)}::{stock_code or 'na'}"
    payload = st.session_state.get(cache_key)
    if payload is None:
        with st.spinner("선택 종목의 상세 정보를 보강하는 중입니다..."):
            try:
                payload = load_issue_detail_overlay_cached(stock_code, corp_name)
            except Exception:
                payload = {}
        st.session_state[cache_key] = payload
    if isinstance(payload, dict):
        for key, value in payload.items():
            if not has_value(value):
                continue
            if not has_value(row.get(key)) or key in {
                "lockup_commitment_ratio",
                "employee_subscription_ratio",
                "employee_forfeit_ratio",
                "circulating_shares_on_listing",
                "circulating_shares_ratio_on_listing",
                "existing_shareholder_ratio",
                "total_offer_shares",
                "new_shares",
                "selling_shares",
                "secondary_sale_ratio",
                "post_listing_total_shares",
                "dart_receipt_no",
                "dart_viewer_url",
                "dart_report_nm",
                "dart_filing_date",
                "notes",
            }:
                row[key] = value
    return row


def count_issue_sources(issues: pd.DataFrame) -> dict[str, int]:
    if issues is None or issues.empty or "source" not in issues.columns:
        return {"total": 0, "real": 0, "sample": 0}
    sources = issues["source"].fillna("unknown").astype(str).str.lower()
    sample = int(sources.isin(["sample", "demo"]).sum())
    real = int(len(issues) - sample)
    return {"total": int(len(issues)), "real": real, "sample": sample}


def build_listing_hold_snapshot(issues: pd.DataFrame, today: pd.Timestamp | None = None, limit: int | None = 20) -> pd.DataFrame:
    if issues is None or issues.empty:
        return pd.DataFrame()
    now = pd.Timestamp(today or today_kst()).normalize()
    work = issues.copy()
    for col in ["listing_date", "offer_price", "current_price"]:
        if col not in work.columns:
            return pd.DataFrame()
    work["listing_date"] = pd.to_datetime(work["listing_date"], errors="coerce")
    work["offer_price"] = pd.to_numeric(work["offer_price"], errors="coerce")
    work["current_price"] = pd.to_numeric(work["current_price"], errors="coerce")
    work = work[
        work["listing_date"].notna()
        & (work["listing_date"] <= now)
        & work["offer_price"].gt(0)
        & work["current_price"].gt(0)
    ].copy()
    if work.empty:
        return pd.DataFrame()
    work["hold_days"] = (now - work["listing_date"]).dt.days.astype(int)
    work["hold_multiple"] = work["current_price"] / work["offer_price"]
    work["hold_return_pct"] = (work["hold_multiple"] - 1.0) * 100.0
    keep = [c for c in [
        "name",
        "symbol",
        "market",
        "listing_date",
        "offer_price",
        "current_price",
        "hold_days",
        "hold_multiple",
        "hold_return_pct",
        "underwriters",
        "source",
    ] if c in work.columns]
    work = work[keep].copy()
    work = work.sort_values(["hold_return_pct", "listing_date"], ascending=[False, False]).reset_index(drop=True)
    if limit is not None:
        work = work.head(int(limit)).copy()
    return work


def render_sample_data_warning(source_mode: str, issue_counts: dict[str, int], snapshot_source: str | None = None) -> None:
    if source_mode == "샘플만":
        st.warning("현재 화면은 **내장 샘플/데모 데이터**입니다. 실제 공모주 일정이나 현재 시장 지수와 다를 수 있습니다.")
        return

    sample_rows = int(issue_counts.get("sample", 0) or 0)
    real_rows = int(issue_counts.get("real", 0) or 0)
    if snapshot_source == "sample" or sample_rows > 0:
        detail = []
        if snapshot_source == "sample":
            detail.append("시장 스냅샷은 샘플")
        if sample_rows > 0:
            detail.append(f"종목 {sample_rows}건이 샘플")
        if real_rows > 0:
            detail.append(f"실데이터 종목 {real_rows}건")
        st.warning("실데이터가 충분히 연결되지 않아 일부 화면에 샘플/데모 데이터가 섞여 있습니다. " + " · ".join(detail))


def render_metric_cards(snapshot: pd.DataFrame, limit: int = 6) -> None:
    if snapshot.empty:
        st.info("시장 스냅샷 데이터가 없습니다.")
        return
    cols = st.columns(min(limit, len(snapshot)))
    for col, (_, row) in zip(cols, snapshot.head(limit).iterrows()):
        with col:
            col.metric(
                row["name"],
                fmt_num(row["last"], 2),
                fmt_pct(row["change_pct"], 2, signed=True),
            )


def render_market_diagnostics(diagnostics: pd.DataFrame, *, title: str = "시장 데이터 진단", only_failures: bool = True) -> None:
    if diagnostics is None or diagnostics.empty:
        return
    display = diagnostics.copy()
    if "ok" in display.columns:
        display["ok"] = display["ok"].map(lambda x: bool(x) if not pd.isna(x) else False)
        if only_failures:
            display = display[~display["ok"]]
        display["상태"] = display["ok"].map({True: "OK", False: "FAIL"})
    if display.empty:
        return
    if "asof" in display.columns:
        display["asof"] = pd.to_datetime(display["asof"], errors="coerce").dt.strftime("%Y-%m-%d %H:%M:%S")
    cols = [c for c in ["상태", "category", "name", "ticker", "provider", "detail", "rows", "asof"] if c in display.columns]
    renamed = display[cols].rename(columns={
        "category": "구분",
        "name": "이름",
        "ticker": "티커",
        "provider": "제공자",
        "detail": "상세",
        "rows": "행수",
        "asof": "시각",
    })
    with st.expander(title, expanded=False):
        st.dataframe(renamed, hide_index=True, use_container_width=True)


@st.cache_data(show_spinner=False, ttl=900)
def add_issue_scores(issues: pd.DataFrame) -> pd.DataFrame:
    base = issues.copy() if isinstance(issues, pd.DataFrame) else pd.DataFrame()
    scorer = IPOScorer()
    try:
        scored = scorer.add_scores(base)
    except Exception:
        scored = base.copy()
    for col in ["subscription_score", "listing_quality_score", "unlock_pressure_score", "overall_score"]:
        if col not in scored.columns:
            scored[col] = pd.Series(index=scored.index, dtype="float64")
    return scored


def safe_sort_values(df: pd.DataFrame, by: str | list[str], ascending: bool | list[bool] = True) -> pd.DataFrame:
    out = df.copy()
    columns = [by] if isinstance(by, str) else list(by)
    for col in columns:
        if col not in out.columns:
            out[col] = pd.Series(index=out.index, dtype="object")
    try:
        return out.sort_values(columns, ascending=ascending, na_position="last")
    except Exception:
        return out


def render_download_button(label: str, df: pd.DataFrame, filename: str) -> None:
    st.download_button(label, data=to_csv_bytes(df), file_name=filename, mime="text/csv", use_container_width=True)


def issue_selector(df: pd.DataFrame, key: str) -> pd.Series | None:
    if df.empty:
        return None
    options = [f"{text_value(row.get('name'))} · {text_value(row.get('market'))} · {text_value(row.get('stage'))}" for _, row in df.iterrows()]
    idx = st.selectbox("상세 종목", options=range(len(options)), format_func=lambda i: options[i], key=key)
    return df.iloc[int(idx)]


def strategy_candidate_selector(df: pd.DataFrame, key: str) -> pd.Series | None:
    if df.empty:
        return None
    labels = []
    for _, row in df.iterrows():
        labels.append(
            f"{row.get('name', '-')} · {row.get('term', '-')} · {fmt_date(row.get('unlock_date'))} · {row.get('decision', '-')} · {row.get('priority_tier', '-')}"
        )
    idx = st.selectbox("상세 전략 후보", options=range(len(labels)), format_func=lambda i: labels[i], key=key)
    return df.iloc[int(idx)]


def render_turnover_candidate_context(candidate: pd.Series, unified_bundle: UnifiedLabBundle) -> None:
    if unified_bundle.paths.workspace is None:
        st.info("통합 프로젝트에 포함된 integrated_lab/ipo_lockup_unified_lab/workspace 또는 외부 Unified Lab workspace를 연결하면 이 후보의 분봉 신호·큐 상태·turnover 백테스트를 함께 볼 수 있습니다.")
        return

    service = UnifiedLabBridgeService(DATA_DIR)
    context = service.candidate_context(candidate, unified_bundle)
    signals = context["signals"]
    trades = context["trades"]
    misses = context["misses"]
    coverage = context["coverage"]
    jobs = context["jobs"]
    beta = context.get("beta", {}) if isinstance(context.get("beta"), dict) else {}

    avg_net_ret_pct = None
    if not trades.empty and "net_ret" in trades.columns:
        avg_net_ret = pd.to_numeric(trades["net_ret"], errors="coerce").mean()
        if pd.notna(avg_net_ret):
            avg_net_ret_pct = float(avg_net_ret) * 100.0
    first_signal_ts = pd.to_datetime(signals["entry_ts"], errors="coerce").min() if not signals.empty and "entry_ts" in signals.columns else pd.NaT
    best_ratio = pd.to_numeric(signals.get("turnover_ratio"), errors="coerce").max() if not signals.empty and "turnover_ratio" in signals.columns else pd.NA
    bars_loaded = pd.to_numeric(coverage.get("bars"), errors="coerce").sum() if not coverage.empty and "bars" in coverage.columns else pd.NA

    c1, c2, c3, c4, c5, c6 = st.columns(6)
    c1.metric("큐 상태", text_value(candidate.get("minute_job_status"), "미설정"))
    c2.metric("분봉 신호 수", int(len(signals)))
    c3.metric("첫 신호 시각", "-" if pd.isna(first_signal_ts) else pd.Timestamp(first_signal_ts).strftime("%Y-%m-%d %H:%M"))
    c4.metric("최대 turnover", fmt_num(best_ratio, 2))
    c5.metric("turnover 평균 순수익", fmt_pct(avg_net_ret_pct, 2, signed=True))
    c6.metric("beta proxy", fmt_num(beta.get("beta_proxy"), 2))

    if not jobs.empty:
        st.markdown("**minute 수집 큐 / 작업 상태**")
        job_view = jobs[[c for c in ["job_id", "status", "priority", "start_ts", "end_ts", "reason", "last_error"] if c in jobs.columns]].copy()
        for col in ["start_ts", "end_ts"]:
            if col in job_view.columns:
                job_view[col] = pd.to_datetime(job_view[col], errors="coerce").dt.strftime("%Y-%m-%d %H:%M")
        st.dataframe(job_view, hide_index=True, use_container_width=True)
    elif unified_bundle.paths.minute_db_path is not None:
        st.info("이 후보에 연결된 minute job이 아직 없습니다.")

    if not coverage.empty:
        st.markdown("**minute 적재 범위**")
        cov_view = coverage[[c for c in ["unlock_date", "unlock_type", "unlock_shares", "bars", "min_ts", "max_ts"] if c in coverage.columns]].copy()
        for col in ["unlock_date", "min_ts", "max_ts"]:
            if col in cov_view.columns:
                cov_view[col] = pd.to_datetime(cov_view[col], errors="coerce").dt.strftime("%Y-%m-%d %H:%M")
        st.dataframe(cov_view, hide_index=True, use_container_width=True)
    elif pd.notna(bars_loaded):
        st.caption(f"적재된 분봉 수: {int(bars_loaded)}")

    st.markdown("**turnover signal hits**")
    if signals.empty:
        st.info("아직 이 후보와 매칭된 turnover signal이 없습니다.")
    else:
        sig_view = signals[[c for c in [
            "entry_ts",
            "entry_price",
            "multiple",
            "turnover_ratio",
            "price_filter",
            "days_from_unlock",
            "cum_volume",
            "signal_name",
        ] if c in signals.columns]].copy()
        if "entry_ts" in sig_view.columns:
            sig_view["entry_ts"] = pd.to_datetime(sig_view["entry_ts"], errors="coerce").dt.strftime("%Y-%m-%d %H:%M")
        st.dataframe(sig_view, hide_index=True, use_container_width=True)
        render_download_button(
            "선택 후보 turnover signal CSV",
            signals,
            f"turnover_signals_{normalize_name_key(candidate.get('name'))}_{text_value(candidate.get('term'))}.csv",
        )

    st.markdown("**turnover signal misses**")
    if misses.empty:
        st.caption("매칭된 miss 레코드가 없습니다.")
    else:
        miss_view = misses[[c for c in ["unlock_date", "multiple", "price_filter", "reason", "max_cum_volume", "unlock_shares"] if c in misses.columns]].copy()
        if "unlock_date" in miss_view.columns:
            miss_view["unlock_date"] = pd.to_datetime(miss_view["unlock_date"], errors="coerce").dt.strftime("%Y-%m-%d")
        st.dataframe(miss_view, hide_index=True, use_container_width=True)

    st.markdown("**turnover backtest trades**")
    if trades.empty:
        st.info("이 후보와 직접 매칭된 turnover backtest 거래는 아직 없습니다.")
    else:
        trade_view = trades[[c for c in [
            "entry_dt",
            "exit_dt",
            "entry_price",
            "exit_price",
            "multiple",
            "price_filter",
            "turnover_ratio",
            "net_ret",
            "gross_ret",
            "hold_days_after_entry",
            "signal_name",
        ] if c in trades.columns]].copy()
        for col in ["entry_dt", "exit_dt"]:
            if col in trade_view.columns:
                trade_view[col] = pd.to_datetime(trade_view[col], errors="coerce").dt.strftime("%Y-%m-%d")
        if "net_ret" in trade_view.columns:
            trade_view["net_ret_pct"] = pd.to_numeric(trade_view["net_ret"], errors="coerce") * 100.0
        if "gross_ret" in trade_view.columns:
            trade_view["gross_ret_pct"] = pd.to_numeric(trade_view["gross_ret"], errors="coerce") * 100.0
        st.dataframe(trade_view, hide_index=True, use_container_width=True)
        render_download_button(
            "선택 후보 turnover trade CSV",
            trades,
            f"turnover_trades_{normalize_name_key(candidate.get('name'))}_{text_value(candidate.get('term'))}.csv",
        )

    st.markdown("**term beta / alpha proxy**")
    if beta:
        st.write({
            "term": beta.get("term"),
            "signal_name": beta.get("signal_name"),
            "trades": beta.get("trades"),
            "beta_proxy": beta.get("beta_proxy"),
            "corr": beta.get("corr"),
            "alpha_proxy": None if pd.isna(beta.get("alpha_proxy")) else round(float(beta.get("alpha_proxy")) * 100.0, 2),
        })
    else:
        st.caption("beta proxy 요약이 없습니다.")


def render_issue_dart_overlay_from_issue(issue: pd.Series) -> None:
    def safe_text(value: Any) -> str:
        if value is None or pd.isna(value):
            return "-"
        text = str(value).strip()
        return text if text else "-"

    fields = [
        "dart_receipt_no",
        "dart_report_nm",
        "dart_filing_date",
        "lockup_commitment_ratio",
        "circulating_shares_on_listing",
        "circulating_shares_ratio_on_listing",
        "existing_shareholder_ratio",
        "employee_forfeit_ratio",
        "total_offer_shares",
        "new_shares",
        "selling_shares",
        "secondary_sale_ratio",
        "post_listing_total_shares",
    ]
    has_values = any(pd.notna(issue.get(col)) for col in fields)
    if not has_values:
        st.info("선택 종목에 저장된 DART 보강값이 없습니다. 데이터 허브 배치 추출이나 DART 원문 분석을 실행해 보세요.")
        return

    st.write(
        {
            "DART 접수번호": safe_text(issue.get("dart_receipt_no")),
            "보고서명": safe_text(issue.get("dart_report_nm")),
            "접수일": fmt_date(issue.get("dart_filing_date")),
            "뷰어": safe_text(issue.get("dart_viewer_url")),
        }
    )
    c1, c2, c3, c4, c5, c6 = st.columns(6)
    c1.metric("확약비율", fmt_pct(issue.get("lockup_commitment_ratio")))
    c2.metric("상장 유통비율", fmt_pct(issue.get("circulating_shares_ratio_on_listing")))
    c3.metric("기존주주비율", fmt_pct(issue.get("existing_shareholder_ratio")))
    c4.metric("우리사주 실권", fmt_pct(issue.get("employee_forfeit_ratio")))
    c5.metric("구주매출 비중", fmt_pct(issue.get("secondary_sale_ratio")))
    c6.metric("공모주식수", fmt_num(issue.get("total_offer_shares"), 0))

    d1, d2, d3, d4 = st.columns(4)
    d1.metric("유통가능주식수", fmt_num(issue.get("circulating_shares_on_listing"), 0))
    d2.metric("신주모집수", fmt_num(issue.get("new_shares"), 0))
    d3.metric("구주매출수", fmt_num(issue.get("selling_shares"), 0))
    d4.metric("상장후 총주식수", fmt_num(issue.get("post_listing_total_shares"), 0))

    if has_value(issue.get("notes")):
        st.caption(text_value(issue.get("notes"), ""))


def render_issue_overview(issue: pd.Series) -> None:
    issue = hydrate_issue_for_display(issue)
    premium_pct = None
    if pd.notna(issue.get("current_price")) and pd.notna(issue.get("offer_price")) and float(issue.get("offer_price")) != 0:
        premium_pct = (float(issue["current_price"]) / float(issue["offer_price"]) - 1.0) * 100
    c1, c2, c3, c4, c5, c6 = st.columns(6)
    c1.metric("공모가", fmt_won(issue.get("offer_price")))
    c2.metric("현재가", fmt_won(issue.get("current_price")))
    c3.metric("상장일", fmt_date(issue.get("listing_date")))
    c4.metric("청약 일정", f"{fmt_date(issue.get('subscription_start'))} ~ {fmt_date(issue.get('subscription_end'))}")
    c5.metric("프리미엄", "-" if premium_pct is None else fmt_pct(premium_pct, 2, signed=True))
    c6.metric("데이터 출처", humanize_source(issue.get("source")))

    c7, c8, c9, c10 = st.columns(4)
    c7.metric("기관경쟁률", fmt_ratio(issue.get("institutional_competition_ratio")))
    c8.metric("청약경쟁률 live", fmt_ratio(issue.get("retail_competition_ratio_live")))
    c9.metric("확약비율", fmt_pct(issue.get("lockup_commitment_ratio")))
    c10.metric("유통가능물량", fmt_pct(issue.get("circulating_shares_ratio_on_listing")))

    detail_payload = {
        "종목코드": text_value(issue.get("symbol")),
        "시장": text_value(issue.get("market")),
        "업종": text_value(issue.get("sector")),
        "주관사": text_value(issue.get("underwriters")),
        "기존주주비율": fmt_pct(issue.get("existing_shareholder_ratio")),
        "우리사주 실권": fmt_pct(issue.get("employee_forfeit_ratio")),
        "구주매출비중": fmt_pct(issue.get("secondary_sale_ratio")),
        "총공모주식수": fmt_num(issue.get("total_offer_shares"), 0),
        "상장후총주식수": fmt_num(issue.get("post_listing_total_shares"), 0),
        "메모": text_value(issue.get("notes"), ""),
    }
    visible_details = {
        key: value
        for key, value in detail_payload.items()
        if value not in {"-", "", "미상"}
    }
    if visible_details:
        st.write(visible_details)
    else:
        st.info("선택 종목의 세부 공모 정보가 아직 확보되지 않았습니다.")
    missing_dart_fields = [
        issue.get("existing_shareholder_ratio"),
        issue.get("employee_forfeit_ratio"),
        issue.get("secondary_sale_ratio"),
        issue.get("total_offer_shares"),
        issue.get("post_listing_total_shares"),
    ]
    if all(pd.isna(x) for x in missing_dart_fields):
        st.caption("기존주주비율·우리사주 실권·구주매출비중·총공모주식수·상장후총주식수는 38 상세수집 또는 DART 본문 분석 후 채워질 수 있습니다.")
    if pd.isna(issue.get("symbol")) and pd.notna(issue.get("listing_date")) and pd.Timestamp(issue.get("listing_date")) >= pd.Timestamp.today().normalize():
        st.caption("상장 전 단계라 종목코드가 아직 비어 있을 수 있습니다.")


def render_dart_snapshot(snapshot: dict[str, Any], issue: pd.Series | None = None) -> None:
    if not snapshot:
        st.info("표시할 DART 분석 결과가 없습니다.")
        return
    if snapshot.get("error"):
        st.error(f"DART 분석 실패: {snapshot.get('error')}")
        return
    filing = snapshot.get("filing", {})
    metrics = snapshot.get("metrics", {})
    summary = snapshot_summary_text(snapshot)
    if summary:
        st.caption(summary)
    st.write(
        {
            "회사": snapshot.get("company", {}).get("corp_name") or "-",
            "DART 접수번호": filing.get("rcept_no") or "-",
            "보고서명": filing.get("report_nm") or "-",
            "접수일": filing.get("rcept_dt") or "-",
            "뷰어": filing.get("viewer_url") or "-",
            "문서 파일수": snapshot.get("document_file_count"),
            "파싱시각": snapshot.get("parsed_at") or "-",
        }
    )
    c1, c2, c3, c4, c5, c6 = st.columns(6)
    c1.metric("확약비율", fmt_pct(metrics.get("lockup_commitment_ratio")))
    c2.metric("상장일 유통가능", fmt_pct(metrics.get("circulating_shares_ratio_on_listing")))
    c3.metric("기존주주비율", fmt_pct(metrics.get("existing_shareholder_ratio")))
    c4.metric("우리사주 실권", fmt_pct(metrics.get("employee_forfeit_ratio")))
    c5.metric("구주매출 비중", fmt_pct(metrics.get("secondary_sale_ratio")))
    c6.metric("공모주식수", fmt_num(metrics.get("total_offer_shares"), 0))

    d1, d2, d3, d4 = st.columns(4)
    d1.metric("공모가", fmt_won(metrics.get("offer_price"), 0))
    d2.metric("신주모집수", fmt_num(metrics.get("new_shares"), 0))
    d3.metric("구주매출수", fmt_num(metrics.get("selling_shares"), 0))
    d4.metric("상장후 총주식수", fmt_num(metrics.get("post_listing_total_shares"), 0))

    if issue is not None:
        st.markdown("**기존 앱 값 대비 프리뷰**")
        preview = snapshot_overlay_frame(issue, snapshot)
        st.dataframe(preview, hide_index=True, use_container_width=True)

    evidence_df = snapshot_evidence_frame(snapshot)
    if not evidence_df.empty:
        st.markdown("**추출 근거**")
        st.dataframe(evidence_df, hide_index=True, use_container_width=True)

    structured = snapshot.get("structured_tables", {}) or {}
    if structured:
        st.markdown("**구조화 테이블 미리보기**")
        titles = [title for title, rows in structured.items() if rows]
        if titles:
            selected = st.selectbox("구조화 테이블", options=titles, key=f"dart_struct_{filing.get('rcept_no', 'x')}")
            df = pd.DataFrame(structured.get(selected, []))
            if not df.empty:
                st.dataframe(df, hide_index=True, use_container_width=True)


def render_dashboard(
    bundle: IPODataBundle,
    today: pd.Timestamp,
    prefer_live: bool,
    allow_sample_fallback: bool,
    backtest_version: str,
    source_mode: str,
) -> None:
    repo = IPORepository(DATA_DIR)
    issues = add_issue_scores(bundle.issues)
    snapshot_bundle = load_market_snapshot_bundle_cached(prefer_live, True)
    snapshot = snapshot_bundle["frame"]
    snapshot_source = snapshot_bundle["source"]
    snapshot_diag = snapshot_bundle.get("diagnostics", pd.DataFrame())
    market_service = MarketService(DATA_DIR, kis_client=KISClient.from_env())
    mood = market_service.market_mood(snapshot)
    issue_counts = count_issue_sources(issues)

    subscription_count = int(len(repo.upcoming_subscriptions(issues, today, window_days=30)))
    listing_count = int(len(repo.upcoming_listings(issues, today, window_days=30)))
    unlock_count = int(len(repo.upcoming_unlocks(bundle.all_unlocks, today, window_days=30)))
    alert_count = int(len(repo.alert_candidates(issues, bundle.all_unlocks, today)))

    market_priority = ["KOSPI", "KOSDAQ", "USD/KRW"]
    market_rows: list[dict[str, Any]] = []
    if isinstance(snapshot, pd.DataFrame) and not snapshot.empty:
        work = snapshot.copy()
        work["name"] = work.get("name", pd.Series(dtype="object")).astype(str)
        for name in market_priority:
            hit = work[work["name"] == name]
            if not hit.empty:
                market_rows.append(hit.iloc[0].to_dict())
        if not market_rows:
            market_rows = work.head(3).to_dict("records")

    top_cols = st.columns(4)
    if market_rows:
        for col, row in zip(top_cols[: min(3, len(market_rows))], market_rows[:3]):
            col.metric(str(row.get("name") or "-"), fmt_num(row.get("last"), 2), fmt_pct(row.get("change_pct"), 2, signed=True))
    else:
        top_cols[0].metric("시장", "준비 중")
        top_cols[1].metric("시장", "준비 중")
        top_cols[2].metric("시장", "준비 중")
    mood_delta = "-" if mood.get("score") is None else f"score {fmt_num(mood.get('score'), 2)}"
    top_cols[3].metric("시장 분위기", str(mood.get("label") or "데이터없음"), mood_delta)

    count_cols = st.columns(4)
    count_cols[0].metric("30일 내 청약", subscription_count)
    count_cols[1].metric("30일 내 상장", listing_count)
    count_cols[2].metric("30일 내 보호예수", unlock_count)
    count_cols[3].metric("알림 후보", alert_count)

    render_sample_data_warning(source_mode, issue_counts, snapshot_source)
    st.caption(f"시장 소스: {snapshot_source}")
    st.markdown("---")
    render_calendar_page(bundle, issues, today, show_header=False, show_summary=False)

    if prefer_live and isinstance(snapshot_diag, pd.DataFrame) and not snapshot_diag.empty:
        with st.expander("시장 진단 로그", expanded=False):
            render_market_diagnostics(snapshot_diag, title="시장 진단 로그", only_failures=False)



def render_explorer(bundle: IPODataBundle, prefer_live: bool) -> None:
    st.subheader("딜 탐색기")
    st.caption("샘플, 실데이터, 전략 데이터를 한 화면에서 필터링하고 상세 확인하는 화면입니다.")
    issues = add_issue_scores(bundle.issues)
    if issues.empty:
        st.info("표시할 종목이 없습니다.")
        return

    f1, f2, f3, f4 = st.columns([1, 1, 1, 1.2])
    market = f1.selectbox("시장", ["전체"] + normalized_string_options(issues.get("market", pd.Series(dtype="object"))), index=0)
    stage = f2.selectbox("단계", ["전체"] + normalized_string_options(issues.get("stage", pd.Series(dtype="object"))), index=0)
    source = f3.selectbox("출처", ["전체"] + normalized_string_options(issues.get("source", pd.Series(dtype="object"))), index=0)
    query = f4.text_input("검색", placeholder="종목명 / 주관사 / 업종")

    filtered = issues.copy()
    if market != "전체":
        filtered = filtered[filtered["market"].fillna("").astype(str).str.replace("\xa0", " ").str.strip() == market]
    if stage != "전체":
        filtered = filtered[filtered["stage"].fillna("").astype(str).str.replace("\xa0", " ").str.strip() == stage]
    if source != "전체":
        filtered = filtered[filtered["source"].fillna("").astype(str).str.replace("\xa0", " ").str.strip().str.replace(r"\.0$", "", regex=True) == source]
    if query:
        mask = (
            filtered["name"].fillna("").str.contains(query, case=False, regex=False)
            | filtered["underwriters"].fillna("").str.contains(query, case=False, regex=False)
            | filtered["sector"].fillna("").str.contains(query, case=False, regex=False)
        )
        filtered = filtered[mask]
    filtered = issue_recency_sort(filtered).reset_index(drop=True)

    display = filtered[[
        "name",
        "market",
        "stage",
        "underwriters",
        "listing_date",
        "offer_price",
        "current_price",
        "subscription_score",
        "listing_quality_score",
        "unlock_pressure_score",
        "source",
    ]].copy()
    display["listing_date"] = pd.to_datetime(display["listing_date"], errors="coerce").dt.strftime("%Y-%m-%d")
    st.dataframe(display, hide_index=True, use_container_width=True)
    render_download_button("탐색 결과 CSV 내려받기", display, "deal_explorer.csv")

    issue = issue_selector(filtered.reset_index(drop=True), key="explorer_issue")
    if issue is None:
        return

    t1, t2, t3 = st.tabs(["개요", "공시/IR", "기술/시세"])
    with t1:
        render_issue_overview(issue)
    with t2:
        st.markdown("**연결 가능한 문서 링크**")
        if has_value(issue.get("kind_url")):
            st.markdown(f"- KIND 신규상장기업현황: {text_value(issue.get('kind_url'))}")
        if has_value(issue.get("ir_url")):
            st.markdown(f"- KIND IR자료실: {text_value(issue.get('ir_url'))}")
        if has_value(issue.get("dart_viewer_url")):
            st.markdown(f"- DART 뷰어: {text_value(issue.get('dart_viewer_url'))}")
        if DartClient.from_env() is None:
            st.info("DART API 키를 넣으면 최근 공시를 종목별로 바로 조회할 수 있습니다.")
        else:
            filings = load_company_filings_cached(text_value(issue.get("symbol"), ""), text_value(issue.get("name"), ""))
            if filings.empty:
                st.info("조회된 최근 공시가 없습니다. 종목코드/법인명 매칭이 필요할 수 있습니다.")
            else:
                view = filings[[c for c in ["rcept_dt", "report_nm", "corp_name", "viewer_url"] if c in filings.columns]].copy()
                if "rcept_dt" in view.columns:
                    view["rcept_dt"] = pd.to_datetime(view["rcept_dt"], errors="coerce").dt.strftime("%Y-%m-%d")
                st.dataframe(view.head(12), hide_index=True, use_container_width=True)
    with t3:
        current_price = issue.get("current_price")
        ma20 = issue.get("ma20")
        ma60 = issue.get("ma60")
        rsi14 = issue.get("rsi14")
        history = pd.DataFrame()
        source_label = "sample"
        live_signal = None
        if prefer_live and str(issue.get("symbol", "")).isdigit():
            live_signal = load_kis_signal_cached(str(issue["symbol"]), prefer_live=prefer_live)
        if live_signal:
            current_price = live_signal.get("current_price")
            ma20 = live_signal.get("ma20")
            ma60 = live_signal.get("ma60")
            rsi14 = live_signal.get("rsi14")
            history = live_signal.get("history", pd.DataFrame())
            source_label = "KIS"
        signal = signal_from_values(current_price, ma20, ma60, rsi14)
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("기술신호", signal)
        c2.metric("MA20", fmt_won(ma20))
        c3.metric("MA60", fmt_won(ma60))
        c4.metric("RSI14", fmt_num(rsi14, 1))
        st.caption(f"기술신호 소스: {source_label}")
        if not history.empty:
            chart_df = history[["date", "close"]].rename(columns={"date": "날짜", "close": "종가"}).set_index("날짜")
            st.line_chart(chart_df)
        else:
            st.info("KIS 연결이 없거나 데이터가 부족해 샘플 수치만 표시했습니다.")


def select_subscription_candidates(issues: pd.DataFrame, today: pd.Timestamp | None = None) -> pd.DataFrame:
    today = pd.Timestamp(today or today_kst()).normalize()
    work = add_issue_scores(issues)
    if work.empty:
        return work
    stage = work.get("stage", pd.Series(index=work.index, dtype="object")).fillna("").astype(str)
    sub_start = pd.to_datetime(work.get("subscription_start"), errors="coerce")
    sub_end = pd.to_datetime(work.get("subscription_end"), errors="coerce")
    primary = stage.isin(["청약예정", "청약중", "청약완료"])
    fallback = (
        (sub_start.notna() & (sub_start >= today - pd.Timedelta(days=30)) & (sub_start <= today + pd.Timedelta(days=180)))
        | (sub_end.notna() & (sub_end >= today - pd.Timedelta(days=30)) & (sub_end <= today + pd.Timedelta(days=180)))
    )
    out = work.loc[primary | fallback].copy()
    if out.empty:
        return out
    return issue_recency_sort(out, today=today)


def select_listing_candidates(issues: pd.DataFrame, today: pd.Timestamp | None = None) -> pd.DataFrame:
    today = pd.Timestamp(today or today_kst()).normalize()
    work = add_issue_scores(issues)
    if work.empty:
        return work
    stage = work.get("stage", pd.Series(index=work.index, dtype="object")).fillna("").astype(str)
    listing = pd.to_datetime(work.get("listing_date"), errors="coerce")
    primary = stage.isin(["상장예정", "상장후"])
    fallback = listing.notna() & (listing >= today - pd.Timedelta(days=720))
    out = work.loc[primary | fallback].copy()
    if out.empty:
        return out
    return out


def render_subscription_page(issues: pd.DataFrame, today: pd.Timestamp) -> None:
    st.subheader("청약 단계")
    st.caption("기관경쟁률, 증권사, 공모가와 비례청약 손익분기까지 같이 보는 화면입니다.")
    df = select_subscription_candidates(issues, today=today)
    if df.empty:
        st.info("현재 불러온 일정 기준으로 미래/최근 청약 종목이 없습니다. 캐시를 새로고침했는데도 비어 있으면 실제 예정 종목이 없는 상태일 가능성이 큽니다.")
        return

    left, right = st.columns([1.35, 1])
    sorted_df = safe_sort_values(df, ["subscription_start", "subscription_score"], ascending=[True, False]).reset_index(drop=True)
    with left:
        display = sorted_df[[
            "name",
            "market",
            "stage",
            "subscription_start",
            "subscription_end",
            "underwriters",
            "price_band_low",
            "price_band_high",
            "offer_price",
            "retail_competition_ratio_live",
            "institutional_competition_ratio",
            "subscription_score",
            "source",
        ]].copy()
        for col in ["subscription_start", "subscription_end"]:
            display[col] = pd.to_datetime(display[col], errors="coerce").dt.strftime("%Y-%m-%d")
        st.dataframe(display, hide_index=True, use_container_width=True)
        render_download_button("청약 후보 CSV 내려받기", display, "subscriptions.csv")

        issue = issue_selector(sorted_df, key="subscription_issue")
        if issue is not None:
            render_issue_overview(issue)

    with right:
        st.markdown("**비례청약 손익분기 계산기**")
        issue_names = sorted_df["name"].tolist()
        selected_name = st.selectbox("계산 기준 종목", options=issue_names, key="calc_issue")
        issue = sorted_df[sorted_df["name"] == selected_name].iloc[0]
        deposit_amount = st.number_input("투입 증거금(원)", min_value=100000, step=100000, value=1000000)
        offer_price = st.number_input("공모가(원)", min_value=1000, step=100, value=int(float(issue["offer_price"]) if pd.notna(issue["offer_price"]) else 10000))
        target_sell_price = st.number_input("예상 매도가(원)", min_value=1000, step=100, value=int((float(issue["offer_price"]) if pd.notna(issue["offer_price"]) else 10000) * 1.3))
        default_ratio = issue.get("retail_competition_ratio_live")
        default_ratio = 500.0 if pd.isna(default_ratio) else float(default_ratio)
        competition_ratio = st.number_input("비례 경쟁률(대 1)", min_value=1.0, value=float(default_ratio), step=10.0)
        fee = st.number_input("청약 수수료(원)", min_value=0, value=2000, step=500)
        result = proportional_subscription_model(
            deposit_amount=deposit_amount,
            offer_price=offer_price,
            target_sell_price=target_sell_price,
            competition_ratio=competition_ratio,
            fee=fee,
        )
        c1, c2 = st.columns(2)
        c1.metric("예상 배정 주수", f"{result.expected_allocated_shares:,.2f}주")
        c2.metric("예상 손익", fmt_won(result.expected_pnl, 0))
        c3, c4 = st.columns(2)
        c3.metric("주당 예상 차익", fmt_won(result.expected_profit_per_share, 0))
        be_ratio = "-" if result.break_even_competition_ratio is None else f"{result.break_even_competition_ratio:,.2f}:1"
        c4.metric("손익분기 경쟁률", be_ratio)
        st.caption("실제 배정은 증권사별 균등/비례 구조와 반올림 규칙에 따라 달라질 수 있습니다.")



def render_listing_page(issues: pd.DataFrame, prefer_live: bool, today: pd.Timestamp) -> None:
    st.subheader("상장 단계")
    st.caption("확약, 우리사주 실권, 유통가능물량, 기존주주비율, 현재가와 기술신호를 같이 봅니다.")
    target_df = select_listing_candidates(issues, today=today)
    if target_df.empty:
        st.info("표시할 상장 종목이 없습니다.")
        return

    sorted_target = safe_sort_values(target_df, ["listing_date", "listing_quality_score"], ascending=[False, False]).reset_index(drop=True)
    display = sorted_target[[
        "name",
        "market",
        "listing_date",
        "offer_price",
        "lockup_commitment_ratio",
        "employee_forfeit_ratio",
        "circulating_shares_ratio_on_listing",
        "existing_shareholder_ratio",
        "current_price",
        "day_change_pct",
        "listing_quality_score",
        "source",
    ]].copy()
    display["listing_date"] = pd.to_datetime(display["listing_date"], errors="coerce").dt.strftime("%Y-%m-%d")
    st.dataframe(display, hide_index=True, use_container_width=True)
    render_download_button("상장 종목 CSV 내려받기", display, "listings.csv")

    issue = issue_selector(sorted_target, key="listing_issue")
    if issue is None:
        return

    render_issue_overview(issue)
    current_price = issue.get("current_price")
    ma20 = issue.get("ma20")
    ma60 = issue.get("ma60")
    rsi14 = issue.get("rsi14")
    signal = signal_from_values(current_price, ma20, ma60, rsi14)
    history = pd.DataFrame()
    source_label = text_value(issue.get("source"), "sample")
    if prefer_live and str(issue.get("symbol", "")).isdigit():
        live_signal = load_kis_signal_cached(str(issue["symbol"]), prefer_live=prefer_live)
        if live_signal:
            current_price = live_signal.get("current_price")
            ma20 = live_signal.get("ma20")
            ma60 = live_signal.get("ma60")
            rsi14 = live_signal.get("rsi14")
            signal = live_signal.get("signal")
            history = live_signal.get("history", pd.DataFrame())
            source_label = "KIS"
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("기술신호", signal)
    c2.metric("MA20", fmt_won(ma20))
    c3.metric("MA60", fmt_won(ma60))
    c4.metric("RSI14", fmt_num(rsi14, 1))
    c5.metric("품질점수", fmt_num(issue.get("listing_quality_score"), 1))
    st.caption(f"기술신호 소스: {source_label}")
    if not history.empty:
        chart_df = history[["date", "close"]].rename(columns={"date": "날짜", "close": "종가"}).set_index("날짜")
        st.line_chart(chart_df)



def render_unlock_page(issues: pd.DataFrame, all_unlocks: pd.DataFrame, today: pd.Timestamp) -> None:
    st.subheader("보호예수 해제 / 알림")
    st.caption("보호예수 해제 캘린더와 이례적 가격변동, 기술신호를 함께 관리합니다.")
    issues = add_issue_scores(issues)
    alert_engine = AlertEngine()

    unlock_window_days = st.slider("보호예수 해제 표시 범위(일)", min_value=7, max_value=180, value=45, step=7)
    alert_days = st.slider("해제 임박 알림 기준(일)", min_value=1, max_value=30, value=7, step=1)
    move_threshold = st.slider("가격변동 알림 기준(%)", min_value=3.0, max_value=15.0, value=5.0, step=0.5)
    volume_threshold = st.slider("거래량 급증 알림 배수", min_value=1.5, max_value=8.0, value=3.0, step=0.5)

    repo = IPORepository(DATA_DIR)
    unlocks = repo.upcoming_unlocks(all_unlocks, today, window_days=unlock_window_days)
    if unlocks.empty:
        st.info("표시할 보호예수 해제 일정이 없습니다.")
    else:
        joined = unlocks.merge(
            issues[["name", "unlock_pressure_score", "market", "current_price", "offer_price"]],
            on="name",
            how="left",
        )
        joined["listing_date"] = pd.to_datetime(joined["listing_date"], errors="coerce").dt.strftime("%Y-%m-%d")
        joined["unlock_date"] = pd.to_datetime(joined["unlock_date"], errors="coerce").dt.strftime("%Y-%m-%d")
        st.dataframe(joined, hide_index=True, use_container_width=True)
        render_download_button("unlock 일정 CSV 내려받기", joined, "unlock_calendar.csv")

    settings = AlertSettings(
        unlock_alert_days=alert_days,
        move_threshold_pct=move_threshold,
        volume_spike_ratio=volume_threshold,
        include_technical=True,
        unlock_window_days=unlock_window_days,
    )
    alerts = alert_engine.generate(issues, all_unlocks, today, settings=settings)
    st.markdown("**알림 후보**")
    if alerts.empty:
        st.success("현재 기준 알림 후보가 없습니다.")
    else:
        display = alerts.copy()
        display["when"] = pd.to_datetime(display["when"], errors="coerce").dt.strftime("%Y-%m-%d")
        st.dataframe(display, hide_index=True, use_container_width=True)
        render_download_button("알림 CSV 내려받기", display, "alerts.csv")


def render_dart_page(bundle: IPODataBundle) -> None:
    st.subheader("DART 자동추출")
    st.caption("증권신고서 주요정보 API + 공시 원문 ZIP(document.xml)을 합쳐 공모주 핵심 수치를 자동 추출합니다.")
    dart_client = DartClient.from_env()
    if dart_client is None:
        st.info("DART_API_KEY를 설정하면 선택 종목의 투자설명서/증권신고서 본문을 분석할 수 있습니다.")
        return

    issues = add_issue_scores(bundle.issues)
    if issues.empty:
        st.info("표시할 종목이 없습니다.")
        return

    candidates = issue_recency_sort(issues).reset_index(drop=True)
    issue = issue_selector(candidates, key="dart_issue")
    if issue is None:
        return

    lcol, rcol = st.columns([1.1, 0.9])
    with lcol:
        st.markdown("**선택 종목 개요**")
        render_issue_overview(issue)
        filings = load_company_filings_cached(text_value(issue.get("symbol"), ""), text_value(issue.get("name"), ""))
        st.markdown("**최근 공시 후보**")
        if filings.empty:
            st.info("최근 공시 후보가 없습니다.")
        else:
            view = filings[[c for c in ["rcept_dt", "report_nm", "corp_name", "viewer_url"] if c in filings.columns]].copy()
            if "rcept_dt" in view.columns:
                view["rcept_dt"] = pd.to_datetime(view["rcept_dt"], errors="coerce").dt.strftime("%Y-%m-%d")
            st.dataframe(view.head(15), hide_index=True, use_container_width=True)
    with rcol:
        st.markdown("**분석 실행**")
        force = st.checkbox("캐시 무시하고 다시 파싱", value=False, key="dart_force")
        snapshot_key = f"dart_snapshot::{normalize_name_key(issue.get('name'))}"
        if st.button("선택 종목 DART 본문 분석", use_container_width=True):
            with st.spinner("증권신고서/투자설명서를 분석하는 중입니다..."):
                if force:
                    load_dart_ipo_snapshot_cached.clear()
                snapshot = load_dart_ipo_snapshot_cached(
                    text_value(issue.get("symbol"), ""),
                    text_value(issue.get("name"), ""),
                    force=force,
                )
                st.session_state[snapshot_key] = snapshot
        snapshot = st.session_state.get(snapshot_key)
        if snapshot is None:
            st.info("분석 버튼을 누르면 DART 공모주 지표를 추출합니다.")
        else:
            render_dart_snapshot(snapshot, issue=issue)


def render_market_page(prefer_live: bool, allow_sample_fallback: bool, source_mode: str) -> None:
    st.subheader("시장")
    top_left, top_right = st.columns([0.75, 0.25])
    with top_left:
        st.caption("지수, 선물, 원자재, 환율을 같이 보는 간단한 매크로 보드입니다.")
    with top_right:
        if st.button("시장 데이터 새로고침", use_container_width=True):
            load_market_snapshot_bundle_cached.clear()
            load_market_history_bundle_cached.clear()
            st.rerun()

    snapshot_bundle = load_market_snapshot_bundle_cached(prefer_live, allow_sample_fallback)
    snapshot = snapshot_bundle["frame"]
    source = snapshot_bundle["source"]
    snapshot_diag = snapshot_bundle.get("diagnostics", pd.DataFrame())
    market_service = MarketService(DATA_DIR, kis_client=KISClient.from_env())
    mood = market_service.market_mood(snapshot)
    render_sample_data_warning(source_mode, {"sample": 0, "real": 0}, source)
    st.caption(f"스냅샷 소스: {source} · 시장 분위기: {mood['label']} ({mood['score']})")

    failed = snapshot_diag[~snapshot_diag["ok"].fillna(False)] if isinstance(snapshot_diag, pd.DataFrame) and not snapshot_diag.empty and "ok" in snapshot_diag.columns else pd.DataFrame()
    if str(source).startswith("cache("):
        st.info("실시간 시장 조회가 실패해 마지막 성공 캐시를 사용 중입니다. 아래 진단 로그에서 실패 이유를 볼 수 있습니다.")
    elif prefer_live and not failed.empty:
        st.warning(f"실시간 시장 소스 일부가 실패했습니다. 실패 {len(failed)}건을 진단 로그에서 확인하세요.")

    if snapshot.empty:
        st.error("시장 실데이터를 아직 가져오지 못했습니다. 아래 진단 로그에 실패 원인을 남겨두었습니다.")
        st.markdown("**현재 구현된 실데이터 경로**")
        st.write({"국내지수": "KIS", "해외지수/선물/원자재/환율": "Yahoo Finance HTTP"})
        render_market_diagnostics(snapshot_diag, title="시장 스냅샷 진단 로그", only_failures=False)
        return

    render_metric_cards(snapshot, limit=9)
    option_names = snapshot["name"].tolist()
    selected_name = st.selectbox("히스토리 차트", options=option_names, key="market_chart")
    row = snapshot[snapshot["name"] == selected_name].iloc[0]
    period = st.selectbox("기간", ["1mo", "3mo", "6mo", "1y"], index=2)
    history_bundle = load_market_history_bundle_cached(str(row["ticker"]), prefer_live=prefer_live, period=period, allow_sample_fallback=allow_sample_fallback)
    history = history_bundle["frame"]
    hist_source = history_bundle["source"]
    history_diag = history_bundle.get("diagnostics", pd.DataFrame())
    st.caption(f"차트 소스: {hist_source}")
    if not history.empty:
        chart = history[["date", "close"]].rename(columns={"date": "날짜", "close": selected_name}).set_index("날짜")
        st.line_chart(chart)
    else:
        st.info("선택한 자산의 히스토리 차트를 불러오지 못했습니다.")
    render_market_diagnostics(history_diag, title="히스토리 조회 진단 로그", only_failures=True)

    display = snapshot.copy()
    display["last"] = display["last"].map(lambda x: fmt_num(x, 2))
    display["change_pct"] = display["change_pct"].map(lambda x: fmt_pct(x, 2, signed=True))
    if "asof" in display.columns:
        display["asof"] = pd.to_datetime(display["asof"], errors="coerce").dt.strftime("%Y-%m-%d %H:%M:%S")
    st.dataframe(display, hide_index=True, use_container_width=True)
    render_market_diagnostics(snapshot_diag, title="시장 스냅샷 진단 로그", only_failures=False)



def _format_date_columns_for_display(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    display = df.copy()
    for col in columns:
        if col in display.columns:
            display[col] = pd.to_datetime(display[col], errors="coerce").dt.strftime("%Y-%m-%d")
    return display


def build_month_calendar_html(events: pd.DataFrame, year: int, month: int, today: pd.Timestamp | None = None) -> str:
    today = pd.Timestamp(today or pd.Timestamp.now()).normalize()
    month_events = events.copy()
    if month_events.empty:
        month_events = pd.DataFrame(columns=["date", "event_type", "name", "detail"])
    month_events["date"] = pd.to_datetime(month_events.get("date"), errors="coerce")
    month_events = month_events.dropna(subset=["date"]).copy()
    month_events = month_events[(month_events["date"].dt.year == year) & (month_events["date"].dt.month == month)].copy()
    month_events["day"] = month_events["date"].dt.day.astype(int)

    event_groups: dict[int, list[dict[str, Any]]] = {}
    for day, group in month_events.groupby("day", dropna=False):
        event_groups[int(day)] = group.sort_values(["date", "event_type", "name"]).to_dict(orient="records")

    event_class = {
        "청약시작": "sub-start",
        "청약종료": "sub-end",
        "상장": "listing",
        "보호예수해제": "unlock",
    }
    weekdays = ["일", "월", "화", "수", "목", "금", "토"]
    cal = calendar.Calendar(firstweekday=6)
    weeks = cal.monthdayscalendar(year, month)

    body_rows: list[str] = []
    for week in weeks:
        cells: list[str] = []
        for day in week:
            if day == 0:
                cells.append('<td class="empty"></td>')
                continue
            items = event_groups.get(day, [])
            chips: list[str] = []
            for event in items[:3]:
                cls = event_class.get(str(event.get("event_type") or ""), "generic")
                detail = " · ".join([str(event.get("event_type") or "").strip(), str(event.get("name") or "").strip(), str(event.get("detail") or "").strip()]).strip(" ·")
                label = f"{escape(str(event.get('name') or '-'))}"
                chips.append(f'<div class="event-chip {cls}" title="{escape(detail)}">{label}</div>')
            if len(items) > 3:
                chips.append(f'<div class="more-chip">+{len(items) - 3}건 더</div>')
            today_cls = " today" if today.year == year and today.month == month and today.day == day else ""
            cells.append(
                f'<td class="day-cell{today_cls}"><div class="day-num">{day}</div>{"".join(chips) if chips else "<div class=\"day-spacer\"></div>"}</td>'
            )
        body_rows.append("<tr>" + "".join(cells) + "</tr>")

    css = """
    <style>
      .ipo-calendar-wrap {margin-top: 0.25rem;}
      .ipo-calendar {width: 100%; border-collapse: collapse; table-layout: fixed;}
      .ipo-calendar th {padding: 0.55rem 0.25rem; text-align: center; border-bottom: 1px solid rgba(49, 51, 63, 0.2);}
      .ipo-calendar td {vertical-align: top; height: 128px; border: 1px solid rgba(49, 51, 63, 0.10); padding: 0.35rem;}
      .ipo-calendar td.empty {background: rgba(250, 250, 250, 0.4);}
      .ipo-calendar .day-cell.today {outline: 2px solid rgba(255, 75, 75, 0.45); outline-offset: -2px;}
      .ipo-calendar .day-num {font-weight: 700; margin-bottom: 0.3rem;}
      .ipo-calendar .event-chip {font-size: 0.72rem; line-height: 1.25; border-radius: 0.55rem; padding: 0.14rem 0.45rem; margin-bottom: 0.22rem; overflow: hidden; text-overflow: ellipsis; white-space: nowrap;}
      .ipo-calendar .event-chip.sub-start {background: rgba(59, 130, 246, 0.12); border: 1px solid rgba(59, 130, 246, 0.28);}
      .ipo-calendar .event-chip.sub-end {background: rgba(14, 165, 233, 0.12); border: 1px solid rgba(14, 165, 233, 0.28);}
      .ipo-calendar .event-chip.listing {background: rgba(16, 185, 129, 0.14); border: 1px solid rgba(16, 185, 129, 0.28);}
      .ipo-calendar .event-chip.unlock {background: rgba(239, 68, 68, 0.12); border: 1px solid rgba(239, 68, 68, 0.28);}
      .ipo-calendar .event-chip.generic {background: rgba(107, 114, 128, 0.10); border: 1px solid rgba(107, 114, 128, 0.22);}
      .ipo-calendar .more-chip {font-size: 0.7rem; opacity: 0.78; margin-top: 0.1rem;}
      .ipo-calendar .day-spacer {height: 0.85rem;}
      .ipo-calendar-legend {display: flex; flex-wrap: wrap; gap: 0.45rem; margin: 0.2rem 0 0.65rem;}
      .ipo-calendar-legend span {display: inline-flex; align-items: center; gap: 0.3rem; font-size: 0.8rem;}
      .ipo-calendar-legend i {display: inline-block; width: 0.8rem; height: 0.8rem; border-radius: 999px;}
      .ipo-calendar-legend i.sub-start {background: rgba(59, 130, 246, 0.55);}
      .ipo-calendar-legend i.sub-end {background: rgba(14, 165, 233, 0.55);}
      .ipo-calendar-legend i.listing {background: rgba(16, 185, 129, 0.55);}
      .ipo-calendar-legend i.unlock {background: rgba(239, 68, 68, 0.55);}
    </style>
    """
    legend = """
    <div class="ipo-calendar-legend">
      <span><i class="sub-start"></i>청약시작</span>
      <span><i class="sub-end"></i>청약종료</span>
      <span><i class="listing"></i>상장</span>
      <span><i class="unlock"></i>보호예수해제</span>
    </div>
    """
    header = "".join([f"<th>{day}</th>" for day in weekdays])
    table = f'<div class="ipo-calendar-wrap">{legend}<table class="ipo-calendar"><thead><tr>{header}</tr></thead><tbody>{"".join(body_rows)}</tbody></table></div>'
    return css + table


def current_calendar_periods(today: pd.Timestamp, months: int = 6) -> list[pd.Period]:
    base = pd.Period(pd.Timestamp(today).strftime("%Y-%m"), freq="M")
    return [base + offset for offset in range(months)]


def format_calendar_period(period: pd.Period) -> str:
    return f"{period.year}년 {period.month:02d}월"


def render_calendar_page(
    bundle: IPODataBundle,
    issues: pd.DataFrame,
    today: pd.Timestamp,
    *,
    show_header: bool = True,
    show_summary: bool = True,
) -> None:
    if show_header:
        st.subheader("일정 캘린더")
        st.caption("현재월부터 6개월 동안의 청약, 상장, 보호예수 해제 일정을 달력으로 봅니다.")
    repo = IPORepository(DATA_DIR)
    timeline = repo.build_event_timeline(issues, bundle.all_unlocks)
    if timeline.empty:
        st.info("달력에 표시할 일정이 없습니다.")
        return

    period_options = current_calendar_periods(today, months=6)
    period_labels = [format_calendar_period(period) for period in period_options]
    label_to_period = {label: period for label, period in zip(period_labels, period_options)}

    timeline = timeline.copy()
    timeline["date"] = pd.to_datetime(timeline["date"], errors="coerce")
    timeline = timeline.dropna(subset=["date"]).copy()
    calendar_start = period_options[0].start_time.normalize()
    calendar_end = period_options[-1].end_time.normalize()
    timeline = timeline[(timeline["date"] >= calendar_start) & (timeline["date"] <= calendar_end)].reset_index(drop=True)
    controls = st.columns([0.38, 0.62])
    selected_period_label = controls[0].selectbox(
        "월 선택",
        options=period_labels,
        index=0,
        key=f"calendar_month_{'header' if show_header else 'dashboard'}",
    )
    selected_types = controls[1].multiselect(
        "일정 종류",
        options=["청약시작", "청약종료", "상장", "보호예수해제"],
        default=["청약시작", "청약종료", "상장", "보호예수해제"],
        key=f"calendar_types_{'header' if show_header else 'dashboard'}",
    )

    selected_period = label_to_period[selected_period_label]
    month_events = timeline[
        (timeline["date"].dt.year == selected_period.year)
        & (timeline["date"].dt.month == selected_period.month)
    ].copy()
    if selected_types:
        month_events = month_events[month_events["event_type"].isin(selected_types)].copy()

    if show_summary:
        m1, m2, m3, m4 = st.columns(4)
        m1.metric("이달 일정", int(len(month_events)))
        m2.metric("청약", int(month_events["event_type"].isin(["청약시작", "청약종료"]).sum()) if not month_events.empty else 0)
        m3.metric("상장", int((month_events["event_type"] == "상장").sum()) if not month_events.empty else 0)
        m4.metric("보호예수해제", int((month_events["event_type"] == "보호예수해제").sum()) if not month_events.empty else 0)

    st.markdown(build_month_calendar_html(month_events, selected_period.year, selected_period.month, today=today), unsafe_allow_html=True)

    if show_header:
        st.markdown("**월간 일정 목록**")
    if month_events.empty:
        st.info("선택한 달에는 일정이 없습니다.")
        return

    display = month_events.copy()
    display["date"] = pd.to_datetime(display["date"], errors="coerce").dt.strftime("%Y-%m-%d")
    display = display.rename(columns={"date": "날짜", "event_type": "이벤트", "name": "종목명", "symbol": "종목코드", "market": "시장", "detail": "상세"})
    st.dataframe(display[["날짜", "이벤트", "종목명", "종목코드", "시장", "상세"]], hide_index=True, use_container_width=True)
    render_download_button("월간 일정 CSV 내려받기", display, f"ipo_calendar_{selected_period}.csv")



def render_backtest_page(issues: pd.DataFrame, today: pd.Timestamp) -> None:
    repo = BacktestRepository(DATA_DIR)
    st.subheader("백테스트")
    st.caption("보호예수 해제 자동매수 전략 성과와 상장 후 보유 가정 성과를 함께 확인합니다.")
    versions_df = repo.versions_summary()
    if versions_df.empty:
        st.info("백테스트 결과가 없습니다.")
        return

    st.markdown("**사전 계산 버전 비교**")
    st.dataframe(versions_df, hide_index=True, use_container_width=True)

    mode_cols = st.columns([0.38, 0.62])
    view_mode = mode_cols[0].radio("보기 방식", ["사전 계산 버전", "웹 슬라이더"], horizontal=True)
    if view_mode == "사전 계산 버전":
        version = mode_cols[1].selectbox("상세 버전", options=repo.available_versions(), index=0)
        summary_df, annual_df, trades_df, skip_summary_df, skip_reasons_df = load_backtest_version(version)

        t1, t2, t3, t4, t5 = st.tabs(["전략 요약", "연도별", "거래 로그", "Skip 요약", "Skip 상세"])
        with t1:
            if summary_df.empty:
                st.info("요약 데이터가 없습니다.")
            else:
                st.dataframe(summary_df, hide_index=True, use_container_width=True)
        with t2:
            if annual_df.empty:
                st.info("연도별 데이터가 없습니다.")
            else:
                st.dataframe(annual_df, hide_index=True, use_container_width=True)
        with t3:
            if trades_df.empty:
                st.info("거래 로그가 없습니다.")
            else:
                display = _format_date_columns_for_display(trades_df, ["listing_date", "unlock_date", "entry_dt", "exit_dt", "prev_close_date"])
                st.dataframe(display, hide_index=True, use_container_width=True)
                render_download_button("거래 로그 CSV 내려받기", display, f"backtest_{version.replace('.', '_')}_trades.csv")
        with t4:
            if skip_summary_df.empty:
                st.info("Skip 요약 데이터가 없습니다.")
            else:
                st.dataframe(skip_summary_df, hide_index=True, use_container_width=True)
        with t5:
            if skip_reasons_df.empty:
                st.info("Skip 상세 데이터가 없습니다.")
            else:
                st.dataframe(_format_date_columns_for_display(skip_reasons_df, ["listing_date", "unlock_date"]), hide_index=True, use_container_width=True)
    else:
        threshold = mode_cols[1].slider("공모가 대비 최소 배수", min_value=1.00, max_value=3.00, value=1.20, step=0.05)
        custom_view = load_custom_backtest_view(round(float(threshold), 2))
        summary_df = custom_view.get("summary", pd.DataFrame())
        annual_df = custom_view.get("annual", pd.DataFrame())
        trades_df = custom_view.get("trades", pd.DataFrame())
        excluded_summary_df = custom_view.get("excluded_summary", pd.DataFrame())
        metrics = custom_view.get("metrics", {}) if isinstance(custom_view, dict) else {}
        matching_preset = metrics.get("matching_preset_version")

        st.caption("기준 버전 1.0 거래 로그를 다시 집계하는 방식이라 1.20 / 1.50 / 2.00은 저장된 버전 결과와 정확히 일치합니다.")
        k1, k2, k3, k4 = st.columns(4)
        k1.metric("적용 최소 배수", f"{threshold:.2f}x")
        k2.metric("남은 거래수", int(metrics.get("filtered_trade_count", len(trades_df) if isinstance(trades_df, pd.DataFrame) else 0)))
        k3.metric("추가 제외 거래수", int(metrics.get("excluded_trade_count", 0)))
        k4.metric("일치하는 사전 버전", f"v{matching_preset}" if matching_preset else "-")
        if matching_preset:
            st.info(f"현재 슬라이더 값은 사전 계산 버전 v{matching_preset}와 같습니다. 아래 커스텀 결과와 저장된 공식 결과가 동일해야 합니다.")

        t1, t2, t3, t4 = st.tabs(["전략 요약", "연도별", "거래 로그", "제외 요약"])
        with t1:
            if summary_df.empty:
                st.info("조건에 맞는 거래가 없습니다.")
            else:
                st.dataframe(summary_df, hide_index=True, use_container_width=True)
        with t2:
            if annual_df.empty:
                st.info("연도별 데이터가 없습니다.")
            else:
                st.dataframe(annual_df, hide_index=True, use_container_width=True)
        with t3:
            if trades_df.empty:
                st.info("거래 로그가 없습니다.")
            else:
                display = _format_date_columns_for_display(trades_df, ["listing_date", "unlock_date", "entry_dt", "exit_dt", "prev_close_date"])
                st.dataframe(display, hide_index=True, use_container_width=True)
                render_download_button("커스텀 거래 로그 CSV 내려받기", display, f"custom_backtest_trades_{threshold:.2f}x.csv")
        with t4:
            if excluded_summary_df.empty:
                st.success("기준 1.0 버전 대비 추가로 제외된 거래가 없습니다.")
            else:
                display = excluded_summary_df.rename(
                    columns={
                        "term": "만기",
                        "strategy_name": "전략",
                        "entry_mode": "진입방식",
                        "count": "추가 제외건수",
                        "min_prev_close_vs_ipo": "적용 최소 배수",
                        "min_ratio": "최소 배수",
                        "max_ratio": "최대 배수",
                        "avg_ratio": "평균 배수",
                    }
                )
                st.dataframe(display, hide_index=True, use_container_width=True)

        if matching_preset:
            with st.expander(f"v{matching_preset} 공식 Skip 결과 보기", expanded=False):
                skip_summary_df = repo.load_skip_summary(matching_preset)
                skip_reasons_df = repo.load_skip_reasons(matching_preset)
                if skip_summary_df.empty and skip_reasons_df.empty:
                    st.info("저장된 공식 skip 결과가 없습니다.")
                else:
                    if not skip_summary_df.empty:
                        st.markdown("**Skip 요약**")
                        st.dataframe(skip_summary_df, hide_index=True, use_container_width=True)
                    if not skip_reasons_df.empty:
                        st.markdown("**Skip 상세**")
                        st.dataframe(_format_date_columns_for_display(skip_reasons_df, ["listing_date", "unlock_date"]), hide_index=True, use_container_width=True)

    st.markdown("---")
    st.markdown("**상장일부터 지금까지 보유 가정**")
    hold_df = build_listing_hold_snapshot(issues, today=today, limit=50)
    if hold_df.empty:
        st.info("상장일·공모가·현재가가 모두 있는 종목이 부족해 장기 보유 가정을 계산하지 못했습니다.")
    else:
        top = hold_df.iloc[0]
        h1, h2, h3 = st.columns(3)
        h1.metric("집계 종목수", int(len(hold_df)))
        h2.metric("최고 수익률", fmt_pct(top.get("hold_return_pct"), 2, signed=True))
        h3.metric("대표 사례", text_value(top.get("name")))
        st.caption(
            f"예: {text_value(top.get('name'))} · 상장일 {fmt_date(top.get('listing_date'))} · 공모가 {fmt_won(top.get('offer_price'))} → 현재가 {fmt_won(top.get('current_price'))} · {fmt_pct(top.get('hold_return_pct'), 2, signed=True)}"
        )
        display = hold_df.copy()
        display["listing_date"] = pd.to_datetime(display["listing_date"], errors="coerce").dt.strftime("%Y-%m-%d")
        display["hold_return_pct"] = pd.to_numeric(display["hold_return_pct"], errors="coerce").round(2)
        display["hold_multiple"] = pd.to_numeric(display["hold_multiple"], errors="coerce").round(2)
        st.dataframe(
            display[[c for c in ["name", "symbol", "listing_date", "offer_price", "current_price", "hold_multiple", "hold_return_pct", "hold_days", "underwriters"] if c in display.columns]],
            hide_index=True,
            use_container_width=True,
        )
        render_download_button("상장후 보유 가정 CSV 내려받기", display, "listing_hold_snapshot.csv")




def _parse_float_csv(text: str, *, default: tuple[float, ...] = (1.0,)) -> tuple[float, ...]:
    values: list[float] = []
    for token in str(text or "").replace(";", ",").split(","):
        token = token.strip()
        if not token:
            continue
        try:
            values.append(round(float(token), 4))
        except Exception:
            continue
    uniq = tuple(sorted({v for v in values if v > 0}))
    return uniq or default


def _prettify_turnover_summary(df: pd.DataFrame) -> pd.DataFrame:
    if not isinstance(df, pd.DataFrame) or df.empty:
        return pd.DataFrame()
    out = df.copy()
    pct_cols = ["win_rate", "avg_ret", "median_ret", "sum_ret", "compound_ret", "min_ret", "max_ret", "geo_ann"]
    for col in pct_cols:
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce") * 100.0
    if "year" in out.columns:
        out["year"] = pd.to_numeric(out["year"], errors="coerce").astype("Int64")
    numeric_round_cols = [c for c in ["win_rate", "avg_ret", "median_ret", "sum_ret", "compound_ret", "min_ret", "max_ret", "geo_ann", "avg_log_ret_per_day", "bp_per_day", "multiple"] if c in out.columns]
    for col in numeric_round_cols:
        out[col] = pd.to_numeric(out[col], errors="coerce").round(2)
    return out


def render_turnover_research_page(unified_bundle: UnifiedLabBundle, unified_workspace_path: str, today: pd.Timestamp, allow_packaged_sample: bool) -> None:
    st.subheader("턴오버 전략 연구실")
    st.caption("보호예수 해제 물량이 거래량으로 소화되는 시점을 기준으로 진입하는 전략을 웹에서 바로 테스트합니다.")

    service = TurnoverStrategyService(DATA_DIR, kis_client=KISClient.from_env())
    workspace_label = unified_workspace_path or str(unified_bundle.paths.workspace or "")
    unlocks = unified_bundle.unlocks.copy() if isinstance(unified_bundle.unlocks, pd.DataFrame) else pd.DataFrame()
    if unlocks.empty and workspace_label:
        result_probe = service.run_workspace_research(workspace_label, TurnoverStrategyParams(max_events=1), allow_packaged_sample=allow_packaged_sample)
        unlocks = result_probe.get("unlocks", pd.DataFrame()) if isinstance(result_probe, dict) else pd.DataFrame()

    top1, top2, top3, top4 = st.columns(4)
    top1.metric("workspace", "연결됨" if unified_bundle.paths.workspace is not None else "미연결")
    top2.metric("unlock events", int(len(unlocks)))
    top3.metric("minute DB", "연결됨" if unified_bundle.paths.minute_db_path is not None else "없음")
    top4.metric("일봉 소스", "KIS" if KISClient.from_env() is not None else "minute fallback")
    if workspace_label:
        st.caption(f"현재 workspace: {workspace_label}")

    tabs = st.tabs(["새 전략 계산", "기존 workspace 결과", "가이드"])
    with tabs[0]:
        if unified_bundle.paths.workspace is None and not allow_packaged_sample:
            st.info("연결된 Unified Lab workspace가 없습니다. 사이드바에서 workspace를 지정하거나 내장 데모 workspace 자동연결을 켜 주세요.")
        if unified_bundle.paths.minute_db_path is None:
            st.warning("minute DB가 없으면 신규 턴오버 전략 계산이 불가능합니다.")

        term_options = sorted({str(v).upper() for v in unlocks.get("term", pd.Series(dtype="object")).dropna().astype(str).tolist()})
        type_options = sorted({str(v) for v in unlocks.get("unlock_type", pd.Series(dtype="object")).dropna().astype(str).tolist()})
        min_unlock = pd.to_datetime(unlocks.get("unlock_date"), errors="coerce").min() if not unlocks.empty and "unlock_date" in unlocks.columns else pd.NaT
        max_unlock = pd.to_datetime(unlocks.get("unlock_date"), errors="coerce").max() if not unlocks.empty and "unlock_date" in unlocks.columns else pd.NaT
        default_end = pd.Timestamp(max_unlock).date() if pd.notna(max_unlock) else pd.Timestamp(today).date()
        suggested_start = pd.Timestamp(default_end) - pd.Timedelta(days=180)
        default_start = max(pd.Timestamp(min_unlock).date(), suggested_start.date()) if pd.notna(min_unlock) else suggested_start.date()

        c1, c2, c3 = st.columns(3)
        multiples_text = c1.text_input("진입 거래량 배수", value="1.0, 1.5, 2.0", help="예: 1.0, 1.5, 2.0")
        price_filters = c2.multiselect(
            "가격 필터",
            options=sorted(["reclaim_open_or_vwap", "reclaim_open", "reclaim_vwap", "open_and_vwap", "range_top40", "none"]),
            default=["reclaim_open_or_vwap"],
        )
        max_days_after = c3.slider("해제 후 최대 탐색일", min_value=0, max_value=20, value=5, step=1)

        c4, c5, c6 = st.columns(3)
        aggregate_by = c4.selectbox("이벤트 묶음 기준", options=["type", "term", "day", "none"], index=0)
        cum_scope = c5.selectbox("누적 거래량 범위", options=["through_window", "same_day"], index=0)
        max_events = c6.number_input("최대 이벤트 수", min_value=1, max_value=max(1, min(300, len(unlocks) if not unlocks.empty else 300)), value=min(40, max(1, len(unlocks) if not unlocks.empty else 40)), step=1)

        c7, c8, c9 = st.columns(3)
        selected_terms = c7.multiselect("만기 필터", options=term_options, default=term_options)
        selected_types = c8.multiselect("unlock type 필터", options=type_options, default=type_options[: min(4, len(type_options))] if type_options else [])
        interval_min = c9.selectbox("분봉 간격", options=[1, 3, 5, 10, 15], index=2)

        c10, c11 = st.columns(2)
        unlock_start_date = c10.date_input("unlock 시작일", value=default_start)
        unlock_end_date = c11.date_input("unlock 종료일", value=default_end)

        with st.expander("백테스트 필터 / 보유일 설정", expanded=False):
            d1, d2 = st.columns(2)
            min_prev = d1.number_input("진입 전일 종가 / 공모가 최소배수", min_value=0.0, max_value=5.0, value=0.0, step=0.05, help="0이면 필터를 적용하지 않습니다.")
            max_prev = d2.number_input("진입 전일 종가 / 공모가 최대배수", min_value=0.0, max_value=10.0, value=0.0, step=0.05, help="0이면 필터를 적용하지 않습니다.")
            hold_cols = st.columns(max(1, min(5, len(term_options) if term_options else 5)))
            hold_map: dict[str, int] = {}
            base_hold = {"15D": 5, "1M": 21, "3M": 32, "6M": 63, "1Y": 126}
            target_terms = term_options or list(base_hold.keys())
            for idx, term in enumerate(target_terms):
                hold_map[term] = int(hold_cols[idx % len(hold_cols)].number_input(f"{term} 보유일", min_value=1, max_value=252, value=int(base_hold.get(term, 21)), step=1, key=f"turnover_hold_{term}"))
            buy_cost = st.number_input("매수 비용", min_value=0.0, max_value=0.02, value=0.00015, step=0.00005, format="%.5f")
            sell_cost = st.number_input("매도 비용", min_value=0.0, max_value=0.03, value=0.00215, step=0.00005, format="%.5f")

        force = st.checkbox("턴오버 연구 캐시 무시", value=False)
        params = TurnoverStrategyParams(
            interval_min=int(interval_min),
            multiples=_parse_float_csv(multiples_text),
            price_filters=tuple(price_filters or ["reclaim_open_or_vwap"]),
            max_days_after=int(max_days_after),
            aggregate_by=aggregate_by,
            cum_scope=cum_scope,
            unlock_terms=tuple(selected_terms),
            unlock_types=tuple(selected_types),
            unlock_start_date=str(unlock_start_date),
            unlock_end_date=str(unlock_end_date),
            max_events=int(max_events),
            min_prev_close_vs_ipo=None if float(min_prev) <= 0 else float(min_prev),
            max_prev_close_vs_ipo=None if float(max_prev) <= 0 else float(max_prev),
            buy_cost=float(buy_cost),
            sell_cost=float(sell_cost),
            hold_days_by_term=hold_map,
        ).normalized()

        if st.button("턴오버 전략 계산 실행", use_container_width=True):
            if force:
                run_turnover_research_cached.clear()
            with st.spinner("minute DB와 일봉 데이터를 기준으로 턴오버 전략을 계산하는 중입니다..."):
                result = run_turnover_research_cached(workspace_label, allow_packaged_sample, params.cache_key())
                st.session_state["turnover_research_result"] = result

        result = st.session_state.get("turnover_research_result")
        if not isinstance(result, dict):
            st.info("설정을 고른 뒤 실행 버튼을 누르면 신규 턴오버 전략 결과가 계산됩니다.")
        else:
            signals = result.get("signals", pd.DataFrame())
            misses = result.get("misses", pd.DataFrame())
            trades = result.get("trades", pd.DataFrame())
            summary_df = _prettify_turnover_summary(result.get("summary", pd.DataFrame()))
            annual_df = _prettify_turnover_summary(result.get("annual", pd.DataFrame()))
            skips = result.get("skip_reasons", pd.DataFrame())
            skip_summary = result.get("skip_summary", pd.DataFrame())
            diagnostics = result.get("diagnostics", pd.DataFrame())
            selected_unlocks = result.get("unlocks", pd.DataFrame())

            r1, r2, r3, r4, r5 = st.columns(5)
            r1.metric("선택 unlock", int(len(selected_unlocks)))
            r2.metric("생성 signal", int(len(signals)))
            r3.metric("백테스트 거래", int(len(trades)))
            r4.metric("misses", int(len(misses)))
            r5.metric("skip rows", int(len(skips)))
            if KISClient.from_env() is None:
                st.caption("현재는 KIS 키가 없어 minute DB fallback 일봉이 사용됩니다. 보유일이 긴 전략은 결과가 비어 있을 수 있습니다.")

            rt1, rt2, rt3, rt4, rt5, rt6 = st.tabs(["요약", "연도별", "거래 로그", "signals / misses", "skip / 진단", "선택 unlock"])
            with rt1:
                if summary_df.empty:
                    st.info("요약 결과가 없습니다.")
                else:
                    st.dataframe(summary_df, hide_index=True, use_container_width=True)
                    render_download_button("턴오버 요약 CSV", summary_df, "turnover_research_summary.csv")
            with rt2:
                if annual_df.empty:
                    st.info("연도별 결과가 없습니다.")
                else:
                    st.dataframe(annual_df, hide_index=True, use_container_width=True)
            with rt3:
                if trades.empty:
                    st.info("거래 로그가 없습니다.")
                else:
                    display = _format_date_columns_for_display(trades, ["listing_date", "unlock_date", "entry_dt", "exit_dt", "prev_close_date"])
                    st.dataframe(display, hide_index=True, use_container_width=True)
                    render_download_button("턴오버 거래 로그 CSV", display, "turnover_research_trades.csv")
            with rt4:
                c_sig, c_miss = st.columns(2)
                with c_sig:
                    st.markdown("**signals**")
                    if signals.empty:
                        st.info("signal이 없습니다.")
                    else:
                        display = _format_date_columns_for_display(signals, ["listing_date", "unlock_date", "entry_ts", "entry_trade_date"])
                        st.dataframe(display, hide_index=True, use_container_width=True)
                        render_download_button("turnover signals CSV", display, "turnover_research_signals.csv")
                with c_miss:
                    st.markdown("**misses**")
                    if misses.empty:
                        st.info("misses가 없습니다.")
                    else:
                        display = _format_date_columns_for_display(misses, ["listing_date", "unlock_date"])
                        st.dataframe(display, hide_index=True, use_container_width=True)
                        render_download_button("turnover misses CSV", display, "turnover_research_misses.csv")
            with rt5:
                if not skip_summary.empty:
                    st.markdown("**skip summary**")
                    st.dataframe(skip_summary, hide_index=True, use_container_width=True)
                if not skips.empty:
                    st.markdown("**skip details**")
                    st.dataframe(_format_date_columns_for_display(skips, ["listing_date", "unlock_date", "entry_trade_date"]), hide_index=True, use_container_width=True)
                if diagnostics.empty:
                    st.info("진단 로그가 없습니다.")
                else:
                    st.markdown("**diagnostics**")
                    st.dataframe(diagnostics, hide_index=True, use_container_width=True)
            with rt6:
                if selected_unlocks.empty:
                    st.info("선택된 unlock 이벤트가 없습니다.")
                else:
                    display = _format_date_columns_for_display(selected_unlocks, ["listing_date", "unlock_date", "lockup_end_date"])
                    st.dataframe(display, hide_index=True, use_container_width=True)

    with tabs[1]:
        st.caption("현재 workspace에 저장된 turnover 백테스트 결과를 필터링해서 빠르게 다시 봅니다.")
        if unified_bundle.turnover_trades.empty:
            st.info("현재 workspace에 저장된 turnover trades가 없습니다. 위 탭에서 신규 계산을 실행해 보세요.")
        else:
            trades = unified_bundle.turnover_trades.copy()
            existing_service = TurnoverStrategyService(DATA_DIR)
            e1, e2, e3, e4 = st.columns(4)
            mul_options = sorted(pd.to_numeric(trades.get("multiple"), errors="coerce").dropna().unique().tolist())
            pf_options = sorted(trades.get("price_filter", pd.Series(dtype="object")).dropna().astype(str).unique().tolist())
            term_options = sorted(trades.get("term", pd.Series(dtype="object")).dropna().astype(str).unique().tolist())
            type_options = sorted(trades.get("unlock_type", pd.Series(dtype="object")).dropna().astype(str).unique().tolist())
            selected_multiples = e1.multiselect("배수", options=mul_options, default=mul_options)
            selected_pfs = e2.multiselect("가격 필터", options=pf_options, default=pf_options)
            selected_terms = e3.multiselect("만기", options=term_options, default=term_options)
            selected_types = e4.multiselect("unlock type", options=type_options, default=type_options)
            f1, f2 = st.columns(2)
            min_prev = f1.number_input("기존 결과 재필터 최소배수", min_value=0.0, max_value=5.0, value=0.0, step=0.05, key="existing_turnover_min_prev")
            max_prev = f2.number_input("기존 결과 재필터 최대배수", min_value=0.0, max_value=10.0, value=0.0, step=0.05, key="existing_turnover_max_prev")
            view = existing_service.summarize_existing_workspace_results(
                trades,
                multiples=selected_multiples,
                price_filters=selected_pfs,
                terms=selected_terms,
                unlock_types=selected_types,
                min_prev_close_vs_ipo=None if float(min_prev) <= 0 else float(min_prev),
                max_prev_close_vs_ipo=None if float(max_prev) <= 0 else float(max_prev),
            )
            summary_df = _prettify_turnover_summary(view.get("summary", pd.DataFrame()))
            annual_df = _prettify_turnover_summary(view.get("annual", pd.DataFrame()))
            trades_df = view.get("trades", pd.DataFrame())
            k1, k2, k3 = st.columns(3)
            k1.metric("필터 후 거래수", int(len(trades_df)))
            k2.metric("signal 조합수", int(len(summary_df)))
            k3.metric("연도별 행수", int(len(annual_df)))
            et1, et2, et3 = st.tabs(["요약", "연도별", "거래 로그"])
            with et1:
                if summary_df.empty:
                    st.info("요약 결과가 없습니다.")
                else:
                    st.dataframe(summary_df, hide_index=True, use_container_width=True)
            with et2:
                if annual_df.empty:
                    st.info("연도별 결과가 없습니다.")
                else:
                    st.dataframe(annual_df, hide_index=True, use_container_width=True)
            with et3:
                if trades_df.empty:
                    st.info("거래 로그가 없습니다.")
                else:
                    display = _format_date_columns_for_display(trades_df, ["listing_date", "unlock_date", "entry_dt", "exit_dt", "prev_close_date"])
                    st.dataframe(display, hide_index=True, use_container_width=True)
                    render_download_button("기존 turnover trades CSV", display, "turnover_existing_filtered.csv")

    with tabs[2]:
        st.markdown("**권장 사용법**")
        st.write({
            "1": "workspace가 연결되면 unlock_out + minute DB를 자동으로 읽습니다.",
            "2": "배수와 가격 필터를 고른 뒤 신규 계산을 실행합니다.",
            "3": "KIS 키가 있으면 일봉 기준 백테스트가 더 안정적으로 계산됩니다.",
            "4": "KIS 키가 없으면 minute DB fallback으로 빠르게 검토할 수 있지만, 보유일이 긴 전략은 거래가 줄 수 있습니다.",
        })


def render_shorts_studio_page(bundle: IPODataBundle, issues: pd.DataFrame, today: pd.Timestamp, source_mode: str) -> None:
    st.subheader("쇼츠 스튜디오")
    st.caption("일정·종목·시장 요약을 바탕으로 세로형 쇼츠 스크립트와 자산을 만듭니다.")
    studio = ShortsStudioService(DATA_DIR)

    c1, c2, c3 = st.columns([1.2, 0.8, 0.8])
    title = c1.text_input("쇼츠 제목", value=f"공모주 알리미 데일리 {pd.Timestamp(today).strftime('%Y-%m-%d')}")
    window_days = c2.slider("일정 포함 기간", min_value=3, max_value=14, value=7, step=1)
    create_video = c3.checkbox("MP4도 함께 생성", value=False)

    market_bundle = load_market_snapshot_bundle_cached(source_mode == "실데이터 우선", allow_sample_fallback=True)
    market_source = market_bundle.get("source", "sample")

    if st.button("스크립트 초안 생성", use_container_width=True):
        with st.spinner("쇼츠 스크립트 초안을 만드는 중입니다..."):
            payload = studio.build_daily_payload(
                bundle,
                issues,
                today,
                window_days=window_days,
                source_label=source_mode,
                market_snapshot=market_bundle.get("frame", pd.DataFrame()),
                market_source=market_source,
            )
            script_text = studio.build_script(payload, title=title)
            st.session_state["shorts_draft_state"] = {
                "payload": payload,
                "title": title,
                "window_days": window_days,
                "market_source": market_source,
                "script": script_text,
            }
            st.session_state["shorts_script_text"] = script_text

    state = st.session_state.get("shorts_draft_state")
    if not isinstance(state, dict):
        st.info("먼저 스크립트 초안 생성을 눌러 초안을 만든 뒤, 내용을 손보고 자산을 생성하세요.")
        return

    payload = state.get("payload", {}) if isinstance(state, dict) else {}
    preview_slides = studio.build_slides(payload, title=title)
    preview_rows = pd.DataFrame(
        [
            {"scene": idx, "title": slide.title, "subtitle": slide.subtitle, "duration_sec": slide.duration_sec}
            for idx, slide in enumerate(preview_slides, start=1)
        ]
    )

    st.markdown("**씬 구성 미리보기**")
    st.dataframe(preview_rows, hide_index=True, use_container_width=True)

    script_text = st.text_area(
        "스크립트 편집",
        value=st.session_state.get("shorts_script_text", state.get("script", "")),
        key="shorts_script_text",
        height=420,
        help="Scene 단위로 문장을 고치면 captions.srt와 narration_script.txt에 반영됩니다.",
    )

    dl_cols = st.columns([0.35, 0.65])
    dl_cols[0].download_button(
        "현재 스크립트 TXT",
        data=script_text.encode("utf-8"),
        file_name="daily_shorts_script.txt",
        mime="text/plain",
        use_container_width=True,
    )
    dl_cols[1].caption(f"시장 소스: {market_source} · 편집본 기준으로 PNG/SRT/JSON/ZIP이 생성됩니다.")

    if st.button("편집본으로 쇼츠 자산 생성", use_container_width=True):
        with st.spinner("쇼츠 자산을 생성하는 중입니다..."):
            out_dir = runtime_dir() / "daily_shorts" / pd.Timestamp.now(tz="Asia/Seoul").strftime("%Y%m%d_%H%M%S")
            result = studio.generate_assets(
                payload,
                out_dir,
                title=title,
                create_video=create_video,
                create_zip=True,
                script_text=script_text,
            )
            st.session_state["shorts_studio_result"] = {
                "payload": payload,
                "result": result,
                "out_dir": str(out_dir),
                "script_text": script_text,
            }

    result_state = st.session_state.get("shorts_studio_result")
    if not isinstance(result_state, dict):
        return

    result = result_state.get("result", {}) if isinstance(result_state, dict) else {}
    manifest = result.get("manifest", pd.DataFrame()) if isinstance(result, dict) else pd.DataFrame()
    slides = result.get("slides", []) if isinstance(result, dict) else []
    zip_path = result.get("zip_path") if isinstance(result, dict) else None
    video_path = result.get("video_path") if isinstance(result, dict) else None
    captions_path = result.get("captions_path") if isinstance(result, dict) else None
    manifest_path = result.get("manifest_path") if isinstance(result, dict) else None
    script_path = result.get("script_path") if isinstance(result, dict) else None
    payload_path = result.get("payload_path") if isinstance(result, dict) else None

    m1, m2, m3, m4 = st.columns(4)
    m1.metric("생성 슬라이드", int(len(slides)))
    m2.metric("영상", "완료" if video_path else "미생성")
    m3.metric("ZIP", "완료" if zip_path else "미생성")
    m4.metric("출력 폴더", str(result_state.get("out_dir", ""))[-18:])

    if slides:
        st.markdown("**슬라이드 미리보기**")
        st.image([str(path) for path in slides[: min(5, len(slides))]], width=240)
    if not manifest.empty:
        st.markdown("**manifest**")
        st.dataframe(manifest, hide_index=True, use_container_width=True)

    dl1, dl2, dl3, dl4, dl5 = st.columns(5)
    if zip_path and Path(zip_path).exists():
        dl1.download_button("전체 자산 ZIP", data=Path(zip_path).read_bytes(), file_name=Path(zip_path).name, mime="application/zip", use_container_width=True)
    if video_path and Path(video_path).exists():
        dl2.download_button("MP4 내려받기", data=Path(video_path).read_bytes(), file_name=Path(video_path).name, mime="video/mp4", use_container_width=True)
    if captions_path and Path(captions_path).exists():
        dl3.download_button("captions.srt", data=Path(captions_path).read_bytes(), file_name=Path(captions_path).name, mime="text/plain", use_container_width=True)
    if manifest_path and Path(manifest_path).exists():
        dl4.download_button("manifest CSV", data=Path(manifest_path).read_bytes(), file_name=Path(manifest_path).name, mime="text/csv", use_container_width=True)
    if script_path and Path(script_path).exists():
        dl5.download_button("narration script", data=Path(script_path).read_bytes(), file_name=Path(script_path).name, mime="text/plain", use_container_width=True)

    extra_cols = st.columns(2)
    if payload_path and Path(payload_path).exists():
        extra_cols[0].download_button("payload JSON", data=Path(payload_path).read_bytes(), file_name=Path(payload_path).name, mime="application/json", use_container_width=True)
    extra_cols[1].download_button("편집 중 스크립트 TXT", data=script_text.encode("utf-8"), file_name="daily_shorts_script_edited.txt", mime="text/plain", use_container_width=True)




def render_data_hub_page(bundle: IPODataBundle, source_mode: str, unified_bundle: UnifiedLabBundle, unified_workspace_path: str) -> None:
    st.subheader("데이터 허브")
    st.caption("실데이터 캐시 갱신, 로컬 KIND 엑셀 업로드, 소스별 상태 점검 화면입니다.")
    repo = IPORepository(DATA_DIR)
    hub = IPODataHub(DATA_DIR, dart_client=DartClient.from_env())

    col1, col2 = st.columns(2)
    with col1:
        st.markdown("**자동 탐지 경로**")
        st.write({
            "external unlock dataset": str(repo.auto_detect_external_unlock_dataset() or ""),
            "local KIND export": str(repo.auto_detect_local_kind_export(include_home_dirs=False) or ""),
            "5분봉 workspace": unified_workspace_path or str(unified_bundle.paths.workspace or ""),
            "source mode": source_mode,
        })
        if st.button("KIND / 38 캐시 새로고침", use_container_width=True):
            with st.spinner("공식/보조 소스를 갱신하는 중입니다..."):
                report = hub.refresh_live_cache(fetch_kind=True, fetch_38=True)
                st.session_state["refresh_report"] = report
                load_bundle_cached.clear()
                load_market_snapshot_bundle_cached.clear()
                load_market_history_bundle_cached.clear()
            st.rerun()
        if "refresh_report" in st.session_state:
            st.write(st.session_state["refresh_report"])
    with col2:
        st.markdown("**KIND 로컬 파일 업로드**")
        uploaded = st.file_uploader("신규상장기업현황 / 공모가대비주가정보 엑셀·CSV 업로드", type=["xlsx", "xls", "csv", "html", "htm"])
        if uploaded is not None:
            if st.button("업로드 파일 저장 및 반영", use_container_width=True):
                saved_path = hub.write_uploaded_kind_file(uploaded.getvalue(), uploaded.name)
                st.success(f"저장 완료: {saved_path}")
                load_bundle_cached.clear()
                st.rerun()
        if DartClient.from_env() is not None and st.button("DART corp code 캐시 갱신", use_container_width=True):
            with st.spinner("DART corp code를 갱신하는 중입니다..."):
                dart = DartClient.from_env()
                assert dart is not None
                table = dart.download_corp_codes(base_dir=DATA_DIR / "cache", force=True)
                st.success(f"갱신 완료: {len(table)} rows")

    st.markdown("**소스 상태**")
    if bundle.source_status.empty:
        st.info("현재 표시할 소스 상태가 없습니다.")
    else:
        st.dataframe(bundle.source_status, hide_index=True, use_container_width=True)

    st.markdown("**5분봉 / Unified Lab 연결 상태**")
    if unified_bundle.source_status.empty:
        st.info("연결된 Unified Lab source 상태가 없습니다.")
    else:
        st.dataframe(unified_bundle.source_status, hide_index=True, use_container_width=True)

    st.markdown("**DART IPO 배치 추출**")
    if DartClient.from_env() is None:
        st.info("DART_API_KEY를 넣으면 상장예정/상장후 종목에 대해 공시 원문 배치 추출을 수행할 수 있습니다.")
    else:
        target_issues = add_issue_scores(bundle.issues)
        target_issues = target_issues[target_issues["stage"].isin(["상장예정", "상장후", "청약완료", "청약중"])].copy()
        if target_issues.empty:
            st.info("배치 추출 대상 종목이 없습니다.")
        else:
            c1, c2, c3 = st.columns(3)
            max_default = max(1, min(10, len(target_issues)))
            max_items = c1.slider("최대 추출 종목수", min_value=1, max_value=max(1, min(30, len(target_issues))), value=max_default, step=1)
            only_missing = c2.checkbox("값 비어있는 종목 우선", value=True)
            force = c3.checkbox("캐시 무시", value=False)
            if st.button("DART IPO 지표 배치 추출", use_container_width=True):
                with st.spinner("선택 종목들의 증권신고서/투자설명서를 분석하는 중입니다..."):
                    if force:
                        load_dart_ipo_snapshot_cached.clear()
                    result = hub.batch_enrich_issues_from_dart(
                        target_issues,
                        max_items=max_items,
                        only_missing=only_missing,
                        force=force,
                    )
                    st.session_state["dart_batch_result"] = result
            if "dart_batch_result" in st.session_state:
                result = st.session_state["dart_batch_result"]
                if result is None or result.empty:
                    st.info("배치 추출 결과가 없습니다.")
                else:
                    preview_cols = [
                        c for c in [
                            "name",
                            "symbol",
                            "listing_date",
                            "lockup_commitment_ratio",
                            "circulating_shares_ratio_on_listing",
                            "existing_shareholder_ratio",
                            "employee_forfeit_ratio",
                            "secondary_sale_ratio",
                            "total_offer_shares",
                            "post_listing_total_shares",
                            "dart_receipt_no",
                            "status",
                            "detail",
                            "dart_summary",
                        ] if c in result.columns
                    ]
                    preview = result[preview_cols].copy()
                    if "listing_date" in preview.columns:
                        preview["listing_date"] = pd.to_datetime(preview["listing_date"], errors="coerce").dt.strftime("%Y-%m-%d")
                    st.dataframe(preview, hide_index=True, use_container_width=True)
                    render_download_button("DART 배치 추출 CSV 내려받기", result, "dart_batch_enriched.csv")
                    if st.button("배치 추출 결과를 앱 데이터로 저장", use_container_width=True):
                        save_path = DATA_DIR / "uploads" / "dart_enriched_latest.csv"
                        standardize_issue_frame(result).to_csv(save_path, index=False, encoding="utf-8-sig")
                        st.success(f"저장 완료: {save_path}")
                        load_bundle_cached.clear()
                        st.rerun()

    st.markdown("**캐시 인벤토리**")
    if bundle.cache_inventory.empty:
        st.info("캐시 파일이 없습니다.")
    else:
        st.dataframe(bundle.cache_inventory, hide_index=True, use_container_width=True)

    table_names = sorted(bundle.raw_tables.keys())
    if table_names:
        selected = st.selectbox("미리보기 테이블", options=table_names)
        df = bundle.raw_tables[selected]
        if df.empty:
            st.info("선택한 테이블이 비어 있습니다.")
        else:
            st.dataframe(df.head(200), hide_index=True, use_container_width=True)


def render_settings_page(source_mode: str, prefer_live: bool, external_unlock_path: str, local_kind_export_path: str, unified_workspace_path: str, unified_bundle: UnifiedLabBundle) -> None:
    st.subheader("설정 / 소스 연결 / 실행 준비")
    kis_client = KISClient.from_env()
    dart_client = DartClient.from_env()
    env_file = detect_project_env_file(APP_ROOT)

    status_badge("샘플 IPO 데이터", True, "앱에 기본 포함")
    status_badge("외부 unlock dataset", bool(external_unlock_path), external_unlock_path or "")
    status_badge("로컬 KIND export", bool(local_kind_export_path), local_kind_export_path or "")
    status_badge("5분봉 / Unified Lab workspace", unified_bundle.paths.workspace is not None, unified_workspace_path or str(unified_bundle.paths.workspace or ""))
    status_badge("KIS API", kis_client is not None, mask_secret(os.getenv("KIS_APP_KEY", "")) or "KIS_APP_KEY / KIS_APP_SECRET")
    status_badge("DART API", dart_client is not None, mask_secret(os.getenv("DART_API_KEY", "")) or "DART_API_KEY")
    status_badge(".env 자동로드", env_file is not None, str(env_file or "프로젝트 루트에 .env 또는 .env.local 배치"))
    status_badge("실데이터 우선", prefer_live, source_mode)

    st.markdown("---")
    c1, c2 = st.columns([1.05, 0.95])
    with c1:
        st.markdown("**권장 환경변수 형식**")
        st.code(
            "KIS_APP_KEY=...\n"
            "KIS_APP_SECRET=...\n"
            "KIS_ENV=real\n"
            "DART_API_KEY=..."
        )
        st.markdown(
            "운영 권장 흐름은 **KIND 엑셀/CSV 다운로드 → 앱 데이터 허브 업로드 → 38/KIND 캐시 갱신 → unlock 전략 데이터 연결 → 5분봉 workspace 연결 → prepare_local_test 실행 → 앱 확인** 순서입니다."
        )
        st.markdown("**권장 커맨드**")
        st.code(
            "python scripts/preflight_check.py\n"
            "python scripts/prepare_local_test.py --workspace data/sample_unified_lab_workspace\n"
            "streamlit run app.py"
        )
    with c2:
        st.markdown("**자동 탐지 상태**")
        st.write({
            ".env": str(env_file or ""),
            "external unlock": external_unlock_path,
            "local KIND export": local_kind_export_path,
            "unified workspace": str(unified_bundle.paths.workspace or ""),
            "runtime dir": str(runtime_dir()),
        })

    preflight = load_latest_preflight_report()
    st.markdown("**최근 preflight 결과**")
    if preflight is None:
        st.info("아직 preflight report가 없습니다. `python scripts/preflight_check.py`를 먼저 실행해 보세요.")
    else:
        s1, s2, s3 = st.columns(3)
        s1.metric("생성시각", str(preflight.get("generated_at") or "-"))
        s2.metric("critical_failures", int(preflight.get("critical_failures", 0) or 0))
        s3.metric("warnings", int(preflight.get("warnings", 0) or 0))
        checks = pd.DataFrame(preflight.get("checks", []))
        if not checks.empty:
            checks = checks.rename(columns={"name": "항목", "ok": "정상", "severity": "등급", "detail": "상세"})
            st.dataframe(checks, hide_index=True, use_container_width=True)

    st.markdown("**최근 export / runtime 산출물**")
    export_rows = list_artifact_rows(DATA_DIR / "exports", ["*.csv", "*.json"], limit=12)
    runtime_rows = list_artifact_rows(runtime_dir(), ["*.csv", "*.json"], limit=12)
    left, right = st.columns(2)
    with left:
        st.caption("data/exports")
        if export_rows.empty:
            st.info("아직 export 산출물이 없습니다.")
        else:
            st.dataframe(export_rows.head(12), hide_index=True, use_container_width=True)
    with right:
        st.caption("data/runtime")
        if runtime_rows.empty:
            st.info("아직 runtime 산출물이 없습니다.")
        else:
            st.dataframe(runtime_rows.head(12), hide_index=True, use_container_width=True)



def render_lab_page(
    bundle: IPODataBundle,
    issues: pd.DataFrame,
    today: pd.Timestamp,
    version: str,
    prefer_live: bool,
    unified_bundle: UnifiedLabBundle,
    unified_workspace_path: str,
    allow_packaged_sample: bool,
    source_mode: str,
) -> None:
    if not render_experimental_lab_gate():
        return
    st.subheader("실험실")
    if getattr(unified_bundle.paths, "workspace", None) is not None:
        workspace_label = str(unified_bundle.paths.workspace)
        if "sample_unified_lab_workspace" in workspace_label:
            st.caption("전략 연구실은 내장 데모 workspace를 기준으로 열었습니다.")
    if not unified_bundle.source_status.empty and "ok" in unified_bundle.source_status.columns:
        failed = unified_bundle.source_status[~unified_bundle.source_status["ok"].fillna(False)]
        hard_fail = failed[~failed["source"].astype(str).isin(["turnover summary", "turnover trades", "turnover skip summary", "beta summary", "minute bar stats"])]
        if not hard_fail.empty:
            first_detail = str(hard_fail.iloc[0].get("detail") or "")
            st.warning(f"Unified Lab 핵심 입력 일부가 비어 있습니다. {first_detail}")
    tabs = st.tabs(["전략 연구실", "백테스트", "쇼츠 스튜디오"])
    with tabs[0]:
        inner_tabs = st.tabs(["락업 실행보드", "5분봉 브리지", "전략 브릿지", "턴오버 전략"])
        with inner_tabs[0]:
            render_lockup_strategy_page(bundle, issues, today, version, prefer_live, unified_bundle)
        with inner_tabs[1]:
            render_minute_bridge_page(bundle, issues, today, version, unified_bundle)
        with inner_tabs[2]:
            render_strategy_bridge_page(bundle, issues, today, version)
        with inner_tabs[3]:
            active_workspace_path = str(unified_bundle.paths.workspace or unified_workspace_path or "")
            render_turnover_research_page(unified_bundle, active_workspace_path, today, allow_packaged_sample)
    with tabs[1]:
        render_backtest_page(issues, today)
    with tabs[2]:
        render_shorts_studio_page(bundle, issues, today, source_mode)



def render_data_admin_page(
    bundle: IPODataBundle,
    source_mode: str,
    prefer_live: bool,
    allow_sample_fallback: bool,
    unified_bundle: UnifiedLabBundle,
    unified_workspace_path: str,
    external_unlock_path: str,
    local_kind_export_path: str,
) -> None:
    st.subheader("데이터 / 설정")
    tabs = st.tabs(["시장", "데이터 허브", "설정"])
    with tabs[0]:
        render_market_page(prefer_live, allow_sample_fallback, source_mode)
    with tabs[1]:
        render_data_hub_page(bundle, source_mode, unified_bundle, unified_workspace_path)
    with tabs[2]:
        render_settings_page(source_mode, prefer_live, external_unlock_path, local_kind_export_path, unified_workspace_path, unified_bundle)


def main() -> None:
    st.title("공모주 알리미")

    repo = IPORepository(DATA_DIR)
    unified_service = UnifiedLabBridgeService(DATA_DIR)
    sidebar = st.sidebar
    sidebar.header("앱 설정")
    page = sidebar.radio(
        "메뉴",
        ["대시보드", "딜 탐색기", "청약", "상장", "보호예수", "실험실", "데이터 / 설정"],
    )
    default_source_mode = str(os.getenv("DEFAULT_SOURCE_MODE", "실데이터 우선")).strip() or "실데이터 우선"
    if default_source_mode not in SOURCE_MODE_OPTIONS:
        default_source_mode = "실데이터 우선"
    source_mode = sidebar.selectbox("데이터 모드", SOURCE_MODE_OPTIONS, index=SOURCE_MODE_OPTIONS.index(default_source_mode))
    sidebar.caption("기본값은 실데이터 우선입니다. 실시간 조회에 성공하면 저장본 캐시도 같이 최신화합니다.")
    prefer_live = source_mode == "실데이터 우선"
    allow_sample_fallback = source_mode == "샘플만"
    if sidebar.button("데이터 다시 읽기", use_container_width=True):
        load_bundle_cached.clear()
        load_market_snapshot_bundle_cached.clear()
        load_market_history_bundle_cached.clear()
        load_unified_lab_bundle_cached.clear()
        load_kis_signal_cached.clear()
        st.rerun()
    allow_packaged_sample = sidebar.checkbox(
        "내장 데모 workspace 자동연결",
        value=source_mode == "샘플만",
        help="앱에 포함된 demo unlock/5분봉 workspace만 연결합니다. 통합 프로젝트의 integrated_lab/ipo_lockup_unified_lab/workspace 자동탐지는 이 체크와 무관하게 항상 시도합니다.",
    )
    default_external = repo.auto_detect_external_unlock_dataset(allow_packaged_sample=allow_packaged_sample)
    default_kind = repo.auto_detect_local_kind_export(include_home_dirs=False)
    default_unified_workspace = unified_service.auto_detect_workspace(allow_packaged_sample=allow_packaged_sample)

    external_unlock_path = sidebar.text_input(
        "외부 unlock dataset 경로",
        value=str(default_external) if default_external else "",
        help="synthetic_ipo_events.csv 또는 unlock_events_backtest_input.csv 경로를 넣으면 전략용 보호예수 해제 데이터가 붙습니다.",
    )
    local_kind_export_path = sidebar.text_input(
        "로컬 KIND export 경로",
        value=str(default_kind) if default_kind else "",
        help="신규상장기업현황 또는 공모가대비주가정보 엑셀/CSV 경로입니다.",
    )
    unified_workspace_path = sidebar.text_input(
        "5분봉 lab workspace 경로",
        value=str(default_unified_workspace) if default_unified_workspace else "",
        help="unlock_out / signal_out / turnover_backtest_out / dataset_out / data/curated/lockup_minute.db 를 포함한 workspace 경로입니다.",
    )

    resolved_paths = unified_service.resolve_paths(unified_workspace_path or None, allow_packaged_sample=allow_packaged_sample)
    resolved_external_unlock_path = external_unlock_path or str(resolved_paths.unlock_csv or "")
    resolved_unified_workspace_path = unified_workspace_path or str(resolved_paths.workspace or "")

    today = pd.Timestamp(sidebar.date_input("기준일", value=pd.Timestamp.now(tz="Asia/Seoul").date()))
    backtest_versions = BacktestRepository(DATA_DIR).available_versions()
    backtest_version = backtest_versions[0]
    if page == "실험실":
        backtest_version = sidebar.selectbox("락업 전략 기준 버전", options=backtest_versions, index=0)
        configured_lab_password = experimental_lab_password()
        if configured_lab_password and st.session_state.get("lab_unlocked"):
            sidebar.success("실험실 잠금 해제됨")
        elif configured_lab_password:
            sidebar.warning("실험실 잠금 사용 중")
        else:
            sidebar.caption("실험실 비밀번호 미설정")

    unified_bundle = empty_unified_bundle()
    if page in PAGES_REQUIRING_UNIFIED:
        unified_bundle = load_unified_lab_bundle_cached(resolved_unified_workspace_path, allow_packaged_sample=allow_packaged_sample)

    bundle: IPODataBundle | None = None
    issues = pd.DataFrame()
    if page in PAGES_REQUIRING_BUNDLE:
        bundle = load_bundle_cached(
            source_mode,
            resolved_external_unlock_path,
            local_kind_export_path,
            allow_sample_fallback,
            allow_packaged_sample,
        )
        issues = add_issue_scores(bundle.issues)

    if page == "대시보드":
        assert bundle is not None
        render_dashboard(bundle, today, prefer_live, allow_sample_fallback, backtest_version, source_mode)
    elif page == "딜 탐색기":
        assert bundle is not None
        render_explorer(bundle, prefer_live)
    elif page == "청약":
        render_subscription_page(issues, today)
    elif page == "상장":
        render_listing_page(issues, prefer_live, today)
    elif page == "보호예수":
        assert bundle is not None
        render_unlock_page(issues, bundle.all_unlocks, today)
    elif page == "실험실":
        assert bundle is not None
        render_lab_page(bundle, issues, today, backtest_version, prefer_live, unified_bundle, resolved_unified_workspace_path, allow_packaged_sample, source_mode)
    else:
        assert bundle is not None
        render_data_admin_page(bundle, source_mode, prefer_live, allow_sample_fallback, unified_bundle, resolved_unified_workspace_path, resolved_external_unlock_path, local_kind_export_path)


if __name__ == "__main__":
    main()
