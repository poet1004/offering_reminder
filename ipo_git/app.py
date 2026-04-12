from __future__ import annotations

import calendar
import json
import os
import re
from html import escape
from pathlib import Path
from typing import Any

import pandas as pd
import streamlit as st
import streamlit.components.v1 as components

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
from src.services.public_quote_service import PublicQuoteService
from src.services.ipo_scrapers import fetch_38_demand_results, fetch_38_ir_links, fetch_38_new_listing_table, fetch_38_schedule, fetch_kind_corp_download_table, fetch_kind_listing_table, fetch_kind_public_offering_table, fetch_kind_pubprice_table, fetch_seibro_release_schedule, load_kind_export_from_path, standardize_38_new_listing_table, standardize_38_schedule_table, standardize_kind_listing_table, standardize_kind_public_offering_table, standardize_kind_pubprice_table
from src.services.shorts_service import ShortsStudioService
from src.services.scoring import IPOScorer
from src.services.strategy_bridge import StrategyBridge
from src.services.turnover_strategy_service import TurnoverStrategyParams, TurnoverStrategyService
from src.services.unified_lab_bridge import UnifiedLabBridgeService, UnifiedLabBundle, UnifiedLabPaths
from src.utils import detect_project_env_file, fmt_date, fmt_num, fmt_pct, fmt_ratio, fmt_won, humanize_source, issue_recency_sort, load_project_env, mask_secret, normalize_name_key, normalize_symbol_text, runtime_dir, safe_float, standardize_issue_frame, to_csv_bytes, today_kst


APP_ROOT = Path(__file__).resolve().parent
DATA_DIR = APP_ROOT / "data"
CACHE_REV = "20260412_v32_official_api_cache_first"
DEFAULT_DART_API_KEY = "d6023038ffd78ee5d4ad800d7d3811663ff3a18b"

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
        "PUBLIC_DATA_SERVICE_KEY",
        "KSD_PUBLIC_DATA_SERVICE_KEY",
        "DATA_GO_SERVICE_KEY",
        "SEIBRO_SERVICE_KEY",
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
if not os.environ.get("DART_API_KEY"):
    os.environ["DART_API_KEY"] = DEFAULT_DART_API_KEY


def make_streamlit_arrow_safe(df: pd.DataFrame) -> pd.DataFrame:
    if not isinstance(df, pd.DataFrame):
        return df
    work = df.copy()
    code_tokens = ("종목코드", "symbol", "ticker", "티커", "접수번호", "receipt")

    def _stringify(value: Any) -> Any:
        try:
            if pd.isna(value):
                return None
        except Exception:
            pass
        if isinstance(value, dict):
            return json.dumps(value, ensure_ascii=False)
        if isinstance(value, (list, tuple, set)):
            return ", ".join(str(item) for item in value)
        text_value = str(value).strip()
        return None if text_value.lower() in {"nan", "nat", "none", "<na>"} else text_value

    for col in work.columns:
        series = work[col]
        label = str(col)
        if pd.api.types.is_datetime64_any_dtype(series):
            continue
        if any(token.lower() in label.lower() for token in code_tokens):
            work[col] = series.map(_stringify)
            continue
        if str(series.dtype) == "object":
            sample = series.dropna().head(200).tolist()
            if not sample:
                continue
            has_nested = any(isinstance(value, (list, tuple, set, dict)) for value in sample)
            mixed_types = len({type(value).__name__ for value in sample}) > 1
            if has_nested or mixed_types:
                work[col] = series.map(_stringify)
    return work


if not getattr(st, "_ipo_arrow_safe_patched", False):
    _original_streamlit_dataframe = st.dataframe

    def _arrow_safe_dataframe(data: Any = None, *args: Any, **kwargs: Any):
        if isinstance(data, pd.DataFrame):
            data = make_streamlit_arrow_safe(data)
        return _original_streamlit_dataframe(data, *args, **kwargs)

    st.dataframe = _arrow_safe_dataframe
    setattr(st, "_ipo_arrow_safe_patched", True)


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


def compact_date_text(value: Any, default: str = "-") -> str:
    ts = pd.to_datetime(value, errors="coerce")
    if pd.isna(ts):
        return default
    return pd.Timestamp(ts).strftime("%y.%m.%d")


def compact_date_range_text(start_value: Any, end_value: Any, default: str = "-") -> str:
    start_ts = pd.to_datetime(start_value, errors="coerce")
    end_ts = pd.to_datetime(end_value, errors="coerce")
    if pd.isna(start_ts) and pd.isna(end_ts):
        return default
    if pd.isna(start_ts):
        return compact_date_text(end_ts, default=default)
    if pd.isna(end_ts):
        return compact_date_text(start_ts, default=default)
    start_ts = pd.Timestamp(start_ts)
    end_ts = pd.Timestamp(end_ts)
    start_fmt = start_ts.strftime("%y.%m.%d")
    if start_ts.year == end_ts.year and start_ts.month == end_ts.month:
        end_fmt = end_ts.strftime("%d")
    elif start_ts.year == end_ts.year:
        end_fmt = end_ts.strftime("%m.%d")
    else:
        end_fmt = end_ts.strftime("%y.%m.%d")
    return f"{start_fmt}~{end_fmt}"


def compact_date_text_short(value: Any, default: str = "-") -> str:
    ts = pd.to_datetime(value, errors="coerce")
    if pd.isna(ts):
        return default
    return pd.Timestamp(ts).strftime("%m.%d")


def compact_date_range_text_short(start_value: Any, end_value: Any, default: str = "-") -> str:
    start_ts = pd.to_datetime(start_value, errors="coerce")
    end_ts = pd.to_datetime(end_value, errors="coerce")
    if pd.isna(start_ts) and pd.isna(end_ts):
        return default
    if pd.isna(start_ts):
        return compact_date_text_short(end_ts, default=default)
    if pd.isna(end_ts):
        return compact_date_text_short(start_ts, default=default)
    start_ts = pd.Timestamp(start_ts)
    end_ts = pd.Timestamp(end_ts)
    start_fmt = start_ts.strftime("%m.%d")
    end_fmt = end_ts.strftime("%d") if start_ts.month == end_ts.month else end_ts.strftime("%m.%d")
    return f"{start_fmt}~{end_fmt}"


def compact_price_band_text(low: Any, high: Any, default: str = "-") -> str:
    low_v = safe_float(low)
    high_v = safe_float(high)
    if low_v is None and high_v is None:
        return default
    if low_v is None:
        return fmt_num(high_v, 0)
    if high_v is None:
        return fmt_num(low_v, 0)
    if float(low_v) == float(high_v):
        return fmt_num(low_v, 0)
    return f"{fmt_num(low_v, 0)}~{fmt_num(high_v, 0)}"


def compact_offer_text(value: Any, default: str = "-") -> str:
    number = safe_float(value)
    return default if number is None else fmt_num(number, 0)


def compact_ratio_text(value: Any, digits: int = 1, signed: bool = False, default: str = "-") -> str:
    number = safe_float(value)
    if number is None:
        return default
    return fmt_pct(number, digits=digits, signed=signed)


def compact_datetime_text(value: Any, default: str = "-") -> str:
    ts = pd.to_datetime(value, errors="coerce")
    if pd.isna(ts):
        return default
    ts = pd.Timestamp(ts)
    if ts.hour == 0 and ts.minute == 0 and ts.second == 0:
        return ts.strftime("%y.%m.%d")
    return ts.strftime("%y.%m.%d %H:%M")


def market_asof_summary(snapshot: pd.DataFrame) -> str:
    if not isinstance(snapshot, pd.DataFrame) or snapshot.empty or "asof" not in snapshot.columns:
        return "-"
    work = snapshot.copy()
    work["asof"] = pd.to_datetime(work["asof"], errors="coerce")
    work = work.dropna(subset=["asof"])
    if work.empty:
        return "-"
    group_series = work["group"].astype(str) if "group" in work.columns else pd.Series([""] * len(work), index=work.index, dtype="object")
    domestic = work[group_series == "국내지수"]
    other = work[group_series != "국내지수"]
    parts: list[str] = []
    if not domestic.empty:
        parts.append(f"국내지수 기준 {compact_datetime_text(domestic['asof'].max())}")
    if not other.empty:
        parts.append(f"기타 자산 기준 {compact_datetime_text(other['asof'].max())}")
    return " · ".join(parts) if parts else "-"


def render_scrollable_table(df: pd.DataFrame, key: str) -> None:
    if df.empty:
        st.info("표시할 데이터가 없습니다.")
        return

    def _column_role(label: str) -> str:
        text = str(label or "").strip()
        compact = re.sub(r"[^0-9A-Za-z가-힣]+", "", text).lower()
        if text in {"주관사", "상세", "비고", "note"} or "underwriter" in compact:
            return "wrap"
        if text in {"종목명", "회사명", "name"}:
            return "name"
        if text in {"공모가", "현재가", "희망가", "예상 해제주식수", "실제 해제주식수", "예수잔량"} or "price" in compact:
            return "price"
        if text.endswith("일") or text in {"날짜", "청약", "수요예측", "상장일", "해제일"} or "date" in compact:
            return "date"
        if text in {"시장", "단계", "IR", "점수", "확약", "유통", "기존주주", "출처", "term", "이벤트", "압력점수", "예상 비중", "전체주식대비"}:
            return "short"
        return "default"

    work = df.copy().fillna("-")
    safe_key = re.sub(r"[^a-zA-Z0-9_-]+", "-", str(key))
    table_class = f"ipo-table-{safe_key}"

    columns = [str(col) for col in work.columns]
    column_roles = {col: _column_role(col) for col in columns}
    header_html = "".join(f'<th class="cell-{column_roles[col]}">{escape(col)}</th>' for col in columns)
    body_rows: list[str] = []
    for _, row in work.iterrows():
        cells: list[str] = []
        for col in columns:
            value = row.get(col)
            text_cell = "-" if not has_value(value) else str(value)
            role = column_roles[col]
            cells.append(f'<td class="cell-{role}" title="{escape(text_cell)}">{escape(text_cell)}</td>')
        body_rows.append("<tr>" + "".join(cells) + "</tr>")

    html = f"""
    <html>
      <head>
        <meta charset="utf-8" />
        <style>
          body {{ margin: 0; padding: 0; background: transparent; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; }}
          .ipo-table-wrap {{ width: 100%; overflow-x: auto; border: 1px solid rgba(49, 51, 63, 0.18); border-radius: 0.5rem; }}
          table.{table_class} {{ border-collapse: collapse; width: max-content; min-width: 100%; table-layout: auto; font-size: 10.6px; line-height: 1.24; }}
          table.{table_class} thead th {{ position: sticky; top: 0; background: #fafafa; z-index: 2; }}
          table.{table_class} th, table.{table_class} td {{ padding: 5px 6px; border-bottom: 1px solid rgba(49, 51, 63, 0.08); white-space: nowrap; overflow: visible; text-overflow: clip; text-align: left; vertical-align: top; word-break: keep-all; }}
          table.{table_class} th.cell-wrap, table.{table_class} td.cell-wrap {{ white-space: normal; min-width: 108px; max-width: 148px; word-break: keep-all; }}
          table.{table_class} th.cell-name, table.{table_class} td.cell-name {{ white-space: normal; min-width: 102px; max-width: 136px; }}
          table.{table_class} th.cell-price, table.{table_class} td.cell-price {{ min-width: 82px; }}
          table.{table_class} th.cell-date, table.{table_class} td.cell-date {{ min-width: 82px; }}
          table.{table_class} th.cell-short, table.{table_class} td.cell-short {{ min-width: 66px; }}
          table.{table_class} tbody tr:nth-child(even) {{ background: rgba(250, 250, 250, 0.55); }}
        </style>
      </head>
      <body>
        <div class="ipo-table-wrap">
          <table class="{table_class}">
            <thead><tr>{header_html}</tr></thead>
            <tbody>{''.join(body_rows)}</tbody>
          </table>
        </div>
      </body>
    </html>
    """
    height = min(max(220, 54 + 34 * min(len(work), 12)), 540)
    components.html(html, height=height, scrolling=True)


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
        "institutional_competition_ratio",
        "lockup_commitment_ratio",
        "ir_pdf_url",
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

    def match_by_name(frame: pd.DataFrame) -> pd.DataFrame:
        if frame is None or frame.empty or not corp_name:
            return pd.DataFrame()
        work = frame.copy()
        name_col = next((c for c in ["name", "기업명", "종목명", "회사명"] if c in work.columns), None)
        if name_col is None:
            return pd.DataFrame()
        work["name_key"] = work[name_col].map(normalize_name_key)
        target_key = normalize_name_key(corp_name)
        subset = work[work["name_key"] == target_key].copy()
        if subset.empty:
            compact_target = target_key.replace("구", "")
            mask = work["name_key"].astype(str).str.contains(target_key, na=False)
            if compact_target and compact_target != target_key:
                mask = mask | work["name_key"].astype(str).str.contains(compact_target, na=False)
            subset = work.loc[mask].copy()
        return subset

    if corp_name:
        allow_inline_fetch = os.getenv("IPO_ALLOW_INLINE_DETAIL_FETCH", "0").strip() == "1"
        try:
            support = load_issue_support_tables_cached()
        except Exception:
            support = {}

        for table_name, fields in [
            ("schedule", ["market", "symbol", "sector", "underwriters", "subscription_start", "subscription_end", "listing_date", "price_band_low", "price_band_high", "offer_price", "institutional_competition_ratio", "retail_competition_ratio_live", "lockup_commitment_ratio"]),
            ("new_listing", ["listing_date", "offer_price", "current_price", "day_change_pct", "market", "symbol"]),
            ("seed_38", ["listing_date", "offer_price", "market", "symbol"]),
            ("demand", ["underwriters", "forecast_date", "price_band_low", "price_band_high", "offer_price", "institutional_competition_ratio", "lockup_commitment_ratio"]),
            ("ir", ["ir_title", "ir_date", "ir_pdf_url", "ir_source_page"]),
            ("kind_corp", ["symbol", "market", "sector", "listing_date"]),
            ("local_master", ["symbol", "market", "sector", "underwriters", "listing_date", "offer_price"]),
            ("kind_listing", ["symbol", "market", "underwriters", "listing_date", "offer_price"]),
            ("kind_public", ["symbol", "market", "underwriters", "subscription_start", "subscription_end", "listing_date", "offer_price"]),
            ("kind_pubprice", ["current_price", "day_change_pct", "offer_price", "listing_date"]),
        ]:
            frame = support.get(table_name, pd.DataFrame()) if isinstance(support, dict) else pd.DataFrame()
            subset = match_by_name(frame)
            if subset.empty:
                continue
            first = subset.sort_values([c for c in ["forecast_date", "listing_date", "ir_date", "subscription_start", "name_key"] if c in subset.columns], ascending=[False, False, False, False, True], na_position="last").iloc[0].to_dict()
            for key in fields:
                value = first.get(key)
                if has_value(value):
                    overlay.setdefault(key, value)

        if allow_inline_fetch:
            try:
                raw_38 = fetch_38_schedule(timeout=6, include_detail_links=True)
                subset = match_by_name(raw_38)
                if not subset.empty:
                    detail_df = standardize_38_schedule_table(subset.drop(columns=["name_key"], errors="ignore"), fetch_details=True)
                    if not detail_df.empty:
                        first = detail_df.iloc[0].to_dict()
                        for key, value in first.items():
                            if has_value(value):
                                overlay.setdefault(key, value)
            except Exception:
                pass

            try:
                new_listing_df = standardize_38_new_listing_table(fetch_38_new_listing_table(timeout=6, max_pages=40))
                new_listing_subset = match_by_name(new_listing_df)
                if not new_listing_subset.empty:
                    first = new_listing_subset.sort_values([c for c in ["listing_date", "name_key"] if c in new_listing_subset.columns], ascending=[False, True], na_position="last").iloc[0].to_dict()
                    for key in ["listing_date", "offer_price", "current_price", "day_change_pct"]:
                        value = first.get(key)
                        if has_value(value):
                            overlay.setdefault(key, value)
            except Exception:
                pass

            try:
                seed_df = standardize_issue_frame(IPORepository(DATA_DIR).load_38_seed_export())
                seed_subset = match_by_name(seed_df)
                if not seed_subset.empty:
                    first = seed_subset.sort_values([c for c in ["listing_date", "name_key"] if c in seed_subset.columns], ascending=[False, True], na_position="last").iloc[0].to_dict()
                    for key in ["listing_date", "offer_price"]:
                        value = first.get(key)
                        if has_value(value):
                            overlay.setdefault(key, value)
            except Exception:
                pass

            try:
                demand_df = fetch_38_demand_results(timeout=6, max_pages=50)
                demand_subset = match_by_name(demand_df)
                if not demand_subset.empty:
                    first = demand_subset.sort_values([c for c in ["forecast_date", "name_key"] if c in demand_subset.columns], ascending=[False, True], na_position="last").iloc[0].to_dict()
                    for key in [
                        "underwriters",
                        "forecast_date",
                        "price_band_low",
                        "price_band_high",
                        "offer_price",
                        "institutional_competition_ratio",
                        "lockup_commitment_ratio",
                    ]:
                        value = first.get(key)
                        if has_value(value):
                            overlay.setdefault(key, value)
            except Exception:
                pass

            try:
                ir_df = fetch_38_ir_links(timeout=6, max_pages=40)
                ir_subset = match_by_name(ir_df)
                if not ir_subset.empty:
                    first = ir_subset.sort_values([c for c in ["ir_date", "name_key"] if c in ir_subset.columns], ascending=[False, True], na_position="last").iloc[0].to_dict()
                    for key in ["ir_title", "ir_date", "ir_pdf_url", "ir_source_page"]:
                        value = first.get(key)
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
    row = prefill_issue_frame_for_display(pd.DataFrame([issue])).iloc[0].copy()
    if not issue_needs_enrichment(row):
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
                "institutional_competition_ratio",
                "lockup_commitment_ratio",
                "forecast_date",
                "ir_title",
                "ir_date",
                "ir_pdf_url",
                "ir_source_page",
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

    top_cards = [
        {"title": "DART 연결", "value": "연결됨" if has_value(issue.get("dart_viewer_url")) else "미연결", "sub": fmt_date(issue.get("dart_filing_date")), "tone": "good" if has_value(issue.get("dart_viewer_url")) else "neutral"},
        {"title": "접수번호", "value": safe_text(issue.get("dart_receipt_no")), "sub": "DART 접수번호", "tone": "neutral"},
    ]
    render_soft_cards(top_cards, columns=2)
    if has_value(issue.get("dart_viewer_url")):
        link_button_compat("DART 보고서 열기", issue.get("dart_viewer_url"))
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




def render_issue_resource_links(issue: pd.Series, *, show_header: bool = True) -> None:
    lines: list[str] = []
    ir_pdf_url = text_value(issue.get("ir_pdf_url"), "").strip()
    ir_title = text_value(issue.get("ir_title"), "IR 자료 PDF")
    ir_date = compact_date_text(issue.get("ir_date"), default="")
    if ir_pdf_url:
        suffix: list[str] = []
        if ir_title and ir_title != "-":
            suffix.append(ir_title)
        if ir_date and ir_date != "-":
            suffix.append(ir_date)
        lines.append(f"- [IR PDF 열기]({ir_pdf_url})" + (f" · {' / '.join(suffix)}" if suffix else ""))
    ir_source_page = text_value(issue.get("ir_source_page"), "").strip()
    if ir_source_page:
        lines.append(f"- [IR 자료실]({ir_source_page})")
    if should_show_kind_link(issue.get("kind_url")):
        lines.append(f"- [KIND 문서 링크]({text_value(issue.get('kind_url'))})")
    if should_show_kind_link(issue.get("ir_url")):
        lines.append(f"- [KIND IR 문서]({text_value(issue.get('ir_url'))})")
    if show_header and has_value(issue.get("dart_viewer_url")):
        label = text_value(issue.get("dart_report_nm"), "DART 보고서")
        lines.append(f"- [{label}]({text_value(issue.get('dart_viewer_url'))})")
    if not lines:
        return
    if show_header:
        st.markdown("**문서 링크**")
    for line in lines:
        st.markdown(line)




def inject_global_styles() -> None:
    st.markdown(
        """
        <style>
        div[data-testid="stMetric"] {
            background: rgba(248, 250, 252, 0.96);
            border: 1px solid rgba(148, 163, 184, 0.22);
            padding: 12px 14px;
            border-radius: 16px;
        }
        div[data-testid="stMetricLabel"] {
            font-weight: 600;
        }
        .ipo-hero {
            padding: 18px 20px;
            border-radius: 22px;
            background: linear-gradient(135deg, rgba(239, 246, 255, 0.96) 0%, rgba(248, 250, 252, 0.98) 52%, rgba(240, 253, 250, 0.96) 100%);
            border: 1px solid rgba(148, 163, 184, 0.18);
            box-shadow: 0 10px 32px rgba(15, 23, 42, 0.05);
            margin: 0 0 14px 0;
        }
        .ipo-hero h2 {
            margin: 0 0 6px 0;
            font-size: 1.36rem;
            color: #0f172a;
        }
        .ipo-hero p {
            margin: 0;
            color: #475569;
            font-size: 0.94rem;
            line-height: 1.55;
        }
        .ipo-chip-row {
            margin: 2px 0 10px 0;
        }
        .ipo-chip {
            display: inline-block;
            margin: 0 6px 6px 0;
            padding: 5px 10px;
            border-radius: 999px;
            background: rgba(14, 165, 233, 0.10);
            color: #0f172a;
            font-size: 0.83rem;
            font-weight: 600;
            border: 1px solid rgba(14, 165, 233, 0.14);
        }
        .ipo-soft-card {
            border: 1px solid rgba(148, 163, 184, 0.20);
            border-radius: 18px;
            padding: 14px 16px;
            background: rgba(255, 255, 255, 0.90);
            min-height: 96px;
            box-shadow: 0 8px 24px rgba(15, 23, 42, 0.04);
            overflow: hidden;
        }
        .ipo-soft-card.good {
            background: linear-gradient(180deg, rgba(240, 253, 244, 0.95) 0%, rgba(255,255,255,0.96) 100%);
        }
        .ipo-soft-card.warn {
            background: linear-gradient(180deg, rgba(255, 251, 235, 0.95) 0%, rgba(255,255,255,0.96) 100%);
        }
        .ipo-soft-card.bad {
            background: linear-gradient(180deg, rgba(254, 242, 242, 0.95) 0%, rgba(255,255,255,0.96) 100%);
        }
        .ipo-soft-card.neutral {
            background: linear-gradient(180deg, rgba(248, 250, 252, 0.95) 0%, rgba(255,255,255,0.96) 100%);
        }
        .ipo-soft-title {
            font-size: 0.82rem;
            color: #475569;
            margin-bottom: 6px;
            font-weight: 600;
        }
        .ipo-soft-value {
            font-size: 1.16rem;
            color: #0f172a;
            font-weight: 700;
            margin-bottom: 4px;
            line-height: 1.24;
            overflow-wrap: anywhere;
            word-break: keep-all;
        }
        .ipo-soft-sub {
            font-size: 0.82rem;
            color: #64748b;
            line-height: 1.45;
        }
        .ipo-fact-card {
            border: 1px solid rgba(148, 163, 184, 0.18);
            border-radius: 14px;
            padding: 11px 12px;
            background: rgba(248, 250, 252, 0.84);
            margin-bottom: 10px;
        }
        .ipo-fact-label {
            font-size: 0.78rem;
            color: #64748b;
            margin-bottom: 4px;
        }
        .ipo-fact-value {
            font-size: 0.98rem;
            color: #0f172a;
            font-weight: 600;
            word-break: break-word;
            line-height: 1.45;
        }
        .ipo-note {
            font-size: 0.87rem;
            color: #475569;
            line-height: 1.55;
        }
        .ipo-fact-grid {
            display: grid;
            gap: 10px;
            margin: 0.15rem 0 0.2rem 0;
        }
        .ipo-fact-grid.cols-1 {
            grid-template-columns: repeat(1, minmax(0, 1fr));
        }
        .ipo-fact-grid.cols-2 {
            grid-template-columns: repeat(2, minmax(0, 1fr));
        }
        .ipo-fact-grid.cols-3 {
            grid-template-columns: repeat(3, minmax(0, 1fr));
        }
        .ipo-link-row {
            display: flex;
            flex-wrap: wrap;
            gap: 0.45rem;
            margin-top: 0.45rem;
        }
        .ipo-link-chip {
            display: inline-flex;
            align-items: center;
            gap: 0.2rem;
            padding: 0.42rem 0.72rem;
            border-radius: 999px;
            border: 1px solid rgba(59, 130, 246, 0.20);
            background: rgba(239, 246, 255, 0.92);
            color: #0f172a;
            font-size: 0.82rem;
            font-weight: 600;
            text-decoration: none;
        }
        .ipo-link-chip:hover {
            background: rgba(219, 234, 254, 0.96);
        }
        .ipo-brief-list {
            margin: 0.25rem 0 0 0;
            padding-left: 1.1rem;
            color: #475569;
        }
        .ipo-brief-list li {
            margin: 0.12rem 0;
            line-height: 1.42;
        }
        @media (max-width: 900px) {
            .ipo-fact-grid.cols-2,
            .ipo-fact-grid.cols-3 {
                grid-template-columns: repeat(1, minmax(0, 1fr));
            }
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def render_soft_cards(cards: list[dict[str, Any]], *, columns: int | None = None) -> None:
    if not cards:
        return
    work = [card for card in cards if card]
    if not work:
        return
    cols_per_row = max(1, min(columns or len(work), len(work), 4))
    for start in range(0, len(work), cols_per_row):
        row_cards = work[start:start + cols_per_row]
        cols = st.columns(cols_per_row)
        for idx, card in enumerate(row_cards):
            tone = text_value(card.get("tone"), "neutral")
            title = escape(text_value(card.get("title"), ""))
            value = escape(text_value(card.get("value"), "-"))
            sub = escape(text_value(card.get("sub"), ""))
            value_color = text_value(card.get("value_color"), "").strip()
            sub_color = text_value(card.get("sub_color"), "").strip()
            value_style = f" style='color:{escape(value_color)};'" if value_color and value_color != "-" else ""
            sub_style = f" style='color:{escape(sub_color)};'" if sub_color and sub_color != "-" else ""
            html = (
                f"<div class='ipo-soft-card {tone}'>"
                f"<div class='ipo-soft-title'>{title}</div>"
                f"<div class='ipo-soft-value'{value_style}>{value}</div>"
                f"<div class='ipo-soft-sub'{sub_style}>{sub}</div>"
                f"</div>"
            )
            cols[idx].markdown(html, unsafe_allow_html=True)


def render_badge_row(labels: list[str]) -> None:
    labels = [str(label).strip() for label in labels if str(label).strip()]
    if not labels:
        return
    chips = "".join(f"<span class='ipo-chip'>{escape(label)}</span>" for label in labels)
    st.markdown(f"<div class='ipo-chip-row'>{chips}</div>", unsafe_allow_html=True)


def render_link_chip_row(items: list[tuple[str, Any]]) -> None:
    links: list[str] = []
    for label, url in items:
        href = text_value(url, "").strip()
        if not href or href == "-":
            continue
        links.append(
            f"<a class='ipo-link-chip' href='{escape(href, quote=True)}' target='_blank' rel='noopener noreferrer'>{escape(str(label))} ↗</a>"
        )
    if not links:
        st.caption("연결된 문서 링크가 없습니다.")
        return
    st.markdown(f"<div class='ipo-link-row'>{''.join(links)}</div>", unsafe_allow_html=True)




def link_chip_html(label: str, url: Any) -> str:
    href = text_value(url, "").strip()
    if not href or href == "-":
        return ""
    return (
        f"<a class='ipo-link-chip' href='{escape(href, quote=True)}' target='_blank' rel='noopener noreferrer'>"
        f"{escape(str(label))} ↗</a>"
    )
def link_button_compat(label: str, url: Any, *, use_container_width: bool = True) -> None:
    href = text_value(url, "").strip()
    if not href:
        return
    if hasattr(st, "link_button"):
        st.link_button(label, href, use_container_width=use_container_width)
    else:
        st.markdown(f"[{label}]({href})")


def market_move_colors(change_pct: Any) -> tuple[str | None, str | None]:
    value = safe_float(change_pct)
    if value is None or abs(value) < 1e-12:
        return (None, None)
    if value > 0:
        return ("#dc2626", "#b91c1c")
    return ("#2563eb", "#1d4ed8")


def should_show_kind_link(url: Any) -> bool:
    href = text_value(url, "").strip()
    if not href or href == "-":
        return False
    generic_tokens = [
        "searchListingTypeMain",
        "searchPubofrProgComMain",
        "irschedule.do?gubun=iRMaterials&method=searchIRScheduleMain",
        "corpList.do?method=download",
        "corpList.do?method=download&searchType=13",
    ]
    return not any(token in href for token in generic_tokens)


def issue_needs_enrichment(issue: pd.Series | dict[str, Any]) -> bool:
    row = issue if isinstance(issue, dict) else issue.to_dict()
    if not text_value(row.get("name"), "").strip():
        return False
    critical_fields = [
        "listing_date",
        "offer_price",
        "current_price",
        "institutional_competition_ratio",
        "lockup_commitment_ratio",
        "circulating_shares_ratio_on_listing",
        "existing_shareholder_ratio",
        "ir_pdf_url",
        "dart_viewer_url",
        "dart_report_nm",
    ]
    return any(not has_value(row.get(col)) for col in critical_fields)


def _field_coverage(df: pd.DataFrame, cols: list[str]) -> float:
    if df is None or df.empty:
        return 0.0
    present = [col for col in cols if col in df.columns]
    if not present:
        return 0.0
    mask = pd.DataFrame({col: df[col].map(has_value) for col in present})
    return float(mask.any(axis=1).mean()) if not mask.empty else 0.0


def _kind_fill_ratio(df: pd.DataFrame) -> float:
    return _field_coverage(df, ["market", "symbol", "listing_date", "underwriters", "offer_price", "current_price"])


@st.cache_data(show_spinner=False, ttl=3600)
def load_issue_support_tables_cached(cache_rev: str = CACHE_REV) -> dict[str, pd.DataFrame]:
    _ = cache_rev
    hub = IPODataHub(DATA_DIR, dart_client=DartClient.from_env(), kis_client=KISClient.from_env())
    cache = hub.cache
    now = today_kst()

    def _maybe_read(name: str, *, issue_like: bool = True) -> pd.DataFrame:
        try:
            df = cache.read_frame(name)
            if not isinstance(df, pd.DataFrame) or df.empty:
                return pd.DataFrame()
            return standardize_issue_frame(df) if issue_like else df.copy()
        except Exception:
            return pd.DataFrame()

    official_name_map_df = _maybe_read("official_ksd_name_lookup_live", issue_like=False)
    official_market_codes_df = _maybe_read("official_ksd_market_codes_live", issue_like=False)
    official_listing_df = _maybe_read("official_ksd_listing_info_live", issue_like=False)
    official_corp_basic_df = _maybe_read("official_ksd_corp_basic_live", issue_like=False)
    official_shareholder_df = _maybe_read("official_ksd_shareholder_summary_live", issue_like=False)
    official_issue_overlay_df = _maybe_read("official_issue_overlay_live")
    has_official_support = any(
        isinstance(frame, pd.DataFrame) and not frame.empty
        for frame in [official_name_map_df, official_market_codes_df, official_listing_df, official_issue_overlay_df]
    )
    allow_inline_fetch_raw = os.getenv("IPO_ALLOW_INLINE_FETCH", "").strip()
    allow_inline_fetch = (allow_inline_fetch_raw == "1") if allow_inline_fetch_raw else (not has_official_support)

    def _write_cache(name: str, frame: pd.DataFrame, *, source: str, notes: str) -> None:
        if frame is None or frame.empty:
            return
        cache.write_frame(name, frame, meta={"source": source, "notes": notes, "row_count": int(len(frame)), "saved_at": now.isoformat()})

    schedule_df = _maybe_read("schedule_38_live")
    if allow_inline_fetch and (schedule_df.empty or _field_coverage(schedule_df, ["symbol", "market", "listing_date", "offer_price"]) < 0.40 or _field_coverage(schedule_df, ["institutional_competition_ratio", "lockup_commitment_ratio", "current_price"]) <= 0.02):
        try:
            schedule_df = standardize_issue_frame(standardize_38_schedule_table(fetch_38_schedule(include_detail_links=True), fetch_details=True))
            _write_cache("schedule_38_live", schedule_df, source="38", notes="support-load-refresh")
        except Exception:
            schedule_df = schedule_df if not schedule_df.empty else pd.DataFrame()

    new_listing_df = _maybe_read("schedule_38_new_listing_live")
    if allow_inline_fetch and (new_listing_df.empty or _field_coverage(new_listing_df, ["listing_date", "offer_price", "current_price"]) < 0.20):
        try:
            new_listing_df = standardize_issue_frame(standardize_38_new_listing_table(fetch_38_new_listing_table(timeout=10, max_pages=40)))
            _write_cache("schedule_38_new_listing_live", new_listing_df, source="38", notes="support-load-new-listing")
        except Exception:
            new_listing_df = new_listing_df if not new_listing_df.empty else pd.DataFrame()

    demand_df = _maybe_read("schedule_38_demand_live")
    if allow_inline_fetch and (demand_df.empty or _field_coverage(demand_df, ["forecast_date", "institutional_competition_ratio", "lockup_commitment_ratio"]) < 0.45):
        try:
            demand_df = standardize_issue_frame(fetch_38_demand_results(timeout=10, max_pages=50))
            _write_cache("schedule_38_demand_live", demand_df, source="38", notes="support-load-demand")
        except Exception:
            demand_df = demand_df if not demand_df.empty else pd.DataFrame()

    ir_df = _maybe_read("ir_38_live")
    if allow_inline_fetch and (ir_df.empty or _field_coverage(ir_df, ["ir_pdf_url", "ir_date"]) < 0.30):
        try:
            ir_df = standardize_issue_frame(fetch_38_ir_links(timeout=10, max_pages=40))
            _write_cache("ir_38_live", ir_df, source="38", notes="support-load-ir")
        except Exception:
            ir_df = ir_df if not ir_df.empty else pd.DataFrame()

    kind_corp_df = _maybe_read("kind_corp_download_live")
    if allow_inline_fetch and (kind_corp_df.empty or _field_coverage(kind_corp_df, ["symbol"]) < 0.35 or _field_coverage(kind_corp_df, ["listing_date", "market"]) < 0.35):
        try:
            kind_corp_df = standardize_issue_frame(fetch_kind_corp_download_table(timeout=12))
            _write_cache("kind_corp_download_live", kind_corp_df, source="KIND", notes="support-load-corpdownload")
        except Exception:
            kind_corp_df = kind_corp_df if not kind_corp_df.empty else pd.DataFrame()

    kind_listing_df = _maybe_read("kind_listing_live")
    if allow_inline_fetch and (kind_listing_df.empty or _kind_fill_ratio(kind_listing_df) < 0.20):
        try:
            kind_listing_df = standardize_issue_frame(standardize_kind_listing_table(fetch_kind_listing_table(timeout=10)))
            _write_cache("kind_listing_live", kind_listing_df, source="KIND", notes="support-load-listing")
        except Exception:
            kind_listing_df = kind_listing_df if not kind_listing_df.empty else pd.DataFrame()

    kind_public_df = _maybe_read("kind_public_offering_live")
    if allow_inline_fetch and (kind_public_df.empty or _kind_fill_ratio(kind_public_df) < 0.20):
        try:
            kind_public_df = standardize_issue_frame(standardize_kind_public_offering_table(fetch_kind_public_offering_table(timeout=10)))
            _write_cache("kind_public_offering_live", kind_public_df, source="KIND", notes="support-load-public-offer")
        except Exception:
            kind_public_df = kind_public_df if not kind_public_df.empty else pd.DataFrame()

    kind_pubprice_df = _maybe_read("kind_pubprice_live")
    if allow_inline_fetch and (kind_pubprice_df.empty or _field_coverage(kind_pubprice_df, ["current_price", "offer_price", "listing_date"]) < 0.20):
        try:
            kind_pubprice_df = standardize_issue_frame(standardize_kind_pubprice_table(fetch_kind_pubprice_table(timeout=10)))
            _write_cache("kind_pubprice_live", kind_pubprice_df, source="KIND", notes="support-load-pubprice")
        except Exception:
            kind_pubprice_df = kind_pubprice_df if not kind_pubprice_df.empty else pd.DataFrame()

    try:
        local_kind_path = hub.repo.auto_detect_local_kind_export()
        local_master_df = standardize_issue_frame(load_kind_export_from_path(local_kind_path)) if local_kind_path else pd.DataFrame()
    except Exception:
        local_master_df = pd.DataFrame()

    try:
        seed_38_df = standardize_issue_frame(hub.repo.load_38_seed_export())
    except Exception:
        seed_38_df = pd.DataFrame()

    try:
        dart_df = standardize_issue_frame(hub.repo.load_dart_enriched_export())
    except Exception:
        dart_df = pd.DataFrame()

    return {
        "schedule": schedule_df,
        "new_listing": new_listing_df,
        "demand": demand_df,
        "ir": ir_df,
        "seed_38": seed_38_df,
        "local_master": local_master_df,
        "dart": dart_df,
        "kind_corp": kind_corp_df,
        "kind_listing": kind_listing_df,
        "kind_public": kind_public_df,
        "kind_pubprice": kind_pubprice_df,
        "official_name_map": official_name_map_df,
        "official_market_codes": official_market_codes_df,
        "official_listing": official_listing_df,
        "official_corp_basic": official_corp_basic_df,
        "official_shareholder": official_shareholder_df,
        "official_issue_overlay": official_issue_overlay_df,
    }


@st.cache_data(show_spinner=False, ttl=3600)
def load_official_security_index_cached(cache_rev: str = CACHE_REV) -> dict[str, Any]:
    _ = cache_rev
    tables = load_issue_support_tables_cached(cache_rev=cache_rev)
    today = today_kst().normalize()
    symbol_by_query_name_key: dict[str, str] = {}
    symbol_by_name_key: dict[str, str] = {}
    market_by_symbol: dict[str, str] = {}
    market_by_name_key: dict[str, str] = {}
    status_by_symbol: dict[str, str] = {}
    status_by_name_key: dict[str, str] = {}
    listed_symbols: set[str] = set()
    listed_name_keys: set[str] = set()
    delisted_symbols: set[str] = set()
    delisted_name_keys: set[str] = set()

    name_map = tables.get("official_name_map", pd.DataFrame())
    if isinstance(name_map, pd.DataFrame) and not name_map.empty:
        work = name_map.copy()
        work["query_name_key"] = work.get("query_name_key", pd.Series(dtype="object")).fillna("").astype(str)
        work["name_key"] = work.get("name_key", pd.Series(dtype="object")).fillna("").astype(str)
        work["symbol"] = work.get("symbol", pd.Series(dtype="object")).map(normalize_symbol_text)
        for _, row in work.iterrows():
            query_key = str(row.get("query_name_key") or "").strip()
            name_key = str(row.get("name_key") or "").strip()
            symbol = normalize_symbol_text(row.get("symbol"))
            if query_key and symbol and query_key not in symbol_by_query_name_key:
                symbol_by_query_name_key[query_key] = symbol
            if name_key and symbol and name_key not in symbol_by_name_key:
                symbol_by_name_key[name_key] = symbol

    market_codes = tables.get("official_market_codes", pd.DataFrame())
    if isinstance(market_codes, pd.DataFrame) and not market_codes.empty:
        work = market_codes.copy()
        work["name_key"] = work.get("name_key", pd.Series(dtype="object")).fillna(work.get("name", pd.Series(dtype="object"))).map(normalize_name_key)
        work["symbol"] = work.get("symbol", pd.Series(dtype="object")).map(normalize_symbol_text)
        for _, row in work.iterrows():
            symbol = normalize_symbol_text(row.get("symbol"))
            name_key = str(row.get("name_key") or "").strip()
            market = str(row.get("market") or "").strip()
            if symbol and market and symbol not in market_by_symbol:
                market_by_symbol[symbol] = market
            if name_key and market and name_key not in market_by_name_key:
                market_by_name_key[name_key] = market
            if symbol:
                listed_symbols.add(symbol)
            if name_key:
                listed_name_keys.add(name_key)

    listing = tables.get("official_listing", pd.DataFrame())
    if isinstance(listing, pd.DataFrame) and not listing.empty:
        work = listing.copy()
        work["name_key"] = work.get("name_key", pd.Series(dtype="object")).fillna(work.get("name", pd.Series(dtype="object"))).map(normalize_name_key)
        work["symbol"] = work.get("symbol", pd.Series(dtype="object")).map(normalize_symbol_text)
        work["delisting_date"] = pd.to_datetime(work.get("delisting_date"), errors="coerce")
        for _, row in work.iterrows():
            symbol = normalize_symbol_text(row.get("symbol"))
            name_key = str(row.get("name_key") or "").strip()
            status = str(row.get("listing_status") or "").strip()
            delisting_date = pd.to_datetime(row.get("delisting_date"), errors="coerce")
            is_delisted = pd.notna(delisting_date) and pd.Timestamp(delisting_date).normalize() <= today
            if is_delisted:
                if symbol:
                    delisted_symbols.add(symbol)
                    status_by_symbol[symbol] = "청산/상장폐지" if "스팩" in str(row.get("name") or "") else "상장폐지"
                if name_key:
                    delisted_name_keys.add(name_key)
                    status_by_name_key[name_key] = "청산/상장폐지" if "스팩" in str(row.get("name") or "") else "상장폐지"
            else:
                if symbol:
                    listed_symbols.add(symbol)
                if name_key:
                    listed_name_keys.add(name_key)
                if symbol and status and symbol not in status_by_symbol:
                    status_by_symbol[symbol] = status
                if name_key and status and name_key not in status_by_name_key:
                    status_by_name_key[name_key] = status

    return {
        "symbol_by_query_name_key": symbol_by_query_name_key,
        "symbol_by_name_key": symbol_by_name_key,
        "market_by_symbol": market_by_symbol,
        "market_by_name_key": market_by_name_key,
        "listed_symbols": tuple(sorted(listed_symbols)),
        "listed_name_keys": tuple(sorted(listed_name_keys)),
        "delisted_symbols": tuple(sorted(delisted_symbols)),
        "delisted_name_keys": tuple(sorted(delisted_name_keys)),
        "status_by_symbol": status_by_symbol,
        "status_by_name_key": status_by_name_key,
    }


def apply_official_symbol_mapping(df: pd.DataFrame, tables: dict[str, pd.DataFrame] | None = None) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=getattr(df, "columns", []))
    work = standardize_issue_frame(df.copy())
    work["name_key"] = work.get("name_key", pd.Series(dtype="object")).fillna(work.get("name", pd.Series(dtype="object"))).map(normalize_name_key)
    work["symbol"] = work.get("symbol", pd.Series(dtype="object")).map(normalize_symbol_text)
    index = load_official_security_index_cached()
    symbol_by_query = index.get("symbol_by_query_name_key", {})
    symbol_by_name = index.get("symbol_by_name_key", {})
    market_by_symbol = index.get("market_by_symbol", {})
    market_by_name = index.get("market_by_name_key", {})

    missing_symbol = ~work["symbol"].map(has_value)
    if missing_symbol.any():
        fill_symbol = work.loc[missing_symbol, "name_key"].map(symbol_by_query)
        fill_symbol = fill_symbol.combine_first(work.loc[missing_symbol, "name_key"].map(symbol_by_name))
        work.loc[missing_symbol, "symbol"] = fill_symbol.combine_first(work.loc[missing_symbol, "symbol"])

    if "market" not in work.columns:
        work["market"] = pd.NA
    market_series = work.get("market", pd.Series(dtype="object"))
    missing_market = ~market_series.map(has_value)
    if missing_market.any():
        fill_market = work.loc[missing_market, "symbol"].map(market_by_symbol)
        fill_market = fill_market.combine_first(work.loc[missing_market, "name_key"].map(market_by_name))
        work.loc[missing_market, "market"] = fill_market.combine_first(work.loc[missing_market, "market"])

    return standardize_issue_frame(work)


def prefill_issue_frame_for_display(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=getattr(df, "columns", []))
    tables = load_issue_support_tables_cached()
    work = apply_official_symbol_mapping(standardize_issue_frame(df.copy()), tables=tables)
    hub = IPODataHub(DATA_DIR, dart_client=DartClient.from_env(), kis_client=KISClient.from_env())
    for key in ["official_issue_overlay", "local_master", "seed_38", "kind_corp", "schedule", "new_listing", "demand", "ir", "kind_listing", "kind_public", "kind_pubprice", "dart"]:
        overlay = tables.get(key, pd.DataFrame())
        if isinstance(overlay, pd.DataFrame) and not overlay.empty:
            work = hub._overlay_issues(work, overlay, append_new=False)
    work = apply_official_symbol_mapping(work, tables=tables)
    work = collapse_duplicate_issues_for_ui(work)
    return add_issue_scores(work)


@st.cache_data(show_spinner=False, ttl=3600)
def load_current_listing_index_cached(cache_rev: str = CACHE_REV) -> dict[str, Any]:
    _ = cache_rev
    official = load_official_security_index_cached(cache_rev=cache_rev)
    listed_symbols = set(official.get("listed_symbols", ()))
    listed_name_keys = set(official.get("listed_name_keys", ()))
    delisted_symbols = set(official.get("delisted_symbols", ()))
    delisted_name_keys = set(official.get("delisted_name_keys", ()))
    status_by_symbol = dict(official.get("status_by_symbol", {}))
    status_by_name_key = dict(official.get("status_by_name_key", {}))

    tables = load_issue_support_tables_cached(cache_rev=cache_rev)
    kind_corp = tables.get("kind_corp", pd.DataFrame()) if isinstance(tables, dict) else pd.DataFrame()
    if isinstance(kind_corp, pd.DataFrame) and not kind_corp.empty:
        merged = standardize_issue_frame(kind_corp.copy())
        listed_symbols |= {symbol for symbol in merged.get("symbol", pd.Series(dtype="object")).map(normalize_symbol_text).dropna().tolist() if symbol}
        listed_name_keys |= {key for key in merged.get("name", pd.Series(dtype="object")).map(normalize_name_key).dropna().tolist() if key}

    return {
        "symbols": tuple(sorted(listed_symbols)),
        "name_keys": tuple(sorted(listed_name_keys)),
        "delisted_symbols": tuple(sorted(delisted_symbols)),
        "delisted_name_keys": tuple(sorted(delisted_name_keys)),
        "status_by_symbol": status_by_symbol,
        "status_by_name_key": status_by_name_key,
    }


def collapse_duplicate_issues_for_ui(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=getattr(df, "columns", []))
    work = apply_official_symbol_mapping(standardize_issue_frame(df.copy()))
    work["symbol"] = work.get("symbol", pd.Series(dtype="object")).map(normalize_symbol_text)
    work["name_key"] = work.get("name_key", pd.Series(dtype="object")).fillna(work.get("name", pd.Series(dtype="object"))).map(normalize_name_key)

    symbol_by_name: dict[str, str] = {}
    if "name_key" in work.columns and "symbol" in work.columns:
        symbol_rows = work.loc[work["name_key"].map(has_value) & work["symbol"].map(has_value), ["name_key", "symbol"]].drop_duplicates()
        if not symbol_rows.empty:
            counts = symbol_rows.groupby("name_key")["symbol"].nunique()
            unique_names = counts[counts == 1].index.tolist()
            if unique_names:
                symbol_by_name = (
                    symbol_rows[symbol_rows["name_key"].isin(unique_names)]
                    .drop_duplicates(subset=["name_key"], keep="first")
                    .set_index("name_key")["symbol"]
                    .to_dict()
                )
    if symbol_by_name:
        missing_symbol = ~work["symbol"].map(has_value)
        work.loc[missing_symbol, "symbol"] = work.loc[missing_symbol, "name_key"].map(symbol_by_name).combine_first(work.loc[missing_symbol, "symbol"])

    score_cols = [
        "current_price",
        "subscription_start",
        "subscription_end",
        "forecast_date",
        "listing_date",
        "underwriters",
        "institutional_competition_ratio",
        "lockup_commitment_ratio",
        "existing_shareholder_ratio",
        "circulating_shares_ratio_on_listing",
        "market",
        "offer_price",
        "symbol",
    ]
    work["_fill_score"] = work.apply(lambda row: sum(1 for col in score_cols if has_value(row.get(col))), axis=1)
    work["_listing_date"] = pd.to_datetime(work.get("listing_date"), errors="coerce")
    work["_subscription_start"] = pd.to_datetime(work.get("subscription_start"), errors="coerce")
    work["_group_key"] = work["symbol"].where(work["symbol"].map(has_value), work["name_key"])

    rows: list[dict[str, Any]] = []
    for _, subset in work.groupby("_group_key", dropna=False):
        ordered = subset.sort_values(["_fill_score", "_listing_date", "_subscription_start", "name_key"], ascending=[False, False, False, True], na_position="last")
        merged = ordered.iloc[0].to_dict()
        for _, row in ordered.iloc[1:].iterrows():
            record = row.to_dict()
            for col in work.columns:
                if col.startswith("_"):
                    continue
                if not has_value(merged.get(col)) and has_value(record.get(col)):
                    merged[col] = record.get(col)
        rows.append({k: v for k, v in merged.items() if not str(k).startswith("_")})
    return standardize_issue_frame(pd.DataFrame(rows))


def current_price_cell_text(row: pd.Series | dict[str, Any], default: str = "-") -> str:
    record = row if isinstance(row, dict) else row.to_dict()
    price = safe_float(record.get("current_price"))
    if price is not None:
        return fmt_num(price, 0)
    listing_date = pd.to_datetime(record.get("listing_date"), errors="coerce")
    if pd.isna(listing_date):
        return default
    if pd.Timestamp(listing_date).normalize() > today_kst().normalize():
        return default
    index = load_current_listing_index_cached()
    listed_symbols = set(index.get("symbols", ()))
    listed_name_keys = set(index.get("name_keys", ()))
    status_by_symbol = index.get("status_by_symbol", {}) or {}
    status_by_name_key = index.get("status_by_name_key", {}) or {}
    symbol = normalize_symbol_text(record.get("symbol"))
    name_key = normalize_name_key(record.get("name") or record.get("name_key"))
    is_currently_listed = bool((symbol and symbol in listed_symbols) or (name_key and name_key in listed_name_keys))
    if is_currently_listed:
        return default
    if symbol and status_by_symbol.get(symbol):
        return str(status_by_symbol.get(symbol))
    if name_key and status_by_name_key.get(name_key):
        return str(status_by_name_key.get(name_key))
    name_text = text_value(record.get("name"), "")
    return "청산/상장폐지" if "스팩" in name_text else "상장폐지"


@st.cache_data(show_spinner=False, ttl=1800)
def load_public_quotes_cached(symbol_name_pairs: tuple[tuple[str, str], ...], use_kis: bool = False, cache_rev: str = CACHE_REV) -> pd.DataFrame:
    _ = cache_rev
    if not symbol_name_pairs:
        return pd.DataFrame(columns=["name_key", "symbol", "market", "current_price", "day_change_pct", "quote_asof", "quote_provider"])
    req_df = pd.DataFrame(symbol_name_pairs, columns=["symbol", "name"])
    kis_client = KISClient.from_env() if use_kis else None
    service = PublicQuoteService(DATA_DIR, kis_client=kis_client)
    return service.get_quotes(req_df, max_items=min(max(len(req_df), 0), 1500))


@st.cache_data(show_spinner=False, ttl=21600)
def load_seibro_release_schedule_cached(cache_rev: str = CACHE_REV) -> pd.DataFrame:
    _ = cache_rev
    try:
        return fetch_seibro_release_schedule(timeout=12)
    except Exception:
        return pd.DataFrame(columns=["name", "name_key", "release_date", "release_shares", "remaining_locked_shares", "market", "source", "source_detail", "last_refresh_ts"])


def overlay_public_quotes_on_frame(df: pd.DataFrame, *, max_items: int = 40) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=getattr(df, "columns", []))
    work = apply_official_symbol_mapping(standardize_issue_frame(df.copy()))
    listing = pd.to_datetime(work.get("listing_date"), errors="coerce")
    work["symbol"] = work.get("symbol", pd.Series(dtype="object")).map(normalize_symbol_text)
    work["name_key"] = work.get("name_key", pd.Series(dtype="object")).fillna(work.get("name", pd.Series(dtype="object"))).map(normalize_name_key)
    need_quote = listing.notna()
    if "current_price" in work.columns:
        need_quote = need_quote & pd.to_numeric(work.get("current_price"), errors="coerce").isna()
    request_rows: list[tuple[str, str]] = []
    ordered = work.loc[need_quote].copy()
    if not ordered.empty:
        ordered["_listing_date"] = pd.to_datetime(ordered.get("listing_date"), errors="coerce")
        ordered = ordered.sort_values(["_listing_date", "name_key"], ascending=[False, True], na_position="last")
        for _, row in ordered.head(max_items).iterrows():
            symbol = text_value(row.get("symbol"), "").strip()
            name = text_value(row.get("name"), "").strip()
            if name:
                request_rows.append((symbol, name))
    quotes = load_public_quotes_cached(tuple(request_rows), use_kis=KISClient.from_env() is not None) if request_rows else pd.DataFrame()
    if quotes.empty:
        return work
    quotes = quotes.copy()
    quotes["symbol"] = quotes.get("symbol", pd.Series(dtype="object")).map(normalize_symbol_text)
    quotes["name_key"] = quotes.get("name_key", pd.Series(dtype="object")).fillna(quotes.get("name", pd.Series(dtype="object"))).map(normalize_name_key)

    merged = work.copy()
    if "symbol" in quotes.columns and quotes["symbol"].notna().any():
        symbol_quotes = quotes[[c for c in ["symbol", "current_price", "day_change_pct", "market"] if c in quotes.columns]].copy()
        symbol_quotes = symbol_quotes.dropna(subset=["symbol"]).drop_duplicates(subset=["symbol"], keep="first")
        if not symbol_quotes.empty:
            merged = merged.merge(symbol_quotes.rename(columns={
                "current_price": "current_price_quote_symbol",
                "day_change_pct": "day_change_pct_quote_symbol",
                "market": "market_quote_symbol",
            }), on="symbol", how="left")
            for left_col, right_col in [("current_price", "current_price_quote_symbol"), ("day_change_pct", "day_change_pct_quote_symbol"), ("market", "market_quote_symbol")]:
                if right_col in merged.columns:
                    merged[left_col] = merged.get(left_col).combine_first(merged[right_col])

    name_quotes = quotes[[c for c in ["name_key", "current_price", "day_change_pct", "market"] if c in quotes.columns]].copy()
    if not name_quotes.empty:
        name_quotes = name_quotes.dropna(subset=["name_key"]).drop_duplicates(subset=["name_key"], keep="first")
        merged = merged.merge(name_quotes.rename(columns={
            "current_price": "current_price_quote_name",
            "day_change_pct": "day_change_pct_quote_name",
            "market": "market_quote_name",
        }), on="name_key", how="left")
        for left_col, right_col in [("current_price", "current_price_quote_name"), ("day_change_pct", "day_change_pct_quote_name"), ("market", "market_quote_name")]:
            if right_col in merged.columns:
                merged[left_col] = merged.get(left_col).combine_first(merged[right_col])

    drop_cols = [
        "current_price_quote_symbol",
        "day_change_pct_quote_symbol",
        "market_quote_symbol",
        "current_price_quote_name",
        "day_change_pct_quote_name",
        "market_quote_name",
    ]
    return standardize_issue_frame(merged.drop(columns=[c for c in drop_cols if c in merged.columns]))


def overlay_detail_rows_on_frame(df: pd.DataFrame, *, max_items: int = 18) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=getattr(df, "columns", []))
    work = standardize_issue_frame(df.copy())
    target_cols = [
        "market",
        "symbol",
        "sector",
        "underwriters",
        "subscription_start",
        "subscription_end",
        "forecast_date",
        "listing_date",
        "offer_price",
        "current_price",
        "institutional_competition_ratio",
        "lockup_commitment_ratio",
        "circulating_shares_ratio_on_listing",
        "existing_shareholder_ratio",
        "employee_forfeit_ratio",
        "ir_pdf_url",
        "dart_viewer_url",
        "total_offer_shares",
        "post_listing_total_shares",
    ]
    rows: list[dict[str, Any]] = []
    attempted = 0
    for _, row in work.iterrows():
        current = row.to_dict()
        needs = any(not has_value(current.get(col)) for col in target_cols)
        if needs and attempted < max_items:
            payload = load_issue_detail_overlay_cached(text_value(current.get("symbol"), ""), text_value(current.get("name"), ""))
            if isinstance(payload, dict):
                for key, value in payload.items():
                    if not has_value(value):
                        continue
                    if not has_value(current.get(key)) or key in {
                        "underwriters",
                        "market",
                        "symbol",
                        "sector",
                        "subscription_start",
                        "subscription_end",
                        "forecast_date",
                        "listing_date",
                        "offer_price",
                        "current_price",
                        "institutional_competition_ratio",
                        "lockup_commitment_ratio",
                        "circulating_shares_ratio_on_listing",
                        "existing_shareholder_ratio",
                        "employee_forfeit_ratio",
                        "ir_pdf_url",
                        "dart_viewer_url",
                        "dart_report_nm",
                        "dart_filing_date",
                        "total_offer_shares",
                        "post_listing_total_shares",
                    }:
                        current[key] = value
            attempted += 1
        rows.append(current)
    return standardize_issue_frame(pd.DataFrame(rows))


def prepare_issue_frame_for_page(df: pd.DataFrame, *, detail_budget: int = 18, quote_budget: int = 40) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=getattr(df, "columns", []))
    work = prefill_issue_frame_for_display(df)
    if int(detail_budget or 0) > 0:
        work = overlay_detail_rows_on_frame(work, max_items=int(detail_budget))
    if int(quote_budget or 0) > 0:
        work = overlay_public_quotes_on_frame(work, max_items=int(quote_budget))
    work = collapse_duplicate_issues_for_ui(work)
    return add_issue_scores(work)


def prepare_issue_frame_for_table(df: pd.DataFrame, *, quote_budget: int = 12, detail_budget: int = 0) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=getattr(df, "columns", []))
    work = prefill_issue_frame_for_display(df)
    if int(detail_budget or 0) > 0:
        work = overlay_detail_rows_on_frame(work, max_items=int(detail_budget))
    if int(quote_budget or 0) > 0:
        work = overlay_public_quotes_on_frame(work, max_items=int(quote_budget))
    work = collapse_duplicate_issues_for_ui(work)
    return add_issue_scores(work)


def build_issue_coverage_summary(issues: pd.DataFrame) -> pd.DataFrame:
    if issues is None or issues.empty:
        return pd.DataFrame(columns=["필드", "채움", "비율"])
    rows = []
    total = len(issues)
    fields = {
        "종목코드": "symbol",
        "시장": "market",
        "주관사": "underwriters",
        "상장일": "listing_date",
        "공모가": "offer_price",
        "현재가": "current_price",
        "기관경쟁률": "institutional_competition_ratio",
        "확약비율": "lockup_commitment_ratio",
        "IR PDF": "ir_pdf_url",
    }
    for label, col in fields.items():
        if col not in issues.columns:
            continue
        filled = int(issues[col].map(has_value).sum())
        rows.append({"필드": label, "채움": filled, "비율": f"{(filled / total * 100.0):.1f}%"})
    return pd.DataFrame(rows)

def score_formula_frames(issue: pd.Series) -> dict[str, pd.DataFrame]:
    inst = safe_float(issue.get("institutional_competition_ratio"))
    retail = safe_float(issue.get("retail_competition_ratio_live"))
    offer_price = safe_float(issue.get("offer_price"))
    lockup = safe_float(issue.get("lockup_commitment_ratio"))
    float_ratio = safe_float(issue.get("circulating_shares_ratio_on_listing"))
    existing = safe_float(issue.get("existing_shareholder_ratio"))
    employee_forfeit = safe_float(issue.get("employee_forfeit_ratio"))
    current_price = safe_float(issue.get("current_price"))
    premium = None
    if current_price not in (None, 0) and offer_price not in (None, 0):
        premium = (float(current_price) / float(offer_price) - 1.0) * 100.0
    day_change = safe_float(issue.get("day_change_pct"))

    subscription = pd.DataFrame([
        {"항목": "기관경쟁률", "현재값": fmt_ratio(inst), "구간": "0→0, 300→25, 800→55, 1500→85, 2500→100", "설명": "높을수록 가점"},
        {"항목": "청약경쟁률", "현재값": fmt_ratio(retail), "구간": "0→0, 100→10, 300→25, 700→45, 1500→55", "설명": "높을수록 가점"},
        {"항목": "공모가", "현재값": fmt_won(offer_price), "구간": "2천→5, 1만→8, 2만→12, 5만→8, 10만→5", "설명": "중간 가격대에 소폭 가점"},
    ])
    listing = pd.DataFrame([
        {"항목": "의무보유확약", "현재값": fmt_pct(lockup), "구간": "0→0, 5→10, 10→20, 20→35, 40→45", "설명": "높을수록 품질 가점"},
        {"항목": "유통가능물량", "현재값": fmt_pct(float_ratio), "구간": "15→30, 25→22, 35→12, 50→5, 80→0", "설명": "낮을수록 품질 가점"},
        {"항목": "기존주주비율", "현재값": fmt_pct(existing), "구간": "20→20, 40→16, 60→10, 80→4", "설명": "낮을수록 품질 가점"},
        {"항목": "우리사주 실권", "현재값": fmt_pct(employee_forfeit), "구간": "0→10, 1→8, 3→5, 5→1", "설명": "낮을수록 품질 가점"},
        {"항목": "공모가 대비 등락", "현재값": fmt_pct(premium, 2, signed=True), "구간": "-30→0, -10→6, 0→10, 20→15, 60→8, 120→4", "설명": "상장 후 가격대 컨텍스트"},
    ])
    unlock = pd.DataFrame([
        {"항목": "유통가능물량", "현재값": fmt_pct(float_ratio), "구간": "10→5, 20→15, 35→35, 50→60, 80→90", "설명": "높을수록 압력 증가"},
        {"항목": "기존주주비율", "현재값": fmt_pct(existing), "구간": "20→5, 40→15, 60→30, 80→45", "설명": "높을수록 압력 증가"},
        {"항목": "당일 변동성", "현재값": fmt_pct(day_change, 2, signed=True), "구간": "0→0, 3→5, 7→12, 12→18", "설명": "변동성이 크면 압력 가중"},
        {"항목": "의무보유확약", "현재값": fmt_pct(lockup), "구간": "0→0, 10→4, 20→10, 40→18", "설명": "높을수록 압력 완화(감점)"},
    ])
    return {"subscription": subscription, "listing": listing, "unlock": unlock}


def render_score_formula_explainer(issue: pd.Series) -> None:
    with st.expander("점수 계산 방식 보기", expanded=False):
        st.caption("모든 점수는 구간 점수표를 선형보간한 뒤 0~100 범위로 절단합니다.")
        frames = score_formula_frames(issue)
        st.markdown("**청약 점수 = 기관경쟁률 + 청약경쟁률 + 공모가 보정**")
        render_scrollable_table(frames["subscription"], key=f"score_formula_subscription_{normalize_name_key(issue.get('name'))}")
        st.markdown("**상장 품질 = 확약 + 낮은 유통비율 + 낮은 기존주주비율 + 낮은 우리사주 실권 + 상장 후 가격대**")
        render_scrollable_table(frames["listing"], key=f"score_formula_listing_{normalize_name_key(issue.get('name'))}")
        st.markdown("**락업 압력 = 유통비율 압력 + 기존주주 압력 + 변동성 압력 - 확약 완화**")
        render_scrollable_table(frames["unlock"], key=f"score_formula_unlock_{normalize_name_key(issue.get('name'))}")


def render_dart_snapshot(snapshot: dict[str, Any], issue: pd.Series | None = None) -> None:
    if not snapshot:
        st.info("분석된 DART 스냅샷이 없습니다.")
        return
    if snapshot.get("error"):
        st.error(text_value(snapshot.get("error"), "DART 분석 중 오류가 발생했습니다."))
        return
    filing = snapshot.get("filing", {}) or {}
    company = snapshot.get("company", {}) or {}
    report_nm = text_value(filing.get("report_nm") or filing.get("report_name"), "DART 보고서")
    viewer_url = text_value(filing.get("viewer_url") or filing.get("dart_viewer_url"), "").strip()
    receipt_no = text_value(filing.get("rcept_no") or filing.get("receipt_no"), "")
    filing_date = compact_date_text(filing.get("rcept_dt") or filing.get("filing_date"), default="-")

    cards = [
        {"title": "회사", "value": text_value(company.get("corp_name") or company.get("name"), text_value(issue.get("name") if issue is not None else None, "-")), "sub": "분석 대상", "tone": "neutral"},
        {"title": "보고서", "value": report_nm, "sub": filing_date, "tone": "good" if viewer_url else "neutral"},
        {"title": "접수번호", "value": receipt_no or "-", "sub": "DART 접수번호", "tone": "neutral"},
    ]
    render_soft_cards(cards, columns=3)
    if viewer_url:
        link_button_compat("DART 보고서 열기", viewer_url)
    summary = snapshot_summary_text(snapshot)
    if summary:
        st.caption(summary)
    if issue is not None:
        overlay_df = snapshot_overlay_frame(issue, snapshot)
        if not overlay_df.empty:
            st.markdown("**보강 전 / 후 비교**")
            render_scrollable_table(overlay_df.rename(columns={"field": "필드", "label": "라벨", "before": "기존값", "after": "보강값"}), key=f"dart_overlay_{normalize_name_key(issue.get('name'))}")
    evidence_df = snapshot_evidence_frame(snapshot)
    if not evidence_df.empty:
        view = evidence_df.copy()
        if "value" in view.columns:
            view["value"] = view["value"].map(lambda v: text_value(v, "-"))
        st.markdown("**추출 근거**")
        evidence_name = text_value(issue.get('name') if issue is not None else report_nm, report_nm)
        render_scrollable_table(view, key=f"dart_evidence_{normalize_name_key(evidence_name)}")


def score_descriptor(score: Any, *, inverse: bool = False) -> tuple[str, str]:
    value = safe_float(score)
    if value is None:
        return ("데이터 없음", "neutral")
    if inverse:
        if value >= 70:
            return ("압력 큼", "bad")
        if value >= 45:
            return ("주의", "warn")
        return ("완화", "good")
    if value >= 75:
        return ("강함", "good")
    if value >= 55:
        return ("보통", "warn")
    return ("약함", "bad")


def build_issue_takeaways(issue: pd.Series) -> list[str]:
    takeaways: list[str] = []
    stage = text_value(issue.get("stage"), "")
    market = text_value(issue.get("market"), "")
    if market != "-":
        takeaways.append(market)
    if stage != "-":
        takeaways.append(stage)
    institutional = safe_float(issue.get("institutional_competition_ratio"))
    if institutional is not None and institutional >= 300:
        takeaways.append(f"기관 {fmt_ratio(institutional)}")
    lockup = safe_float(issue.get("lockup_commitment_ratio"))
    if lockup is not None and lockup > 0:
        takeaways.append(f"확약 {fmt_pct(lockup)}")
    retail = safe_float(issue.get("retail_competition_ratio_live"))
    if retail is not None and retail >= 50:
        takeaways.append(f"청약 {fmt_ratio(retail)}")
    existing = safe_float(issue.get("existing_shareholder_ratio"))
    if existing is not None and existing > 0:
        takeaways.append(f"기존주주 {fmt_pct(existing)}")
    offer = safe_float(issue.get("offer_price"))
    current = safe_float(issue.get("current_price"))
    if offer and current:
        premium = (current / offer - 1.0) * 100.0
        if abs(premium) >= 5:
            takeaways.append(f"공모가대비 {fmt_pct(premium, digits=1, signed=True)}")
    if has_value(issue.get("ir_pdf_url")):
        takeaways.append("IR 자료")
    if has_value(issue.get("dart_viewer_url")):
        takeaways.append("DART")
    deduped: list[str] = []
    seen: set[str] = set()
    for item in takeaways:
        if item in seen:
            continue
        seen.add(item)
        deduped.append(item)
    return deduped[:8]


def render_fact_grid(facts: dict[str, Any], *, columns: int = 2) -> None:
    items = [(str(label), text_value(value, "-")) for label, value in facts.items() if text_value(value, "").strip()]
    if not items:
        st.caption("표시할 상세 항목이 없습니다.")
        return
    cols_per_row = max(1, min(columns, len(items), 3))
    cards = []
    for label, value in items:
        cards.append(
            "<div class='ipo-fact-card'>"
            f"<div class='ipo-fact-label'>{escape(label)}</div>"
            f"<div class='ipo-fact-value'>{escape(value)}</div>"
            "</div>"
        )
    st.markdown(
        f"<div class='ipo-fact-grid cols-{cols_per_row}'>" + "".join(cards) + "</div>",
        unsafe_allow_html=True,
    )


def render_issue_header(issue: pd.Series) -> None:
    name = escape(text_value(issue.get("name"), "종목"))
    summary_bits = [
        text_value(issue.get("market"), ""),
        text_value(issue.get("stage"), ""),
        text_value(issue.get("underwriters"), ""),
    ]
    summary_bits = [bit for bit in summary_bits if bit and bit != "-"]
    summary = " · ".join(summary_bits) if summary_bits else "공모주 상세"
    st.markdown(
        f"<div class='ipo-hero'><h2>{name}</h2><p>{escape(summary)}</p></div>",
        unsafe_allow_html=True,
    )


def render_issue_score_cards(issue: pd.Series) -> None:
    sub_label, sub_tone = score_descriptor(issue.get("subscription_score"))
    list_label, list_tone = score_descriptor(issue.get("listing_quality_score"))
    unlock_label, unlock_tone = score_descriptor(issue.get("unlock_pressure_score"), inverse=True)
    cards = [
        {
            "title": "청약 점수",
            "value": fmt_num(issue.get("subscription_score"), 1),
            "sub": sub_label,
            "tone": sub_tone,
        },
        {
            "title": "상장 품질",
            "value": fmt_num(issue.get("listing_quality_score"), 1),
            "sub": list_label,
            "tone": list_tone,
        },
        {
            "title": "락업 압력",
            "value": fmt_num(issue.get("unlock_pressure_score"), 1),
            "sub": unlock_label,
            "tone": unlock_tone,
        },
    ]
    render_soft_cards(cards, columns=3)


def build_dashboard_spotlight_cards(issues: pd.DataFrame, today: pd.Timestamp) -> list[dict[str, Any]]:
    cards: list[dict[str, Any]] = []
    repo = IPORepository(DATA_DIR)
    prepared = prefill_issue_frame_for_display(issues)

    subscriptions = repo.upcoming_subscriptions(prepared, today, window_days=45)
    subscriptions = subscriptions.loc[pd.to_datetime(subscriptions.get("subscription_start"), errors="coerce").notna()].copy()
    if not subscriptions.empty:
        subs = safe_sort_values(subscriptions, ["subscription_start", "subscription_score"], ascending=[True, False]).head(2)
        for _, row in subs.iterrows():
            cards.append(
                {
                    "title": text_value(row.get("name"), "청약 종목"),
                    "value": compact_date_range_text_short(row.get("subscription_start"), row.get("subscription_end")),
                    "sub": f"청약 · {text_value(row.get('underwriters'), '-')}",
                    "tone": "good",
                }
            )

    listings = repo.upcoming_listings(prepared, today, window_days=60)
    listings = listings.loc[pd.to_datetime(listings.get("listing_date"), errors="coerce").notna()].copy()
    if not listings.empty:
        lst = safe_sort_values(listings, ["listing_date", "listing_quality_score"], ascending=[True, False]).head(2)
        for _, row in lst.iterrows():
            cards.append(
                {
                    "title": text_value(row.get("name"), "상장 종목"),
                    "value": compact_date_text_short(row.get("listing_date")),
                    "sub": f"상장 · {text_value(row.get('market'), '-')}",
                    "tone": "neutral",
                }
            )
    return cards[:4]

def build_dashboard_briefing_lines(bundle: IPODataBundle, issues: pd.DataFrame, snapshot: pd.DataFrame, today: pd.Timestamp) -> list[str]:
    repo = IPORepository(DATA_DIR)
    timeline = repo.build_timeline(issues, bundle.all_unlocks, today, window_days=7)

    def unique_texts(values: list[str]) -> list[str]:
        out: list[str] = []
        seen: set[str] = set()
        for value in values:
            cleaned = str(value or "").strip()
            if not cleaned or cleaned in seen:
                continue
            seen.add(cleaned)
            out.append(cleaned)
        return out

    def summarize_names(df: pd.DataFrame, column: str = "name", limit: int = 2) -> str:
        if df is None or df.empty or column not in df.columns:
            return ""
        return "·".join(unique_texts([text_value(row.get(column)) for _, row in df.head(limit).iterrows()])[:limit])

    lines: list[str] = []
    today_norm = pd.Timestamp(today).normalize()

    market_clause = ""
    if isinstance(snapshot, pd.DataFrame) and not snapshot.empty:
        snapshot_map = {str(row.get("name") or ""): row for _, row in snapshot.iterrows()}
        abnormal_bits: list[str] = []
        for source_name, label in [("NASDAQ", "나스닥"), ("NASDAQ100 Futures", "나스닥선물"), ("KOSDAQ", "코스닥"), ("KOSPI", "코스피"), ("USD/KRW", "원달러")]:
            row = snapshot_map.get(source_name)
            change = safe_float(row.get("change_pct")) if row is not None else None
            if change is None:
                continue
            if abs(change) >= 2.0:
                abnormal_bits.append(f"{label} {fmt_pct(change, 2, signed=True)}")
        if abnormal_bits:
            market_clause = "시장 특이: " + ", ".join(abnormal_bits[:3])
        else:
            summary_bits: list[str] = []
            for source_name, label in [("KOSPI", "코스피"), ("KOSDAQ", "코스닥"), ("NASDAQ", "나스닥")]:
                row = snapshot_map.get(source_name)
                change = safe_float(row.get("change_pct")) if row is not None else None
                if change is not None:
                    summary_bits.append(f"{label} {fmt_pct(change, 2, signed=True)}")
            if summary_bits:
                market_clause = "지수: " + ", ".join(summary_bits)

    today_clause = ""
    if isinstance(timeline, pd.DataFrame) and not timeline.empty:
        dates = pd.to_datetime(timeline.get("date"), errors="coerce")
        today_events = timeline[dates.dt.normalize() == today_norm].copy()
        if not today_events.empty:
            preview = unique_texts([f"{text_value(row.get('name'))}({text_value(row.get('event_type'))})" for _, row in today_events.head(6).iterrows()])[:3]
            extra = len(today_events) - len(preview)
            suffix = f" 외 {extra}건" if extra > 0 else ""
            today_clause = f"오늘 일정 {len(today_events)}건: {', '.join(preview)}{suffix}"

    first_line_parts = [part for part in [market_clause, today_clause] if part]
    if first_line_parts:
        lines.append(" / ".join(first_line_parts) + ".")

    subscriptions = repo.upcoming_subscriptions(issues, today, window_days=7)
    listings = repo.upcoming_listings(issues, today, window_days=7)
    unlocks = repo.upcoming_unlocks(bundle.all_unlocks if isinstance(bundle.all_unlocks, pd.DataFrame) else pd.DataFrame(), today, window_days=7)

    second_parts: list[str] = []
    if not subscriptions.empty:
        top_subs = safe_sort_values(subscriptions, ["subscription_start", "subscription_score"], ascending=[True, False])
        names = summarize_names(top_subs)
        second_parts.append(f"청약 {len(subscriptions)}건" + (f"({names})" if names else ""))
    if not listings.empty:
        top_listings = safe_sort_values(listings, ["listing_date", "listing_quality_score"], ascending=[True, False])
        names = summarize_names(top_listings)
        second_parts.append(f"상장 {len(listings)}건" + (f"({names})" if names else ""))
    if not unlocks.empty:
        unlock_work = unlocks.copy()
        if "unlock_ratio" in unlock_work.columns:
            unlock_work["_unlock_ratio"] = pd.to_numeric(unlock_work.get("unlock_ratio"), errors="coerce")
            unlock_work = safe_sort_values(unlock_work, ["_unlock_ratio", "unlock_date"], ascending=[False, True])
        names = summarize_names(unlock_work)
        second_parts.append(f"보호예수 해제 {len(unlocks)}건" + (f"({names})" if names else ""))
    if second_parts:
        lines.append("7일 캘린더: " + ", ".join(second_parts) + ".")

    if not lines:
        lines.append(f"{compact_date_text(today)} 기준으로 청약·상장·보호예수 일정을 점검할 이슈가 준비 중입니다.")
    return lines[:2]


def render_lab_overview_cards(bundle: IPODataBundle, unified_bundle: UnifiedLabBundle) -> None:
    backtest_versions = BacktestRepository(DATA_DIR).available_versions()
    cards = [
        {
            "title": "락업 이벤트",
            "value": fmt_num(len(bundle.all_unlocks) if isinstance(bundle.all_unlocks, pd.DataFrame) else 0, 0),
            "sub": "보호예수 해제 캘린더",
            "tone": "neutral",
        },
        {
            "title": "실행 신호",
            "value": fmt_num(len(unified_bundle.signals) if isinstance(unified_bundle.signals, pd.DataFrame) else 0, 0),
            "sub": "Unified Lab 엔트리 시그널",
            "tone": "good",
        },
        {
            "title": "턴오버 트레이드",
            "value": fmt_num(len(unified_bundle.turnover_trades) if isinstance(unified_bundle.turnover_trades, pd.DataFrame) else 0, 0),
            "sub": "해제 물량 소화 전략",
            "tone": "warn",
        },
        {
            "title": "기본 백테스트",
            "value": fmt_num(len(backtest_versions), 0),
            "sub": "락업 전략 저장 버전",
            "tone": "neutral",
        },
    ]
    render_soft_cards(cards, columns=4)



def render_issue_overview(issue: pd.Series) -> None:
    issue = hydrate_issue_for_display(issue)
    render_issue_header(issue)
    render_badge_row(build_issue_takeaways(issue))

    top_cards = [
        {"title": "공모가", "value": fmt_won(issue.get("offer_price")), "sub": "확정 공모가", "tone": "neutral"},
        {"title": "현재가", "value": fmt_won(issue.get("current_price")), "sub": "최근 반영 가격", "tone": "neutral"},
        {"title": "상장일", "value": compact_date_text_short(issue.get("listing_date")), "sub": "", "tone": "neutral"},
        {"title": "청약 일정", "value": compact_date_range_text_short(issue.get("subscription_start"), issue.get("subscription_end")), "sub": "", "tone": "neutral"},
    ]
    render_soft_cards(top_cards, columns=4)

    stats_cards = [
        {"title": "기관경쟁률", "value": fmt_ratio(issue.get("institutional_competition_ratio")), "sub": "수요예측 기준", "tone": "good" if safe_float(issue.get("institutional_competition_ratio")) not in (None, 0) else "neutral"},
        {"title": "청약경쟁률", "value": fmt_ratio(issue.get("retail_competition_ratio_live")), "sub": "실시간/저장 청약 경쟁률", "tone": "good" if safe_float(issue.get("retail_competition_ratio_live")) not in (None, 0) else "neutral"},
        {"title": "확약비율", "value": fmt_pct(issue.get("lockup_commitment_ratio")), "sub": "의무보유확약", "tone": "good" if safe_float(issue.get("lockup_commitment_ratio")) not in (None, 0) else "neutral"},
        {"title": "유통가능물량", "value": fmt_pct(issue.get("circulating_shares_ratio_on_listing")), "sub": "상장 직후 유통비율", "tone": "warn" if safe_float(issue.get("circulating_shares_ratio_on_listing")) not in (None, 0) else "neutral"},
    ]
    render_soft_cards(stats_cards, columns=4)
    render_issue_score_cards(issue)
    render_score_formula_explainer(issue)

    detail_payload = {
        "종목코드": text_value(issue.get("symbol")),
        "시장": text_value(issue.get("market")),
        "업종": text_value(issue.get("sector")),
        "주관사": text_value(issue.get("underwriters")),
        "수요예측일": compact_date_text(issue.get("forecast_date")),
        "기존주주비율": fmt_pct(issue.get("existing_shareholder_ratio")),
        "우리사주 실권": fmt_pct(issue.get("employee_forfeit_ratio")),
        "구주매출비중": fmt_pct(issue.get("secondary_sale_ratio")),
        "총공모주식수": fmt_num(issue.get("total_offer_shares"), 0),
        "상장후총주식수": fmt_num(issue.get("post_listing_total_shares"), 0),
    }
    visible_details = {
        key: value
        for key, value in detail_payload.items()
        if key in {"종목코드", "시장", "업종", "주관사"} or (str(value).strip() not in {"-", "", "nan", "NaN"})
    }

    left, right = st.columns([1.15, 0.85])
    with left:
        st.markdown("**핵심 정보**")
        render_fact_grid(visible_details, columns=2)
    with right:
        st.markdown("**수요예측 / 증권신고서**")
        doc_cards: list[str] = []
        doc_cards.append(
            "<div class='ipo-fact-card'>"
            f"<div class='ipo-fact-label'>수요예측일</div>"
            f"<div class='ipo-fact-value'>{escape(compact_date_text_short(issue.get('forecast_date')))}</div>"
            "</div>"
        )
        ir_value = (
            f"준비됨 · {compact_date_text_short(issue.get('ir_date'), default='최근 PDF 미확인')}"
            if has_value(issue.get("ir_pdf_url"))
            else "미연결"
        )
        doc_cards.append(
            "<div class='ipo-fact-card'>"
            f"<div class='ipo-fact-label'>IR 자료</div>"
            f"<div class='ipo-fact-value'>{escape(ir_value)}</div>"
            "</div>"
        )
        filing_value = (
            f"연결됨 · {compact_date_text_short(issue.get('dart_filing_date'), default=text_value(issue.get('dart_report_nm'), '접수일 미확인'))}"
            if has_value(issue.get("dart_viewer_url"))
            else "미연결"
        )
        dart_links = []
        if has_value(issue.get("dart_viewer_url")):
            dart_links.append(link_chip_html("증권신고서 열기", issue.get("dart_viewer_url")))
        if has_value(issue.get("ir_pdf_url")):
            dart_links.append(link_chip_html("IR PDF 열기", issue.get("ir_pdf_url")))
        if has_value(issue.get("ir_source_page")):
            dart_links.append(link_chip_html("IR 자료실", issue.get("ir_source_page")))
        link_html = f"<div class='ipo-link-row'>{''.join(dart_links)}</div>" if dart_links else ""
        doc_cards.append(
            "<div class='ipo-fact-card'>"
            f"<div class='ipo-fact-label'>증권신고서</div>"
            f"<div class='ipo-fact-value'>{escape(filing_value)}</div>"
            f"{link_html}"
            "</div>"
        )
        st.markdown("<div class='ipo-fact-grid cols-1'>" + "".join(doc_cards) + "</div>", unsafe_allow_html=True)
        if has_value(issue.get("notes")):
            st.markdown(f"<div class='ipo-note'>{escape(text_value(issue.get('notes'), ''))}</div>", unsafe_allow_html=True)

def render_dashboard(
    bundle: IPODataBundle,
    today: pd.Timestamp,
    prefer_live: bool,
    allow_sample_fallback: bool,
    backtest_version: str,
    source_mode: str,
) -> None:
    repo = IPORepository(DATA_DIR)
    issues = prefill_issue_frame_for_display(add_issue_scores(bundle.issues))
    snapshot_bundle = load_market_snapshot_bundle_cached(prefer_live, True)
    snapshot = snapshot_bundle["frame"]
    snapshot_source = snapshot_bundle["source"]
    snapshot_diag = snapshot_bundle.get("diagnostics", pd.DataFrame())
    market_service = MarketService(DATA_DIR, kis_client=KISClient.from_env())
    mood = market_service.market_mood(snapshot)
    issue_counts = count_issue_sources(issues)

    subscription_count = int(len(repo.upcoming_subscriptions(issues, today, window_days=30)))
    listing_count = int(len(repo.upcoming_listings(issues, today, window_days=30)))

    snapshot_map: dict[str, dict[str, Any]] = {}
    if isinstance(snapshot, pd.DataFrame) and not snapshot.empty:
        work = snapshot.copy()
        work["name"] = work.get("name", pd.Series(dtype="object")).astype(str)
        for _, row in work.iterrows():
            snapshot_map[str(row.get("name") or "")] = row.to_dict()

    brief_lines = build_dashboard_briefing_lines(bundle, issues, snapshot, today)
    hero_items = "".join(f"<li>{escape(line)}</li>" for line in brief_lines)
    st.markdown(
        f"<div class='ipo-hero'><h2>오늘 브리핑</h2><ul class='ipo-brief-list'>{hero_items}</ul></div>",
        unsafe_allow_html=True,
    )

    market_cards = []
    for source_name, label in [("KOSPI", "코스피"), ("KOSDAQ", "코스닥"), ("NASDAQ100 Futures", "나스닥선물"), ("Gold", "금")]:
        row = snapshot_map.get(source_name, {})
        value_color, sub_color = market_move_colors(row.get("change_pct") if row else None)
        market_cards.append(
            {
                "title": label,
                "value": fmt_num(row.get("last"), 0) if row else "준비 중",
                "sub": fmt_pct(row.get("change_pct"), 2, signed=True) if row else "데이터 미수신",
                "tone": "neutral",
                "value_color": value_color,
                "sub_color": sub_color,
            }
        )
    render_soft_cards(market_cards, columns=4)

    fx_row = snapshot_map.get("USD/KRW")
    mood_delta = "-" if mood.get("score") is None else f"score {fmt_num(mood.get('score'), 1)}"
    fx_value_color, fx_sub_color = market_move_colors(fx_row.get("change_pct") if fx_row else None)
    info_cards = [
        {"title": "30일 내 청약", "value": fmt_num(subscription_count, 0), "sub": "청약 일정 종목 수", "tone": "good" if subscription_count else "neutral"},
        {"title": "30일 내 상장", "value": fmt_num(listing_count, 0), "sub": "상장 예정 종목 수", "tone": "neutral"},
        {"title": "환율", "value": fmt_num(fx_row.get("last"), 0) if fx_row else "준비 중", "sub": fmt_pct(fx_row.get("change_pct"), 2, signed=True) if fx_row else "데이터 미수신", "tone": "neutral", "value_color": fx_value_color, "sub_color": fx_sub_color},
        {"title": "시장 분위기", "value": str(mood.get("label") or "데이터없음"), "sub": mood_delta, "tone": "warn" if str(mood.get("label") or "") == "조심" else "good" if str(mood.get("label") or "") == "우호" else "neutral"},
    ]
    render_soft_cards(info_cards, columns=4)

    spotlight_cards = build_dashboard_spotlight_cards(issues, today)
    if spotlight_cards:
        st.markdown("**가까운 일정**")
        render_soft_cards(spotlight_cards, columns=min(4, len(spotlight_cards)))

    render_sample_data_warning(source_mode, issue_counts, snapshot_source)
    snapshot_saved_at = snapshot_bundle.get("saved_at")
    snapshot_asof = market_asof_summary(snapshot)
    caption_bits = [f"시장 소스: {snapshot_source}"]
    if snapshot_asof != "-":
        caption_bits.append(snapshot_asof)
    if has_value(snapshot_saved_at):
        caption_bits.append(f"저장 {compact_datetime_text(snapshot_saved_at)}")
    st.caption(" · ".join(caption_bits))
    st.markdown("---")
    render_calendar_page(bundle, issues, today, show_header=False, show_summary=False)

    if prefer_live and isinstance(snapshot_diag, pd.DataFrame) and not snapshot_diag.empty:
        render_market_diagnostics(snapshot_diag, title="시장 진단 로그", only_failures=False)


def render_explorer(bundle: IPODataBundle, prefer_live: bool) -> None:
    st.subheader("딜 탐색기")
    st.caption("샘플, 실데이터, 전략 데이터를 한 화면에서 필터링하고 상세 확인하는 화면입니다.")
    issues = prefill_issue_frame_for_display(add_issue_scores(bundle.issues))
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
    detail_budget = min(len(filtered), 96)
    quote_budget = min(len(filtered), 1500)
    filtered = prepare_issue_frame_for_table(filtered, detail_budget=detail_budget, quote_budget=quote_budget)

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
    ]].copy()
    display["listing_date"] = pd.to_datetime(display["listing_date"], errors="coerce").dt.strftime("%y.%m.%d")
    display["offer_price"] = display["offer_price"].map(compact_offer_text)
    display["current_price"] = [current_price_cell_text(row) for _, row in filtered.iterrows()]
    display = display.rename(columns={
        "name": "종목명",
        "market": "시장",
        "stage": "단계",
        "underwriters": "주관사",
        "listing_date": "상장일",
        "offer_price": "공모가",
        "current_price": "현재가",
        "subscription_score": "청약점수",
        "listing_quality_score": "상장품질",
        "unlock_pressure_score": "락업압력",
    })
    render_scrollable_table(display, key="deal_explorer_table")
    render_download_button("탐색 결과 CSV 내려받기", display, "deal_explorer.csv")

    issue = issue_selector(filtered.reset_index(drop=True), key="explorer_issue")
    if issue is None:
        return

    t1, t2, t3 = st.tabs(["개요", "공시/IR", "기술/시세"])
    with t1:
        render_issue_overview(issue)
    with t2:
        st.markdown("**연결 가능한 문서 링크**")
        render_issue_resource_links(hydrate_issue_for_display(issue), show_header=False)
        if DartClient.from_env() is None:
            st.info("DART_API_KEY에 Open DART 인증키를 넣으면 최근 공시를 종목별로 바로 조회할 수 있습니다.")
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
            source_label = text_value(live_signal.get("provider"), "KIS")
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
    work = prefill_issue_frame_for_display(work)
    stage = work.get("stage", pd.Series(index=work.index, dtype="object")).fillna("").astype(str)
    sub_start = pd.to_datetime(work.get("subscription_start"), errors="coerce")
    sub_end = pd.to_datetime(work.get("subscription_end"), errors="coerce")
    in_window = (
        (sub_start.notna() & (sub_start >= today - pd.Timedelta(days=14)) & (sub_start <= today + pd.Timedelta(days=180)))
        | (sub_end.notna() & (sub_end >= today - pd.Timedelta(days=14)) & (sub_end <= today + pd.Timedelta(days=180)))
    )
    primary = stage.isin(["청약예정", "청약중"]) & in_window
    recent_done = stage.eq("청약완료") & in_window
    out = work.loc[primary | recent_done | in_window].copy()
    if out.empty:
        return out
    return issue_recency_sort(out, today=today)


def select_listing_candidates(issues: pd.DataFrame, today: pd.Timestamp | None = None) -> pd.DataFrame:
    today = pd.Timestamp(today or today_kst()).normalize()
    work = add_issue_scores(issues)
    if work.empty:
        return work
    work = prefill_issue_frame_for_display(work)
    listing = pd.to_datetime(work.get("listing_date"), errors="coerce")
    window = listing.notna() & (listing >= today - pd.Timedelta(days=90)) & (listing <= today + pd.Timedelta(days=120))
    out = work.loc[window].copy()
    if out.empty:
        return out
    return safe_sort_values(out, ["listing_date", "listing_quality_score"], ascending=[False, False]).reset_index(drop=True)




def render_subscription_page(issues: pd.DataFrame, today: pd.Timestamp) -> None:
    st.subheader("청약 단계")
    st.caption("기관경쟁률, 의무보유확약, IR 자료, 비례청약 손익분기까지 한 번에 보는 화면입니다.")
    base_candidates = select_subscription_candidates(issues, today=today)
    df = prepare_issue_frame_for_table(base_candidates, detail_budget=len(base_candidates), quote_budget=min(len(base_candidates), 600))
    if not df.empty:
        sub_start = pd.to_datetime(df.get("subscription_start"), errors="coerce")
        sub_end = pd.to_datetime(df.get("subscription_end"), errors="coerce")
        df = df.loc[sub_start.notna() | sub_end.notna()].copy()
    if df.empty:
        st.info("현재 불러온 일정 기준으로 미래/최근 청약 종목이 없습니다. 캐시를 새로고침했는데도 비어 있으면 실제 예정 종목이 없는 상태일 가능성이 큽니다.")
        return

    sorted_df = safe_sort_values(df, ["subscription_start", "subscription_end", "subscription_score"], ascending=[False, False, False]).reset_index(drop=True)
    ir_count = int(sorted_df["ir_pdf_url"].map(has_value).sum()) if "ir_pdf_url" in sorted_df.columns else 0
    demand_count = int(pd.to_numeric(sorted_df.get("institutional_competition_ratio", pd.Series(dtype="object")), errors="coerce").notna().sum())
    if "subscription_start" in sorted_df.columns:
        _sub_dates = pd.to_datetime(sorted_df.get("subscription_start"), errors="coerce")
        _active_mask = sorted_df.get("stage", pd.Series(index=sorted_df.index, dtype="object")).fillna("").astype(str).isin(["청약예정", "청약중", "청약완료"])
        _window_dates = _sub_dates[_active_mask]
        _future_dates = _window_dates[_window_dates >= pd.Timestamp(today).normalize() - pd.Timedelta(days=1)]
        nearest_anchor = _future_dates.min() if not _future_dates.empty else _window_dates.min()
        nearest_date = compact_date_text_short(nearest_anchor)
    else:
        nearest_date = "-"
    summary_cards = [
        {"title": "표시 종목", "value": fmt_num(len(sorted_df), 0), "sub": "청약 예정·진행·완료 포함", "tone": "neutral"},
        {"title": "가장 가까운 청약", "value": nearest_date, "sub": "시작일 기준", "tone": "good"},
        {"title": "수요예측 결과", "value": fmt_num(demand_count, 0), "sub": "기관경쟁률 기재 종목", "tone": "good" if demand_count else "neutral"},
        {"title": "IR 연결", "value": fmt_num(ir_count, 0), "sub": "PDF 링크 포함", "tone": "neutral"},
    ]
    render_soft_cards(summary_cards, columns=4)

    left, right = st.columns([1.35, 0.95])
    issue = None
    with left:
        display = pd.DataFrame(
            {
                "종목명": sorted_df.get("name"),
                "시장": sorted_df.get("market"),
                "단계": sorted_df.get("stage"),
                "청약": [compact_date_range_text_short(s, e) for s, e in zip(sorted_df.get("subscription_start"), sorted_df.get("subscription_end"))],
                "수요예측": sorted_df.get("forecast_date").map(compact_date_text_short),
                "주관사": sorted_df.get("underwriters"),
                "희망가": [compact_price_band_text(low, high) for low, high in zip(sorted_df.get("price_band_low"), sorted_df.get("price_band_high"))],
                "공모가": sorted_df.get("offer_price").map(compact_offer_text),
                "기관경쟁률": sorted_df.get("institutional_competition_ratio").map(fmt_ratio),
                "확약": sorted_df.get("lockup_commitment_ratio").map(lambda v: compact_ratio_text(v, digits=1)),
                "IR": sorted_df.get("ir_pdf_url", pd.Series(index=sorted_df.index, dtype="object")).map(lambda v: "PDF" if has_value(v) else "-"),
                "점수": sorted_df.get("subscription_score").map(lambda v: fmt_num(v, 1)),
            }
        )
        render_scrollable_table(display, key="subscription_table")
        render_download_button("청약 후보 CSV 내려받기", display, "subscriptions.csv")
        issue = issue_selector(sorted_df, key="subscription_issue")

    with right:
        if issue is not None:
            render_issue_overview(issue)
        st.markdown("---")
        st.markdown("**비례청약 손익분기 계산기**")
        issue_names = sorted_df["name"].tolist()
        selected_name = st.selectbox("계산 기준 종목", options=issue_names, key="calc_issue")
        calc_issue = sorted_df[sorted_df["name"] == selected_name].iloc[0]
        default_offer_price = int(safe_float(calc_issue.get("offer_price"), 10000.0) or 10000)
        deposit_amount = st.number_input("투입 증거금(원)", min_value=100000, step=100000, value=1000000)
        offer_price = st.number_input("공모가(원)", min_value=1000, step=100, value=default_offer_price)
        target_sell_price = st.number_input("예상 매도가(원)", min_value=1000, step=100, value=int(default_offer_price * 1.3))
        default_ratio = safe_float(calc_issue.get("retail_competition_ratio_live"), 500.0) or 500.0
        competition_ratio = st.number_input("비례 경쟁률(대 1)", min_value=1.0, value=float(default_ratio), step=10.0)
        fee = st.number_input("청약 수수료(원)", min_value=0, value=2000, step=500)
        result = proportional_subscription_model(
            deposit_amount=float(safe_float(deposit_amount, 0.0) or 0.0),
            offer_price=float(safe_float(offer_price, 1000.0) or 1000.0),
            target_sell_price=float(safe_float(target_sell_price, 0.0) or 0.0),
            competition_ratio=float(safe_float(competition_ratio, 1.0) or 1.0),
            fee=float(safe_float(fee, 0.0) or 0.0),
        )
        calc_cards = [
            {"title": "예상 배정 주수", "value": f"{result.expected_allocated_shares:,.2f}주", "sub": "비례 경쟁률 가정", "tone": "neutral"},
            {"title": "예상 손익", "value": fmt_won(result.expected_pnl, 0), "sub": "수수료 포함", "tone": "good" if safe_float(result.expected_pnl) not in (None, 0) else "neutral"},
            {"title": "주당 예상 차익", "value": fmt_won(result.expected_profit_per_share, 0), "sub": "예상 매도가 기준", "tone": "neutral"},
            {"title": "손익분기 경쟁률", "value": "-" if result.break_even_competition_ratio is None else f"{result.break_even_competition_ratio:,.2f}:1", "sub": "예상 매도가 기준", "tone": "warn"},
        ]
        render_soft_cards(calc_cards, columns=2)
        st.caption("실제 배정은 증권사별 균등/비례 구조와 반올림 규칙에 따라 달라질 수 있습니다.")



def render_listing_page(issues: pd.DataFrame, prefer_live: bool, today: pd.Timestamp) -> None:
    st.subheader("상장 단계")
    st.caption("확약, 유통비율, 기존주주비율, IR 자료와 기술신호를 같이 확인하는 화면입니다.")
    listing_candidates = select_listing_candidates(issues, today=today)
    target_df = prepare_issue_frame_for_table(listing_candidates, detail_budget=len(listing_candidates), quote_budget=min(len(listing_candidates), 500))
    if target_df.empty:
        st.info("표시할 상장 종목이 없습니다.")
        return

    if not target_df.empty:
        target_df = target_df.loc[pd.to_datetime(target_df.get("listing_date"), errors="coerce").notna()].copy()
    sorted_target = safe_sort_values(target_df, ["listing_date", "listing_quality_score"], ascending=[False, False]).reset_index(drop=True)
    ir_count = int(sorted_target["ir_pdf_url"].map(has_value).sum()) if "ir_pdf_url" in sorted_target.columns else 0
    lockup_count = int(pd.to_numeric(sorted_target.get("lockup_commitment_ratio", pd.Series(dtype="object")), errors="coerce").notna().sum())
    latest_listing = compact_date_text_short(pd.to_datetime(sorted_target.get("listing_date"), errors="coerce").max()) if "listing_date" in sorted_target.columns else "-"
    summary_cards = [
        {"title": "표시 종목", "value": fmt_num(len(sorted_target), 0), "sub": "최근 90일 + 예정 120일", "tone": "neutral"},
        {"title": "가장 최근 상장", "value": latest_listing, "sub": "상장일 기준", "tone": "neutral"},
        {"title": "확약 데이터", "value": fmt_num(lockup_count, 0), "sub": "의무보유확약 기재 종목", "tone": "good" if lockup_count else "neutral"},
        {"title": "IR 연결", "value": fmt_num(ir_count, 0), "sub": "PDF 링크 포함", "tone": "neutral"},
    ]
    render_soft_cards(summary_cards, columns=4)

    display = pd.DataFrame(
        {
            "종목명": sorted_target.get("name"),
            "시장": sorted_target.get("market"),
            "상장일": sorted_target.get("listing_date").map(compact_date_text_short),
            "청약": [compact_date_range_text_short(s, e) for s, e in zip(sorted_target.get("subscription_start"), sorted_target.get("subscription_end"))],
            "기관경쟁률": sorted_target.get("institutional_competition_ratio").map(fmt_ratio),
            "공모가": sorted_target.get("offer_price").map(compact_offer_text),
            "확약": sorted_target.get("lockup_commitment_ratio").map(lambda v: compact_ratio_text(v, digits=1)),
            "유통": sorted_target.get("circulating_shares_ratio_on_listing").map(lambda v: compact_ratio_text(v, digits=1)),
            "기존주주": sorted_target.get("existing_shareholder_ratio").map(lambda v: compact_ratio_text(v, digits=1)),
            "현재가": [current_price_cell_text(row) for _, row in sorted_target.iterrows()],
            "IR": sorted_target.get("ir_pdf_url", pd.Series(index=sorted_target.index, dtype="object")).map(lambda v: "PDF" if has_value(v) else "-"),
            "점수": sorted_target.get("listing_quality_score").map(lambda v: fmt_num(v, 1)),
        }
    )
    render_scrollable_table(display, key="listing_table")
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
            source_label = text_value(live_signal.get("provider"), "KIS")
    tech_cards = [
        {"title": "품질점수", "value": fmt_num(issue.get("listing_quality_score"), 1), "sub": "상장 품질 종합", "tone": "good"},
    ]
    if any(safe_float(x) is not None for x in [current_price, ma20, ma60, rsi14]):
        tech_cards = [
            {"title": "기술신호", "value": text_value(signal), "sub": f"소스 {source_label}", "tone": "good" if text_value(signal) in {"강세", "회복", "상승추세"} else "warn" if text_value(signal) in {"중립", "과열권", "과매도권"} else "neutral"},
            {"title": "MA20", "value": fmt_won(ma20), "sub": "20일 이동평균", "tone": "neutral"},
            {"title": "MA60", "value": fmt_won(ma60), "sub": "60일 이동평균", "tone": "neutral"},
            {"title": "RSI14", "value": fmt_num(rsi14, 1), "sub": "상대강도지수", "tone": "neutral"},
            {"title": "품질점수", "value": fmt_num(issue.get("listing_quality_score"), 1), "sub": "상장 품질 종합", "tone": "good"},
        ]
    render_soft_cards(tech_cards, columns=len(tech_cards))
    if not history.empty:
        chart_df = history[["date", "close"]].rename(columns={"date": "날짜", "close": "종가"}).set_index("날짜")
        st.line_chart(chart_df)


def render_strategy_bridge_page(bundle: IPODataBundle, issues: pd.DataFrame, today: pd.Timestamp, version: str) -> None:
    st.subheader("전략 브릿지")
    st.caption("보호예수 해제 자동매수 백테스트와 현재 unlock 캘린더를 붙여 매수 후보를 골라봅니다.")
    strategy_bridge = StrategyBridge(DATA_DIR)
    term_edge = strategy_bridge.term_edge_table(version)
    if term_edge.empty:
        st.info("전략 요약 데이터가 없습니다.")
    else:
        st.markdown("**기간별 히스토리컬 엣지**")
        st.dataframe(term_edge, hide_index=True, use_container_width=True)

    candidates = strategy_bridge.rank_upcoming_unlock_candidates(bundle.all_unlocks, issues, today, version, horizon_days=60)
    st.markdown("**향후 60일 unlock 후보 랭킹**")
    if candidates.empty:
        st.info("향후 60일 내 unlock 후보가 없습니다. 통합 lab는 메뉴 2를 먼저 실행하면 workspace/dataset_out/synthetic_ipo_events.csv가 생기고, 메뉴 4~5 이후에는 unlock_out/unlock_events_backtest_input.csv가 생깁니다. 두 파일이 아직 없으면 이 화면도 비어 있을 수 있습니다.")
    else:
        view = candidates.copy()
        view["unlock_date"] = pd.to_datetime(view["unlock_date"], errors="coerce").dt.strftime("%Y-%m-%d")
        st.dataframe(view, hide_index=True, use_container_width=True)
        render_download_button("전략 후보 CSV 내려받기", view, f"strategy_candidates_v{version.replace('.', '_')}.csv")

    heatmap = strategy_bridge.monthly_unlock_heatmap(bundle.all_unlocks)
    if not heatmap.empty:
        st.markdown("**월별 unlock 분포**")
        st.dataframe(heatmap, hide_index=True, use_container_width=True)


def render_lockup_strategy_page(bundle: IPODataBundle, issues: pd.DataFrame, today: pd.Timestamp, version: str, prefer_live: bool, unified_bundle: UnifiedLabBundle) -> None:
    st.subheader("락업 매수전략")
    st.caption("DART 지표 + 해제 캘린더 + 백테스트 규칙을 한 화면에서 묶어 실제 실행 후보를 정리합니다.")

    def entry_filter_label(value: Any) -> str:
        if value is True:
            return "통과"
        if value is False:
            return "미달"
        return "미확인"

    service = LockupStrategyService(DATA_DIR)
    c1, c2, c3, c4 = st.columns([0.9, 1.25, 1.2, 0.9])
    horizon_days = c1.slider("앞으로 볼 기간(일)", min_value=7, max_value=180, value=90, step=7)

    board = service.build_strategy_board(bundle.all_unlocks, issues, today, version, horizon_days=horizon_days)
    if board.empty:
        st.info("표시할 보호예수 해제 전략 후보가 없습니다. integrated lab 메뉴 2를 먼저 실행하면 workspace/dataset_out/synthetic_ipo_events.csv가 생기고 자동 연결됩니다. 메뉴 4~5 이후 unlock_out 산출물이 생기면 실제 unlock 이벤트 기준으로 더 구체화됩니다.")
        return
    board = board.copy()
    board["strategy_version"] = version
    unified_service = UnifiedLabBridgeService(DATA_DIR)
    if unified_bundle.paths.workspace is not None:
        board = unified_service.enrich_strategy_board(board, unified_bundle, today=today)

    term_options = sorted([x for x in board["term"].dropna().astype(str).unique().tolist() if x])
    decision_order = [x for x in ["우선검토", "관찰강화", "관찰", "보류"] if x in board["decision"].astype(str).unique().tolist()]
    selected_terms = c2.multiselect("term 필터", options=term_options, default=term_options)
    selected_decisions = c3.multiselect("판단 필터", options=decision_order, default=decision_order)
    only_entry_ready = c4.checkbox("진입배수 미달 제외", value=False)

    d1, d2, d3, d4 = st.columns([1.1, 1.0, 1.0, 1.1])
    market_options = sorted([x for x in board.get("market", pd.Series(dtype="object")).dropna().astype(str).unique().tolist() if x])
    selected_markets = d1.multiselect("시장 필터", options=market_options, default=market_options)
    positive_edge_only = d2.checkbox("히스토리컬 edge 양수만", value=False)
    dart_only = d3.checkbox("DART 보강값 있는 종목만", value=False)
    sort_by = d4.selectbox("정렬 기준", ["conviction_score", "combined_score", "days_left"], index=0)

    filtered = board.copy()
    if selected_terms:
        filtered = filtered[filtered["term"].astype(str).isin(selected_terms)]
    if selected_decisions:
        filtered = filtered[filtered["decision"].astype(str).isin(selected_decisions)]
    if selected_markets:
        filtered = filtered[filtered["market"].astype(str).isin(selected_markets)]
    if only_entry_ready and "entry_filter_pass" in filtered.columns:
        filtered = filtered[filtered["entry_filter_pass"] != False]
    if positive_edge_only:
        filtered = filtered[pd.to_numeric(filtered["historical_edge"], errors="coerce").fillna(0) > 0]
    if dart_only:
        filtered = filtered[
            filtered[[c for c in ["dart_receipt_no", "secondary_sale_ratio", "total_offer_shares", "dart_filing_date"] if c in filtered.columns]]
            .notna()
            .any(axis=1)
        ]

    if filtered.empty:
        st.warning("현재 필터 조건을 만족하는 후보가 없습니다.")
        return

    sort_columns = {
        "conviction_score": ["decision_rank", "conviction_score", "combined_score", "days_left"],
        "combined_score": ["decision_rank", "combined_score", "conviction_score", "days_left"],
        "days_left": ["days_left", "decision_rank", "conviction_score"],
    }
    ascending_map = {
        "conviction_score": [True, False, False, True],
        "combined_score": [True, False, False, True],
        "days_left": [True, True, False],
    }
    filtered = filtered.sort_values(sort_columns[sort_by], ascending=ascending_map[sort_by]).reset_index(drop=True)

    order_sheet = service.build_order_sheet(filtered, min_decision_rank=2)
    summary = service.decision_summary(filtered)
    summary_map = summary.set_index("decision").to_dict(orient="index") if not summary.empty else {}
    bridge_count = 0
    if "bridge_status" in filtered.columns:
        bridge_count = int(len(filtered[filtered["bridge_status"].astype(str).isin(["신호발생", "수집대기", "수집중", "데이터적재"]) ]))
    m1, m2, m3, m4, m5, m6 = st.columns(6)
    m1.metric("우선검토", int(summary_map.get("우선검토", {}).get("candidates", 0)))
    m2.metric("관찰강화", int(summary_map.get("관찰강화", {}).get("candidates", 0)))
    m3.metric("관찰", int(summary_map.get("관찰", {}).get("candidates", 0)))
    m4.metric("상위 5개 평균 conviction", fmt_num(filtered.head(5)["conviction_score"].mean(), 1))
    m5.metric("5분봉 연결후보", bridge_count)
    m6.metric("주문시트 후보", len(order_sheet))

    with st.expander("기간별 실행 규칙 / 백테스트 룰", expanded=False):
        rules = service.term_rules(version).copy()
        if rules.empty:
            st.info("기간별 규칙을 읽지 못했습니다.")
        else:
            rules_view = rules[[c for c in [
                "term",
                "entry_rule",
                "hold_days_after_entry",
                "min_prev_close_vs_ipo_pct",
                "trades",
                "win_rate",
                "avg_ret",
                "compound_ret",
                "median_calendar_lag",
            ] if c in rules.columns]].copy()
            if "min_prev_close_vs_ipo_pct" in rules_view.columns:
                rules_view["min_prev_close_vs_ipo_pct"] = rules_view["min_prev_close_vs_ipo_pct"].map(lambda x: "-" if pd.isna(x) else f"{x:,.2f}%")
            st.dataframe(rules_view, hide_index=True, use_container_width=True)
            st.caption("공모가 대비 배수 필터는 백테스트의 prev_close_vs_ipo 최소조건을 현재가 기준으로 대략 대체한 값입니다.")

    board_view = filtered[[c for c in [
        "name",
        "term",
        "unlock_date",
        "days_left",
        "decision",
        "priority_tier",
        "combined_score",
        "conviction_score",
        "historical_edge",
        "win_rate",
        "entry_rule",
        "planned_entry_date",
        "planned_exit_date",
        "current_vs_offer_pct",
        "min_prev_close_vs_ipo_pct",
        "entry_filter_pass",
        "technical_signal",
        "lockup_commitment_ratio",
        "circulating_shares_ratio_on_listing",
        "existing_shareholder_ratio",
        "secondary_sale_ratio",
        "suggested_weight_pct_of_base",
        "bridge_status",
        "minute_job_status",
        "turnover_signal_hits",
        "turnover_first_signal_ts",
        "turnover_best_ratio",
        "turnover_backtest_avg_net_ret_pct",
        "rationale",
    ] if c in filtered.columns]].copy()
    for col in ["unlock_date", "planned_entry_date", "planned_exit_date", "turnover_first_signal_ts"]:
        if col in board_view.columns:
            board_view[col] = pd.to_datetime(board_view[col], errors="coerce").dt.strftime("%Y-%m-%d")
    if "entry_filter_pass" in board_view.columns:
        board_view["entry_filter_pass"] = board_view["entry_filter_pass"].map(entry_filter_label)

    st.markdown("**실행 보드**")
    st.dataframe(board_view, hide_index=True, use_container_width=True)
    dl1, dl2 = st.columns(2)
    with dl1:
        render_download_button("전략 보드 CSV 내려받기", filtered, f"lockup_strategy_board_v{version.replace('.', '_')}.csv")
    with dl2:
        if order_sheet.empty:
            st.info("우선검토/관찰강화 후보가 없어 주문시트를 만들지 않았습니다.")
        else:
            render_download_button("주문시트 CSV 내려받기", order_sheet, f"lockup_order_sheet_v{version.replace('.', '_')}.csv")

    candidate = strategy_candidate_selector(filtered, key="lockup_strategy_candidate")
    if candidate is None:
        return

    st.markdown("---")
    st.markdown("**선택 후보 상세**")
    x1, x2, x3, x4, x5, x6 = st.columns(6)
    x1.metric("판단", text_value(candidate.get("decision")))
    x2.metric("우선순위", text_value(candidate.get("priority_tier")))
    x3.metric("Conviction", fmt_num(candidate.get("conviction_score"), 1))
    x4.metric("진입룰", text_value(candidate.get("entry_rule")))
    x5.metric("예상 진입일", fmt_date(candidate.get("planned_entry_date")))
    x6.metric("예상 종료일", fmt_date(candidate.get("planned_exit_date")))

    t1, t2, t3, t4, t5, t6 = st.tabs(["실행 플랜", "DART / 수급", "5분봉 / turnover", "백테스트 근거", "주가 / 기술", "내보내기"])
    with t1:
        render_issue_overview(candidate)
        timeline = pd.DataFrame(
            [
                {"구분": "사전점검", "일자": fmt_date(candidate.get("planned_check_date")), "내용": "DART/오버행/수급 재확인"},
                {"구분": "진입관찰", "일자": fmt_date(candidate.get("planned_entry_date")), "내용": text_value(candidate.get("entry_rule"))},
                {"구분": "청산예상", "일자": fmt_date(candidate.get("planned_exit_date")), "내용": f"보유 {int(safe_float(candidate.get('hold_days_after_entry'), 0) or 0)} 영업일"},
            ]
        )
        st.markdown("**실행 타임라인**")
        st.dataframe(timeline, hide_index=True, use_container_width=True)
        st.write(
            {
                "판단 메모": text_value(candidate.get("rationale")),
                "강점": text_value(candidate.get("positive_flags")),
                "리스크": text_value(candidate.get("risk_flags")),
                "해제 데이터 출처": humanize_source(candidate.get("unlock_source")),
                "베이스 포지션 대비 권장": f"{int(safe_float(candidate.get('suggested_weight_pct_of_base'), 0) or 0)}%",
            }
        )

    with t2:
        render_issue_dart_overlay_from_issue(candidate)
        dart_client = DartClient.from_env()
        if dart_client is None:
            st.info("DART_API_KEY를 넣으면 선택 후보를 즉시 원문 재분석할 수 있습니다.")
        else:
            force = st.checkbox("캐시 무시하고 다시 분석", value=False, key=f"lockup_dart_force_{normalize_name_key(candidate.get('name'))}_{candidate.get('term')}")
            snapshot_key = f"lockup_dart_snapshot::{normalize_name_key(candidate.get('name'))}::{candidate.get('term')}"
            if st.button("선택 후보 DART 원문 분석", use_container_width=True, key=f"lockup_dart_btn_{normalize_name_key(candidate.get('name'))}_{candidate.get('term')}"):
                with st.spinner("선택 후보의 DART 본문을 분석하는 중입니다..."):
                    if force:
                        load_dart_ipo_snapshot_cached.clear()
                    snapshot = load_dart_ipo_snapshot_cached(
                        text_value(candidate.get("symbol"), ""),
                        text_value(candidate.get("name"), ""),
                        force=force,
                    )
                    st.session_state[snapshot_key] = snapshot
            snapshot = st.session_state.get(snapshot_key)
            if snapshot is not None:
                render_dart_snapshot(snapshot, issue=candidate)

    with t3:
        render_turnover_candidate_context(candidate, unified_bundle)

    with t4:
        rule_df = service.term_rules(version)
        rule_df = rule_df[rule_df["term"].astype(str) == text_value(candidate.get("term"), "")].copy()
        if not rule_df.empty:
            rule = rule_df.iloc[0]
            r1, r2, r3, r4, r5 = st.columns(5)
            r1.metric("백테스트 거래수", fmt_num(rule.get("trades"), 0))
            r2.metric("승률", fmt_pct(rule.get("win_rate"), 2))
            r3.metric("평균수익률", fmt_pct(rule.get("avg_ret"), 2, signed=True))
            r4.metric("복리수익률", fmt_pct(rule.get("compound_ret"), 2, signed=True))
            r5.metric("공모가 대비 최소 배수", "-" if pd.isna(rule.get("min_prev_close_vs_ipo_pct")) else fmt_pct(rule.get("min_prev_close_vs_ipo_pct"), 2))

        examples = service.historical_examples(
            version,
            text_value(candidate.get("term"), ""),
            reference_ratio=safe_float(candidate.get("current_vs_offer_ratio")),
            limit=10,
        )
        st.markdown("**유사 term 과거 거래 예시**")
        if examples.empty:
            st.info("표시할 과거 거래 예시가 없습니다.")
        else:
            ex_view = examples[[c for c in [
                "name",
                "symbol",
                "unlock_date",
                "entry_dt",
                "exit_dt",
                "entry_price_vs_ipo",
                "net_ret_pct",
                "hold_days_after_entry",
                "distance_vs_candidate",
            ] if c in examples.columns]].copy()
            for col in ["unlock_date", "entry_dt", "exit_dt"]:
                if col in ex_view.columns:
                    ex_view[col] = pd.to_datetime(ex_view[col], errors="coerce").dt.strftime("%Y-%m-%d")
            st.dataframe(ex_view, hide_index=True, use_container_width=True)

        skip_breakdown = service.skip_breakdown(version, text_value(candidate.get("term"), ""))
        st.markdown("**같은 term 스킵 사유**")
        if skip_breakdown.empty:
            st.info("스킵 요약 데이터가 없습니다.")
        else:
            skip_view = skip_breakdown[[c for c in ["reason_label", "count", "share_pct"] if c in skip_breakdown.columns]].copy()
            st.dataframe(skip_view, hide_index=True, use_container_width=True)

        recent_skips = service.recent_skip_examples(version, text_value(candidate.get("term"), ""), limit=8)
        if not recent_skips.empty:
            st.markdown("**최근 스킵 예시**")
            rs_view = recent_skips[[c for c in ["name", "symbol", "unlock_date", "reason_label", "prev_close_vs_ipo", "threshold"] if c in recent_skips.columns]].copy()
            if "unlock_date" in rs_view.columns:
                rs_view["unlock_date"] = pd.to_datetime(rs_view["unlock_date"], errors="coerce").dt.strftime("%Y-%m-%d")
            st.dataframe(rs_view, hide_index=True, use_container_width=True)

    with t5:
        current_price = candidate.get("current_price")
        ma20 = candidate.get("ma20")
        ma60 = candidate.get("ma60")
        rsi14 = candidate.get("rsi14")
        history = pd.DataFrame()
        source_label = text_value(candidate.get("source"), "sample")
        if prefer_live and str(candidate.get("symbol", "")).isdigit():
            live_signal = load_kis_signal_cached(str(candidate["symbol"]), prefer_live=prefer_live)
            if live_signal:
                current_price = live_signal.get("current_price")
                ma20 = live_signal.get("ma20")
                ma60 = live_signal.get("ma60")
                rsi14 = live_signal.get("rsi14")
                history = live_signal.get("history", pd.DataFrame())
                source_label = text_value(live_signal.get("provider"), "KIS")
        signal = signal_from_values(current_price, ma20, ma60, rsi14)
        if pd.notna(current_price) and pd.notna(candidate.get("offer_price")) and safe_float(candidate.get("offer_price")) not in {None, 0}:
            premium_live = (float(current_price) / float(candidate.get("offer_price")) - 1.0) * 100
        else:
            premium_live = safe_float(candidate.get("current_vs_offer_pct"))
        q1, q2, q3, q4, q5 = st.columns(5)
        q1.metric("기술신호", signal)
        q2.metric("현재가", fmt_won(current_price))
        q3.metric("공모가 대비", fmt_pct(premium_live, 2, signed=True))
        q4.metric("MA20", fmt_won(ma20))
        q5.metric("RSI14", fmt_num(rsi14, 1))
        st.caption(f"기술신호 소스: {source_label}")
        if not history.empty:
            chart_df = history[["date", "close"]].rename(columns={"date": "날짜", "close": "종가"}).set_index("날짜")
            st.line_chart(chart_df)
        else:
            st.info("KIS 연결이 없거나 데이터가 부족해 저장된 수치만 표시했습니다.")

    with t6:
        selected_df = filtered[(filtered["name"] == candidate.get("name")) & (filtered["term"] == candidate.get("term"))].head(1).copy()
        selected_plan = service.build_order_sheet(selected_df, min_decision_rank=4)
        st.markdown("**선택 후보 실행 프리뷰**")
        if selected_plan.empty:
            st.info("선택 후보 실행 프리뷰를 만들지 못했습니다.")
        else:
            st.dataframe(selected_plan, hide_index=True, use_container_width=True)
            render_download_button(
                "선택 후보 실행 CSV 내려받기",
                selected_plan,
                f"lockup_candidate_{normalize_name_key(candidate.get('name'))}_{candidate.get('term')}.csv",
            )

        st.markdown("**우선검토/관찰강화 전체 주문시트**")
        if order_sheet.empty:
            st.info("현재 기준으로 주문시트 대상이 없습니다.")
        else:
            st.dataframe(order_sheet.head(20), hide_index=True, use_container_width=True)

        payload = {
            "strategy_version": version,
            "symbol": candidate.get("symbol"),
            "name": candidate.get("name"),
            "term": candidate.get("term"),
            "unlock_date": fmt_date(candidate.get("unlock_date")),
            "planned_entry_date": fmt_date(candidate.get("planned_entry_date")),
            "planned_exit_date": fmt_date(candidate.get("planned_exit_date")),
            "entry_rule": candidate.get("entry_rule"),
            "hold_days_after_entry": int(safe_float(candidate.get("hold_days_after_entry"), 0) or 0),
            "decision": candidate.get("decision"),
            "priority_tier": candidate.get("priority_tier"),
            "suggested_weight_pct_of_base": int(safe_float(candidate.get("suggested_weight_pct_of_base"), 0) or 0),
            "memo": candidate.get("rationale"),
        }
        st.markdown("**자동화 연결용 JSON 프리뷰**")
        st.json(payload)


def render_minute_bridge_page(bundle: IPODataBundle, issues: pd.DataFrame, today: pd.Timestamp, version: str, unified_bundle: UnifiedLabBundle) -> None:
    st.subheader("5분봉 브리지")
    st.caption("분리된 키움/CSV minute 연구 파이프라인 산출물을 현재 공모주 앱의 전략 보드와 연결해 봅니다.")
    if unified_bundle.paths.workspace is None:
        st.info("Unified Lab workspace를 아직 찾지 못했습니다. 통합 프로젝트에서는 integrated_lab/ipo_lockup_unified_lab/workspace를 자동 탐지하고, 외부 workspace도 사이드바에서 직접 지정할 수 있습니다.")
        return

    bridge_service = UnifiedLabBridgeService(DATA_DIR)
    st.write(
        {
            "workspace": str(unified_bundle.paths.workspace),
            "unlock csv": str(unified_bundle.paths.unlock_csv or ""),
            "signals csv": str(unified_bundle.paths.signals_csv or ""),
            "minute db": str(unified_bundle.paths.minute_db_path or ""),
            "turnover backtest": str(unified_bundle.paths.turnover_backtest_dir or ""),
        }
    )
    a1, a2, a3, a4, a5, a6 = st.columns(6)
    a1.metric("unlock events", len(unified_bundle.unlocks))
    a2.metric("signal hits", len(unified_bundle.signals))
    a3.metric("signal misses", len(unified_bundle.misses))
    a4.metric("turnover trades", len(unified_bundle.turnover_trades))
    queue_pending = 0
    if not unified_bundle.minute_job_counts.empty and {"status", "jobs"}.issubset(unified_bundle.minute_job_counts.columns):
        queue_pending = int(unified_bundle.minute_job_counts[unified_bundle.minute_job_counts["status"].astype(str).isin(["queued", "running"])] ["jobs"].sum())
    a5.metric("minute queue", queue_pending)
    bars_total = 0
    if not unified_bundle.minute_bar_stats.empty and "bars" in unified_bundle.minute_bar_stats.columns:
        bars_total = int(pd.to_numeric(unified_bundle.minute_bar_stats["bars"], errors="coerce").sum())
    a6.metric("loaded bars", bars_total)

    t1, t2, t3, t4 = st.tabs(["브리지 보드", "신호 / misses", "minute DB", "turnover 백테스트"])

    with t1:
        horizon_days = st.slider("브리지 보드 범위(일)", min_value=14, max_value=180, value=120, step=7, key="minute_bridge_horizon")
        lockup_service = LockupStrategyService(DATA_DIR)
        board = lockup_service.build_strategy_board(bundle.all_unlocks, issues, today, version, horizon_days=horizon_days)
        if board.empty:
            st.info("표시할 향후 unlock 전략 후보가 없습니다.")
        else:
            board = board.copy()
            board["strategy_version"] = version
            board = bridge_service.enrich_strategy_board(board, unified_bundle, today=today)
            bridge_options = sorted([x for x in board.get("bridge_status", pd.Series(dtype="object")).dropna().astype(str).unique().tolist() if x])
            selected_bridge = st.multiselect("bridge 상태 필터", options=bridge_options, default=bridge_options)
            filtered = board.copy()
            if selected_bridge:
                filtered = filtered[filtered["bridge_status"].astype(str).isin(selected_bridge)]
            view = filtered[[c for c in [
                "name",
                "term",
                "unlock_date",
                "decision",
                "priority_tier",
                "bridge_status",
                "minute_job_status",
                "turnover_signal_hits",
                "turnover_first_signal_ts",
                "turnover_best_ratio",
                "turnover_backtest_avg_net_ret_pct",
                "planned_entry_date",
                "planned_exit_date",
                "rationale",
            ] if c in filtered.columns]].copy()
            for col in ["unlock_date", "planned_entry_date", "planned_exit_date", "turnover_first_signal_ts"]:
                if col in view.columns:
                    view[col] = pd.to_datetime(view[col], errors="coerce").dt.strftime("%Y-%m-%d")
            st.dataframe(view, hide_index=True, use_container_width=True)
            render_download_button("브리지 보드 CSV", filtered, f"minute_bridge_board_v{version.replace('.', '_')}.csv")
            export_df = bridge_service.build_execution_bridge_export(filtered, unified_bundle, today=today, min_decision_rank=4)
            if not export_df.empty:
                render_download_button("자동화 브리지 CSV", export_df, f"execution_bridge_v{version.replace('.', '_')}.csv")
            candidate = strategy_candidate_selector(filtered, key="minute_bridge_candidate")
            if candidate is not None:
                render_turnover_candidate_context(candidate, unified_bundle)

    with t2:
        summary = bridge_service.signal_summary(unified_bundle.signals, unified_bundle.misses)
        st.markdown("**signal summary**")
        if summary.empty:
            st.info("signal summary를 만들 데이터가 없습니다.")
        else:
            st.dataframe(summary, hide_index=True, use_container_width=True)
        st.markdown("**signal hits**")
        if unified_bundle.signals.empty:
            st.info("turnover_signals.csv가 비어 있습니다.")
        else:
            hits = unified_bundle.signals.copy()
            for col in ["unlock_date", "entry_ts", "entry_trade_date"]:
                if col in hits.columns:
                    hits[col] = pd.to_datetime(hits[col], errors="coerce").dt.strftime("%Y-%m-%d %H:%M")
            st.dataframe(hits.head(200), hide_index=True, use_container_width=True)
            render_download_button("turnover_signals.csv 내려받기", unified_bundle.signals, "turnover_signals.csv")
        st.markdown("**signal misses**")
        if unified_bundle.misses.empty:
            st.caption("miss 데이터가 없습니다.")
        else:
            misses = unified_bundle.misses.copy()
            if "unlock_date" in misses.columns:
                misses["unlock_date"] = pd.to_datetime(misses["unlock_date"], errors="coerce").dt.strftime("%Y-%m-%d")
            st.dataframe(misses.head(200), hide_index=True, use_container_width=True)
            render_download_button("turnover_signals_misses.csv 내려받기", unified_bundle.misses, "turnover_signals_misses.csv")

    with t3:
        st.markdown("**minute DB source 상태**")
        if unified_bundle.source_status.empty:
            st.info("source 상태가 없습니다.")
        else:
            st.dataframe(unified_bundle.source_status, hide_index=True, use_container_width=True)
        st.markdown("**minute queue counts**")
        if unified_bundle.minute_job_counts.empty:
            st.info("minute queue counts가 없습니다.")
        else:
            st.dataframe(unified_bundle.minute_job_counts, hide_index=True, use_container_width=True)
        st.markdown("**minute job preview**")
        if unified_bundle.minute_job_preview.empty:
            st.caption("minute job preview가 없습니다.")
        else:
            preview = unified_bundle.minute_job_preview.copy()
            for col in ["job_unlock_date", "start_ts", "end_ts", "created_at", "updated_at"]:
                if col in preview.columns:
                    preview[col] = pd.to_datetime(preview[col], errors="coerce").dt.strftime("%Y-%m-%d %H:%M")
            st.dataframe(preview, hide_index=True, use_container_width=True)
        st.markdown("**minute bar stats**")
        if unified_bundle.minute_bar_stats.empty:
            st.caption("bar stats가 없습니다.")
        else:
            bar_stats = unified_bundle.minute_bar_stats.copy()
            for col in ["min_ts", "max_ts"]:
                if col in bar_stats.columns:
                    bar_stats[col] = pd.to_datetime(bar_stats[col], errors="coerce").dt.strftime("%Y-%m-%d %H:%M")
            st.dataframe(bar_stats, hide_index=True, use_container_width=True)
        st.markdown("**unlock coverage**")
        if unified_bundle.minute_symbol_coverage.empty:
            st.caption("unlock coverage가 없습니다.")
        else:
            cov = unified_bundle.minute_symbol_coverage.copy()
            for col in ["unlock_date", "min_ts", "max_ts"]:
                if col in cov.columns:
                    cov[col] = pd.to_datetime(cov[col], errors="coerce").dt.strftime("%Y-%m-%d %H:%M")
            st.dataframe(cov, hide_index=True, use_container_width=True)

    with t4:
        term_summary = bridge_service.turnover_term_summary(unified_bundle.turnover_summary_raw)
        st.markdown("**term edge summary**")
        if term_summary.empty:
            st.info("term 기준 edge 요약이 없습니다.")
        else:
            st.dataframe(term_summary, hide_index=True, use_container_width=True)
        st.markdown("**turnover summary**")
        if unified_bundle.turnover_summary_pretty.empty:
            st.info("summary_all.csv가 없습니다.")
        else:
            st.dataframe(unified_bundle.turnover_summary_pretty, hide_index=True, use_container_width=True)
        st.markdown("**annual summary**")
        if not unified_bundle.turnover_annual_pretty.empty:
            st.dataframe(unified_bundle.turnover_annual_pretty, hide_index=True, use_container_width=True)
        st.markdown("**beta proxy summary**")
        if unified_bundle.beta_summary.empty:
            st.caption("beta proxy summary가 없습니다.")
        else:
            beta = unified_bundle.beta_summary.copy()
            if "alpha_proxy" in beta.columns:
                beta["alpha_proxy_pct"] = pd.to_numeric(beta["alpha_proxy"], errors="coerce") * 100.0
            st.dataframe(beta, hide_index=True, use_container_width=True)
        st.markdown("**turnover trades**")
        if unified_bundle.turnover_trades.empty:
            st.caption("turnover trades가 없습니다.")
        else:
            trades = unified_bundle.turnover_trades.copy()
            for col in ["entry_dt", "exit_dt", "unlock_date"]:
                if col in trades.columns:
                    trades[col] = pd.to_datetime(trades[col], errors="coerce").dt.strftime("%Y-%m-%d")
            st.dataframe(trades.head(200), hide_index=True, use_container_width=True)
        st.markdown("**skip summary**")
        if not unified_bundle.turnover_skip_summary.empty:
            st.dataframe(unified_bundle.turnover_skip_summary, hide_index=True, use_container_width=True)
        if not unified_bundle.turnover_skip_reasons.empty:
            skip_reasons = unified_bundle.turnover_skip_reasons.copy()
            if "unlock_date" in skip_reasons.columns:
                skip_reasons["unlock_date"] = pd.to_datetime(skip_reasons["unlock_date"], errors="coerce").dt.strftime("%Y-%m-%d")
            st.dataframe(skip_reasons.head(200), hide_index=True, use_container_width=True)



def _merge_unlocks_with_seibro(
    unlocks: pd.DataFrame,
    issues: pd.DataFrame,
    seibro_releases: pd.DataFrame | None = None,
) -> pd.DataFrame:
    issue_cols = [
        c
        for c in [
            "name",
            "unlock_pressure_score",
            "market",
            "current_price",
            "offer_price",
            "post_listing_total_shares",
            "existing_shareholder_ratio",
            "lockup_commitment_ratio",
        ]
        if c in issues.columns
    ]
    joined = unlocks.merge(issues[issue_cols], on="name", how="left") if issue_cols else unlocks.copy()
    joined["name_key"] = joined.get("name", pd.Series(dtype="object")).map(normalize_name_key)
    joined["_unlock_date_raw"] = pd.to_datetime(joined.get("unlock_date"), errors="coerce").dt.normalize()

    if any(col in joined.columns for col in ["unlock_shares", "unlock_ratio"]):
        agg_map: dict[str, Any] = {}
        for col in [
            "name",
            "symbol",
            "market",
            "listing_date",
            "unlock_date",
            "term",
            "unlock_type",
            "offer_price",
            "current_price",
            "unlock_pressure_score",
            "post_listing_total_shares",
            "existing_shareholder_ratio",
            "lockup_commitment_ratio",
            "source",
            "note",
        ]:
            if col in joined.columns:
                agg_map[col] = "first"
        if "unlock_shares" in joined.columns:
            agg_map["unlock_shares"] = "sum"
        if "unlock_ratio" in joined.columns:
            agg_map["unlock_ratio"] = "sum"
        group_cols = [c for c in ["name_key", "_unlock_date_raw", "term"] if c in joined.columns]
        joined = joined.groupby(group_cols, dropna=False, as_index=False).agg(agg_map)

    if isinstance(seibro_releases, pd.DataFrame) and not seibro_releases.empty:
        seibro = seibro_releases.copy()
        seibro["name_key"] = seibro.get("name_key", seibro.get("name", pd.Series(dtype="object"))).map(normalize_name_key)
        seibro["release_date"] = pd.to_datetime(seibro.get("release_date"), errors="coerce").dt.normalize()
        seibro = seibro.dropna(subset=["release_date"]).copy()
        if not seibro.empty:
            agg = seibro.groupby(["name_key", "release_date"], dropna=False, as_index=False).agg({
                "name": "first",
                "market": "first",
                "release_shares": "sum",
                "remaining_locked_shares": "sum",
                "source_detail": "first",
            })
            joined = joined.merge(agg, left_on=["name_key", "_unlock_date_raw"], right_on=["name_key", "release_date"], how="left", suffixes=("", "_seibro"))
            denom = pd.to_numeric(joined.get("post_listing_total_shares"), errors="coerce")
            numer = pd.to_numeric(joined.get("release_shares"), errors="coerce")
            joined["release_ratio_of_total"] = ((numer / denom) * 100.0).where((denom > 0) & numer.notna())
    return joined


def render_unlock_page(issues: pd.DataFrame, all_unlocks: pd.DataFrame, today: pd.Timestamp, seibro_releases: pd.DataFrame | None = None) -> None:
    st.subheader("보호예수 해제 / 알림")
    st.caption("보호예수 해제 캘린더와 이례적 가격변동, 기술신호를 함께 관리합니다. Seibro 실제 해제물량은 최근 조회 범위에서 이름/날짜가 맞는 항목만 함께 표시합니다.")
    issues = add_issue_scores(issues)
    alert_engine = AlertEngine()
    if not isinstance(seibro_releases, pd.DataFrame) or seibro_releases.empty:
        seibro_releases = load_seibro_release_schedule_cached()

    unlock_window_days = st.slider("보호예수 해제 표시 범위(일)", min_value=7, max_value=180, value=45, step=7)
    alert_days = st.slider("해제 임박 알림 기준(일)", min_value=1, max_value=30, value=7, step=1)
    move_threshold = st.slider("가격변동 알림 기준(%)", min_value=3.0, max_value=15.0, value=5.0, step=0.5)
    volume_threshold = st.slider("거래량 급증 알림 배수", min_value=1.5, max_value=8.0, value=3.0, step=0.5)

    repo = IPORepository(DATA_DIR)
    unlocks = repo.upcoming_unlocks(all_unlocks, today, window_days=unlock_window_days)
    if unlocks.empty:
        st.info("표시할 보호예수 해제 일정이 없습니다.")
    else:
        joined = _merge_unlocks_with_seibro(unlocks, issues, seibro_releases)

        def col(name: str) -> pd.Series:
            if name in joined.columns:
                return joined[name]
            return pd.Series([pd.NA] * len(joined), index=joined.index, dtype="object")

        display = pd.DataFrame({
            "종목명": col("name"),
            "시장": col("market"),
            "term": col("term"),
            "상장일": pd.to_datetime(col("listing_date"), errors="coerce").dt.strftime("%y.%m.%d"),
            "해제일": pd.to_datetime(col("unlock_date"), errors="coerce").dt.strftime("%y.%m.%d"),
            "예상 해제주식수": col("unlock_shares").map(lambda v: fmt_num(v, 0)),
            "예상 비중": col("unlock_ratio").map(lambda v: compact_ratio_text(v, digits=2)),
            "실제 해제주식수": col("release_shares").map(lambda v: fmt_num(v, 0)),
            "전체주식대비": col("release_ratio_of_total").map(lambda v: compact_ratio_text(v, digits=2)),
            "예수잔량": col("remaining_locked_shares").map(lambda v: fmt_num(v, 0)),
            "공모가": col("offer_price").map(compact_offer_text),
            "현재가": col("current_price").map(compact_offer_text),
            "압력점수": col("unlock_pressure_score").map(lambda v: fmt_num(v, 1)),
        })
        render_scrollable_table(display, key="unlock_table")
        matched_count = int(pd.to_numeric(col("release_shares"), errors="coerce").notna().sum())
        estimated_count = int(pd.to_numeric(col("unlock_shares"), errors="coerce").notna().sum())
        if matched_count and estimated_count:
            st.caption(f"예상 해제물량 {estimated_count}건 · Seibro 실제 해제물량 매칭 {matched_count}건")
        elif matched_count:
            st.caption(f"Seibro 실제 해제물량 매칭 {matched_count}건")
        elif estimated_count:
            st.caption(f"예상 해제물량이 있는 일정 {estimated_count}건")
        else:
            st.caption("현재 조회 범위에서는 예상/실제 해제물량이 연결된 종목이 없습니다.")
        export_joined = joined.copy()
        for export_col in ["listing_date", "unlock_date", "release_date"]:
            if export_col in export_joined.columns:
                export_joined[export_col] = pd.to_datetime(export_joined[export_col], errors="coerce").dt.strftime("%Y-%m-%d")
        render_download_button("unlock 일정 CSV 내려받기", export_joined, "unlock_calendar.csv")

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
        st.info("DART_API_KEY에 Open DART 인증키를 넣으면 선택 종목의 투자설명서/증권신고서 본문을 분석할 수 있습니다.")
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
    snapshot_saved_at = snapshot_bundle.get("saved_at")
    snapshot_asof = market_asof_summary(snapshot)
    top_caption = [f"스냅샷 소스: {source}"]
    if snapshot_asof != "-":
        top_caption.append(snapshot_asof)
    if has_value(snapshot_saved_at):
        top_caption.append(f"저장 {compact_datetime_text(snapshot_saved_at)}")
    top_caption.append(f"시장 분위기: {mood['label']} ({mood['score']})")
    st.caption(" · ".join(top_caption))

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
    history_saved_at = history_bundle.get("saved_at")
    history_diag = history_bundle.get("diagnostics", pd.DataFrame())
    hist_caption = [f"차트 소스: {hist_source}"]
    if not history.empty and "date" in history.columns:
        hist_dates = pd.to_datetime(history["date"], errors="coerce").dropna()
        if not hist_dates.empty:
            hist_caption.append(f"기준 {compact_datetime_text(hist_dates.max())}")
    if has_value(history_saved_at):
        hist_caption.append(f"저장 {compact_datetime_text(history_saved_at)}")
    st.caption(" · ".join(hist_caption))
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

    def render_chip(event: dict[str, Any], *, show_type: bool = False) -> str:
        cls = event_class.get(str(event.get("event_type") or ""), "generic")
        detail = " · ".join([
            str(event.get("event_type") or "").strip(),
            str(event.get("name") or "").strip(),
            str(event.get("detail") or "").strip(),
        ]).strip(" ·")
        name = str(event.get("name") or "-").strip()
        event_type = str(event.get("event_type") or "").strip()
        label = f"{event_type} · {name}" if show_type and event_type else name
        return f'<div class="event-chip {cls}" title="{escape(detail)}">{escape(label)}</div>'

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
            chips: list[str] = [render_chip(event) for event in items[:3]]
            if len(items) > 3:
                hidden_html = "".join(render_chip(event, show_type=True) for event in items[3:])
                chips.append(
                    f'<details class="more-details"><summary class="more-chip">+{len(items) - 3}건 더</summary><div class="more-panel">{hidden_html}</div></details>'
                )
            today_cls = " today" if today.year == year and today.month == month and today.day == day else ""
            spacer_html = '<div class="day-spacer"></div>'
            cells.append(
                f'<td class="day-cell{today_cls}"><div class="day-num">{day}</div>{"".join(chips) if chips else spacer_html}</td>'
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
      .ipo-calendar .more-details {margin-top: 0.08rem;}
      .ipo-calendar .more-details summary {cursor: pointer; list-style: none;}
      .ipo-calendar .more-details summary::-webkit-details-marker {display: none;}
      .ipo-calendar .more-chip {font-size: 0.7rem; opacity: 0.82; margin-top: 0.1rem; color: #334155;}
      .ipo-calendar .more-panel {margin-top: 0.22rem; padding-top: 0.08rem;}
      .ipo-calendar .more-panel .event-chip {white-space: normal; overflow: visible; text-overflow: clip;}
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
    render_lab_overview_cards(bundle, unified_bundle)
    st.caption("락업 자동매수, 5분봉 브리지, 턴오버 전략, 백테스트, 쇼츠 자동화를 한곳에서 관리합니다.")
    if getattr(unified_bundle.paths, "workspace", None) is not None:
        workspace_label = str(unified_bundle.paths.workspace)
        if "sample_unified_lab_workspace" in workspace_label:
            st.caption("전략 연구실은 내장 데모 workspace를 기준으로 열었습니다.")
    if not unified_bundle.source_status.empty and "ok" in unified_bundle.source_status.columns:
        failed = unified_bundle.source_status[~unified_bundle.source_status["ok"].fillna(False)]
        hard_fail = failed[~failed["source"].astype(str).isin(["turnover summary", "turnover trades", "turnover skip summary", "beta summary", "minute bar stats", "misses csv"])]
        if not hard_fail.empty:
            first_detail = str(hard_fail.iloc[0].get("detail") or "")
            has_core_data = any(
                isinstance(getattr(unified_bundle, attr, pd.DataFrame()), pd.DataFrame) and not getattr(unified_bundle, attr, pd.DataFrame()).empty
                for attr in ["unlocks", "signals", "turnover_summary_pretty", "turnover_trades", "beta_summary"]
            )
            if has_core_data:
                st.caption(f"일부 확장 데이터가 비어 있어 몇몇 표가 제한될 수 있습니다. {first_detail}")
            else:
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
    inject_global_styles()
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
        load_public_quotes_cached.clear()
        load_seibro_release_schedule_cached.clear()
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
        seibro_releases = bundle.raw_tables.get("seibro_release", pd.DataFrame()) if isinstance(bundle.raw_tables, dict) else pd.DataFrame()
        render_unlock_page(issues, bundle.all_unlocks, today, seibro_releases=seibro_releases)
    elif page == "실험실":
        assert bundle is not None
        render_lab_page(bundle, issues, today, backtest_version, prefer_live, unified_bundle, resolved_unified_workspace_path, allow_packaged_sample, source_mode)
    else:
        assert bundle is not None
        render_data_admin_page(bundle, source_mode, prefer_live, allow_sample_fallback, unified_bundle, resolved_unified_workspace_path, resolved_external_unlock_path, local_kind_export_path)


if __name__ == "__main__":
    main()
