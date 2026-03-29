from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import pandas as pd
import streamlit as st

st.set_page_config(page_title="공모주 레이더 Lockup Lab", page_icon="📈", layout="wide")

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
from src.services.scoring import IPOScorer
from src.services.strategy_bridge import StrategyBridge
from src.services.unified_lab_bridge import UnifiedLabBridgeService, UnifiedLabBundle
from src.utils import detect_project_env_file, fmt_date, fmt_num, fmt_pct, fmt_ratio, fmt_won, humanize_source, issue_recency_sort, mask_secret, normalize_name_key, runtime_dir, safe_float, standardize_issue_frame, to_csv_bytes


APP_ROOT = Path(__file__).resolve().parent
DATA_DIR = APP_ROOT / "data"
CACHE_REV = "20260329_v10_hotfix"


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
    return service.load_bundle(workspace_path or None, allow_packaged_sample=allow_packaged_sample)


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


def count_issue_sources(issues: pd.DataFrame) -> dict[str, int]:
    if issues is None or issues.empty or "source" not in issues.columns:
        return {"total": 0, "real": 0, "sample": 0}
    sources = issues["source"].fillna("unknown").astype(str).str.lower()
    sample = int(sources.isin(["sample", "demo"]).sum())
    real = int(len(issues) - sample)
    return {"total": int(len(issues)), "real": real, "sample": sample}


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

    st.write(
        {
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
    )
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
    strategy_bridge = StrategyBridge(DATA_DIR)
    metrics = repo.dashboard_metrics(issues, bundle.all_unlocks, today)

    st.subheader("오늘 기준 한눈에 보기")
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("청약 예정/진행", metrics["subscription_count"])
    c2.metric("상장 예정/직후", metrics["listing_count"])
    c3.metric("보호예수 해제", metrics["unlock_count"])
    c4.metric("알림 후보", metrics["alert_count"])

    snapshot_bundle = load_market_snapshot_bundle_cached(prefer_live, allow_sample_fallback)
    snapshot = snapshot_bundle["frame"]
    snapshot_source = snapshot_bundle["source"]
    snapshot_diag = snapshot_bundle.get("diagnostics", pd.DataFrame())
    market_service = MarketService(DATA_DIR, kis_client=KISClient.from_env())
    mood = market_service.market_mood(snapshot)
    issue_counts = count_issue_sources(issues)
    render_sample_data_warning(source_mode, issue_counts, snapshot_source)
    st.caption(f"시장 스냅샷 소스: {snapshot_source} · 시장 분위기: {mood['label']} ({mood['score']})")
    render_metric_cards(snapshot, limit=6)
    render_market_diagnostics(snapshot_diag, title="시장 스냅샷 실패 로그", only_failures=True)

    top_sub = safe_sort_values(repo.upcoming_subscriptions(issues, today, window_days=30), "subscription_score", ascending=False)
    top_list = safe_sort_values(repo.upcoming_listings(issues, today, window_days=30), "listing_quality_score", ascending=False)
    top_unlock = strategy_bridge.rank_upcoming_unlock_candidates(bundle.all_unlocks, issues, today, backtest_version, horizon_days=45)

    col_a, col_b = st.columns([1.1, 0.9])
    with col_a:
        st.markdown("**가까운 일정 타임라인**")
        timeline = repo.build_timeline(issues, bundle.all_unlocks, today, window_days=30)
        if timeline.empty:
            st.info("30일 안 일정이 없습니다.")
        else:
            display = timeline.copy()
            display["date"] = display["date"].dt.strftime("%Y-%m-%d")
            st.dataframe(display, hide_index=True, use_container_width=True)
    with col_b:
        st.markdown("**데이터 상태**")
        latest_ts = repo.latest_data_timestamp(issues)
        issue_counts = count_issue_sources(issues)
        st.metric("최근 데이터 시각", "-" if latest_ts is None else latest_ts.strftime("%Y-%m-%d"))
        st.metric("실데이터 종목수", issue_counts.get("real", 0))
        st.metric("샘플 종목수", issue_counts.get("sample", 0))
        st.metric("전략 unlock 종목수", len(bundle.external_unlocks))
        st.metric("캐시 파일수", len(bundle.cache_inventory))

    s1, s2, s3 = st.columns(3)
    with s1:
        st.markdown("**청약 우선순위**")
        if top_sub.empty:
            st.info("표시할 청약 후보가 없습니다.")
        else:
            display = top_sub[["name", "subscription_start", "underwriters", "subscription_score", "institutional_competition_ratio"]].copy()
            display["subscription_start"] = pd.to_datetime(display["subscription_start"], errors="coerce").dt.strftime("%Y-%m-%d")
            st.dataframe(display.head(6), hide_index=True, use_container_width=True)
    with s2:
        st.markdown("**상장 체크리스트 상위**")
        if top_list.empty:
            st.info("표시할 상장 후보가 없습니다.")
        else:
            display = top_list[["name", "listing_date", "listing_quality_score", "lockup_commitment_ratio", "circulating_shares_ratio_on_listing"]].copy()
            display["listing_date"] = pd.to_datetime(display["listing_date"], errors="coerce").dt.strftime("%Y-%m-%d")
            st.dataframe(display.head(6), hide_index=True, use_container_width=True)
    with s3:
        st.markdown("**전략 브릿지 상위 후보**")
        if top_unlock.empty:
            st.info("표시할 보호예수 해제 후보가 없습니다.")
        else:
            display = top_unlock[["name", "unlock_date", "term", "combined_score", "technical_signal"]].copy()
            display["unlock_date"] = pd.to_datetime(display["unlock_date"], errors="coerce").dt.strftime("%Y-%m-%d")
            st.dataframe(display.head(6), hide_index=True, use_container_width=True)


def render_explorer(bundle: IPODataBundle, prefer_live: bool) -> None:
    st.subheader("딜 탐색기")
    st.caption("샘플, 실데이터, 전략 데이터를 한 화면에서 필터링하고 상세 확인하는 화면입니다.")
    issues = add_issue_scores(bundle.issues)
    if issues.empty:
        st.info("표시할 종목이 없습니다.")
        return

    f1, f2, f3, f4 = st.columns([1, 1, 1, 1.2])
    market = f1.selectbox("시장", ["전체"] + sorted([x for x in issues["market"].dropna().unique().tolist() if x]), index=0)
    stage = f2.selectbox("단계", ["전체"] + sorted([x for x in issues["stage"].dropna().unique().tolist() if x]), index=0)
    source = f3.selectbox("출처", ["전체"] + sorted([x for x in issues["source"].dropna().unique().tolist() if x]), index=0)
    query = f4.text_input("검색", placeholder="종목명 / 주관사 / 업종")

    filtered = issues.copy()
    if market != "전체":
        filtered = filtered[filtered["market"] == market]
    if stage != "전체":
        filtered = filtered[filtered["stage"] == stage]
    if source != "전체":
        filtered = filtered[filtered["source"] == source]
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


def render_subscription_page(issues: pd.DataFrame) -> None:
    st.subheader("청약 단계")
    st.caption("기관경쟁률, 증권사, 공모가와 비례청약 손익분기까지 같이 보는 화면입니다.")
    issues = add_issue_scores(issues)
    df = issues[issues["stage"].isin(["청약예정", "청약중", "청약완료"])].copy()
    if df.empty:
        st.info("표시할 청약 종목이 없습니다.")
        return

    left, right = st.columns([1.35, 1])
    with left:
        display = df[[
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
        st.dataframe(safe_sort_values(display, ["subscription_score", "subscription_start"], ascending=[False, True]), hide_index=True, use_container_width=True)
        render_download_button("청약 후보 CSV 내려받기", display, "subscriptions.csv")

        issue = issue_selector(safe_sort_values(df, ["subscription_score", "subscription_start"], ascending=[False, True]).reset_index(drop=True), key="subscription_issue")
        if issue is not None:
            render_issue_overview(issue)

    with right:
        st.markdown("**비례청약 손익분기 계산기**")
        issue_names = df["name"].tolist()
        selected_name = st.selectbox("계산 기준 종목", options=issue_names, key="calc_issue")
        issue = df[df["name"] == selected_name].iloc[0]
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


def render_listing_page(issues: pd.DataFrame, prefer_live: bool) -> None:
    st.subheader("상장 단계")
    st.caption("확약, 우리사주 실권, 유통가능물량, 기존주주비율, 현재가와 기술신호를 같이 봅니다.")
    issues = add_issue_scores(issues)
    target_df = issues[issues["stage"].isin(["상장예정", "상장후"])].copy()
    if target_df.empty:
        st.info("표시할 상장 종목이 없습니다.")
        return

    display = target_df[[
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
    st.dataframe(safe_sort_values(display, ["listing_quality_score", "listing_date"], ascending=[False, True]), hide_index=True, use_container_width=True)
    render_download_button("상장 종목 CSV 내려받기", display, "listings.csv")

    issue = issue_selector(safe_sort_values(target_df, ["listing_quality_score", "listing_date"], ascending=[False, True]).reset_index(drop=True), key="listing_issue")
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
                source_label = "KIS"
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


def render_backtest_page() -> None:
    repo = BacktestRepository(DATA_DIR)
    st.subheader("백테스트")
    st.caption("보호예수 해제 자동매수 전략의 버전별 성과와 skip 사유까지 같이 봅니다.")
    versions_df = repo.versions_summary()
    if versions_df.empty:
        st.info("백테스트 결과가 없습니다.")
        return
    st.markdown("**버전 비교**")
    st.dataframe(versions_df, hide_index=True, use_container_width=True)
    version = st.selectbox("상세 버전", options=repo.available_versions(), index=0)
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
            display = trades_df.copy()
            for col in ["listing_date", "unlock_date", "entry_dt", "exit_dt", "prev_close_date"]:
                if col in display.columns:
                    display[col] = pd.to_datetime(display[col], errors="coerce").dt.strftime("%Y-%m-%d")
            st.dataframe(display, hide_index=True, use_container_width=True)
    with t4:
        if skip_summary_df.empty:
            st.info("skip 요약 데이터가 없습니다.")
        else:
            st.dataframe(skip_summary_df, hide_index=True, use_container_width=True)
    with t5:
        if skip_reasons_df.empty:
            st.info("skip 상세 데이터가 없습니다.")
        else:
            display = skip_reasons_df.copy()
            for col in ["listing_date", "unlock_date"]:
                if col in display.columns:
                    display[col] = pd.to_datetime(display[col], errors="coerce").dt.strftime("%Y-%m-%d")
            st.dataframe(display, hide_index=True, use_container_width=True)


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


def main() -> None:
    st.title("공모주 레이더 Lockup Lab")
    st.caption("청약 → 상장 → 보호예수 해제 → 락업 매수전략 실행보드까지 이어서 보는 공모주 전용 대시보드")

    repo = IPORepository(DATA_DIR)
    sidebar = st.sidebar
    sidebar.header("앱 설정")
    page = sidebar.radio(
        "메뉴",
        ["대시보드", "딜 탐색기", "청약", "상장", "락업 매수전략", "5분봉 브리지", "전략 브릿지", "보호예수/알림", "DART 자동추출", "시장", "백테스트", "데이터 허브", "설정"],
    )
    source_mode = sidebar.selectbox("데이터 모드", ["실데이터 우선", "캐시 우선", "샘플만"], index=0)
    prefer_live = source_mode == "실데이터 우선"
    allow_sample_fallback = source_mode == "샘플만"
    allow_packaged_sample = sidebar.checkbox(
        "내장 데모 workspace 자동연결",
        value=source_mode == "샘플만",
        help="앱에 포함된 demo unlock/5분봉 workspace만 연결합니다. 통합 프로젝트의 integrated_lab/ipo_lockup_unified_lab/workspace 자동탐지는 이 체크와 무관하게 항상 시도합니다.",
    )
    default_external = repo.auto_detect_external_unlock_dataset(allow_packaged_sample=allow_packaged_sample)
    default_kind = repo.auto_detect_local_kind_export(include_home_dirs=False)
    default_unified_workspace = UnifiedLabBridgeService(DATA_DIR).auto_detect_workspace(allow_packaged_sample=allow_packaged_sample)
    external_unlock_path = sidebar.text_input(
        "외부 unlock dataset 경로",
        value=str(default_external) if default_external else "",
        help="synthetic_ipo_events.csv 또는 unlock_events_backtest_input.csv 경로를 넣으면 전략용 보호예수 해제 데이터가 붙습니다.",
    )
    local_kind_export_path = sidebar.text_input(
        "로컬 KIND export 경로",
        value=str(default_kind) if default_kind else "",
        help="신규상장기업현황 또는 공모가대비주가정보 엑셀/CSV 경로입니다. Desktop/Downloads의 오래된 파일은 자동으로 끌어오지 않으므로 필요할 때만 직접 지정하세요.",
    )
    unified_workspace_path = sidebar.text_input(
        "5분봉 lab workspace 경로",
        value=str(default_unified_workspace) if default_unified_workspace else "",
        help="unlock_out / signal_out / turnover_backtest_out / dataset_out / data/curated/lockup_minute.db 를 포함한 workspace 경로입니다. 통합 프로젝트를 쓰면 integrated_lab/ipo_lockup_unified_lab/workspace를 자동 탐지합니다.",
    )
    unified_bundle = load_unified_lab_bundle_cached(unified_workspace_path, allow_packaged_sample=allow_packaged_sample)
    resolved_external_unlock_path = external_unlock_path or str(unified_bundle.paths.unlock_csv or "")
    today = pd.Timestamp(sidebar.date_input("기준일", value=pd.Timestamp.now(tz="Asia/Seoul").date()))
    backtest_version = sidebar.selectbox("락업 전략 기준 버전", options=BacktestRepository(DATA_DIR).available_versions(), index=0)

    bundle = load_bundle_cached(source_mode, resolved_external_unlock_path, local_kind_export_path, allow_sample_fallback, allow_packaged_sample)
    issues = add_issue_scores(bundle.issues)

    if page == "대시보드":
        render_dashboard(bundle, today, prefer_live, allow_sample_fallback, backtest_version, source_mode)
    elif page == "딜 탐색기":
        render_explorer(bundle, prefer_live)
    elif page == "청약":
        render_subscription_page(issues)
    elif page == "상장":
        render_listing_page(issues, prefer_live)
    elif page == "락업 매수전략":
        render_lockup_strategy_page(bundle, issues, today, backtest_version, prefer_live, unified_bundle)
    elif page == "5분봉 브리지":
        render_minute_bridge_page(bundle, issues, today, backtest_version, unified_bundle)
    elif page == "전략 브릿지":
        render_strategy_bridge_page(bundle, issues, today, backtest_version)
    elif page == "보호예수/알림":
        render_unlock_page(issues, bundle.all_unlocks, today)
    elif page == "DART 자동추출":
        render_dart_page(bundle)
    elif page == "시장":
        render_market_page(prefer_live, allow_sample_fallback, source_mode)
    elif page == "백테스트":
        render_backtest_page()
    elif page == "데이터 허브":
        render_data_hub_page(bundle, source_mode, unified_bundle, unified_workspace_path)
    else:
        render_settings_page(source_mode, prefer_live, resolved_external_unlock_path, local_kind_export_path, unified_workspace_path, unified_bundle)


if __name__ == "__main__":
    main()
