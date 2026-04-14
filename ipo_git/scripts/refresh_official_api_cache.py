from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any

import pandas as pd
import requests

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.services.dart_client import DartClient
from src.services.ipo_pipeline import IPODataHub
from src.services.kis_client import KISClient
from src.services.live_cache import LiveCacheStore
from src.services.public_data_client import KSDPublicDataClient, MARKET_CODE_TO_LABEL
from src.utils import normalize_name_key, normalize_symbol_text, parse_date_columns, safe_float, standardize_issue_frame, today_kst, load_project_env


KRX_LISTED_INFO_URL = "https://apis.data.go.kr/1160100/service/GetKrxListedInfoService/getItemInfo"
KRX_MARKET_LABELS = {
    "KOSPI": "유가증권",
    "KOSDAQ": "코스닥",
    "KONEX": "코넥스",
    "K-OTC": "K-OTC",
    "KOTC": "K-OTC",
}


def _is_missing_scalar(value: Any) -> bool:
    try:
        return pd.isna(value)
    except Exception:
        return value is None


def _clean_str(value: Any) -> str:
    if _is_missing_scalar(value):
        return ""
    return str(value).strip()


def _value_or(value: Any, fallback: Any) -> Any:
    return fallback if _is_missing_scalar(value) else value


def _resolve_public_service_key() -> str:
    for env_name in ("PUBLIC_DATA_SERVICE_KEY", "KRX_LISTED_INFO_SERVICE_KEY", "KSD_PUBLIC_DATA_SERVICE_KEY", "DATA_GO_SERVICE_KEY", "SEIBRO_SERVICE_KEY"):
        value = os.getenv(env_name, "").strip()
        if value:
            return value
    return ""


def _candidate_bas_dates(days: int = 7) -> list[str]:
    today = today_kst()
    return [(today - pd.Timedelta(days=offset)).strftime("%Y%m%d") for offset in range(days + 1)]


def _fetch_krx_listed_info(service_key: str, *, days: int = 7, timeout: int = 20) -> tuple[pd.DataFrame, dict[str, Any]]:
    if not service_key:
        return pd.DataFrame(), {"ok": False, "reason": "PUBLIC_DATA_SERVICE_KEY missing"}
    attempts: list[dict[str, Any]] = []
    for bas_dt in _candidate_bas_dates(days):
        params = {
            "serviceKey": service_key,
            "numOfRows": 4000,
            "pageNo": 1,
            "resultType": "json",
            "basDt": bas_dt,
        }
        try:
            response = requests.get(KRX_LISTED_INFO_URL, params=params, timeout=timeout)
            response.raise_for_status()
            payload = response.json()
        except Exception as exc:
            attempts.append({"basDt": bas_dt, "ok": False, "error": str(exc)})
            continue
        header = (payload.get("response") or {}).get("header") or {}
        body = (payload.get("response") or {}).get("body") or {}
        items = ((body.get("items") or {}).get("item") or [])
        if isinstance(items, dict):
            items = [items]
        total_count = int(body.get("totalCount") or 0)
        code = str(header.get("resultCode") or "")
        msg = str(header.get("resultMsg") or "")
        attempts.append({"basDt": bas_dt, "ok": code in {"", "00"} and total_count > 0, "resultCode": code, "resultMsg": msg, "totalCount": total_count})
        if code not in {"", "00"} or total_count <= 0 or not items:
            continue
        rows: list[dict[str, Any]] = []
        refreshed_at = today_kst()
        for item in items:
            symbol = normalize_symbol_text(item.get("srtnCd"))
            name = str(item.get("itmsNm") or "").strip()
            if not symbol or not name:
                continue
            market = KRX_MARKET_LABELS.get(str(item.get("mrktCtg") or "").strip().upper(), str(item.get("mrktCtg") or "").strip() or pd.NA)
            rows.append(
                {
                    "bas_dt": bas_dt,
                    "symbol": symbol,
                    "isin": str(item.get("isinCd") or "").strip() or pd.NA,
                    "name": name,
                    "name_key": normalize_name_key(name),
                    "market": market,
                    "crno": str(item.get("crno") or "").strip() or pd.NA,
                    "corp_name": str(item.get("corpNm") or "").strip() or pd.NA,
                    "listing_status": "상장",
                    "delisting_date": pd.NaT,
                    "source": "KRX-상장종목정보",
                    "source_detail": response.url,
                    "last_refresh_ts": refreshed_at,
                }
            )
        frame = pd.DataFrame(rows)
        if not frame.empty:
            frame = parse_date_columns(frame)
            return frame, {"ok": True, "basDt": bas_dt, "totalCount": total_count, "rows": int(len(frame)), "attempts": attempts}
    return pd.DataFrame(), {"ok": False, "rows": 0, "attempts": attempts}


def _krx_name_lookup_frame(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()
    out = df.copy()
    out["query_name"] = out.get("name")
    out["query_name_key"] = out.get("name_key")
    out["issuco_custno"] = pd.NA
    out["share_type"] = "보통주"
    out["issue_date"] = pd.NaT
    out["source"] = "KRX-상장종목정보"
    cols = [c for c in ["query_name", "query_name_key", "name", "name_key", "symbol", "isin", "issuco_custno", "share_type", "issue_date", "source", "source_detail", "last_refresh_ts"] if c in out.columns]
    return out[cols].drop_duplicates(subset=[c for c in ["query_name_key", "symbol"] if c in out.columns], keep="first").reset_index(drop=True)


def _krx_market_codes_frame(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()
    out = df.copy()
    code_map = {"유가증권": "11", "코스닥": "12", "코넥스": "14", "K-OTC": "13"}
    out["market_code"] = out.get("market").map(lambda x: code_map.get(str(x), pd.NA))
    out["source"] = "KRX-상장종목정보"
    cols = [c for c in ["market_code", "symbol", "name", "name_key", "market", "listing_status", "delisting_date", "source", "source_detail", "last_refresh_ts"] if c in out.columns]
    return out[cols].drop_duplicates(subset=[c for c in ["symbol", "name_key"] if c in out.columns], keep="first").reset_index(drop=True)


def issue_frame_from_krx_master(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()
    rows: list[dict[str, Any]] = []
    for _, row in df.iterrows():
        rows.append(
            {
                "ipo_id": f"krx_{row.get('name_key')}",
                "name": row.get("name"),
                "name_key": row.get("name_key"),
                "symbol": row.get("symbol"),
                "market": row.get("market"),
                "listing_date": pd.NaT,
                "notes": row.get("corp_name"),
                "source": "공공데이터-KRX상장종목정보",
                "source_detail": row.get("source_detail"),
                "last_refresh_ts": _value_or(row.get("last_refresh_ts"), today_kst()),
            }
        )
    return pd.DataFrame(rows)


def merge_cache(existing: pd.DataFrame, updates: pd.DataFrame, subset: list[str]) -> pd.DataFrame:
    frames = [frame for frame in [existing, updates] if isinstance(frame, pd.DataFrame) and not frame.empty]
    if not frames:
        return pd.DataFrame(columns=(existing.columns if isinstance(existing, pd.DataFrame) else updates.columns))
    combined = pd.concat(frames, ignore_index=True)
    combined = parse_date_columns(combined)
    sort_cols = [c for c in ["last_refresh_ts", "distribution_date", "listing_date", "delisting_date"] if c in combined.columns]
    if sort_cols:
        combined = combined.sort_values(sort_cols, ascending=[False] * len(sort_cols), na_position="last")
    dedupe = [col for col in subset if col in combined.columns]
    if dedupe:
        combined = combined.drop_duplicates(subset=dedupe, keep="first")
    return combined.reset_index(drop=True)


def build_candidate_universe(data_dir: Path, *, max_items: int = 80) -> pd.DataFrame:
    hub = IPODataHub(data_dir, dart_client=DartClient.from_env(), kis_client=KISClient.from_env())
    bundle = hub.load_bundle(prefer_live=False, use_cache=True, allow_sample_fallback=True)
    issues = standardize_issue_frame(bundle.issues.copy()) if bundle.issues is not None else pd.DataFrame()
    if issues.empty:
        return issues
    today = today_kst()
    issues["listing_date"] = pd.to_datetime(issues.get("listing_date"), errors="coerce")
    issues["subscription_start"] = pd.to_datetime(issues.get("subscription_start"), errors="coerce")
    issues["subscription_end"] = pd.to_datetime(issues.get("subscription_end"), errors="coerce")
    issues["name_key"] = issues.get("name_key", pd.Series(dtype="object")).fillna(issues.get("name", pd.Series(dtype="object"))).map(normalize_name_key)
    issues["symbol"] = issues.get("symbol", pd.Series(dtype="object")).map(normalize_symbol_text)
    recent_mask = (
        issues["subscription_start"].between(today - pd.Timedelta(days=240), today + pd.Timedelta(days=240), inclusive="both")
        | issues["listing_date"].between(today - pd.Timedelta(days=540), today + pd.Timedelta(days=120), inclusive="both")
    )
    priority = issues.get("source", pd.Series(dtype="object")).map(
        {
            "38": 6,
            "KIND-공모기업": 5,
            "KIND-공모가비교": 5,
            "local-kind": 4,
            "strategy-overlay": 4,
            "KIND-corpList": 2,
        }
    ).fillna(3)
    missing_symbol = (~issues.get("symbol", pd.Series(dtype="object")).map(lambda v: bool(str(v or "").strip())))
    missing_market = issues.get("market", pd.Series(dtype="object")).isna()
    issues["__priority"] = priority + missing_symbol.astype(int) * 2 + missing_market.astype(int)
    issues["__recent"] = recent_mask.astype(int)
    issues["__ref"] = issues["subscription_start"].combine_first(issues["listing_date"]).combine_first(issues["subscription_end"])
    issues = issues.sort_values(["__recent", "__priority", "__ref", "name_key"], ascending=[False, False, False, True], na_position="last")
    issues = issues.drop_duplicates(subset=[c for c in ["symbol", "name_key"] if c in issues.columns], keep="first")
    return issues.head(max_items).reset_index(drop=True)


def select_name_lookup_candidates(issues: pd.DataFrame, existing_map: pd.DataFrame, *, max_items: int) -> list[str]:
    if issues is None or issues.empty:
        return []
    existing_keys: set[str] = set()
    existing_symbols: set[str] = set()
    if isinstance(existing_map, pd.DataFrame) and not existing_map.empty:
        existing_keys = set(existing_map.get("query_name_key", pd.Series(dtype="object")).dropna().astype(str).tolist())
        existing_keys |= set(existing_map.get("name_key", pd.Series(dtype="object")).dropna().astype(str).tolist())
        existing_symbols = set(existing_map.get("symbol", pd.Series(dtype="object")).dropna().astype(str).tolist())
    out: list[str] = []
    for _, row in issues.iterrows():
        name = _clean_str(row.get("name"))
        if not name:
            continue
        name_key = normalize_name_key(name)
        symbol = normalize_symbol_text(row.get("symbol"))
        needs_lookup = (not symbol) or (symbol and symbol not in existing_symbols) or (name_key and name_key not in existing_keys)
        if not needs_lookup:
            continue
        if name in out:
            continue
        out.append(name)
        if len(out) >= max_items:
            break
    return out


def issue_frame_from_official_listing(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()
    out = df.copy()
    out["name_key"] = out.get("name_key", pd.Series(dtype="object")).fillna(out.get("name", pd.Series(dtype="object"))).map(normalize_name_key)
    out["symbol"] = out.get("symbol", pd.Series(dtype="object")).map(normalize_symbol_text)
    out["listing_date"] = pd.to_datetime(out.get("listing_date"), errors="coerce")
    out["delisting_date"] = pd.to_datetime(out.get("delisting_date"), errors="coerce")
    rows: list[dict[str, Any]] = []
    for _, row in out.iterrows():
        rows.append(
            {
                "ipo_id": f"official_{row.get('name_key')}",
                "name": row.get("name"),
                "name_key": row.get("name_key"),
                "symbol": row.get("symbol"),
                "market": row.get("market"),
                "listing_date": row.get("listing_date"),
                "notes": row.get("listing_status") if pd.notna(row.get("delisting_date")) else pd.NA,
                "source": "공공데이터-상장정보",
                "source_detail": row.get("source_detail"),
                "last_refresh_ts": _value_or(row.get("last_refresh_ts"), today_kst()),
            }
        )
    return pd.DataFrame(rows)


def issue_frame_from_official_basic(df: pd.DataFrame, mapping: pd.DataFrame | None = None) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()
    work = df.copy()
    if mapping is not None and not mapping.empty:
        ref = mapping.copy()
        ref["issuco_custno"] = ref.get("issuco_custno", pd.Series(dtype="object")).astype(str)
        ref["symbol"] = ref.get("symbol", pd.Series(dtype="object")).map(normalize_symbol_text)
        ref["name_key"] = ref.get("name_key", pd.Series(dtype="object")).fillna(ref.get("name", pd.Series(dtype="object"))).map(normalize_name_key)
        ref = ref.sort_values([c for c in ["symbol", "name_key"] if c in ref.columns])
        ref = ref.drop_duplicates(subset=[c for c in ["issuco_custno"] if c in ref.columns], keep="first")
        work["issuco_custno"] = work.get("issuco_custno", pd.Series(dtype="object")).astype(str)
        work = work.merge(ref[[c for c in ["issuco_custno", "symbol", "name", "name_key"] if c in ref.columns]], on="issuco_custno", how="left", suffixes=("", "_map"))
        work["name"] = work.get("name").combine_first(work.get("name_map"))
        work["name_key"] = work.get("name_key").combine_first(work.get("name_key_map"))
        work["symbol"] = work.get("symbol").combine_first(work.get("symbol_map"))
    work["name_key"] = work.get("name_key", pd.Series(dtype="object")).fillna(work.get("name", pd.Series(dtype="object"))).map(normalize_name_key)
    work["symbol"] = work.get("symbol", pd.Series(dtype="object")).map(normalize_symbol_text)
    work["listing_date"] = pd.to_datetime(work.get("listing_date"), errors="coerce")
    rows: list[dict[str, Any]] = []
    for _, row in work.iterrows():
        rows.append(
            {
                "ipo_id": f"official_basic_{row.get('name_key')}",
                "name": row.get("name"),
                "name_key": row.get("name_key"),
                "symbol": row.get("symbol"),
                "listing_date": row.get("listing_date"),
                "post_listing_total_shares": row.get("post_listing_total_shares"),
                "notes": row.get("homep_url"),
                "source": "공공데이터-기업개요",
                "source_detail": row.get("source_detail"),
                "last_refresh_ts": _value_or(row.get("last_refresh_ts"), today_kst()),
            }
        )
    return pd.DataFrame(rows)


def main() -> None:
    parser = argparse.ArgumentParser(description="공식 API(KSD) 캐시 갱신")
    parser.add_argument("--data-dir", default=str(ROOT / "data"))
    parser.add_argument("--max-issues", type=int, default=80)
    parser.add_argument("--max-name-lookups", type=int, default=60)
    parser.add_argument("--max-corp-lookups", type=int, default=40)
    parser.add_argument("--skip-market-codes", action="store_true")
    parser.add_argument("--skip-name-lookup", action="store_true")
    parser.add_argument("--skip-corp", action="store_true")
    args = parser.parse_args()

    load_project_env()
    data_dir = Path(args.data_dir).expanduser().resolve()
    cache = LiveCacheStore(data_dir / "cache")
    client = KSDPublicDataClient.from_env(cache_dir=data_dir / "cache")
    public_service_key = _resolve_public_service_key()
    if client is None and not public_service_key:
        print(json.dumps({"ok": False, "reason": "PUBLIC_DATA_SERVICE_KEY not configured"}, ensure_ascii=False))
        return

    today = today_kst()
    report: dict[str, Any] = {"ok": True, "saved_at": today.isoformat(), "warnings": []}

    issues = build_candidate_universe(data_dir, max_items=args.max_issues)
    report["candidate_issues"] = int(len(issues))

    existing_name_map = cache.read_frame("official_ksd_name_lookup_live")
    existing_listing = cache.read_frame("official_ksd_listing_info_live")
    existing_market_codes = cache.read_frame("official_ksd_market_codes_live")
    existing_corp_basic = cache.read_frame("official_ksd_corp_basic_live")
    existing_shareholder = cache.read_frame("official_ksd_shareholder_summary_live")
    existing_krx_master = cache.read_frame("official_krx_listed_info_live")

    krx_master = pd.DataFrame()
    if public_service_key:
        krx_updates, krx_meta = _fetch_krx_listed_info(public_service_key)
        report["krx_probe"] = krx_meta
        if krx_updates is not None and not krx_updates.empty:
            krx_master = merge_cache(existing_krx_master, krx_updates, ["bas_dt", "symbol"])
            cache.write_frame(
                "official_krx_listed_info_live",
                krx_master,
                meta={"source": "KRX", "notes": "KRX 상장종목정보 API", "row_count": int(len(krx_master)), "saved_at": today.isoformat()},
            )
            krx_name_updates = _krx_name_lookup_frame(krx_updates)
            if not krx_name_updates.empty:
                existing_name_map = merge_cache(existing_name_map, krx_name_updates, ["query_name_key", "name_key", "symbol"])
                cache.write_frame(
                    "official_ksd_name_lookup_live",
                    existing_name_map,
                    meta={"source": "KRX+KSD", "notes": "종목명→단축코드/ISIN (KRX fallback 포함)", "row_count": int(len(existing_name_map)), "saved_at": today.isoformat()},
                )
            krx_market_updates = _krx_market_codes_frame(krx_updates)
            if not krx_market_updates.empty:
                existing_market_codes = merge_cache(existing_market_codes, krx_market_updates, ["symbol", "name_key"])
                cache.write_frame(
                    "official_ksd_market_codes_live",
                    existing_market_codes,
                    meta={"source": "KRX+KSD", "notes": "시장별 단축코드 전체 조회 (KRX fallback 포함)", "row_count": int(len(existing_market_codes)), "saved_at": today.isoformat()},
                )
        else:
            report["warnings"].append("KRX listed info refresh returned zero rows")

    name_map_updates = pd.DataFrame()
    if client is not None and not args.skip_name_lookup:
        names = select_name_lookup_candidates(issues, existing_name_map, max_items=args.max_name_lookups)
        rows: list[pd.DataFrame] = []
        for name in names:
            try:
                frame = client.lookup_stock_by_name(name)
            except Exception:
                continue
            if frame is None or frame.empty:
                continue
            rows.append(frame)
        if rows:
            name_map_updates = pd.concat(rows, ignore_index=True)
            merged_name_map = merge_cache(existing_name_map, name_map_updates, ["query_name_key", "name_key", "symbol"])
            cache.write_frame(
                "official_ksd_name_lookup_live",
                merged_name_map,
                meta={"source": "KSD", "notes": "종목명→단축코드/ISIN/회사번호", "row_count": int(len(merged_name_map)), "saved_at": today.isoformat()},
            )
            existing_name_map = merged_name_map
        report["name_lookup_rows"] = int(len(existing_name_map))
        report["name_lookup_added"] = int(len(name_map_updates))

    if client is not None and not args.skip_market_codes:
        market_frames: list[pd.DataFrame] = []
        for market_code in ["11", "12", "14", "50"]:
            try:
                frame = client.list_market_short_codes(market_code)
            except Exception:
                continue
            if frame is not None and not frame.empty:
                market_frames.append(frame)
        if market_frames:
            market_updates = pd.concat(market_frames, ignore_index=True)
            merged_market_codes = merge_cache(existing_market_codes, market_updates, ["market_code", "symbol", "name_key"])
            cache.write_frame(
                "official_ksd_market_codes_live",
                merged_market_codes,
                meta={"source": "KSD", "notes": "시장별 단축코드 전체 조회", "row_count": int(len(merged_market_codes)), "saved_at": today.isoformat()},
            )
            existing_market_codes = merged_market_codes
        report["market_code_rows"] = int(len(existing_market_codes))

    # listing info for mapped ISINs
    if client is not None and isinstance(existing_name_map, pd.DataFrame) and not existing_name_map.empty:
        listing_frames: list[pd.DataFrame] = []
        seen_isins: set[str] = set(existing_listing.get("isin", pd.Series(dtype="object")).dropna().astype(str).tolist()) if isinstance(existing_listing, pd.DataFrame) else set()
        candidate_map = existing_name_map.copy()
        candidate_map["isin"] = candidate_map.get("isin", pd.Series(dtype="object")).astype(str)
        candidate_map = candidate_map[candidate_map["isin"].astype(str).str.len() >= 12].copy()
        for isin in candidate_map["isin"].drop_duplicates().tolist()[: max(args.max_name_lookups, 20)]:
            if isin in seen_isins:
                continue
            try:
                frame = client.get_listing_info(isin)
            except Exception:
                continue
            if frame is None or frame.empty:
                continue
            # carry symbol/name from map when listing info is sparse
            ref = candidate_map[candidate_map["isin"] == isin].iloc[0].to_dict()
            frame["symbol"] = frame.get("symbol", pd.Series(dtype="object")).combine_first(pd.Series([ref.get("symbol")] * len(frame)))
            frame["name"] = frame.get("name", pd.Series(dtype="object")).combine_first(pd.Series([ref.get("name")] * len(frame)))
            frame["name_key"] = frame.get("name_key", pd.Series(dtype="object")).combine_first(pd.Series([ref.get("name_key")] * len(frame)))
            frame["issuco_custno"] = frame.get("issuco_custno", pd.Series(dtype="object")).combine_first(pd.Series([ref.get("issuco_custno")] * len(frame)))
            listing_frames.append(frame)
        if listing_frames:
            listing_updates = pd.concat(listing_frames, ignore_index=True)
            merged_listing = merge_cache(existing_listing, listing_updates, ["isin"])
            cache.write_frame(
                "official_ksd_listing_info_live",
                merged_listing,
                meta={"source": "KSD", "notes": "주식상장정보 조회", "row_count": int(len(merged_listing)), "saved_at": today.isoformat()},
            )
            existing_listing = merged_listing
        report["listing_info_rows"] = int(len(existing_listing))

    if client is not None and not args.skip_corp and isinstance(existing_name_map, pd.DataFrame) and not existing_name_map.empty:
        corp_frames: list[pd.DataFrame] = []
        dist_summary_frames: list[pd.DataFrame] = []
        seen_custno: set[str] = set(existing_corp_basic.get("issuco_custno", pd.Series(dtype="object")).astype(str).tolist()) if isinstance(existing_corp_basic, pd.DataFrame) else set()
        seen_shareholder: set[str] = set(existing_shareholder.get("issuco_custno", pd.Series(dtype="object")).astype(str).tolist()) if isinstance(existing_shareholder, pd.DataFrame) else set()
        recent_candidates = existing_name_map.copy()
        recent_candidates = recent_candidates.sort_values([c for c in ["last_refresh_ts", "query_name", "name"] if c in recent_candidates.columns], ascending=[False, True, True], na_position="last")
        for _, row in recent_candidates.drop_duplicates(subset=[c for c in ["issuco_custno"] if c in recent_candidates.columns], keep="first").iterrows():
            custno = _clean_str(row.get("issuco_custno"))
            if not custno:
                continue
            if custno not in seen_custno and len(corp_frames) < args.max_corp_lookups:
                try:
                    basic = client.get_corp_basic_info(custno)
                except Exception:
                    basic = pd.DataFrame()
                if basic is not None and not basic.empty:
                    basic["symbol"] = row.get("symbol")
                    basic["name"] = basic.get("name", pd.Series(dtype="object")).combine_first(pd.Series([row.get("name")] * len(basic)))
                    basic["name_key"] = basic.get("name_key", pd.Series(dtype="object")).combine_first(pd.Series([row.get("name_key")] * len(basic)))
                    corp_frames.append(basic)
            if custno not in seen_shareholder and len(dist_summary_frames) < args.max_corp_lookups:
                try:
                    dist_dates = client.get_distribution_dates(custno)
                except Exception:
                    dist_dates = pd.DataFrame()
                if dist_dates is not None and not dist_dates.empty:
                    latest_date = pd.to_datetime(dist_dates.get("distribution_date"), errors="coerce").dropna().max()
                    if pd.notna(latest_date):
                        try:
                            dist = client.get_shareholder_distribution(custno, latest_date)
                        except Exception:
                            dist = pd.DataFrame()
                        if dist is not None and not dist.empty:
                            summary = client.summarize_shareholder_distribution(dist)
                            summary["symbol"] = row.get("symbol")
                            summary["name"] = row.get("name")
                            summary["name_key"] = row.get("name_key")
                            dist_summary_frames.append(summary)
        if corp_frames:
            corp_updates = pd.concat(corp_frames, ignore_index=True)
            merged_corp_basic = merge_cache(existing_corp_basic, corp_updates, ["issuco_custno"])
            cache.write_frame(
                "official_ksd_corp_basic_live",
                merged_corp_basic,
                meta={"source": "KSD", "notes": "기업기본정보 기업개요 조회", "row_count": int(len(merged_corp_basic)), "saved_at": today.isoformat()},
            )
            existing_corp_basic = merged_corp_basic
        if dist_summary_frames:
            shareholder_updates = pd.concat(dist_summary_frames, ignore_index=True)
            merged_shareholder = merge_cache(existing_shareholder, shareholder_updates, ["issuco_custno"])
            cache.write_frame(
                "official_ksd_shareholder_summary_live",
                merged_shareholder,
                meta={"source": "KSD", "notes": "주식분포내역 주주별현황 요약", "row_count": int(len(merged_shareholder)), "saved_at": today.isoformat()},
            )
            existing_shareholder = merged_shareholder
        report["corp_basic_rows"] = int(len(existing_corp_basic))
        report["shareholder_summary_rows"] = int(len(existing_shareholder))

    report["name_lookup_rows"] = int(len(existing_name_map)) if isinstance(existing_name_map, pd.DataFrame) else 0
    report["market_code_rows"] = int(len(existing_market_codes)) if isinstance(existing_market_codes, pd.DataFrame) else 0
    report["listing_info_rows"] = int(len(existing_listing)) if isinstance(existing_listing, pd.DataFrame) else 0
    report["corp_basic_rows"] = int(len(existing_corp_basic)) if isinstance(existing_corp_basic, pd.DataFrame) else 0
    report["shareholder_summary_rows"] = int(len(existing_shareholder)) if isinstance(existing_shareholder, pd.DataFrame) else 0
    report["krx_listed_rows"] = int(len(krx_master)) if isinstance(krx_master, pd.DataFrame) else 0

    # issue-friendly overlays for app consumption
    official_issue_frames: list[pd.DataFrame] = []
    if isinstance(existing_listing, pd.DataFrame) and not existing_listing.empty:
        official_issue_frames.append(issue_frame_from_official_listing(existing_listing))
    if isinstance(existing_corp_basic, pd.DataFrame) and not existing_corp_basic.empty:
        official_issue_frames.append(issue_frame_from_official_basic(existing_corp_basic, mapping=existing_name_map))
    if isinstance(krx_master, pd.DataFrame) and not krx_master.empty:
        official_issue_frames.append(issue_frame_from_krx_master(krx_master))
    if official_issue_frames:
        official_issue_overlay = pd.concat([frame for frame in official_issue_frames if frame is not None and not frame.empty], ignore_index=True)
        cache.write_frame(
            "official_issue_overlay_live",
            official_issue_overlay,
            meta={"source": "KSD", "notes": "앱 표시용 공식 API 오버레이", "row_count": int(len(official_issue_overlay)), "saved_at": today.isoformat()},
        )
        report["official_issue_overlay_rows"] = int(len(official_issue_overlay))

    report["ok"] = any(int(report.get(key) or 0) > 0 for key in ["name_lookup_rows", "market_code_rows", "listing_info_rows", "corp_basic_rows", "shareholder_summary_rows", "official_issue_overlay_rows", "krx_listed_rows"])
    print(json.dumps(report, ensure_ascii=False))


if __name__ == "__main__":
    main()
