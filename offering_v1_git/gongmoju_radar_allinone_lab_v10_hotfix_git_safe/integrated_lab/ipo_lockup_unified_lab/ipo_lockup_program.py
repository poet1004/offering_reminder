
from __future__ import annotations

import argparse
import json
import math
import os
import re
import sys
import time
import subprocess
from dataclasses import dataclass, asdict
from io import StringIO
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import pandas as pd
import requests

try:
    import mojito  # type: ignore
except Exception:
    mojito = None

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
)

KIND_CORP_DOWNLOAD_URLS = [
    "https://kind.krx.co.kr/corpgeneral/corpList.do?method=download&searchType=13",
    "https://kind.krx.co.kr/corpgeneral/corpList.do?method=download",
]
THIRTYEIGHT_MOBILE_FUND_URL = "https://m.38.co.kr/ipo/fund.php"

PREFERRED_PATTERNS = [r"우$", r"우B$", r"우C$", r"우선주"]
SPAC_PATTERNS = [r"스팩", r"SPAC"]

MIDHOLD_BY_TERM = {
    "15D": 5,
    "1M": 21,
    "3M": 32,
    "6M": 63,
    "1Y": 126,
}


@dataclass
class UniverseConfig:
    start_date: str = "20200101"
    end_date: Optional[str] = None
    terms: List[str] = None
    exclude_spac: bool = True
    exclude_preferred: bool = True
    post_regime_change_only: bool = True
    regime_change_date: str = "2023-06-26"
    ipo_start_page: int = 1
    ipo_end_page: int = 120
    fetch_38_detail_pages: bool = True
    use_cache_kind: bool = True
    use_cache_ipo: bool = True
    # local KIND export files placed next to program or passed via config
    local_kind_master_file: Optional[str] = None
    require_ipo_price_for_dataset: bool = False

    def __post_init__(self):
        if self.terms is None:
            self.terms = ["15D", "1M", "3M", "6M", "1Y"]


@dataclass
class StrategySpec:
    name: str
    term: str
    entry_mode: str = "close"  # open, close, next_day_open, next_day_close
    days_after_unlock: int = 0
    hold_days_after_entry: int = 0
    min_prev_close_vs_ipo: Optional[float] = None
    max_prev_close_vs_ipo: Optional[float] = None
    min_entry_price_vs_ipo: Optional[float] = None
    max_entry_price_vs_ipo: Optional[float] = None


@dataclass
class CostConfig:
    buy_cost: float = 0.00015
    sell_cost: float = 0.00215


def load_json(path: str | Path) -> Dict[str, Any]:
    return json.loads(Path(path).read_text(encoding="utf-8"))


def load_kis_credentials(path: str | Path) -> Tuple[str, str, str]:
    lines = [x.strip() for x in Path(path).read_text(encoding="utf-8").splitlines() if x.strip()]
    if len(lines) < 3:
        raise ValueError("키 파일은 최소 3줄(app key, app secret, 계좌번호)이 필요합니다.")
    return lines[0], lines[1], lines[2]


def make_broker(key_file: str | Path, mock: bool = False):
    if mojito is None:
        raise ImportError("mojito2가 필요합니다. 먼저 00_install_packages.bat 를 실행하세요.")
    api_key, api_secret, acc_no = load_kis_credentials(key_file)
    return mojito.KoreaInvestment(api_key=api_key, api_secret=api_secret, acc_no=acc_no, mock=mock)


def normalize_name_key(name: Any) -> str:
    s = str(name or "").strip()
    s = re.sub(r"\(.*?\)", "", s)
    s = s.replace("㈜", "").replace("(주)", "").replace("주식회사", "")
    s = re.sub(r"\s+", "", s)
    s = re.sub(r"[^0-9A-Za-z가-힣]", "", s)
    return s.lower()


def _parse_any_date_series(s: pd.Series) -> pd.Series:
    x = s.astype(str).str.strip()
    x = x.str.replace(r"[^0-9./-]", "", regex=True)
    x = x.str.replace(".", "/", regex=False).str.replace("-", "/", regex=False)
    out = pd.to_datetime(x, format="%Y/%m/%d", errors="coerce")
    mask = out.isna()
    if mask.any():
        y = x[mask].str.replace("/", "", regex=False)
        out.loc[mask] = pd.to_datetime(y, format="%Y%m%d", errors="coerce")
    return out


def _http_get_text(url: str, timeout: int = 10) -> str:
    headers = {"User-Agent": USER_AGENT}
    r = requests.get(url, headers=headers, timeout=timeout)
    r.raise_for_status()
    if not r.encoding or r.encoding.lower() == "iso-8859-1":
        r.encoding = r.apparent_encoding or "euc-kr"
    return r.text


def _normalize_col_label(label: Any) -> str:
    s = str(label if label is not None else "").replace("\xa0", " ").strip()
    s = re.sub(r"\s+", "", s)
    s = s.replace("(", "").replace(")", "")
    return s.lower()


def _flatten_col_label(label: Any) -> str:
    if isinstance(label, tuple):
        parts = []
        for p in label:
            t = str(p if p is not None else "").replace("\xa0", " ").strip()
            if not t or t.lower().startswith("unnamed"):
                continue
            parts.append(t)
        label = " ".join(parts)
    else:
        label = str(label if label is not None else "").replace("\xa0", " ").strip()
    label = re.sub(r"\s+", " ", label).strip()
    return label


def _detect_col(df: pd.DataFrame, keywords: Sequence[str]) -> Optional[str]:
    norm = {c: _normalize_col_label(c) for c in df.columns}
    for col, n in norm.items():
        if any(k.lower() in n for k in keywords):
            return col
    return None


def _load_table_file(path: Path) -> List[pd.DataFrame]:
    suffix = path.suffix.lower()
    sample = b""
    try:
        sample = path.read_bytes()[:4096]
    except Exception:
        sample = b""
    low = sample.lower()
    htmlish = (b"<html" in low) or (b"<table" in low) or (b"<!doctype html" in low)

    def _read_htmlish_file(p: Path) -> List[pd.DataFrame]:
        for enc in ["euc-kr", "cp949", "utf-8-sig", "utf-8", "latin1"]:
            try:
                txt = p.read_text(encoding=enc, errors="ignore")
                if "<table" not in txt.lower():
                    continue
                return pd.read_html(StringIO(txt), displayed_only=False)
            except Exception:
                continue
        return []

    if suffix in {".xlsx", ".xls"}:
        tables: List[pd.DataFrame] = []
        if not htmlish:
            try:
                xls = pd.ExcelFile(path)
                for sn in xls.sheet_names:
                    for kwargs in ({}, {"header": [0, 1]}, {"header": None}):
                        try:
                            tables.append(pd.read_excel(path, sheet_name=sn, **kwargs))
                        except Exception:
                            pass
            except Exception:
                pass
        if not tables:
            tables = _read_htmlish_file(path)
        return tables

    if suffix == ".csv":
        for enc in ["utf-8-sig", "cp949", "euc-kr", "utf-8"]:
            try:
                return [pd.read_csv(path, encoding=enc), pd.read_csv(path, encoding=enc, header=None)]
            except Exception:
                continue
        raise RuntimeError(f"CSV를 읽지 못했습니다: {path}")

    if suffix in {".html", ".htm"} or htmlish:
        return _read_htmlish_file(path)

    return []


_KIND_HEADER_TOKENS = [
    "회사명", "종목명", "기업명", "종목코드", "단축코드", "상장일", "신규상장일",
    "공모가", "수정공모가", "확정공모가", "시장구분", "주관사", "상장주선인", "최초상장주식수",
]


def _prepare_kind_table(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()

    work = df.copy()
    work = work.dropna(axis=1, how="all")
    work.columns = [_flatten_col_label(c) or f"col_{i}" for i, c in enumerate(work.columns)]

    col_blob = " ".join(_normalize_col_label(c) for c in work.columns)
    token_hits = sum(tok.lower() in col_blob for tok in [t.lower() for t in _KIND_HEADER_TOKENS])

    if token_hits < 2:
        best_idx = None
        best_score = 0
        for i in range(min(len(work), 8)):
            row_vals = [_normalize_col_label(v) for v in work.iloc[i].tolist()]
            score = sum(any(tok.lower() in rv for rv in row_vals) for tok in _KIND_HEADER_TOKENS)
            if score > best_score:
                best_score = score
                best_idx = i
        if best_idx is not None and best_score >= 2:
            new_cols = [_flatten_col_label(v) or f"col_{j}" for j, v in enumerate(work.iloc[best_idx].tolist())]
            work = work.iloc[best_idx + 1:].copy()
            work.columns = new_cols

    work = work.dropna(axis=1, how="all").reset_index(drop=True)
    work.columns = [_flatten_col_label(c) or f"col_{i}" for i, c in enumerate(work.columns)]
    return work


def _first_series_by_matchers(work: pd.DataFrame, matchers: Sequence) -> Optional[pd.Series]:
    labels = [_normalize_col_label(c) for c in work.columns]
    chosen: Optional[pd.Series] = None
    for matcher in matchers:
        cur: Optional[pd.Series] = None
        for idx, (raw, norm) in enumerate(zip(work.columns, labels)):
            if matcher(str(raw), norm):
                s = work.iloc[:, idx]
                if isinstance(s, pd.DataFrame):
                    s = s.iloc[:, 0]
                s = s.reset_index(drop=True)
                cur = s if cur is None else cur.combine_first(s)
        if cur is not None:
            chosen = cur if chosen is None else chosen.combine_first(cur)
    return chosen


def _normalize_kind_table(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()

    work = _prepare_kind_table(df)
    if work.empty:
        return pd.DataFrame()

    def has_any(norm: str, keywords: Sequence[str]) -> bool:
        return any(k.lower() in norm for k in keywords)

    company_s = _first_series_by_matchers(work, [
        lambda raw, norm: has_any(norm, ["회사명", "종목명", "기업명"]) and "자동완성" not in raw,
        lambda raw, norm: has_any(norm, ["name"]),
    ])
    symbol_s = _first_series_by_matchers(work, [
        lambda raw, norm: has_any(norm, ["종목코드", "단축코드", "회사코드"]),
        lambda raw, norm: has_any(norm, ["symbol", "ticker"]),
    ])
    listing_s = _first_series_by_matchers(work, [
        lambda raw, norm: has_any(norm, ["신규상장일", "상장일"]) and "상장유형" not in raw and "상장주식" not in raw,
        lambda raw, norm: has_any(norm, ["listingdate", "listing_date"]),
    ])

    # 사용자 요청: 공모가 우선 -> 확정공모가 -> (수정)공모가
    ipo_s = _first_series_by_matchers(work, [
        lambda raw, norm: ("공모가" in raw or "공모가" in norm) and not any(x in raw for x in ["수정", "확정", "희망", "대비", "주가", "금액"]),
        lambda raw, norm: "확정공모가" in raw or "확정공모가" in norm,
        lambda raw, norm: any(x in raw or x in norm for x in ["수정공모가", "(수정)공모가"]),
        lambda raw, norm: "ipo_price" in norm or "ipoprice" in norm,
    ])

    market_s = _first_series_by_matchers(work, [
        lambda raw, norm: has_any(norm, ["시장구분"]) or (norm == "시장"),
        lambda raw, norm: has_any(norm, ["market"]),
    ])
    mgr_s = _first_series_by_matchers(work, [
        lambda raw, norm: has_any(norm, ["상장주선인", "주관사", "지정자문인"]),
        lambda raw, norm: has_any(norm, ["leadmanager", "lead_manager"]),
    ])
    shares_s = _first_series_by_matchers(work, [
        lambda raw, norm: has_any(norm, ["최초상장주식수", "상장주식수", "상장주식", "주식수"]),
        lambda raw, norm: has_any(norm, ["listedshares", "listed_shares"]),
    ])

    if company_s is None or listing_s is None:
        return pd.DataFrame()

    out = pd.DataFrame(index=range(len(work)))
    out["name"] = company_s.astype(str).str.strip()
    out["name_key"] = out["name"].map(normalize_name_key)
    if symbol_s is not None:
        out["symbol"] = symbol_s.astype(str).str.extract(r"(\d{1,6})", expand=False).str.zfill(6)
    else:
        out["symbol"] = pd.NA

    out["listing_date"] = _parse_any_date_series(listing_s.astype(str))

    if ipo_s is not None:
        out["ipo_price"] = pd.to_numeric(
            ipo_s.astype(str).str.replace(",", "", regex=False).str.extract(r"([0-9]+(?:\.[0-9]+)?)", expand=False),
            errors="coerce",
        )
    else:
        out["ipo_price"] = math.nan

    out["market"] = market_s.astype(str).str.strip() if market_s is not None else pd.NA
    out["lead_manager"] = mgr_s.astype(str).str.strip() if mgr_s is not None else pd.NA
    out["listed_shares"] = (
        pd.to_numeric(shares_s.astype(str).str.replace(",", "", regex=False).str.extract(r"([0-9]+(?:\.[0-9]+)?)", expand=False), errors="coerce")
        if shares_s is not None else math.nan
    )

    out = out.dropna(subset=["name", "listing_date"]).copy()
    bad_name = out["name"].astype(str).str.contains(r"회사명|종목명|기업명|조회 결과|선택조건|신규상장기업현황|공모가대비", na=False)
    out = out[~bad_name].copy()
    out = out[out["name"].astype(str).str.len() > 0].reset_index(drop=True)
    return out


def fetch_kind_corp_master(cache_file: Path, use_cache: bool = True) -> pd.DataFrame:
    if use_cache and cache_file.exists():
        return pd.read_csv(cache_file, parse_dates=["listing_date"])

    last_err = None
    for url in KIND_CORP_DOWNLOAD_URLS:
        try:
            text = _http_get_text(url)
            tables = pd.read_html(StringIO(text), displayed_only=False)
            candidates = [_normalize_kind_table(t) for t in tables]
            candidates = [t for t in candidates if not t.empty]
            if not candidates:
                continue
            out = pd.concat(candidates, ignore_index=True)
            out = (
                out.sort_values(["listing_date", "name_key"])
                .drop_duplicates(subset=["name_key"], keep="last")
                .reset_index(drop=True)
            )
            cache_file.parent.mkdir(parents=True, exist_ok=True)
            out.to_csv(cache_file, index=False, encoding="utf-8-sig")
            return out
        except Exception as e:
            last_err = e
            continue
    raise RuntimeError(f"KIND corpList 다운로드 실패: {last_err}")


def merge_kind_local_exports(base_master: pd.DataFrame, root: Path, override_file: Optional[str] = None) -> pd.DataFrame:
    def _candidate_files() -> List[Path]:
        files: List[Path] = []
        if override_file:
            p = Path(override_file)
            if not p.is_absolute():
                p = (root / p).resolve()
            if p.exists():
                files.append(p)

        explicit_names = [
            "kind_master.xlsx", "kind_master.xls", "kind_master.csv",
            "kind_listing.xlsx", "kind_listing.xls", "kind_listing.csv",
            "kind_pubprc.xlsx", "kind_pubprc.xls", "kind_pubprc.csv",
            "신규상장기업현황.xlsx", "신규상장기업현황.xls", "신규상장기업현황.csv",
            "종목별 공모가 대비 주가등락률 현황.xlsx", "종목별 공모가 대비 주가등락률 현황.xls", "종목별 공모가 대비 주가등락률 현황.csv",
        ]
        for base in [root, Path.home() / "Downloads", Path.home() / "Desktop"]:
            for name in explicit_names:
                p = base / name
                if p.exists() and p not in files:
                    files.append(p)

            for pat in ["*kind*.xlsx", "*kind*.xls", "*kind*.csv", "*신규상장*.xlsx", "*공모가*.xlsx", "*공모가*.xls", "*pubprc*.xlsx", "*listing*.xlsx"]:
                for p in sorted(base.glob(pat), key=lambda x: x.stat().st_mtime, reverse=True):
                    low = p.name.lower()
                    if any(bad in low for bad in ["kind_ipo_master", "combined_master", "synthetic_ipo_events", "dataset_funnel", "backtest"]):
                        continue
                    if p not in files:
                        files.append(p)
        return files

    files = _candidate_files()
    if not files:
        return base_master

    frames = []
    debug_rows = []
    for f in files:
        try:
            tables = _load_table_file(f)
            if not tables:
                debug_rows.append({"file": str(f), "tables": 0, "rows": 0, "ipo_price_notna": 0, "status": "no_tables"})
                continue
            any_ok = False
            for t in tables:
                n = _normalize_kind_table(t)
                if not n.empty:
                    any_ok = True
                    frames.append(n)
                    debug_rows.append({
                        "file": str(f),
                        "tables": len(tables),
                        "rows": int(len(n)),
                        "ipo_price_notna": int(pd.to_numeric(n.get("ipo_price"), errors="coerce").notna().sum()) if "ipo_price" in n.columns else 0,
                        "status": "ok",
                    })
            if not any_ok:
                debug_rows.append({"file": str(f), "tables": len(tables), "rows": 0, "ipo_price_notna": 0, "status": "normalized_empty"})
        except Exception as e:
            debug_rows.append({"file": str(f), "tables": 0, "rows": 0, "ipo_price_notna": 0, "status": f"error:{type(e).__name__}"})
            continue
    if debug_rows:
        try:
            pd.DataFrame(debug_rows).to_csv(root / "kind_local_parse_debug.csv", index=False, encoding="utf-8-sig")
        except Exception:
            pass
    if not frames:
        return base_master

    local = pd.concat(frames, ignore_index=True)
    local = (
        local.sort_values(["listing_date", "name_key"])
        .drop_duplicates(subset=["name_key"], keep="last")
        .reset_index(drop=True)
    )

    master = base_master.copy()
    master["name_key"] = master["name"].map(normalize_name_key)
    local["name_key"] = local["name"].map(normalize_name_key)

    temp = local[["name_key", "ipo_price", "listing_date", "market", "lead_manager", "listed_shares"]].rename(
        columns={
            "ipo_price": "ipo_price_local",
            "listing_date": "listing_date_local",
            "market": "market_local",
            "lead_manager": "lead_manager_local",
            "listed_shares": "listed_shares_local",
        }
    )
    out = master.merge(temp, on="name_key", how="left")
    for col in ["ipo_price", "listing_date", "market", "lead_manager", "listed_shares"]:
        local_col = f"{col}_local"
        if local_col in out.columns:
            out[col] = out[col].combine_first(out[local_col])
            out = out.drop(columns=[local_col])
    return out



def _load_local_seed_master(root: Path, override_file: Optional[str] = None) -> pd.DataFrame:
    candidates: List[Path] = []
    if override_file:
        p = Path(override_file)
        if not p.is_absolute():
            p = (root / p).resolve()
        candidates.append(p)

    candidates.extend([
        root / "kind_master.csv",
        root / "kind_listing.csv",
        root / "ipo_seed_master.csv",
        root / "workspace" / "dataset_out" / "live_issue_seed.csv",
        root / "workspace" / "dataset_out" / "kind_ipo_master.csv",
    ])

    seen: set[str] = set()
    for candidate in candidates:
        try:
            resolved = str(candidate.resolve())
        except Exception:
            resolved = str(candidate)
        if resolved in seen or not candidate.exists():
            continue
        seen.add(resolved)
        try:
            tables = _load_table_file(candidate)
        except Exception:
            tables = []
        for table in tables:
            normalized = _normalize_kind_table(table)
            if normalized is not None and not normalized.empty:
                return normalized
    return pd.DataFrame(columns=["name", "name_key", "symbol", "listing_date", "ipo_price", "market", "lead_manager", "listed_shares"])

def _request_38_text(url: str, timeout: int = 8) -> str:
    headers = {"User-Agent": USER_AGENT}
    r = requests.get(url, headers=headers, timeout=timeout)
    r.raise_for_status()
    r.encoding = "euc-kr"
    return r.text


def _html_to_text(html: str) -> str:
    x = re.sub(r"(?is)<script.*?</script>", " ", html)
    x = re.sub(r"(?is)<style.*?</style>", " ", x)
    x = re.sub(r"(?is)<!--.*?-->", " ", x)
    x = x.replace("&nbsp;", " ").replace("&#160;", " ")
    x = re.sub(r"(?is)<[^>]+>", " ", x)
    x = re.sub(r"\s+", " ", x)
    return x.strip()


def _extract_38_detail_links(html: str) -> pd.DataFrame:
    pattern = re.compile(
        r"""href=["'](?P<href>(?:/html/fund/\?(?:l=&)?no=\d+&o=v|/ipo/fund_view\.php\?no=\d+(?:&page=\d+)?|fund_view\.php\?no=\d+(?:&page=\d+)?))["'][^>]*>(?P<text>.*?)</a>""",
        re.I | re.S,
    )
    rows = []
    for m in pattern.finditer(html):
        href = m.group("href")
        text = re.sub(r"(?is)<[^>]+>", " ", m.group("text"))
        text = re.sub(r"\s+", " ", text).strip()
        if not text:
            continue
        if any(bad in text for bad in ["분석보기", "상세", "기업개요"]):
            continue
        rows.append({
            "detail_url": requests.compat.urljoin("https://m.38.co.kr" if "/ipo/" in href or href.startswith("fund_view.php") else "https://www.38.co.kr", href),
            "name_hint": text,
        })
    if not rows:
        return pd.DataFrame(columns=["detail_url", "name_hint"])
    return pd.DataFrame(rows).drop_duplicates(subset=["detail_url"], keep="first").reset_index(drop=True)


def _parse_value_near_labels_from_tables(html: str, labels: Sequence[str], value_regex: str, max_ahead_cells: int = 8) -> Optional[str]:
    try:
        tables = pd.read_html(StringIO(html))
    except Exception:
        return None
    for t in tables:
        arr = t.fillna("").astype(str).replace({r"\xa0": " "}, regex=True).values.flatten().tolist()
        arr = [re.sub(r"\s+", " ", str(x)).strip() for x in arr if str(x).strip()]
        if not arr:
            continue
        for i, cell in enumerate(arr):
            if any(lbl in cell for lbl in labels):
                window = " ".join(arr[i:i + max_ahead_cells])
                m = re.search(value_regex, window)
                if m:
                    return m.group(1)
                for nxt in arr[i + 1:i + max_ahead_cells]:
                    m = re.search(value_regex, nxt)
                    if m:
                        return m.group(1)
    return None


def _parse_38_detail_page(html: str, fallback_name: str = "", source_url: str = "") -> Dict[str, object]:
    text = _html_to_text(html)

    name = fallback_name.strip()
    m = re.search(r"IPO공모\s*>\s*(.*?)\s*공모주청약", text)
    if m:
        name = m.group(1).strip()

    listing_raw = None
    for pat in [
        r"(?:주요일정\s*[·ㆍ]?\s*)?(?:신규상장일|상장일)\s*[:：,;]?\s*([0-9]{4}[./-][0-9]{1,2}[./-][0-9]{1,2})",
        r"(?:신규상장|상장)\s*[:：,;]?\s*([0-9]{4}[./-][0-9]{1,2}[./-][0-9]{1,2})",
    ]:
        m = re.search(pat, text)
        if m:
            listing_raw = m.group(1)
            break
    if listing_raw is None:
        listing_raw = _parse_value_near_labels_from_tables(
            html,
            labels=("신규상장일", "상장일"),
            value_regex=r"([0-9]{4}[./-][0-9]{1,2}[./-][0-9]{1,2})",
        )
    listing_date = pd.NaT
    if listing_raw:
        parsed = _parse_any_date_series(pd.Series([listing_raw]))
        listing_date = parsed.iloc[0] if not parsed.empty else pd.NaT

    ipo_raw = None
    for pat in [
        r"확정공모가\s*[:：,;]?\s*([0-9][0-9,]{2,})\s*원",
        r"확정공모가\s*[:：,;]?\s*([0-9][0-9,]{2,})",
    ]:
        m = re.search(pat, text)
        if m:
            ipo_raw = m.group(1)
            break
    if ipo_raw is None:
        ipo_raw = _parse_value_near_labels_from_tables(
            html,
            labels=("확정공모가",),
            value_regex=r"([0-9][0-9,]{2,})",
        )
    ipo_price = pd.to_numeric(pd.Series([ipo_raw]).astype(str).str.replace(",", "", regex=False), errors="coerce").iloc[0]

    return {
        "name": name if name else math.nan,
        "ipo_price": ipo_price,
        "listing_date": listing_date,
        "source_kind": "detail",
        "source_url": source_url,
    }


def fetch_ipo_master_from_38(start_page: int = 1, end_page: int = 120, sleep_sec: float = 0.25, use_cache_file: Optional[Path] = None, fetch_detail_pages: bool = True, request_timeout: int = 8, max_consecutive_failures: int = 4) -> pd.DataFrame:
    if use_cache_file and use_cache_file.exists():
        cached = pd.read_csv(use_cache_file, parse_dates=["listing_date"])
        if "ipo_price" in cached.columns and len(cached) > 0 and pd.to_numeric(cached["ipo_price"], errors="coerce").notna().mean() >= 0.20:
            return cached

    frames: List[pd.DataFrame] = []
    detail_frames: List[pd.DataFrame] = []
    candidates = [
        (THIRTYEIGHT_MOBILE_FUND_URL + "?page={page}", "mobile_subscription"),
        ("https://www.38.co.kr/html/fund/index.htm?o=nw&page={page}", "new_listing"),
        ("https://www.38.co.kr/html/fund/index.htm?o=k&page={page}", "subscription"),
    ]

    consecutive_failures = 0
    abort_early = False
    for page in range(start_page, end_page + 1):
        for tmpl, source_kind in candidates:
            url = tmpl.format(page=page)
            try:
                html = _request_38_text(url, timeout=request_timeout)
                consecutive_failures = 0
            except Exception as exc:
                consecutive_failures += 1
                if consecutive_failures >= max_consecutive_failures and not frames:
                    print(f"[경고] 38 연결 실패가 연속 {consecutive_failures}회 발생하여 조기 종료합니다: {exc}")
                    abort_early = True
                    break
                continue

            if fetch_detail_pages:
                links = _extract_38_detail_links(html)
                if not links.empty:
                    links = links.copy()
                    links["from_kind"] = source_kind
                    detail_frames.append(links)

            try:
                tables = pd.read_html(StringIO(html))
            except Exception:
                tables = []
            for t in tables:
                cols = [str(c).strip().replace("\xa0", " ") for c in t.columns]
                if not any(("종목명" in c) or ("기업명" in c) or ("회사명" in c) for c in cols):
                    continue
                t = t.copy()
                t.columns = cols
                name_col = next((c for c in cols if ("종목명" in c) or ("기업명" in c) or ("회사명" in c)), None)
                listing_col = next((c for c in cols if "상장일" in c), None)
                ipo_col = next((c for c in cols if "확정공모가" in c), None)
                if ipo_col is None:
                    ipo_col = next((c for c in cols if ("공모가" in c and "희망" not in c)), None)
                if not name_col or (listing_col is None and ipo_col is None):
                    continue
                out = pd.DataFrame()
                out["name"] = t[name_col].astype(str).str.strip()
                out["listing_date"] = _parse_any_date_series(t[listing_col]) if listing_col else pd.NaT
                if ipo_col:
                    out["ipo_price"] = pd.to_numeric(
                        t[ipo_col].astype(str).str.replace(",", "", regex=False).str.extract(r"([0-9]+)", expand=False),
                        errors="coerce",
                    )
                else:
                    out["ipo_price"] = math.nan
                out["source_kind"] = source_kind
                out["source_url"] = url
                out = out[out["name"].notna()]
                out = out[~out["name"].str.contains(r"종목명|기업명|분석|비고|^nan$", na=False)]
                if not out.empty:
                    frames.append(out)
            time.sleep(sleep_sec)
        if abort_early:
            break

    if fetch_detail_pages and detail_frames:
        all_links = pd.concat(detail_frames, ignore_index=True).drop_duplicates(subset=["detail_url"], keep="first")
        detail_rows = []
        for _, row in all_links.iterrows():
            try:
                html = _request_38_text(row["detail_url"], timeout=request_timeout)
                detail_rows.append(_parse_38_detail_page(html, fallback_name=str(row.get("name_hint", "")), source_url=row["detail_url"]))
            except Exception:
                continue
            time.sleep(sleep_sec)
        if detail_rows:
            frames.append(pd.DataFrame(detail_rows))

    if not frames:
        return pd.DataFrame(columns=["name_key", "name", "ipo_price", "listing_date", "source_kinds"])
    merged = pd.concat(frames, ignore_index=True)
    merged["name"] = merged["name"].astype(str).str.strip()
    merged = merged[merged["name"].notna() & (merged["name"] != "")]
    merged["name_key"] = merged["name"].map(normalize_name_key)
    merged["ipo_price"] = pd.to_numeric(merged["ipo_price"], errors="coerce")
    merged["listing_date"] = pd.to_datetime(merged["listing_date"], errors="coerce")

    def _pick(group: pd.DataFrame) -> pd.Series:
        listing_candidates = group["listing_date"].dropna().sort_values()
        ipo_candidates = group["ipo_price"].dropna()
        non_empty_name = group["name"].dropna().astype(str)
        return pd.Series({
            "name": non_empty_name.iloc[0] if len(non_empty_name) else math.nan,
            "ipo_price": ipo_candidates.iloc[-1] if len(ipo_candidates) else math.nan,
            "listing_date": listing_candidates.iloc[-1] if len(listing_candidates) else pd.NaT,
            "source_kinds": ",".join(sorted(set(group["source_kind"].dropna().astype(str).tolist()))),
        })

    master = merged.groupby("name_key", dropna=False).apply(_pick).reset_index()
    master = master.sort_values(["listing_date", "name_key"], na_position="last").reset_index(drop=True)
    if use_cache_file:
        use_cache_file.parent.mkdir(parents=True, exist_ok=True)
        master.to_csv(use_cache_file, index=False, encoding="utf-8-sig")
    return master


def combine_masters(kind_master: pd.DataFrame, price_master: pd.DataFrame) -> pd.DataFrame:
    base = kind_master.copy()
    base["name_key"] = base["name"].map(normalize_name_key)
    temp = price_master.copy()
    if "name_key" not in temp.columns:
        temp["name_key"] = temp["name"].map(normalize_name_key)
    temp = temp[["name_key", "ipo_price", "listing_date"]].rename(columns={"ipo_price": "ipo_price_ext", "listing_date": "listing_date_ext"})
    temp = temp.drop_duplicates(subset=["name_key"], keep="last")
    out = base.merge(temp, on="name_key", how="left")
    base_price = pd.to_numeric(out["ipo_price"], errors="coerce") if "ipo_price" in out.columns else pd.Series(pd.NA, index=out.index, dtype="float64")
    ext_price = pd.to_numeric(out["ipo_price_ext"], errors="coerce") if "ipo_price_ext" in out.columns else pd.Series(pd.NA, index=out.index, dtype="float64")
    out["ipo_price"] = base_price.where(base_price.notna(), ext_price)
    base_listing = pd.to_datetime(out["listing_date"], errors="coerce") if "listing_date" in out.columns else pd.Series(pd.NaT, index=out.index)
    ext_listing = pd.to_datetime(out["listing_date_ext"], errors="coerce") if "listing_date_ext" in out.columns else pd.Series(pd.NaT, index=out.index)
    out["listing_date"] = base_listing.where(base_listing.notna(), ext_listing)
    out["ipo_price_source"] = pd.NA
    out.loc[pd.to_numeric(out.get("ipo_price_ext"), errors="coerce").notna(), "ipo_price_source"] = "38"
    out.loc[pd.to_numeric(out.get("ipo_price"), errors="coerce").notna() & out["ipo_price_source"].isna(), "ipo_price_source"] = "kind"
    drop_cols = [c for c in ["ipo_price_ext", "listing_date_ext"] if c in out.columns]
    out = out.drop(columns=drop_cols)
    out = out.sort_values(["listing_date", "name_key"], na_position="last").drop_duplicates(subset=["name_key"], keep="last").reset_index(drop=True)
    return out


def _add_offset(date: pd.Timestamp, term: str) -> pd.Timestamp:
    date = pd.Timestamp(date).normalize()
    term = str(term).upper()
    if term == "15D":
        return date + pd.Timedelta(days=15)
    if term == "1M":
        return date + pd.DateOffset(months=1)
    if term == "3M":
        return date + pd.DateOffset(months=3)
    if term == "6M":
        return date + pd.DateOffset(months=6)
    if term == "1Y":
        return date + pd.DateOffset(years=1)
    raise ValueError(f"지원하지 않는 term: {term}")


def synthesize_events(master: pd.DataFrame, terms: Sequence[str]) -> pd.DataFrame:
    rows = []
    for _, r in master.iterrows():
        listing_date = pd.to_datetime(r["listing_date"], errors="coerce")
        if pd.isna(listing_date):
            continue
        for term in terms:
            rows.append({
                "symbol": str(r["symbol"]).zfill(6),
                "name": r["name"],
                "name_key": r["name_key"],
                "listing_date": listing_date.normalize(),
                "unlock_date": _add_offset(listing_date, term).normalize(),
                "term": term,
                "ipo_price": pd.to_numeric(r.get("ipo_price"), errors="coerce"),
                "market": r.get("market", pd.NA),
                "lead_manager": r.get("lead_manager", pd.NA),
                "listed_shares": pd.to_numeric(r.get("listed_shares"), errors="coerce"),
                "ipo_price_source": r.get("ipo_price_source", pd.NA),
            })
    return pd.DataFrame(rows)


def filter_master(master: pd.DataFrame, cfg: UniverseConfig) -> pd.DataFrame:
    out = master.copy()
    out["listing_date"] = pd.to_datetime(out["listing_date"], errors="coerce")
    start = pd.Timestamp(cfg.start_date)
    out = out[out["listing_date"] >= start].copy()
    if cfg.end_date:
        end = pd.Timestamp(cfg.end_date)
        out = out[out["listing_date"] <= end].copy()
    if cfg.exclude_spac:
        out = out[~out["name"].astype(str).str.contains("|".join(SPAC_PATTERNS), case=False, regex=True, na=False)].copy()
    if cfg.exclude_preferred:
        out = out[~out["name"].astype(str).str.contains("|".join(PREFERRED_PATTERNS), case=False, regex=True, na=False)].copy()
    if cfg.post_regime_change_only:
        cutoff = pd.Timestamp(cfg.regime_change_date).normalize()
        out = out[out["listing_date"].dt.normalize() >= cutoff].copy()
    out = out.reset_index(drop=True)
    return out


def dataset_funnel(kind_master: pd.DataFrame, local_master: pd.DataFrame, price_master: pd.DataFrame, combined_master: pd.DataFrame, filtered_master: pd.DataFrame, events: pd.DataFrame) -> pd.DataFrame:
    rows = [
        {"stage": "kind_corp_list", "rows": len(kind_master)},
        {"stage": "kind_local_master", "rows": len(local_master)},
        {"stage": "price_master_38", "rows": len(price_master)},
        {"stage": "combined_master", "rows": len(combined_master)},
        {"stage": "combined_master_ipo_price_notna", "rows": int(pd.to_numeric(combined_master.get("ipo_price"), errors="coerce").notna().sum()) if not combined_master.empty else 0},
        {"stage": "filtered_master", "rows": len(filtered_master)},
        {"stage": "filtered_master_ipo_price_notna", "rows": int(pd.to_numeric(filtered_master.get("ipo_price"), errors="coerce").notna().sum()) if not filtered_master.empty else 0},
        {"stage": "synthetic_events", "rows": len(events)},
        {"stage": "synthetic_events_ipo_price_notna", "rows": int(pd.to_numeric(events.get("ipo_price"), errors="coerce").notna().sum()) if not events.empty else 0},
    ]
    return pd.DataFrame(rows)


def _convert_daily_response_to_df(resp: Any) -> pd.DataFrame:
    if isinstance(resp, dict):
        # mojito/KIS responses often have output2
        rows = []
        for key in ("output2", "output1", "output"):
            val = resp.get(key)
            if isinstance(val, list):
                rows = val
                break
        if not rows and isinstance(resp.get("output"), list):
            rows = resp["output"]
        if not rows and isinstance(resp, list):
            rows = resp
        df = pd.DataFrame(rows)
    elif isinstance(resp, list):
        df = pd.DataFrame(resp)
    else:
        df = pd.DataFrame(resp)
    if df.empty:
        return pd.DataFrame(columns=["date", "open", "high", "low", "close", "volume"])

    mapping = {}
    for cand in ["stck_bsop_date", "xymd", "date", "bsop_date"]:
        if cand in df.columns:
            mapping[cand] = "date"
            break
    for cand in ["stck_oprc", "open", "oprc"]:
        if cand in df.columns:
            mapping[cand] = "open"
            break
    for cand in ["stck_hgpr", "high", "hgpr"]:
        if cand in df.columns:
            mapping[cand] = "high"
            break
    for cand in ["stck_lwpr", "low", "lwpr"]:
        if cand in df.columns:
            mapping[cand] = "low"
            break
    for cand in ["stck_clpr", "stck_prpr", "close", "clpr", "prpr"]:
        if cand in df.columns:
            mapping[cand] = "close"
            break
    for cand in ["acml_vol", "volume", "vol", "cntg_vol"]:
        if cand in df.columns:
            mapping[cand] = "volume"
            break
    df = df.rename(columns=mapping)

    required = ["date", "open", "high", "low", "close", "volume"]
    for c in required:
        if c not in df.columns:
            df[c] = pd.NA
    df["date"] = _parse_any_date_series(df["date"])
    for c in ["open", "high", "low", "close", "volume"]:
        df[c] = pd.to_numeric(df[c].astype(str).str.replace(",", "", regex=False), errors="coerce")
    df = df.dropna(subset=["date", "close"]).sort_values("date").reset_index(drop=True)
    return df[["date", "open", "high", "low", "close", "volume"]]


def _first_trade_idx_on_or_after(daily: pd.DataFrame, target_date: pd.Timestamp) -> Optional[int]:
    if daily.empty:
        return None
    dates = pd.to_datetime(daily["date"]).reset_index(drop=True)
    target_date = pd.Timestamp(target_date).normalize()
    cand = dates[dates >= target_date]
    if cand.empty:
        return None
    return int(cand.index[0])


class DailyBacktester:
    def __init__(self, broker, cache_dir: str | Path):
        self.broker = broker
        self.cache_dir = Path(cache_dir)
        (self.cache_dir / "daily").mkdir(parents=True, exist_ok=True)

    def fetch_daily_bars(self, symbol: str, start_date: str, end_date: str, adj_price: bool = True, use_cache: bool = True) -> pd.DataFrame:
        # KIS 일봉 조회는 한 번에 최근 약 100건만 내려오는 경우가 있어, 긴 구간은 캘린더 구간을 나눠 합친다.
        # 캐시명에 v2chunk를 붙여 과거 단일호출 캐시와 충돌하지 않게 한다.
        cache_name = f"{symbol}_{start_date}_{end_date}_{'adj' if adj_price else 'raw'}_v3chunk.csv"
        cache_path = self.cache_dir / "daily" / cache_name
        if use_cache and cache_path.exists():
            return pd.read_csv(cache_path, parse_dates=["date"]).sort_values("date").reset_index(drop=True)

        start_ts = pd.Timestamp(start_date)
        end_ts = pd.Timestamp(end_date)
        if end_ts < start_ts:
            return pd.DataFrame(columns=["date", "open", "high", "low", "close", "volume"])

        chunks: List[pd.DataFrame] = []
        # 90캘린더일이면 영업일 기준 대체로 100건 미만이라 KIS 1회 조회 한도에 걸릴 확률이 낮다.
        step_days = 90
        cur = start_ts
        while cur <= end_ts:
            chunk_end = min(cur + pd.Timedelta(days=step_days - 1), end_ts)
            try:
                resp = self.broker.fetch_ohlcv(
                    symbol=symbol,
                    timeframe="D",
                    start_day=cur.strftime("%Y%m%d"),
                    end_day=chunk_end.strftime("%Y%m%d"),
                    adj_price=adj_price,
                )
                df = _convert_daily_response_to_df(resp)
            except Exception:
                df = pd.DataFrame(columns=["date", "open", "high", "low", "close", "volume"])
            if not df.empty:
                chunks.append(df)
            cur = chunk_end + pd.Timedelta(days=1)

        if not chunks:
            return pd.DataFrame(columns=["date", "open", "high", "low", "close", "volume"])

        out = (
            pd.concat(chunks, ignore_index=True)
            .drop_duplicates(subset=["date"], keep="last")
            .sort_values("date")
            .reset_index(drop=True)
        )
        out = out[(out["date"] >= start_ts) & (out["date"] <= end_ts)].reset_index(drop=True)
        if not out.empty:
            out.to_csv(cache_path, index=False, encoding="utf-8-sig")
        return out

    def backtest(self, events: pd.DataFrame, strategies: List[StrategySpec], costs: CostConfig) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        all_trades: List[Dict[str, Any]] = []
        skip_rows: List[Dict[str, Any]] = []

        # 1) 전략/이벤트 조합별로 필요한 조회 구간을 계산한다.
        plan_rows: List[Dict[str, Any]] = []
        symbol_windows: Dict[str, Dict[str, pd.Timestamp]] = {}
        for spec in strategies:
            term_df = events[events["term"] == spec.term].copy().reset_index(drop=True)
            for _, ev in term_df.iterrows():
                symbol = str(ev.get("symbol", "")).zfill(6)
                unlock = pd.Timestamp(ev["unlock_date"]).normalize()
                start_ts = unlock - pd.Timedelta(days=20)
                # hold_days_after_entry는 거래일 기준이라 캘린더상 여유를 2배+30일 둔다.
                cal_pad = max(40, int(spec.days_after_unlock) + int(spec.hold_days_after_entry) * 2 + 30)
                end_ts = unlock + pd.Timedelta(days=cal_pad)
                plan_rows.append({
                    "spec": spec,
                    "ev": ev,
                    "symbol": symbol,
                    "unlock": unlock,
                    "query_start_ts": start_ts,
                    "query_end_ts": end_ts,
                })
                win = symbol_windows.setdefault(symbol, {"min": start_ts, "max": end_ts})
                if start_ts < win["min"]:
                    win["min"] = start_ts
                if end_ts > win["max"]:
                    win["max"] = end_ts

        # 2) 종목별로 일봉을 한 번만 길게 받아온다. (기존 event별 반복 fetch 제거)
        symbol_daily: Dict[str, pd.DataFrame] = {}
        symbol_errors: Dict[str, str] = {}
        total_symbols = len(symbol_windows)
        for idx, (symbol, win) in enumerate(sorted(symbol_windows.items()), start=1):
            start_date = win["min"].strftime("%Y%m%d")
            end_date = win["max"].strftime("%Y%m%d")
            if idx == 1 or idx % 20 == 0 or idx == total_symbols:
                print(f"[backtest preload] {idx}/{total_symbols} {symbol} {start_date}~{end_date}")
            try:
                daily = self.fetch_daily_bars(symbol, start_date, end_date, adj_price=True, use_cache=True)
                symbol_daily[symbol] = daily
            except Exception as e:
                symbol_errors[symbol] = f"{type(e).__name__}: {str(e)[:200]}"
                symbol_daily[symbol] = pd.DataFrame(columns=["date", "open", "high", "low", "close", "volume"])

        # 3) 실제 백테스트
        total_plans = len(plan_rows)
        for n, row in enumerate(plan_rows, start=1):
            spec = row["spec"]
            ev = row["ev"]
            symbol = row["symbol"]
            unlock = row["unlock"]
            start_ts = row["query_start_ts"]
            end_ts = row["query_end_ts"]
            start_date = start_ts.strftime("%Y%m%d")
            end_date = end_ts.strftime("%Y%m%d")

            if n == 1 or n % 100 == 0 or n == total_plans:
                print(f"[backtest events] {n}/{total_plans} {symbol} {spec.name} unlock={unlock.date()}")

            def add_skip(reason: str, **extra: Any) -> None:
                srow: Dict[str, Any] = {
                    "strategy_name": spec.name,
                    "term": spec.term,
                    "entry_mode": spec.entry_mode,
                    "symbol": symbol,
                    "name": ev.get("name"),
                    "listing_date": ev.get("listing_date"),
                    "unlock_date": unlock,
                    "reason": reason,
                    "query_start": start_date,
                    "query_end": end_date,
                }
                srow.update(extra)
                skip_rows.append(srow)

            if symbol_errors.get(symbol):
                add_skip("daily_fetch_error", error_text=symbol_errors[symbol])
                continue

            daily_full = symbol_daily.get(symbol)
            if daily_full is None or daily_full.empty:
                add_skip("daily_empty_symbol")
                continue

            daily = daily_full[(pd.to_datetime(daily_full["date"]) >= start_ts) & (pd.to_datetime(daily_full["date"]) <= end_ts)].reset_index(drop=True)
            if daily.empty:
                add_skip("daily_empty_window")
                continue

            entry_base_day = unlock + pd.Timedelta(days=int(spec.days_after_unlock))
            mode = str(spec.entry_mode).lower()
            if mode == "next_day_open":
                entry_base_day = entry_base_day + pd.Timedelta(days=1)
                entry_price_col = "open"
            elif mode == "next_day_close":
                entry_base_day = entry_base_day + pd.Timedelta(days=1)
                entry_price_col = "close"
            elif mode == "open":
                entry_price_col = "open"
            else:
                entry_price_col = "close"

            entry_idx = _first_trade_idx_on_or_after(daily, entry_base_day)
            if entry_idx is None:
                dmin = pd.to_datetime(daily["date"]).min() if not daily.empty else pd.NaT
                dmax = pd.to_datetime(daily["date"]).max() if not daily.empty else pd.NaT
                add_skip(
                    "entry_not_found",
                    daily_rows=int(len(daily)),
                    daily_min=str(dmin.date()) if pd.notna(dmin) else None,
                    daily_max=str(dmax.date()) if pd.notna(dmax) else None,
                    target_entry_date=str(entry_base_day.date()),
                )
                continue

            ipo_price = pd.to_numeric(pd.Series([ev.get("ipo_price")]), errors="coerce").iloc[0]
            prev_close_vs_ipo = math.nan
            prev_close_date = pd.NaT
            if entry_idx > 0 and pd.notna(ipo_price) and float(ipo_price) > 0:
                prev_row = daily.iloc[entry_idx - 1]
                prev_close = float(prev_row["close"])
                prev_close_date = pd.Timestamp(prev_row["date"])
                if prev_close > 0:
                    prev_close_vs_ipo = prev_close / float(ipo_price)

            if spec.min_prev_close_vs_ipo is not None:
                if pd.isna(ipo_price) or float(ipo_price) <= 0:
                    add_skip("ipo_price_missing_for_prev_filter")
                    continue
                if pd.isna(prev_close_vs_ipo) or prev_close_vs_ipo < float(spec.min_prev_close_vs_ipo):
                    add_skip("prev_close_vs_ipo_below_min", prev_close_vs_ipo=prev_close_vs_ipo, threshold=float(spec.min_prev_close_vs_ipo))
                    continue
            if spec.max_prev_close_vs_ipo is not None:
                if pd.isna(ipo_price) or float(ipo_price) <= 0:
                    add_skip("ipo_price_missing_for_prev_filter")
                    continue
                if pd.isna(prev_close_vs_ipo) or prev_close_vs_ipo > float(spec.max_prev_close_vs_ipo):
                    add_skip("prev_close_vs_ipo_above_max", prev_close_vs_ipo=prev_close_vs_ipo, threshold=float(spec.max_prev_close_vs_ipo))
                    continue

            entry_row = daily.iloc[entry_idx]
            entry_price = float(entry_row[entry_price_col])
            entry_dt = pd.Timestamp(entry_row["date"])
            if not math.isfinite(entry_price) or entry_price <= 0:
                add_skip("entry_price_invalid", entry_price=entry_price)
                continue

            entry_vs_ipo = math.nan
            if pd.notna(ipo_price) and float(ipo_price) > 0:
                entry_vs_ipo = entry_price / float(ipo_price)
                if spec.min_entry_price_vs_ipo is not None and entry_vs_ipo < float(spec.min_entry_price_vs_ipo):
                    add_skip("entry_price_vs_ipo_below_min", entry_price_vs_ipo=entry_vs_ipo, threshold=float(spec.min_entry_price_vs_ipo))
                    continue
                if spec.max_entry_price_vs_ipo is not None and entry_vs_ipo > float(spec.max_entry_price_vs_ipo):
                    add_skip("entry_price_vs_ipo_above_max", entry_price_vs_ipo=entry_vs_ipo, threshold=float(spec.max_entry_price_vs_ipo))
                    continue
            elif spec.min_entry_price_vs_ipo is not None or spec.max_entry_price_vs_ipo is not None:
                add_skip("ipo_price_missing_for_entry_filter")
                continue

            exit_idx = entry_idx + int(spec.hold_days_after_entry)
            if exit_idx >= len(daily):
                add_skip("exit_out_of_range", entry_idx=int(entry_idx), exit_idx=int(exit_idx), daily_rows=int(len(daily)))
                continue
            exit_row = daily.iloc[exit_idx]
            exit_price = float(exit_row["close"])
            exit_dt = pd.Timestamp(exit_row["date"])
            if not math.isfinite(exit_price) or exit_price <= 0:
                add_skip("exit_price_invalid", exit_price=exit_price)
                continue

            gross_ret = exit_price / entry_price - 1.0
            net_ret = (exit_price * (1 - costs.sell_cost)) / (entry_price * (1 + costs.buy_cost)) - 1.0

            all_trades.append({
                "strategy_name": spec.name,
                "term": spec.term,
                "entry_mode": spec.entry_mode,
                "symbol": symbol,
                "name": ev["name"],
                "listing_date": pd.Timestamp(ev["listing_date"]),
                "unlock_date": unlock,
                "entry_dt": entry_dt,
                "entry_price": entry_price,
                "exit_dt": exit_dt,
                "exit_price": exit_price,
                "hold_days_after_entry": int(spec.hold_days_after_entry),
                "ipo_price": ipo_price,
                "ipo_price_source": ev.get("ipo_price_source"),
                "prev_close_vs_ipo": prev_close_vs_ipo,
                "prev_close_date": prev_close_date,
                "entry_price_vs_ipo": entry_vs_ipo,
                "gross_ret": gross_ret,
                "net_ret": net_ret,
            })

        trades = pd.DataFrame(all_trades)
        skips = pd.DataFrame(skip_rows)

        skip_summary = pd.DataFrame()
        if not skips.empty:
            skip_summary = (
                skips.groupby(["strategy_name", "term", "entry_mode", "reason"], dropna=False)
                .size().rename("count").reset_index()
                .sort_values(["strategy_name", "count", "reason"], ascending=[True, False, True])
                .reset_index(drop=True)
            )

        if trades.empty:
            return trades, pd.DataFrame(), pd.DataFrame(), skips, skip_summary

        summary = (
            trades.groupby(["strategy_name", "term", "entry_mode", "hold_days_after_entry"], dropna=False)["net_ret"]
            .agg(
                trades="count",
                win_rate=lambda x: (x > 0).mean(),
                avg_ret="mean",
                median_ret="median",
                sum_ret="sum",
                compound_ret=lambda x: float((1 + x).prod() - 1),
                min_ret="min",
                max_ret="max",
            )
            .reset_index()
            .sort_values(["avg_ret", "win_rate", "trades"], ascending=[False, False, False])
            .reset_index(drop=True)
        )

        annual = (
            trades.assign(year=pd.to_datetime(trades["entry_dt"]).dt.year)
            .groupby(["year", "strategy_name", "term", "entry_mode", "hold_days_after_entry"], dropna=False)["net_ret"]
            .agg(
                trades="count",
                win_rate=lambda x: (x > 0).mean(),
                avg_ret="mean",
                median_ret="median",
                sum_ret="sum",
                compound_ret=lambda x: float((1 + x).prod() - 1),
                min_ret="min",
                max_ret="max",
            )
            .reset_index()
            .sort_values(["year", "strategy_name"])
            .reset_index(drop=True)
        )

        return trades, summary, annual, skips, skip_summary

def make_pretty_pct(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df.copy()
    out = df.copy()
    pct_cols = [c for c in ["win_rate", "avg_ret", "median_ret", "sum_ret", "compound_ret", "min_ret", "max_ret"] if c in out.columns]
    for c in pct_cols:
        out[c] = (pd.to_numeric(out[c], errors="coerce") * 100).round(2)
    return out


def save_csv(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False, encoding="utf-8-sig")


def parse_config(path: str | Path) -> Tuple[UniverseConfig, CostConfig, List[StrategySpec]]:
    cfg = load_json(path)
    universe = UniverseConfig(**cfg.get("universe", {}))
    costs = CostConfig(**cfg.get("costs", {}))
    strategies = [StrategySpec(**x) for x in cfg.get("strategies", [])]
    return universe, costs, strategies


def build_dataset_cli(args) -> None:
    root = Path(__file__).resolve().parent
    universe, _, _ = parse_config(args.config)
    universe.end_date = universe.end_date or pd.Timestamp.today().strftime("%Y%m%d")

    cache_dir = Path(args.cache_dir)
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    kind_cache = cache_dir / "kind" / "corp_master.csv"
    local_seed = _load_local_seed_master(root=root, override_file=universe.local_kind_master_file)
    kind_fetch_error = None
    try:
        base_master = fetch_kind_corp_master(kind_cache, use_cache=universe.use_cache_kind)
    except Exception as exc:
        kind_fetch_error = exc
        if not local_seed.empty:
            base_master = local_seed.copy()
            print(f"[경고] KIND corpList 다운로드 실패로 local seed를 사용합니다: {exc}")
        else:
            raise
    if base_master.empty and not local_seed.empty:
        base_master = local_seed.copy()
        print("[경고] KIND corpList 결과가 비어 있어 local seed를 사용합니다.")

    # 핵심 변경:
    # 1) 같은 repo 안의 앱 live/cache bundle을 seed(kind_master.csv)로 자동 내보낼 수 있다.
    # 2) 사용자가 다운로드한 KIND EXCEL/CSV(신규상장기업현황/공모가대비주가정보)는 여전히 우선 로컬 오버레이 소스로 사용한다.
    # 3) 공모가가 한 건도 없어도 즉시 실패하지 않고 38/app seed 보조값으로 계속 진행한다.
    kind_master = merge_kind_local_exports(base_master, root=root, override_file=universe.local_kind_master_file)
    if kind_master.empty and not local_seed.empty:
        kind_master = local_seed.copy()
    local_master = kind_master.copy()

    local_price_cnt = int(pd.to_numeric(kind_master.get("ipo_price"), errors="coerce").notna().sum()) if not kind_master.empty else 0
    if local_price_cnt == 0:
        print(
            "[경고] 로컬 KIND 공모가를 충분히 읽지 못했습니다. "
            "같은 프로젝트의 app live/cache seed 또는 38 보조 데이터로 계속 진행합니다."
        )
        if kind_fetch_error is not None:
            print(f"[참고] KIND corpList 오류: {kind_fetch_error}")

    # 38은 보조 소스로 사용
    price_cache = cache_dir / "ipo" / f"ipo_master_38_{universe.ipo_start_page}_{universe.ipo_end_page}.csv"
    try:
        price_master = fetch_ipo_master_from_38(
            start_page=universe.ipo_start_page,
            end_page=universe.ipo_end_page,
            sleep_sec=0.2,
            use_cache_file=price_cache if universe.use_cache_ipo else None,
            fetch_detail_pages=universe.fetch_38_detail_pages,
        )
    except Exception as exc:
        print(f"[경고] 38 보조 소스 조회 실패: {exc}")
        price_master = pd.DataFrame(columns=["name_key", "name", "ipo_price", "listing_date", "source_kinds"])

    combined = combine_masters(kind_master, price_master)
    combined_price_cnt = int(pd.to_numeric(combined.get("ipo_price"), errors="coerce").notna().sum()) if not combined.empty else 0
    if combined.empty:
        raise RuntimeError(
            "dataset master를 만들지 못했습니다. KIND/38/app seed가 모두 비어 있습니다. "
            "먼저 앱에서 refresh_live_cache 또는 scripts/export_ipo_seed_to_lab.py 를 실행해 보세요."
        )
    if combined_price_cnt == 0:
        print("[경고] combined_master 공모가가 0건입니다. 가격 기반 필터를 쓰지 않는 전략은 계속 진행할 수 있습니다.")

    filtered = filter_master(combined, universe)
    if universe.require_ipo_price_for_dataset:
        filtered = filtered[pd.to_numeric(filtered["ipo_price"], errors="coerce").notna()].copy().reset_index(drop=True)
    events = synthesize_events(filtered, universe.terms)

    funnel = dataset_funnel(kind_master, local_master, price_master, combined, filtered, events)

    save_csv(kind_master, out_dir / "kind_ipo_master.csv")
    save_csv(price_master, out_dir / "ipo_master_38.csv")
    save_csv(combined, out_dir / "combined_master.csv")
    save_csv(filtered, out_dir / "filtered_master.csv")
    save_csv(events, out_dir / "synthetic_ipo_events.csv")
    save_csv(funnel, out_dir / "dataset_funnel.csv")

    print(f"[완료] dataset 저장: {out_dir}")
    print(funnel.to_string(index=False))
    price_count = int(pd.to_numeric(combined.get("ipo_price"), errors="coerce").notna().sum()) if not combined.empty else 0
    print(f"[정보] combined_master 공모가 채움: {price_count}건")

def backtest_cli(args) -> None:
    _, costs, strategies = parse_config(args.config)
    if not strategies:
        raise RuntimeError("strategies가 비어 있습니다.")
    broker = make_broker(args.key_file, mock=args.mock)
    bt = DailyBacktester(broker, cache_dir=args.cache_dir)

    dataset_csv = Path(args.dataset_csv) if args.dataset_csv else Path(args.out_dir).parent / "dataset_out" / "synthetic_ipo_events.csv"
    if not dataset_csv.exists():
        raise FileNotFoundError(f"dataset csv를 찾을 수 없습니다: {dataset_csv}")
    events = pd.read_csv(dataset_csv, parse_dates=["listing_date", "unlock_date"])

    trades, summary, annual, skips, skip_summary = bt.backtest(events, strategies, costs)
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    save_csv(trades, out_dir / "all_trades.csv")
    save_csv(summary, out_dir / "summary_all.csv")
    save_csv(annual, out_dir / "annual_all.csv")
    save_csv(make_pretty_pct(summary), out_dir / "summary_all_pretty.csv")
    save_csv(make_pretty_pct(annual), out_dir / "annual_all_pretty.csv")
    save_csv(skips, out_dir / "backtest_skip_reasons.csv")
    save_csv(skip_summary, out_dir / "backtest_skip_summary.csv")

    print(f"[완료] backtest 저장: {out_dir}")
    if summary.empty:
        print("주의: summary_all.csv가 비어 있습니다. backtest_skip_summary.csv를 먼저 확인하세요.")
    else:
        print(make_pretty_pct(summary).to_string(index=False))


def cli_main():
    parser = argparse.ArgumentParser(description="IPO lockup synthetic-event daily backtester")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p1 = sub.add_parser("build-dataset")
    p1.add_argument("--config", required=True)
    p1.add_argument("--cache-dir", required=True)
    p1.add_argument("--out-dir", required=True)

    p2 = sub.add_parser("backtest")
    p2.add_argument("--config", required=True)
    p2.add_argument("--key-file", required=True)
    p2.add_argument("--cache-dir", required=True)
    p2.add_argument("--out-dir", required=True)
    p2.add_argument("--dataset-csv", default=None)
    p2.add_argument("--mock", action="store_true")

    args = parser.parse_args()
    if args.cmd == "build-dataset":
        build_dataset_cli(args)
    elif args.cmd == "backtest":
        backtest_cli(args)
    else:
        raise ValueError(args.cmd)


if __name__ == "__main__":
    cli_main()
