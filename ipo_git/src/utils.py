from __future__ import annotations

import math
import os
import re
from io import BytesIO, StringIO
from pathlib import Path
from typing import Any, Iterable

import pandas as pd


DEFAULT_DATE_COLUMNS = [
    "subscription_start",
    "subscription_end",
    "listing_date",
    "forecast_date",
    "ir_date",
    "unlock_date",
    "unlock_date_15d",
    "unlock_date_1m",
    "unlock_date_3m",
    "unlock_date_6m",
    "unlock_date_1y",
    "entry_dt",
    "exit_dt",
    "prev_close_date",
    "last_refresh_ts",
    "asof",
]


STANDARD_ISSUE_COLUMNS = [
    "ipo_id",
    "name",
    "name_key",
    "market",
    "symbol",
    "sector",
    "stage",
    "underwriters",
    "subscription_start",
    "subscription_end",
    "listing_date",
    "price_band_low",
    "price_band_high",
    "offer_price",
    "retail_competition_ratio_live",
    "institutional_competition_ratio",
    "allocation_ratio_retail",
    "allocation_ratio_proportional",
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
    "current_price",
    "day_change_pct",
    "ma20",
    "ma60",
    "rsi14",
    "forecast_date",
    "ir_url",
    "ir_title",
    "ir_date",
    "ir_pdf_url",
    "ir_source_page",
    "dart_receipt_no",
    "dart_viewer_url",
    "dart_report_nm",
    "dart_filing_date",
    "kind_url",
    "notes",
    "unusual_move_flag",
    "volume_spike_ratio",
    "unlock_date_15d",
    "unlock_date_1m",
    "unlock_date_3m",
    "unlock_date_6m",
    "unlock_date_1y",
    "source",
    "source_detail",
    "last_refresh_ts",
]

JUNK_ISSUE_NAME_TOKENS = [
    "[공모뉴스]",
    "function search_corp",
    "document.getElementById",
    "Home IPO/공모",
    "공모주 청약일정",
    "공모주청약 일정",
    "신규상장",
    "증시캘린더",
    "비상장매매",
    "주주동호회",
    "최근 IPO",
    "Copyright",
    "All rights reserved",
    "㈜38커뮤니케이션",
]


def looks_like_junk_issue_name(value: Any) -> bool:
    text = "" if is_missing(value) else str(value).replace("\xa0", " ").strip()
    if not text:
        return True
    compact = re.sub(r"\s+", " ", text)
    lowered = compact.lower()
    if len(compact) > 80:
        return True
    if sum(token.lower() in lowered for token in JUNK_ISSUE_NAME_TOKENS) >= 1:
        return True
    if compact.count("[") >= 3 and compact.count("]") >= 3:
        return True
    if ("function" in lowered or "document.getelementbyid" in lowered) and "공모" in compact:
        return True
    return False


def _normalize_textish_value(value: Any) -> Any:
    try:
        if pd.isna(value):
            return pd.NA
    except Exception:
        pass
    text = str(value).replace("\xa0", " ").strip()
    if not text or text.lower() in {"nan", "none", "null", "nat"}:
        return pd.NA
    if re.fullmatch(r"[+-]?\d+\.0", text):
        text = text[:-2]
    return text


def _infer_issue_stage_series(df: pd.DataFrame, today: pd.Timestamp | None = None) -> pd.Series:
    today = (today or today_kst()).normalize()
    index = df.index
    sub_start = pd.to_datetime(df.get("subscription_start"), errors="coerce")
    sub_end = pd.to_datetime(df.get("subscription_end"), errors="coerce")
    listing = pd.to_datetime(df.get("listing_date"), errors="coerce")
    fallback = df.get("stage", pd.Series(index=index, dtype="object"))
    fallback_text = fallback.fillna("").astype(str).str.strip()

    result = pd.Series("", index=index, dtype="object")
    start_day = sub_start.dt.normalize()
    end_day = sub_end.dt.normalize()
    listing_day = listing.dt.normalize()

    mask_upcoming = sub_start.notna() & (today < start_day)
    result = result.mask(mask_upcoming, "청약예정")

    mask_active = sub_start.notna() & sub_end.notna() & (start_day <= today) & (today <= end_day)
    result = result.mask(mask_active, "청약중")

    mask_after_sub = sub_start.notna() & sub_end.notna() & (today > end_day)
    result = result.mask(mask_after_sub & listing.notna() & (today >= listing_day), "상장후")
    result = result.mask(mask_after_sub & listing.notna() & (today < listing_day), "상장예정")
    result = result.mask(mask_after_sub & listing.isna(), "청약완료")

    unresolved = result.eq("")
    result = result.mask(unresolved & listing.notna() & (today >= listing_day), "상장후")
    result = result.mask(result.eq("") & listing.notna() & (today < listing_day), "상장예정")
    result = result.mask(result.eq(""), fallback_text)
    result = result.replace("", "미분류")
    return result


def _prepare_issue_frame(df: pd.DataFrame, *, preserve_extra: bool = False, today: pd.Timestamp | None = None) -> pd.DataFrame:
    out = ensure_columns(df.copy(), STANDARD_ISSUE_COLUMNS)
    extra_columns = [col for col in out.columns if col not in STANDARD_ISSUE_COLUMNS]
    for col in ["name", "market", "sector", "stage", "underwriters", "source", "source_detail", "notes"]:
        if col in out.columns:
            out[col] = out[col].map(_normalize_textish_value)
    out["name_key"] = out.get("name_key")
    out["name_key"] = out["name_key"].fillna(out.get("name", pd.Series(dtype="object"))).map(normalize_name_key)
    if "symbol" in out.columns:
        out["symbol"] = out["symbol"].map(normalize_symbol_text)
    out = parse_date_columns(out)
    if not out.empty:
        out["stage"] = _infer_issue_stage_series(out, today=today)
    ordered = STANDARD_ISSUE_COLUMNS + extra_columns if preserve_extra else STANDARD_ISSUE_COLUMNS
    return out[ordered].copy()


def clean_issue_frame(df: pd.DataFrame) -> pd.DataFrame:
    out = standardize_issue_frame(df)
    if out.empty:
        return out
    name_mask = ~out["name"].map(looks_like_junk_issue_name)
    key_mask = out["name_key"].fillna("").astype(str).str.len() > 0
    informative_cols = [
        "subscription_start",
        "subscription_end",
        "listing_date",
        "underwriters",
        "price_band_low",
        "price_band_high",
        "offer_price",
        "institutional_competition_ratio",
        "retail_competition_ratio_live",
        "current_price",
        "market",
        "sector",
        "lockup_commitment_ratio",
        "forecast_date",
        "ir_pdf_url",
        "total_offer_shares",
        "post_listing_total_shares",
    ]
    informative = out[[col for col in informative_cols if col in out.columns]].notna().any(axis=1)
    out = out.loc[name_mask & key_mask & informative].copy()
    return out.reset_index(drop=True)


def issue_recency_sort(df: pd.DataFrame, *, today: pd.Timestamp | None = None) -> pd.DataFrame:
    """Sort issues so current and upcoming deals appear first."""
    today = today or today_kst()
    work = _prepare_issue_frame(df.copy(), preserve_extra=True, today=today)
    if work.empty:
        return work
    listing = pd.to_datetime(work.get("listing_date"), errors="coerce")
    sub_start = pd.to_datetime(work.get("subscription_start"), errors="coerce")
    sub_end = pd.to_datetime(work.get("subscription_end"), errors="coerce")
    filing = pd.to_datetime(work.get("dart_filing_date"), errors="coerce")
    ref = listing.combine_first(sub_start)
    ref = ref.combine_first(sub_end)
    ref = ref.combine_first(filing)
    stage_rank = work.get("stage", pd.Series(dtype="object")).map(
        {
            "청약중": 6,
            "청약예정": 5,
            "상장예정": 4,
            "청약완료": 3,
            "상장후": 2,
            "전략데이터": 1,
            "미분류": 0,
        }
    ).fillna(0)
    source_rank = work.get("source", pd.Series(dtype="object")).map(
        {
            "strategy-overlay": 7,
            "DART-auto-overlay": 6,
            "KIND-공모기업": 5,
            "KIND-공모가비교": 5,
            "38": 4,
            "local-kind": 3,
            "KIND-corpList": 1,
        }
    ).fillna(2)
    work = work.assign(
        _ref_date=ref,
        _stage_rank=stage_rank,
        _source_rank=source_rank,
        _missing_ref=ref.isna().astype(int),
    )
    work = work.sort_values(
        ["_missing_ref", "_ref_date", "_stage_rank", "_source_rank", "name_key"],
        ascending=[True, False, False, False, True],
        na_position="last",
    )
    return work.drop(columns=["_ref_date", "_stage_rank", "_source_rank", "_missing_ref"], errors="ignore").reset_index(drop=True)



def project_root() -> Path:
    return Path(__file__).resolve().parents[1]


def data_dir() -> Path:
    return project_root() / "data"


def cache_dir() -> Path:
    path = data_dir() / "cache"
    path.mkdir(parents=True, exist_ok=True)
    return path


def uploads_dir() -> Path:
    path = data_dir() / "uploads"
    path.mkdir(parents=True, exist_ok=True)
    return path


def runtime_dir() -> Path:
    path = data_dir() / "runtime"
    path.mkdir(parents=True, exist_ok=True)
    return path


def project_env_candidates(start: str | Path | None = None) -> list[Path]:
    root = Path(start).expanduser().resolve() if start else project_root()
    candidates = [
        root / ".env",
        root / ".env.local",
        root.parent / ".env",
        root.parent / ".env.local",
    ]
    seen: set[str] = set()
    ordered: list[Path] = []
    for candidate in candidates:
        key = str(candidate.resolve()) if candidate.exists() else str(candidate.expanduser())
        if key in seen:
            continue
        seen.add(key)
        ordered.append(candidate)
    return ordered


def detect_project_env_file(start: str | Path | None = None) -> Path | None:
    for candidate in project_env_candidates(start):
        if candidate.exists() and candidate.is_file():
            return candidate
    return None


def parse_env_text(text: str) -> dict[str, str]:
    parsed: dict[str, str] = {}
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.lower().startswith("export "):
            line = line[7:].strip()
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip()
        if not key:
            continue
        if value and value[0] == value[-1] and value[0] in {"\"", "'"}:
            value = value[1:-1]
        if " #" in value and value[:1] not in {"\"", "'"}:
            value = value.split(" #", 1)[0].rstrip()
        parsed[key] = value
    return parsed


def load_project_env(env_path: str | Path | None = None, *, override: bool = False) -> dict[str, str]:
    candidates = [Path(env_path).expanduser().resolve()] if env_path else project_env_candidates()
    loaded: dict[str, str] = {}
    for candidate in candidates:
        if not candidate.exists() or not candidate.is_file():
            continue
        parsed = parse_env_text(candidate.read_text(encoding="utf-8", errors="ignore"))
        for key, value in parsed.items():
            if override or key not in os.environ:
                os.environ[key] = value
                loaded[key] = value
            elif key in os.environ:
                loaded.setdefault(key, os.environ[key])
        break
    return loaded


def mask_secret(value: Any, *, visible_prefix: int = 4, visible_suffix: int = 2) -> str:
    text = "" if is_missing(value) else str(value)
    if not text:
        return ""
    if len(text) <= visible_prefix + visible_suffix:
        return "*" * len(text)
    return f"{text[:visible_prefix]}{'*' * max(4, len(text) - visible_prefix - visible_suffix)}{text[-visible_suffix:]}"


def ensure_dir(path: str | Path) -> Path:
    out = Path(path)
    out.mkdir(parents=True, exist_ok=True)
    return out


def today_kst() -> pd.Timestamp:
    return pd.Timestamp.now(tz="Asia/Seoul").tz_localize(None).normalize()


def parse_date_columns(df: pd.DataFrame, cols: Iterable[str] | None = None) -> pd.DataFrame:
    out = df.copy()
    cols = cols or DEFAULT_DATE_COLUMNS
    for col in cols:
        if col in out.columns:
            out[col] = pd.to_datetime(out[col], errors="coerce")
    return out


def ensure_columns(df: pd.DataFrame, columns: Iterable[str], fill_value: Any = pd.NA) -> pd.DataFrame:
    out = df.copy()
    for col in columns:
        if col not in out.columns:
            out[col] = fill_value
    return out


def standardize_issue_frame(df: pd.DataFrame) -> pd.DataFrame:
    return _prepare_issue_frame(df, preserve_extra=False)


def safe_float(value: Any, default: float | None = None) -> float | None:
    if value is None:
        return default
    if isinstance(value, str):
        raw = value.strip()
        if raw == "":
            return default
        if raw.lower() in {"-", "--", "nan", "none", "null", "nat", "n/a", "na", "미상"}:
            return default
        cleaned = (
            raw.replace(",", "")
            .replace("%", "")
            .replace("원", "")
            .replace("배", "")
            .replace(":1", "")
            .replace("/1", "")
            .strip()
        )
        try:
            number = float(cleaned)
        except Exception:
            match = re.search(r"[-+]?\d[\d,]*\.?\d*", raw)
            if not match:
                return default
            try:
                number = float(match.group(0).replace(",", ""))
            except Exception:
                return default
    else:
        try:
            number = float(value)
        except Exception:
            return default
    if math.isnan(number) or math.isinf(number):
        return default
    return number


def safe_int(value: Any, default: int | None = None) -> int | None:
    number = safe_float(value)
    if number is None:
        return default
    return int(number)


def is_missing(value: Any) -> bool:
    if value is None:
        return True
    if value is pd.NA or value is pd.NaT:
        return True
    if isinstance(value, str):
        text = value.strip()
        return text == "" or text.lower() in {"nan", "none", "null", "nat", "-", "--", "unknown", "n/a", "na", "미상"}
    if isinstance(value, (pd.Series, pd.Index)):
        if len(value) == 0:
            return True
        if len(value) == 1:
            return is_missing(value[0])
        try:
            return bool(pd.isna(value).all())
        except Exception:
            return False
    if isinstance(value, (list, tuple, set)):
        if len(value) == 0:
            return True
        return all(is_missing(item) for item in value)
    try:
        missing = pd.isna(value)
    except Exception:
        missing = False
    if isinstance(missing, bool):
        return missing
    return False


def safe_bool(value: Any, default: bool = False) -> bool:
    if is_missing(value):
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value != 0
    text = str(value).strip().lower()
    if text in {"1", "y", "yes", "true", "t", "예", "on"}:
        return True
    if text in {"0", "n", "no", "false", "f", "아니오", "off"}:
        return False
    return default


def normalize_symbol_text(value: Any, *, zfill: bool = True) -> str | None:
    if is_missing(value):
        return None
    text = str(value).strip().upper()
    if not text:
        return None
    if re.fullmatch(r"\d+\.0", text):
        text = text[:-2]
    compact = re.sub(r"[^0-9A-Z]", "", text)
    if re.fullmatch(r"[0-9A-Z]{6}", compact):
        return compact
    match = re.search(r"\b([0-9A-Z]{6})\b", text)
    if match:
        return match.group(1).upper()
    match = re.search(r"\b(\d{1,6})\b", text)
    if not match:
        return None
    symbol = match.group(1)
    return symbol.zfill(6) if zfill else symbol


def fmt_won(value: Any, digits: int = 0) -> str:
    number = safe_float(value)
    if number is None:
        return "-"
    return f"{number:,.{digits}f}원"


def fmt_num(value: Any, digits: int = 2) -> str:
    number = safe_float(value)
    if number is None:
        return "-"
    return f"{number:,.{digits}f}"


def fmt_pct(value: Any, digits: int = 2, signed: bool = False) -> str:
    number = safe_float(value)
    if number is None:
        return "-"
    sign = "+" if signed and number > 0 else ""
    return f"{sign}{number:,.{digits}f}%"


def fmt_ratio(value: Any, digits: int = 1) -> str:
    number = safe_float(value)
    if number is None:
        return "-"
    return f"{number:,.{digits}f}:1"


def fmt_date(value: Any) -> str:
    if value is None:
        return "-"
    ts = pd.to_datetime(value, errors="coerce")
    if pd.isna(ts):
        return "-"
    return ts.strftime("%Y-%m-%d")


def coalesce(*values: Any) -> Any:
    for value in values:
        if is_missing(value):
            continue
        return value
    return None


def detect_existing_file(candidates: Iterable[str | Path]) -> Path | None:
    for candidate in candidates:
        if not candidate:
            continue
        path = Path(candidate).expanduser().resolve()
        if path.exists():
            return path
    return None


def normalize_name_key(name: Any) -> str:
    text = "" if is_missing(name) else str(name).strip()
    text = re.sub(r"\(.*?\)", "", text)
    text = text.replace("㈜", "").replace("(주)", "").replace("주식회사", "")
    text = re.sub(r"\s+", "", text)
    text = re.sub(r"[^0-9A-Za-z가-힣]", "", text)
    return text.lower()


def clean_column_label(value: Any) -> str:
    text = "" if is_missing(value) else str(value).replace("\xa0", " ").strip()
    text = re.sub(r"\s+", "", text)
    return text.lower()


def pick_first_present(df: pd.DataFrame, candidates: Iterable[str]) -> str | None:
    lowered = {clean_column_label(c): c for c in df.columns}
    for candidate in candidates:
        key = clean_column_label(candidate)
        if key in lowered:
            return lowered[key]
        for norm, original in lowered.items():
            if key in norm:
                return original
    return None


def to_csv_bytes(df: pd.DataFrame) -> bytes:
    return df.to_csv(index=False).encode("utf-8-sig")


def clip_score(value: Any, lower: float = 0.0, upper: float = 100.0, default: float = 0.0) -> float:
    number = safe_float(value, default)
    assert number is not None
    return float(max(lower, min(upper, number)))


def score_percentile(value: Any, anchors: list[tuple[float, float]], default: float = 0.0) -> float:
    number = safe_float(value)
    if number is None:
        return default
    anchors = sorted(anchors, key=lambda x: x[0])
    if number <= anchors[0][0]:
        return anchors[0][1]
    if number >= anchors[-1][0]:
        return anchors[-1][1]
    for (x1, y1), (x2, y2) in zip(anchors[:-1], anchors[1:]):
        if x1 <= number <= x2:
            ratio = (number - x1) / (x2 - x1)
            return y1 + ratio * (y2 - y1)
    return default


def parse_date_range_text(text: Any, default_year: int | None = None) -> tuple[pd.Timestamp | None, pd.Timestamp | None]:
    raw = "" if is_missing(text) else str(text).strip()
    if not raw:
        return None, None
    default_year = default_year or today_kst().year
    cleaned = raw
    cleaned = re.sub(r"\([^)]*\)", "", cleaned)
    cleaned = cleaned.replace("년", ".").replace("월", ".").replace("일", "")
    cleaned = cleaned.replace("/", ".")
    cleaned = re.sub(r"\s+", "", cleaned)
    cleaned = cleaned.replace("~", "-").replace("–", "-").replace("—", "-")
    cleaned = re.sub(r"\.{2,}", ".", cleaned).strip(".-")

    flexible = re.match(
        r"^(?:(\d{4})[.-])?(\d{1,2})[.-](\d{1,2})-(?:(?:(\d{4})[.-])?(\d{1,2})[.-])?(\d{1,2})$",
        cleaned,
    )
    if flexible:
        y1, m1, d1, y2, m2, d2 = flexible.groups()
        year1 = int(y1) if y1 else int(default_year)
        month1 = int(m1)
        day1 = int(d1)
        year2 = int(y2) if y2 else year1
        month2 = int(m2) if m2 else month1
        day2 = int(d2)
        if not y1 and not y2:
            if month1 == 12 and month2 == 1:
                year2 += 1
            elif month1 == 1 and month2 == 12:
                year1 -= 1
        return pd.Timestamp(year1, month1, day1), pd.Timestamp(year2, month2, day2)

    # 2026.03.25-2026.03.26
    full = re.match(r"(\d{4})[.-]?(\d{1,2})[.-]?(\d{1,2})-(\d{4})[.-]?(\d{1,2})[.-]?(\d{1,2})", cleaned)
    if full:
        y1, m1, d1, y2, m2, d2 = map(int, full.groups())
        return pd.Timestamp(y1, m1, d1), pd.Timestamp(y2, m2, d2)
    # 03.25-03.26 / 3/25-3/26
    short = re.match(r"(\d{1,2})[.-]?(\d{1,2})-(\d{1,2})[.-]?(\d{1,2})", cleaned)
    if short:
        m1, d1, m2, d2 = map(int, short.groups())
        year1 = year2 = default_year
        if m1 == 12 and m2 == 1:
            year2 += 1
        if m1 == 1 and m2 == 12:
            year1 -= 1
        return pd.Timestamp(year1, m1, d1), pd.Timestamp(year2, m2, d2)
    single = parse_date_text(cleaned, default_year=default_year)
    if single is not None:
        return single, single
    return None, None


def parse_date_text(text: Any, default_year: int | None = None) -> pd.Timestamp | None:
    raw = "" if is_missing(text) else str(text).strip()
    if not raw:
        return None
    default_year = default_year or today_kst().year
    cleaned = raw
    cleaned = re.sub(r"\([^)]*\)", "", cleaned)
    cleaned = cleaned.replace("년", ".").replace("월", ".").replace("일", "")
    cleaned = cleaned.replace("/", ".")
    cleaned = re.sub(r"\s+", "", cleaned)
    cleaned = re.sub(r"\.{2,}", ".", cleaned).strip(".-")

    full = re.search(r"(\d{4})[.-](\d{1,2})[.-](\d{1,2})", cleaned)
    if full:
        y, m, d = map(int, full.groups())
        return pd.Timestamp(y, m, d)

    compact = re.search(r"(\d{4})(\d{2})(\d{2})", cleaned)
    if compact:
        y, m, d = map(int, compact.groups())
        return pd.Timestamp(y, m, d)

    short = re.search(r"(\d{1,2})[.-](\d{1,2})", cleaned)
    if short:
        m, d = map(int, short.groups())
        return pd.Timestamp(int(default_year), m, d)

    single = pd.to_datetime(cleaned, errors="coerce")
    if pd.notna(single):
        ts = pd.Timestamp(single)
        if ts.year == 1970 and default_year:
            ts = pd.Timestamp(default_year, ts.month, ts.day)
        return ts
    return None


def infer_issue_stage(
    subscription_start: Any,
    subscription_end: Any,
    listing_date: Any,
    today: pd.Timestamp | None = None,
    fallback: Any = pd.NA,
) -> str:
    today = (today or today_kst()).normalize()
    sub_start = pd.to_datetime(subscription_start, errors="coerce")
    sub_end = pd.to_datetime(subscription_end, errors="coerce")
    listing = pd.to_datetime(listing_date, errors="coerce")

    if pd.notna(sub_start) and today < sub_start.normalize():
        return "청약예정"
    if pd.notna(sub_start) and pd.notna(sub_end):
        start_day = sub_start.normalize()
        end_day = sub_end.normalize()
        if start_day <= today <= end_day:
            return "청약중"
        if today > end_day:
            if pd.notna(listing):
                return "상장후" if today >= listing.normalize() else "상장예정"
            return "청약완료"

    if pd.notna(listing):
        return "상장후" if today >= listing.normalize() else "상장예정"

    fallback_text = "" if is_missing(fallback) else str(fallback).strip()
    return fallback_text or "미분류"


def read_tabular_file(source: str | Path | bytes, filename: str | None = None) -> list[pd.DataFrame]:
    if isinstance(source, (str, Path)):
        path = Path(source)
        suffix = path.suffix.lower()
        payload = path.read_bytes()
    else:
        payload = source
        suffix = Path(filename or "").suffix.lower()

    stream = BytesIO(payload)
    frames: list[pd.DataFrame] = []

    if suffix in {".xlsx", ".xls"}:
        try:
            xls = pd.ExcelFile(stream)
            for sheet in xls.sheet_names:
                for kwargs in ({}, {"header": [0, 1]}, {"header": None}):
                    try:
                        frames.append(pd.read_excel(BytesIO(payload), sheet_name=sheet, **kwargs))
                    except Exception:
                        continue
        except Exception:
            pass
        if frames:
            return frames

    if suffix == ".csv":
        for enc in ["utf-8-sig", "cp949", "euc-kr", "utf-8"]:
            try:
                frames.append(pd.read_csv(BytesIO(payload), encoding=enc))
                return frames
            except Exception:
                continue

    text = None
    for enc in ["utf-8-sig", "cp949", "euc-kr", "utf-8", "latin1"]:
        try:
            text = payload.decode(enc)
            break
        except Exception:
            continue
    if text is None:
        return []
    try:
        html_frames = pd.read_html(StringIO(text), displayed_only=False)
        frames.extend(html_frames)
    except Exception:
        pass
    return frames


def humanize_source(source: Any) -> str:
    text = "" if is_missing(source) else str(source).strip()
    if not text:
        return "-"
    mapping = {
        "sample": "샘플",
        "demo": "샘플",
        "KIND": "KIND",
        "38": "38커뮤니케이션",
        "local-kind": "로컬 KIND",
        "strategy": "전략데이터",
        "strategy-overlay": "전략 오버레이",
        "DART-auto-overlay": "DART 자동보강",
        "KIND-corpList": "KIND corpList",
        "merged-live": "실데이터",
        "yfinance": "Yahoo Finance",
        "YahooHTTP": "Yahoo Finance(HTTP)",
        "KIS": "한국투자증권 API",
    }
    return mapping.get(text, text)
