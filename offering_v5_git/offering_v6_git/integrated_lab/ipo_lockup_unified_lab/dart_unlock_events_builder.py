from __future__ import annotations

"""
Build unlock_events_dart.csv for Korean IPOs using OpenDART filings.

Design goals
------------
1) Institution demand-forecast lockups from "증권발행실적보고서"
2) Post-listing shareholder lockups from prospectus tables such as
   "상장 후 유통제한 및 유통가능주식수 현황"
3) Optional lead-manager mandatory hold parsing from prospectus text

This is intentionally standalone so it can be bolted onto the user's current
IPO runner without changing the existing synthetic-event backtester first.

Example
-------
python dart_unlock_events_builder.py \
  --master-csv dataset_out/filtered_master.csv \
  --dart-key-file dart_key.txt \
  --out-csv dataset_out/unlock_events_dart.csv \
  --cache-dir cache_dart
"""

import argparse
import io
import json
import math
import re
import time
import zipfile
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, List, Optional, Sequence, Tuple
from xml.etree import ElementTree as ET

import pandas as pd
import requests

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
)

DART_CORPCODE_URL = "https://opendart.fss.or.kr/api/corpCode.xml"
DART_LIST_URL = "https://opendart.fss.or.kr/api/list.json"
DART_DOCUMENT_URL = "https://opendart.fss.or.kr/api/document.xml"


@dataclass
class FilingRef:
    rcept_no: str
    report_nm: str
    rcept_dt: str
    corp_code: str
    corp_name: str
    source_priority: int


@dataclass
class UnlockEvent:
    symbol: str
    name: str
    corp_code: str
    listing_date: str
    lockup_term: str
    lockup_end_date: str
    unlock_date: str
    unlock_type: str
    holder_group: str
    holder_name: str
    relation: str
    unlock_shares: float
    unlock_ratio: Optional[float]
    source_report_nm: str
    source_rcept_no: str
    source_section: str
    source_file: str
    parse_confidence: str
    note: str = ""


TERM_PRIORITY = {
    "15D": 0,
    "1M": 1,
    "3M": 2,
    "6M": 3,
    "1Y": 4,
    "2Y": 5,
}


def _sleep(sec: float) -> None:
    if sec > 0:
        time.sleep(sec)


def _read_text(path: Path) -> str:
    for enc in ("utf-8-sig", "utf-8", "cp949", "euc-kr", "latin1"):
        try:
            return path.read_text(encoding=enc)
        except Exception:
            continue
    raise RuntimeError(f"Could not read text file: {path}")


def load_dart_key(path: str | Path) -> str:
    key = _read_text(Path(path)).strip().splitlines()[0].strip()
    if len(key) < 20:
        raise ValueError("DART 키 파일 첫 줄에 인증키를 넣어주세요.")
    return key


class DARTClient:
    def __init__(self, api_key: str, cache_dir: str | Path, sleep_sec: float = 0.15):
        self.api_key = api_key.strip()
        self.cache_dir = Path(cache_dir)
        self.sleep_sec = float(sleep_sec)
        (self.cache_dir / "corp").mkdir(parents=True, exist_ok=True)
        (self.cache_dir / "list").mkdir(parents=True, exist_ok=True)
        (self.cache_dir / "raw").mkdir(parents=True, exist_ok=True)
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": USER_AGENT})

    def _get_json(self, url: str, params: Dict[str, Any]) -> Dict[str, Any]:
        r = self.session.get(url, params=params, timeout=30)
        r.raise_for_status()
        data = r.json()
        status = str(data.get("status", "000"))
        if status != "000":
            raise RuntimeError(f"DART API error {status}: {data.get('message', '')}")
        return data

    def get_corp_code_map(self, use_cache: bool = True) -> pd.DataFrame:
        cache_path = self.cache_dir / "corp" / "corp_codes.csv"
        if use_cache and cache_path.exists():
            return pd.read_csv(cache_path, dtype=str)

        r = self.session.get(
            DART_CORPCODE_URL,
            params={"crtfc_key": self.api_key},
            timeout=60,
        )
        r.raise_for_status()
        zf = zipfile.ZipFile(io.BytesIO(r.content))
        xml_name = next((n for n in zf.namelist() if n.lower().endswith(".xml")), None)
        if not xml_name:
            raise RuntimeError("corpCode.xml zip 안에서 XML을 찾지 못했습니다.")
        root = ET.fromstring(zf.read(xml_name))

        rows: List[Dict[str, str]] = []
        for item in root.findall("list"):
            rows.append(
                {
                    "corp_code": (item.findtext("corp_code") or "").strip(),
                    "corp_name": (item.findtext("corp_name") or "").strip(),
                    "corp_eng_name": (item.findtext("corp_eng_name") or "").strip(),
                    "stock_code": (item.findtext("stock_code") or "").strip(),
                    "modify_date": (item.findtext("modify_date") or "").strip(),
                }
            )
        df = pd.DataFrame(rows)
        if df.empty:
            raise RuntimeError("corp code map이 비어 있습니다.")
        df.to_csv(cache_path, index=False, encoding="utf-8-sig")
        _sleep(self.sleep_sec)
        return df

    def list_filings(
        self,
        corp_code: str,
        bgn_de: str,
        end_de: str,
        page_count: int = 100,
        use_cache: bool = True,
    ) -> pd.DataFrame:
        cache_path = self.cache_dir / "list" / f"{corp_code}_{bgn_de}_{end_de}.json"
        if use_cache and cache_path.exists():
            data = json.loads(_read_text(cache_path))
            return pd.DataFrame(data)

        all_rows: List[Dict[str, Any]] = []
        page_no = 1
        while True:
            params = {
                "crtfc_key": self.api_key,
                "corp_code": corp_code,
                "bgn_de": bgn_de,
                "end_de": end_de,
                "pblntf_ty": "C",  # 발행공시
                "last_reprt_at": "Y",
                "sort": "date",
                "sort_mth": "desc",
                "page_no": page_no,
                "page_count": page_count,
            }
            data = self._get_json(DART_LIST_URL, params)
            rows = data.get("list", []) or []
            all_rows.extend(rows)
            total_count = int(data.get("total_count", 0) or 0)
            if page_no * page_count >= total_count or not rows:
                break
            page_no += 1
            _sleep(self.sleep_sec)

        cache_path.write_text(json.dumps(all_rows, ensure_ascii=False, indent=2), encoding="utf-8")
        _sleep(self.sleep_sec)
        return pd.DataFrame(all_rows)

    def download_raw_document(self, rcept_no: str, use_cache: bool = True) -> Path:
        cache_path = self.cache_dir / "raw" / f"{rcept_no}.zip"
        if use_cache and cache_path.exists() and cache_path.stat().st_size > 0:
            return cache_path
        r = self.session.get(
            DART_DOCUMENT_URL,
            params={"crtfc_key": self.api_key, "rcept_no": rcept_no},
            timeout=60,
        )
        r.raise_for_status()
        cache_path.write_bytes(r.content)
        _sleep(self.sleep_sec)
        return cache_path


def normalize_name_key(name: Any) -> str:
    s = str(name or "").strip()
    s = re.sub(r"\(.*?\)", "", s)
    s = s.replace("㈜", "").replace("(주)", "").replace("주식회사", "")
    s = re.sub(r"\s+", "", s)
    s = re.sub(r"[^0-9A-Za-z가-힣]", "", s)
    return s.lower()


def _normalize_text(s: Any) -> str:
    text = str(s if s is not None else "")
    text = text.replace("\xa0", " ")
    text = re.sub(r"\s+", "", text)
    return text.strip().lower()


def _flatten_label(label: Any) -> str:
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
    label = re.sub(r"\s+", " ", label)
    return label.strip()


def _flatten_df(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out.columns = [_flatten_label(c) or f"col_{i}" for i, c in enumerate(out.columns)]
    return out


def _parse_any_date(x: Any) -> pd.Timestamp:
    if pd.isna(x):
        return pd.NaT
    s = str(x).strip()
    s = re.sub(r"[^0-9./-]", "", s)
    s = s.replace(".", "/").replace("-", "/")
    out = pd.to_datetime(s, errors="coerce")
    if pd.isna(out):
        y = re.sub(r"[^0-9]", "", s)
        out = pd.to_datetime(y, format="%Y%m%d", errors="coerce")
    return out.normalize() if pd.notna(out) else pd.NaT


def _as_int(x: Any) -> Optional[int]:
    if pd.isna(x):
        return None
    s = str(x)
    s = s.replace(",", "")
    s = re.sub(r"[^0-9.-]", "", s)
    if s in {"", "-", ".", "-."}:
        return None
    try:
        return int(round(float(s)))
    except Exception:
        return None


def _as_float(x: Any) -> Optional[float]:
    if pd.isna(x):
        return None
    s = str(x)
    s = s.replace(",", "")
    s = re.sub(r"[^0-9.-]", "", s)
    if s in {"", "-", ".", "-."}:
        return None
    try:
        return float(s)
    except Exception:
        return None


def _decode_bytes(raw: bytes) -> str:
    for enc in ("utf-8-sig", "utf-8", "cp949", "euc-kr", "latin1"):
        try:
            return raw.decode(enc)
        except Exception:
            continue
    return raw.decode("utf-8", errors="ignore")


def _iter_text_members(zip_path: Path) -> Iterator[Tuple[str, str]]:
    try:
        zf = zipfile.ZipFile(zip_path)
    except Exception as e:
        raise RuntimeError(f"원문 zip을 열지 못했습니다: {zip_path} ({e})")

    for name in zf.namelist():
        lower = name.lower()
        if any(lower.endswith(ext) for ext in (".xml", ".htm", ".html", ".xhtml", ".sgm", ".sgml", ".txt")):
            try:
                raw = zf.read(name)
                yield name, _decode_bytes(raw)
            except Exception:
                continue


def _extract_tables(text: str) -> List[pd.DataFrame]:
    try:
        tables = pd.read_html(io.StringIO(text), displayed_only=False)
        return [t for t in tables if isinstance(t, pd.DataFrame) and not t.empty]
    except Exception:
        return []


def _contains_any(text: str, keywords: Sequence[str]) -> bool:
    norm = _normalize_text(text)
    return any(_normalize_text(k) in norm for k in keywords)


def _find_col(work: pd.DataFrame, include_keywords: Sequence[str], exclude_keywords: Sequence[str] = ()) -> Optional[str]:
    for col in work.columns:
        norm = _normalize_text(col)
        if all(k in norm for k in [_normalize_text(k) for k in include_keywords]):
            if exclude_keywords and any(_normalize_text(k) in norm for k in exclude_keywords):
                continue
            return col
    return None


def normalize_lockup_term(raw: Any) -> Optional[str]:
    s = _normalize_text(raw)
    if not s:
        return None
    s = s.replace("개월간", "개월").replace("년간", "년").replace("일간", "일")
    s = s.replace("~", "-")
    if s in {"미확약", "계", "총계", "합계", "-"}:
        return None
    if "15일" in s:
        return "15D"
    if "1개월" in s or "1 개월" in s:
        return "1M"
    if "3개월" in s or "3 개월" in s:
        return "3M"
    if "6개월" in s or "6 개월" in s:
        return "6M"
    if "12개월" in s or "1년" in s:
        return "1Y"
    if "24개월" in s or "2년" in s:
        return "2Y"
    return None


def first_tradeable_date(listing_date: pd.Timestamp, term: str) -> pd.Timestamp:
    listing_date = pd.Timestamp(listing_date).normalize()
    t = str(term).upper()
    if t == "15D":
        return listing_date + pd.Timedelta(days=15)
    if t == "1M":
        return listing_date + pd.DateOffset(months=1)
    if t == "3M":
        return listing_date + pd.DateOffset(months=3)
    if t == "6M":
        return listing_date + pd.DateOffset(months=6)
    if t == "1Y":
        return listing_date + pd.DateOffset(years=1)
    if t == "2Y":
        return listing_date + pd.DateOffset(years=2)
    raise ValueError(f"Unsupported lockup term: {term}")


def lockup_end_date(listing_date: pd.Timestamp, term: str) -> pd.Timestamp:
    return first_tradeable_date(listing_date, term) - pd.Timedelta(days=1)


def _score_inst_table(df: pd.DataFrame) -> int:
    work = _flatten_df(df)
    score = 0
    blob = " ".join(_normalize_text(c) for c in work.columns)
    cells = " ".join(_normalize_text(v) for v in work.fillna("").astype(str).head(20).values.ravel())
    if "확약기간" in blob:
        score += 4
    if "합계" in blob and ("수량" in blob or "비중" in blob):
        score += 4
    for tok in ("미확약", "15일", "1개월", "3개월", "6개월", "계"):
        if _normalize_text(tok) in cells:
            score += 2
    return score


def _pick_term_col(work: pd.DataFrame) -> Optional[str]:
    best_col: Optional[str] = None
    best_score = -1
    term_tokens = ["미확약", "15일", "1개월", "3개월", "6개월", "계"]
    for col in work.columns:
        vals = work[col].fillna("").astype(str).tolist()
        score = sum(any(tok in str(v) for v in vals) for tok in term_tokens)
        if score > best_score:
            best_col = col
            best_score = score
    return best_col if best_score >= 2 else None


def _pick_numeric_series(work: pd.DataFrame, preferred_header_keywords: Sequence[Tuple[str, ...]]) -> Tuple[Optional[str], Optional[str]]:
    """Return (qty_col, ratio_col)."""
    qty_col = None
    ratio_col = None
    for ks in preferred_header_keywords:
        c = _find_col(work, ks)
        if c:
            if any(_normalize_text(k) in _normalize_text(c) for k in ("비중", "지분율", "%")):
                ratio_col = c
            else:
                qty_col = c
            break

    # direct lookups first
    for c in work.columns:
        norm = _normalize_text(c)
        if qty_col is None and "합계" in norm and ("수량" in norm or norm.endswith("주")):
            qty_col = c
        if ratio_col is None and "합계" in norm and ("비중" in norm or "지분율" in norm or "%" in norm):
            ratio_col = c
    # fallback: pick numeric-looking columns from the right
    numericish = []
    for c in work.columns:
        vals = work[c].dropna().astype(str).head(20)
        numeric_ratio = 0.0
        if len(vals) > 0:
            numeric_ratio = sum(bool(re.search(r"[0-9]", v.replace(",", ""))) for v in vals) / len(vals)
        if numeric_ratio >= 0.7:
            numericish.append(c)
    if qty_col is None and numericish:
        qty_col = numericish[-2] if len(numericish) >= 2 else numericish[-1]
    if ratio_col is None and numericish:
        ratio_col = numericish[-1]
    return qty_col, ratio_col


def parse_institution_lockups_from_zip(
    zip_path: Path,
    symbol: str,
    name: str,
    corp_code: str,
    listing_date: pd.Timestamp,
    source_report_nm: str,
    source_rcept_no: str,
) -> List[UnlockEvent]:
    best: Tuple[int, Optional[pd.DataFrame], str] = (-1, None, "")
    heading_tokens = ["기관투자자의무보유확약기간별배정현황", "기관투자자 의무보유확약기간별 배정현황"]

    for fname, text in _iter_text_members(zip_path):
        if not _contains_any(text, heading_tokens):
            continue
        for df in _extract_tables(text):
            score = _score_inst_table(df)
            if score > best[0]:
                best = (score, df, fname)

    if best[1] is None:
        return []

    work = _flatten_df(best[1]).dropna(axis=1, how="all").reset_index(drop=True)
    term_col = _pick_term_col(work)
    if term_col is None:
        return []

    qty_col, ratio_col = _pick_numeric_series(work, preferred_header_keywords=(("합계", "수량"), ("수량",)))
    if qty_col is None:
        return []

    out: List[UnlockEvent] = []
    for _, row in work.iterrows():
        term = normalize_lockup_term(row.get(term_col))
        if not term:
            continue
        qty = _as_int(row.get(qty_col))
        ratio = _as_float(row.get(ratio_col)) if ratio_col else None
        if qty is None or qty <= 0:
            continue
        unlock_dt = first_tradeable_date(listing_date, term)
        end_dt = lockup_end_date(listing_date, term)
        out.append(
            UnlockEvent(
                symbol=str(symbol).zfill(6),
                name=str(name),
                corp_code=str(corp_code),
                listing_date=listing_date.strftime("%Y-%m-%d"),
                lockup_term=term,
                lockup_end_date=end_dt.strftime("%Y-%m-%d"),
                unlock_date=unlock_dt.strftime("%Y-%m-%d"),
                unlock_type="institution_demand_forecast",
                holder_group="institution",
                holder_name="기관투자자 합계",
                relation="기관투자자",
                unlock_shares=float(qty),
                unlock_ratio=float(ratio) if ratio is not None else None,
                source_report_nm=source_report_nm,
                source_rcept_no=source_rcept_no,
                source_section="기관투자자 의무보유확약기간별 배정현황",
                source_file=best[2],
                parse_confidence="high",
            )
        )
    return out


def _score_post_table(df: pd.DataFrame) -> int:
    work = _flatten_df(df)
    score = 0
    blob = " ".join(_normalize_text(c) for c in work.columns)
    cells = " ".join(_normalize_text(v) for v in work.fillna("").astype(str).head(20).values.ravel())
    for tok in ("매각제한기간", "매각제한물량", "유통가능물량", "회사와의관계", "주주명"):
        if tok in blob:
            score += 3
    for tok in ("최대주주", "1개월", "3개월", "6개월", "1년"):
        if _normalize_text(tok) in cells:
            score += 1
    return score


def _classify_holder_group(holder_name: str, relation: str) -> str:
    blob = _normalize_text(f"{holder_name} {relation}")
    if "상장주선인" in blob or "주관회사" in blob or "인수인" in blob:
        return "lead_manager"
    if "최대주주" in blob:
        return "controlling_shareholder"
    if "임원" in blob or "대표이사" in blob or "등기임원" in blob:
        return "executive"
    if any(tok in blob for tok in ("투자조합", "벤처", "vc", "재무적", "fi", "신기술", "창투", "pef", "펀드")):
        return "financial_investor"
    if any(tok in blob for tok in ("전략적", "si", "거래처", "협력사")):
        return "strategic_investor"
    if "특수관계인" in blob:
        return "related_party"
    return "other_shareholder"


def parse_post_listing_lockups_from_zip(
    zip_path: Path,
    symbol: str,
    name: str,
    corp_code: str,
    listing_date: pd.Timestamp,
    listed_shares: Optional[float],
    source_report_nm: str,
    source_rcept_no: str,
) -> List[UnlockEvent]:
    best: Tuple[int, Optional[pd.DataFrame], str] = (-1, None, "")
    heading_tokens = [
        "상장후유통제한및유통가능주식수현황",
        "상장 후 유통제한 및 유통가능주식수 현황",
        "유통가능물량",
    ]
    for fname, text in _iter_text_members(zip_path):
        if not _contains_any(text, heading_tokens):
            continue
        for df in _extract_tables(text):
            score = _score_post_table(df)
            if score > best[0]:
                best = (score, df, fname)
    if best[1] is None:
        return []

    work = _flatten_df(best[1]).dropna(axis=1, how="all").reset_index(drop=True)
    holder_col = _find_col(work, ("주주명",)) or _find_col(work, ("성명",)) or work.columns[0]
    relation_col = _find_col(work, ("회사와의", "관계")) or _find_col(work, ("관계",))
    period_col = _find_col(work, ("매각제한", "기간")) or _find_col(work, ("의무보유", "기간"))
    qty_col = _find_col(work, ("매각제한물량",)) or _find_col(work, ("매각제한", "주식수"))
    ratio_col = _find_col(work, ("매각제한물량", "지분율")) or _find_col(work, ("매각제한", "지분율"))

    if period_col is None or qty_col is None:
        return []

    work[holder_col] = work[holder_col].replace({"": pd.NA}).ffill()
    if relation_col:
        work[relation_col] = work[relation_col].replace({"": pd.NA}).ffill()

    out: List[UnlockEvent] = []
    for _, row in work.iterrows():
        term = normalize_lockup_term(row.get(period_col))
        if not term:
            continue
        qty = _as_int(row.get(qty_col))
        if qty is None or qty <= 0:
            continue
        ratio = _as_float(row.get(ratio_col)) if ratio_col else None
        if ratio is None and listed_shares and listed_shares > 0:
            ratio = float(qty) / float(listed_shares) * 100.0
        holder_name = str(row.get(holder_col) or "").strip()
        relation = str(row.get(relation_col) or "").strip() if relation_col else ""
        unlock_dt = first_tradeable_date(listing_date, term)
        end_dt = lockup_end_date(listing_date, term)
        out.append(
            UnlockEvent(
                symbol=str(symbol).zfill(6),
                name=str(name),
                corp_code=str(corp_code),
                listing_date=listing_date.strftime("%Y-%m-%d"),
                lockup_term=term,
                lockup_end_date=end_dt.strftime("%Y-%m-%d"),
                unlock_date=unlock_dt.strftime("%Y-%m-%d"),
                unlock_type="shareholder_post_listing_lockup",
                holder_group=_classify_holder_group(holder_name, relation),
                holder_name=holder_name,
                relation=relation,
                unlock_shares=float(qty),
                unlock_ratio=float(ratio) if ratio is not None else None,
                source_report_nm=source_report_nm,
                source_rcept_no=source_rcept_no,
                source_section="상장 후 유통제한 및 유통가능주식수 현황",
                source_file=best[2],
                parse_confidence="high" if ratio_col else "medium",
            )
        )
    return out


def _parse_public_offering_shares_from_text(text: str) -> Optional[int]:
    tables = _extract_tables(text)
    best_qty: Optional[int] = None
    for df in tables:
        work = _flatten_df(df).dropna(axis=1, how="all")
        blob = " ".join(_normalize_text(c) for c in work.columns)
        if "증권수량" not in blob and "모집" not in blob and "매출" not in blob:
            continue
        qty_col = _find_col(work, ("증권수량",)) or _find_col(work, ("모집", "주식수"))
        if not qty_col:
            continue
        for val in work[qty_col].tolist():
            qty = _as_int(val)
            if qty and qty > 0:
                if best_qty is None or qty > best_qty:
                    best_qty = qty
    return best_qty


def parse_lead_manager_hold_from_zip(
    zip_path: Path,
    symbol: str,
    name: str,
    corp_code: str,
    listing_date: pd.Timestamp,
    source_report_nm: str,
    source_rcept_no: str,
) -> List[UnlockEvent]:
    heading_tokens = ["상장규정에따른상장주선인의의무취득분에관한사항", "상장주선인의 의무 취득분"]
    out: List[UnlockEvent] = []

    for fname, text in _iter_text_members(zip_path):
        if not _contains_any(text, heading_tokens):
            continue
        norm = _normalize_text(text)
        if "상장일로부터" not in norm:
            continue
        term_match = re.search(r"상장일로부터\s*(\d+)\s*(개월|년|일)", text)
        pct_match = re.search(r"100분의\s*([0-9]+(?:\.[0-9]+)?)\s*에해당하는수량", _normalize_text(text))
        explicit_qty_match = re.search(r"([0-9][0-9,]{2,})\s*주", text)
        if not term_match:
            continue

        term_num = int(term_match.group(1))
        term_unit = term_match.group(2)
        if term_unit == "개월":
            term = f"{term_num}M" if term_num in {1, 3, 6} else None
        elif term_unit == "년":
            term = f"{term_num}Y"
        else:
            term = f"{term_num}D" if term_num == 15 else None
        if term is None:
            continue

        qty: Optional[int] = None
        parse_confidence = "low"
        note = ""
        if explicit_qty_match:
            qty = _as_int(explicit_qty_match.group(1))
            parse_confidence = "medium"
        if qty is None and pct_match:
            pct = float(pct_match.group(1))
            offering_shares = _parse_public_offering_shares_from_text(text)
            if offering_shares:
                qty = int(round(offering_shares * pct / 100.0))
                parse_confidence = "medium"
                note = f"공모주식수 {offering_shares:,}주 x {pct}%로 계산"
        if qty is None or qty <= 0:
            continue

        unlock_dt = first_tradeable_date(listing_date, term)
        end_dt = lockup_end_date(listing_date, term)
        out.append(
            UnlockEvent(
                symbol=str(symbol).zfill(6),
                name=str(name),
                corp_code=str(corp_code),
                listing_date=listing_date.strftime("%Y-%m-%d"),
                lockup_term=term,
                lockup_end_date=end_dt.strftime("%Y-%m-%d"),
                unlock_date=unlock_dt.strftime("%Y-%m-%d"),
                unlock_type="lead_manager_mandatory_hold",
                holder_group="lead_manager",
                holder_name="상장주선인",
                relation="상장주선인 의무취득분",
                unlock_shares=float(qty),
                unlock_ratio=None,
                source_report_nm=source_report_nm,
                source_rcept_no=source_rcept_no,
                source_section="상장규정에 따른 상장주선인의 의무 취득분에 관한 사항",
                source_file=fname,
                parse_confidence=parse_confidence,
                note=note,
            )
        )
        break
    return out


def rank_filing(report_nm: str) -> int:
    nm = str(report_nm)
    if "증권발행실적보고서" in nm:
        return 0
    if "투자설명서" in nm:
        return 1
    if "증권신고서" in nm:
        return 2
    return 9


def choose_best_filings(filings: pd.DataFrame, listing_date: pd.Timestamp) -> Dict[str, Optional[FilingRef]]:
    if filings.empty:
        return {"issue": None, "prospectus": None}
    work = filings.copy()
    work["rcept_dt"] = work["rcept_dt"].map(_parse_any_date)
    work = work[work["report_nm"].astype(str).str.contains("증권발행실적보고서|투자설명서|증권신고서", regex=True, na=False)].copy()
    if work.empty:
        return {"issue": None, "prospectus": None}

    def _make_ref(row: pd.Series) -> FilingRef:
        return FilingRef(
            rcept_no=str(row["rcept_no"]),
            report_nm=str(row["report_nm"]),
            rcept_dt=pd.Timestamp(row["rcept_dt"]).strftime("%Y%m%d"),
            corp_code=str(row.get("corp_code", "")),
            corp_name=str(row.get("corp_name", "")),
            source_priority=int(rank_filing(row["report_nm"])),
        )

    issue_df = work[work["report_nm"].astype(str).str.contains("증권발행실적보고서", na=False)].copy()
    if not issue_df.empty:
        issue_df["dist"] = (issue_df["rcept_dt"] - listing_date).abs().dt.days
        issue_df = issue_df.sort_values(["dist", "rcept_dt"], ascending=[True, False])
        issue = _make_ref(issue_df.iloc[0])
    else:
        issue = None

    pros_df = work[work["report_nm"].astype(str).str.contains("투자설명서|증권신고서", regex=True, na=False)].copy()
    if not pros_df.empty:
        pros_df["dist"] = (pros_df["rcept_dt"] - listing_date).abs().dt.days
        pros_df["priority"] = pros_df["report_nm"].map(rank_filing)
        pros_df = pros_df.sort_values(["priority", "dist", "rcept_dt"], ascending=[True, True, False])
        prospectus = _make_ref(pros_df.iloc[0])
    else:
        prospectus = None

    return {"issue": issue, "prospectus": prospectus}


def dedupe_unlock_events(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df.copy()
    work = df.copy()
    for c in ("unlock_shares", "unlock_ratio"):
        if c in work.columns:
            work[c] = pd.to_numeric(work[c], errors="coerce")
    work = work.sort_values(
        [
            "symbol",
            "unlock_date",
            "unlock_type",
            "holder_group",
            "lockup_term",
            "parse_confidence",
        ],
        ascending=[True, True, True, True, True, True],
    )
    work = work.drop_duplicates(
        subset=["symbol", "unlock_date", "unlock_type", "holder_group", "holder_name", "lockup_term", "unlock_shares"],
        keep="last",
    ).reset_index(drop=True)
    return work


def build_unlock_events_for_row(
    row: pd.Series,
    client: DARTClient,
    corp_map: pd.DataFrame,
    lookback_days: int = 240,
    lookahead_days: int = 120,
) -> pd.DataFrame:
    symbol = str(row.get("symbol", "")).zfill(6)
    name = str(row.get("name", "")).strip()
    listing_date = _parse_any_date(row.get("listing_date"))
    if not symbol or pd.isna(listing_date):
        return pd.DataFrame()

    corp_hit = corp_map[corp_map["stock_code"].astype(str).str.zfill(6) == symbol]
    if corp_hit.empty:
        name_key = normalize_name_key(name)
        corp_hit = corp_map[corp_map["corp_name"].map(normalize_name_key) == name_key]
    if corp_hit.empty:
        return pd.DataFrame()
    corp_code = str(corp_hit.iloc[0]["corp_code"])

    bgn_de = (listing_date - pd.Timedelta(days=lookback_days)).strftime("%Y%m%d")
    end_de = (listing_date + pd.Timedelta(days=lookahead_days)).strftime("%Y%m%d")
    filings = client.list_filings(corp_code=corp_code, bgn_de=bgn_de, end_de=end_de, use_cache=True)
    if filings.empty:
        return pd.DataFrame()

    selected = choose_best_filings(filings, listing_date)
    events: List[UnlockEvent] = []
    listed_shares = _as_float(row.get("listed_shares"))

    issue_ref = selected.get("issue")
    if issue_ref is not None:
        raw_zip = client.download_raw_document(issue_ref.rcept_no)
        events.extend(
            parse_institution_lockups_from_zip(
                zip_path=raw_zip,
                symbol=symbol,
                name=name,
                corp_code=corp_code,
                listing_date=listing_date,
                source_report_nm=issue_ref.report_nm,
                source_rcept_no=issue_ref.rcept_no,
            )
        )

    pros_ref = selected.get("prospectus")
    if pros_ref is not None:
        raw_zip = client.download_raw_document(pros_ref.rcept_no)
        events.extend(
            parse_post_listing_lockups_from_zip(
                zip_path=raw_zip,
                symbol=symbol,
                name=name,
                corp_code=corp_code,
                listing_date=listing_date,
                listed_shares=listed_shares,
                source_report_nm=pros_ref.report_nm,
                source_rcept_no=pros_ref.rcept_no,
            )
        )
        events.extend(
            parse_lead_manager_hold_from_zip(
                zip_path=raw_zip,
                symbol=symbol,
                name=name,
                corp_code=corp_code,
                listing_date=listing_date,
                source_report_nm=pros_ref.report_nm,
                source_rcept_no=pros_ref.rcept_no,
            )
        )

    if not events:
        return pd.DataFrame()
    out = pd.DataFrame([asdict(x) for x in events])
    return dedupe_unlock_events(out)


def load_master_csv(path: str | Path) -> pd.DataFrame:
    df = pd.read_csv(path)
    required = {"symbol", "name", "listing_date"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"master csv에 필요한 컬럼이 없습니다: {sorted(missing)}")
    return df


def build_unlock_events(
    master_csv: str | Path,
    dart_key_file: str | Path,
    out_csv: str | Path,
    cache_dir: str | Path,
    max_symbols: Optional[int] = None,
) -> pd.DataFrame:
    api_key = load_dart_key(dart_key_file)
    client = DARTClient(api_key=api_key, cache_dir=cache_dir)
    corp_map = client.get_corp_code_map(use_cache=True)
    master = load_master_csv(master_csv)
    if max_symbols is not None:
        master = master.head(int(max_symbols)).copy()

    all_rows: List[pd.DataFrame] = []
    error_rows: List[Dict[str, Any]] = []
    for i, (_, row) in enumerate(master.iterrows(), start=1):
        symbol = str(row.get("symbol", "")).zfill(6)
        name = str(row.get("name", "")).strip()
        try:
            df = build_unlock_events_for_row(row, client=client, corp_map=corp_map)
            if not df.empty:
                all_rows.append(df)
            print(f"[{i}/{len(master)}] {symbol} {name}: {len(df)} events")
        except Exception as e:
            print(f"[{i}/{len(master)}] {symbol} {name}: ERROR - {e}")
            error_rows.append({"symbol": symbol, "name": name, "error": str(e)})

    out = pd.concat(all_rows, ignore_index=True) if all_rows else pd.DataFrame()
    out = dedupe_unlock_events(out)
    out_path = Path(out_csv)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(out_path, index=False, encoding="utf-8-sig")

    if error_rows:
        err_path = out_path.with_name(out_path.stem + "_errors.csv")
        pd.DataFrame(error_rows).to_csv(err_path, index=False, encoding="utf-8-sig")
    return out


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Build unlock_events_dart.csv from OpenDART filings")
    p.add_argument("--master-csv", required=True, help="filtered_master.csv or similar")
    p.add_argument("--dart-key-file", required=True, help="text file containing DART API key on first line")
    p.add_argument("--out-csv", required=True)
    p.add_argument("--cache-dir", default="cache_dart")
    p.add_argument("--max-symbols", type=int, default=None)
    return p.parse_args()


def main() -> None:
    args = parse_args()
    out = build_unlock_events(
        master_csv=args.master_csv,
        dart_key_file=args.dart_key_file,
        out_csv=args.out_csv,
        cache_dir=args.cache_dir,
        max_symbols=args.max_symbols,
    )
    print(f"[DONE] unlock events rows: {len(out)}")
    if not out.empty:
        print(out.head(20).to_string(index=False))


if __name__ == "__main__":
    main()
