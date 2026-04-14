from __future__ import annotations

import html
import re
from io import StringIO
from pathlib import Path
from typing import Any, Iterable, Sequence
from urllib.parse import urljoin

import pandas as pd
import requests
from lxml import html as lxml_html

from src.utils import (
    STANDARD_ISSUE_COLUMNS,
    clean_column_label,
    clean_issue_frame,
    coalesce,
    infer_issue_stage,
    is_missing,
    looks_like_junk_issue_name,
    normalize_name_key,
    normalize_symbol_text,
    parse_date_range_text,
    parse_date_text,
    pick_first_present,
    read_tabular_file,
    safe_float,
    standardize_issue_frame,
    today_kst,
)

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
)

DEFAULT_HTTP_HEADERS = {
    "User-Agent": USER_AGENT,
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
    "Upgrade-Insecure-Requests": "1",
}

KIND_LISTING_URL = "https://kind.krx.co.kr/listinvstg/listingcompany.do?method=searchListingTypeMain"
KIND_PUBLIC_OFFER_URL = "https://kind.krx.co.kr/listinvstg/pubofrprogcom.do?method=searchPubofrProgComMain"
KIND_PUBLIC_OFFER_FALLBACK_URLS = [
    KIND_PUBLIC_OFFER_URL,
    "https://kind.krx.co.kr/listinvstg/pubofrschdl.do?method=searchPubofrScholMain",
]
KIND_IR_ROOM_URL = "https://kind.krx.co.kr/corpgeneral/irschedule.do?gubun=iRMaterials&method=searchIRScheduleMain"
KIND_PUB_PRICE_URL = "https://kind.krx.co.kr/listinvstg/pubprcCmpStkprcByIssue.do?method=pubprcCmpStkprcByIssueMain"
THIRTYEIGHT_SCHEDULE_URL = "https://www.38.co.kr/html/fund/?o=k"
THIRTYEIGHT_MOBILE_SCHEDULE_URL = "https://m.38.co.kr/ipo/fund.php"
THIRTYEIGHT_BASE_URL = "https://www.38.co.kr/"
THIRTYEIGHT_MOBILE_BASE_URL = "https://m.38.co.kr/"
THIRTYEIGHT_DEMAND_RESULT_URL = "https://www.38.co.kr/html/fund/?o=r1"
THIRTYEIGHT_NEW_LISTING_URL = "https://www.38.co.kr/html/fund/?o=nw"
THIRTYEIGHT_IR_DATA_URL = "https://www.38.co.kr/html/ipo/ir_data.php"
SEIBRO_RELEASE_URL = "https://m.seibro.or.kr/cnts/company/selectRelease.do"
KIND_CORP_DOWNLOAD_URLS = [
    "https://kind.krx.co.kr/corpgeneral/corpList.do?method=download&searchType=13",
    "https://kind.krx.co.kr/corpgeneral/corpList.do?method=download",
]


SUSPICIOUS_MENU_TOKENS = [
    "비상장매매",
    "비상장(장외)",
    "장외시세",
    "주주동호회",
    "토론방",
    "IPO 뉴스",
    "IPO 일정",
    "공모주 청약일정",
    "공모주청약 일정",
    "신규상장",
    "증시캘린더",
    "실권주",
    "CB / BW",
    "상장 예정일",
    "기업IR 일정",
    "수요예측 일정",
    "수요예측 결과",
    "코넥스 시세",
    "K-OTC",
    "거래소 시세판",
    "코스닥 시세판",
]

_ALLOWED_38_DETAIL_LABELS = [
    "종목명", "회사명", "기업명",
    "시장구분", "시장",
    "종목코드", "단축코드",
    "업종",
    "주간사", "주관사",
    "공모청약일", "청약일정", "공모일정",
    "수요예측일정", "수요예측일", "기관수요예측일", "예측일",
    "신규상장일", "상장일", "상장예정일",
    "희망공모가액", "희망공모가", "공모희망가",
    "확정공모가", "공모가",
    "청약경쟁률", "일반청약경쟁률",
    "기관경쟁률", "수요예측경쟁률",
    "의무보유확약", "확약",
    "현재가", "주가",
    "총공모주식수", "총공모주식",
    "상장공모", "신주모집", "구주매출",
    "상장주식수", "상장후주식수", "상장후총주식수",
]


def _compact_label(value: Any) -> str:
    text = "" if is_missing(value) else str(value).replace("\xa0", " ").strip()
    text = re.sub(r"\s+", "", text)
    text = re.sub(r"[^0-9A-Za-z가-힣]", "", text)
    return text.lower()


def _clean_text_value(value: Any) -> str | None:
    if is_missing(value):
        return None
    text = str(value).replace("\xa0", " ").strip()
    text = re.sub(r"\s+", " ", text)
    return text or None


def _looks_like_menu_blob(value: Any) -> bool:
    text = _clean_text_value(value) or ""
    if not text:
        return False
    if len(text) >= 120:
        return True
    return sum(token in text for token in SUSPICIOUS_MENU_TOKENS) >= 2


def _clean_symbol_value(value: Any) -> str | None:
    return normalize_symbol_text(_clean_text_value(value), zfill=True)


def _clean_market_value(value: Any) -> str | None:
    text = _clean_text_value(value) or ""
    if not text or _looks_like_menu_blob(text):
        return None
    mapping = {
        "코스닥": "코스닥",
        "kosdaq": "코스닥",
        "코스피": "유가증권",
        "kospi": "유가증권",
        "유가증권": "유가증권",
        "유가": "유가증권",
        "거래소": "유가증권",
        "코넥스": "코넥스",
        "konex": "코넥스",
        "k-otc": "K-OTC",
        "kotc": "K-OTC",
        "비상장": "비상장",
    }
    lowered = text.lower()
    for token, mapped in mapping.items():
        if token.lower() in lowered:
            return mapped
    return None


def _clean_sector_value(value: Any) -> str | None:
    text = _clean_text_value(value) or ""
    if not text or _looks_like_menu_blob(text) or len(text) > 80:
        return None
    return text


def _clean_underwriter_value(value: Any) -> str | None:
    text = _clean_text_value(value) or ""
    if not text or _looks_like_menu_blob(text):
        return None
    return text


def _parse_share_count_text(value: Any) -> float | None:
    text = _clean_text_value(value) or ""
    if not text or _looks_like_menu_blob(text):
        return None
    matches = [safe_float(match) for match in re.findall(r"([0-9][0-9,]{2,})\s*주", text)]
    matches = [x for x in matches if x is not None]
    if matches:
        return float(max(matches))
    fallback = [safe_float(match) for match in re.findall(r"([0-9][0-9,]{2,})", text)]
    fallback = [x for x in fallback if x is not None and x >= 1000]
    return float(max(fallback)) if fallback else None


def _extract_offer_structure_counts(text: Any) -> dict[str, float | None]:
    raw = _clean_text_value(text) or ""
    if not raw or _looks_like_menu_blob(raw):
        return {"new_shares": None, "selling_shares": None}
    result = {"new_shares": None, "selling_shares": None}
    patterns = {
        "new_shares": [r"신주모집\s*[:：]?\s*([0-9][0-9,]{2,})\s*주", r"신주\s*[:：]?\s*([0-9][0-9,]{2,})\s*주"],
        "selling_shares": [r"구주매출\s*[:：]?\s*([0-9][0-9,]{2,})\s*주", r"매출\s*[:：]?\s*([0-9][0-9,]{2,})\s*주"],
    }
    for key, exprs in patterns.items():
        for expr in exprs:
            match = re.search(expr, raw)
            if not match:
                continue
            number = safe_float(match.group(1))
            if number is not None:
                result[key] = float(number)
                break
    return result


def _is_probable_schedule_name(value: Any) -> bool:
    text = _clean_text_value(value) or ""
    if not text:
        return False
    if looks_like_junk_issue_name(text) or _looks_like_menu_blob(text):
        return False
    if len(text) > 60:
        return False
    if not re.search(r"[가-힣A-Za-z0-9]", text):
        return False
    return True


def _row_name_matches(detail: dict[str, Any], current_name: Any) -> bool:
    detail_name = normalize_name_key(detail.get("name"))
    current_key = normalize_name_key(current_name)
    if not detail_name or not current_key:
        return True
    return detail_name == current_key


def _read_best_table(html: str, required_keywords: list[str]) -> pd.DataFrame:
    try:
        tables = pd.read_html(StringIO(html), displayed_only=False)
    except Exception:
        return pd.DataFrame()

    best = pd.DataFrame()
    best_score = float("-inf")
    menu_tokens = ["오늘의공시", "회사별검색", "통합검색", "상장법인상세정보", "기업 밸류업", "투자유의사항", "IPO현황", "정보실"]
    normalized_required = [clean_column_label(keyword) for keyword in required_keywords if keyword]

    for table in tables:
        if table is None or table.empty:
            continue
        work = _normalize_columns(table)
        cols = [clean_column_label(c) for c in work.columns]
        preview_cells = [str(v).strip() for v in work.head(20).astype(str).fillna("").to_numpy().ravel().tolist()]
        preview = " ".join(preview_cells)
        preview_clean = clean_column_label(preview)

        header_hits = 0
        preview_hits = 0
        exact_header_hits = 0
        for keyword in normalized_required:
            if not keyword:
                continue
            if any(keyword == col for col in cols):
                exact_header_hits += 1
                header_hits += 1
            elif any(keyword in col or col in keyword for col in cols):
                header_hits += 1
            if keyword and keyword in preview_clean:
                preview_hits += 1

        if header_hits == 0 and preview_hits == 0:
            continue

        row_bonus = min(len(work), 20)
        width_penalty = max(0, len(cols) - 12) * 2
        menu_penalty = sum(token in preview for token in menu_tokens) * 8
        empty_col_penalty = sum(1 for col in work.columns if work[col].astype(str).str.strip().eq("").all())

        score = (exact_header_hits * 120) + (header_hits * 60) + (preview_hits * 12) + row_bonus - width_penalty - menu_penalty - empty_col_penalty
        if score > best_score:
            best = table
            best_score = score

    if best_score == float("-inf"):
        return pd.DataFrame()
    return best


def _dedupe_labels(labels: list[str]) -> list[str]:
    counts: dict[str, int] = {}
    out: list[str] = []
    for label in labels:
        base = str(label or "").strip() or "col"
        counts[base] = counts.get(base, 0) + 1
        if counts[base] == 1:
            out.append(base)
        else:
            out.append(f"{base}__{counts[base]}")
    return out


def _scalarize_cell(value: Any) -> Any:
    if isinstance(value, pd.Series):
        for item in value.tolist():
            scalar = _scalarize_cell(item)
            if scalar is None:
                continue
            if isinstance(scalar, str) and scalar.strip() == "":
                continue
            return scalar
        return None
    if isinstance(value, (list, tuple)):
        for item in value:
            scalar = _scalarize_cell(item)
            if scalar is None:
                continue
            if isinstance(scalar, str) and scalar.strip() == "":
                continue
            return scalar
        return None
    return value


def _http_get(url: str, timeout: int = 15, *, referer: str | None = None, session: requests.Session | None = None) -> requests.Response:
    headers = dict(DEFAULT_HTTP_HEADERS)
    headers["Referer"] = referer or url
    sess = session or requests.Session()
    warmup_targets = []
    if "kind.krx.co.kr" in url:
        warmup_targets = ["https://kind.krx.co.kr/", "https://kind.krx.co.kr/disclosuretoday/main.do?method=searchTodayMain"]
    elif "38.co.kr" in url:
        warmup_targets = [THIRTYEIGHT_BASE_URL, THIRTYEIGHT_BASE_URL.replace('https://', 'http://')]
    for target in warmup_targets:
        try:
            sess.get(target, headers=headers, timeout=min(timeout, 8))
            break
        except Exception:
            continue
    try:
        response = sess.get(url, headers=headers, timeout=timeout)
        response.raise_for_status()
    except Exception as exc:
        if '38.co.kr' not in url or not url.startswith('https://'):
            raise
        fallback_url = 'http://' + url[len('https://'):]
        response = sess.get(fallback_url, headers=headers, timeout=timeout)
        response.raise_for_status()
        response.url = fallback_url
    response.encoding = response.apparent_encoding or response.encoding or 'utf-8'
    return response


def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    work = df.copy()
    new_cols = []
    for i, col in enumerate(work.columns):
        if isinstance(col, tuple):
            parts = [str(x).strip() for x in col if str(x).strip() and not str(x).lower().startswith("unnamed")]
            label = " ".join(parts) or f"col_{i}"
        else:
            label = str(col).strip() or f"col_{i}"
        new_cols.append(label)
    work.columns = _dedupe_labels(new_cols)
    work = work.dropna(axis=1, how="all")
    return work


def _clean_html_text(html_text: str) -> str:
    try:
        root = lxml_html.fromstring(html_text)
        text = root.text_content()
    except Exception:
        text = re.sub(r"<[^>]+>", " ", html_text)
    text = html.unescape(text).replace("\xa0", " ")
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _extract_summary_body(text: str, *, header_patterns: Sequence[str]) -> str:
    compact = re.sub(r"\s+", " ", text or "").strip()
    if not compact:
        return ""
    start = -1
    matched = ""
    lowered = compact.lower()
    for header in header_patterns:
        idx = lowered.find(header.lower())
        if idx >= 0 and (start == -1 or idx < start):
            start = idx
            matched = header
    if start < 0:
        return ""
    section = compact[start:]
    if matched:
        cut = section.lower().find(matched.lower())
        if cut >= 0:
            section = section[cut:]
    for token in ["검색 EXCEL", "검색  EXCEL", "상기 내용은", "본 정보는", "### 공시", "홈 IPO현황", "마이페이지"]:
        idx = section.find(token)
        if idx > 0:
            section = section[:idx]
            break
    return section.strip()


def _parse_market_name_token(token: str) -> tuple[str | None, str | None]:
    text = _clean_text_value(token) or ""
    if not text:
        return None, None
    match = re.match(r"^(유가증권|코스닥|코넥스|K-OTC)\s+(.+)$", text)
    if match:
        return _clean_market_value(match.group(1)), _clean_text_value(match.group(2))
    return None, text


def _split_summary_rows(section: str) -> list[str]:
    if not section:
        return []
    body = section
    body = re.sub(r"^.*?목록\.\s*", "", body, count=1)
    header_end = body.find(". ")
    if header_end >= 0:
        body = body[header_end + 2 :]
    parts = [part.strip(" .") for part in re.split(r"\s*[;；]\s*", body) if part.strip(" .")]
    return parts


def _extract_kind_listing_summary_fallback(html_text: str) -> pd.DataFrame:
    section = _extract_summary_body(_clean_html_text(html_text), header_patterns=["신규상장기업현황 목록.", "목록. 회사명, 상장일, 상장유형"])
    rows: list[dict[str, Any]] = []
    for part in _split_summary_rows(section):
        fields = [field.strip() for field in part.split(',') if field.strip()]
        if len(fields) < 3:
            continue
        market, name = _parse_market_name_token(fields[0])
        if not name:
            name = fields[0]
        rows.append({
            '회사명': name,
            '시장구분': market,
            '상장일': fields[1] if len(fields) >= 2 else None,
            '상장유형': fields[2] if len(fields) >= 3 else None,
            '증권구분': fields[3] if len(fields) >= 4 else None,
            '업종': fields[4] if len(fields) >= 5 else None,
            '국적': fields[5] if len(fields) >= 6 else None,
            '상장주선인': ','.join(fields[6:]) if len(fields) >= 7 else None,
        })
    return pd.DataFrame(rows)


def _extract_kind_public_summary_fallback(html_text: str) -> pd.DataFrame:
    section = _extract_summary_body(_clean_html_text(html_text), header_patterns=["공모기업현황 목록.", "목록. 회사명, 신고서제출일, 수요예측일정"])
    rows: list[dict[str, Any]] = []
    for part in _split_summary_rows(section):
        fields = [field.strip() for field in part.split(',') if field.strip()]
        if len(fields) < 3:
            continue
        market, name = _parse_market_name_token(fields[0])
        if not name:
            name = fields[0]
        rows.append({
            '회사명': name,
            '시장구분': market,
            '신고서제출일': fields[1] if len(fields) >= 2 else None,
            '수요예측일정': fields[2] if len(fields) >= 3 else None,
            '청약일정': fields[3] if len(fields) >= 4 else None,
            '납입일': fields[4] if len(fields) >= 5 else None,
            '확정공모가': fields[5] if len(fields) >= 6 else None,
            '공모금액': fields[6] if len(fields) >= 7 else None,
            '상장예정일': fields[7] if len(fields) >= 8 else None,
            '상장주선인': ','.join(fields[8:]) if len(fields) >= 9 else None,
        })
    return pd.DataFrame(rows)


def _normalize_price_text(value: Any) -> float | None:
    value = _scalarize_cell(value)
    if value is None:
        return None
    return safe_float(value)


def _normalize_ratio_text(value: Any) -> float | None:
    value = _scalarize_cell(value)
    if value is None:
        return None
    return safe_float(value)


def _normalize_band_pair(value: Any) -> tuple[float | None, float | None]:
    value = _scalarize_cell(value)
    text = str(value or "").strip()
    nums = [safe_float(x) for x in re.findall(r"[0-9,]+(?:\.[0-9]+)?", text)]
    nums = [x for x in nums if x is not None]
    if len(nums) >= 2:
        return nums[0], nums[1]
    if len(nums) == 1:
        return nums[0], nums[0]
    return None, None


def _looks_like_price_band(value: Any) -> bool:
    text = str(_scalarize_cell(value) or "").strip()
    if not text:
        return False
    return bool(re.search(r"[0-9,]+\s*[~∼-]\s*[0-9,]+", text))


def _looks_like_single_price(value: Any) -> bool:
    text = str(_scalarize_cell(value) or "").strip()
    if not text or _looks_like_price_band(text):
        return False
    nums = re.findall(r"[0-9,]+", text)
    return len(nums) == 1


def _resolve_38_price_columns(work: pd.DataFrame) -> tuple[str | None, str | None]:
    candidate_cols = [
        col for col in work.columns if any(token in clean_column_label(col) for token in ["확정공모가", "공모가", "희망공모가", "공모희망가", "희망가"])
    ]
    band_col: str | None = None
    price_col: str | None = None

    for col in candidate_cols:
        label = clean_column_label(col)
        if any(token in label for token in ["희망", "밴드"]):
            band_col = band_col or col
        if "확정" in label:
            price_col = price_col or col

    preview_values = {}
    for col in candidate_cols:
        series = work[col] if col in work.columns else pd.Series(dtype="object")
        value = None
        for item in series.tolist():
            scalar = _scalarize_cell(item)
            if scalar is None:
                continue
            if isinstance(scalar, str) and scalar.strip() == "":
                continue
            value = scalar
            break
        preview_values[col] = value

    if band_col is None:
        for col in candidate_cols:
            if _looks_like_price_band(preview_values.get(col)):
                band_col = col
                break
    if price_col is None:
        for col in candidate_cols:
            if col == band_col:
                continue
            if _looks_like_single_price(preview_values.get(col)):
                price_col = col
                break

    if band_col is None:
        band_col = pick_first_present(work, ["희망공모가", "공모희망가", "희망가", "공모가"])
    if price_col is None:
        price_col = pick_first_present(work, ["확정공모가", "공모가"])
    return price_col, band_col


def _is_38_detail_href(href: str) -> bool:
    href = str(href or "")
    return any(
        token in href
        for token in [
            "fund_view.php",
            "/html/fund/",
            "/html/ipo/ipo.htm",
            "/html/forum/board/",
            "/forum/board/",
            "o=cinfo",
        ]
    ) and ("no=" in href or "code=" in href or "fund_view.php" in href)


def _score_38_detail_href(href: str) -> int:
    href = str(href or "")
    if "o=cinfo" in href:
        return 6
    if ("/forum/board/" in href or "/html/forum/board/" in href) and "code=" in href:
        return 5
    if "/html/ipo/ipo.htm" in href and "no=" in href:
        return 4
    if "fund_view.php" in href:
        return 3
    if "/html/fund/" in href and "no=" in href:
        return 2
    return 0


def _extract_38_text_fallback(text_blob: str, *patterns: str) -> str | None:
    if not text_blob:
        return None
    for pattern in patterns:
        match = re.search(pattern, text_blob, flags=re.IGNORECASE)
        if not match:
            continue
        value = match.group(1) if match.groups() else match.group(0)
        value = _clean_text_value(value)
        if value and not _looks_like_menu_blob(value):
            return value
    return None


def _extract_schedule_row_links(html_text: str, *, base_url: str, mobile: bool = False) -> pd.DataFrame:
    try:
        root = lxml_html.fromstring(html_text)
    except Exception:
        return pd.DataFrame(columns=["name_key", "detail_url"])

    rows: list[dict[str, str]] = []
    row_nodes = root.xpath('//tr[.//a[@href]]')
    for tr in row_nodes:
        anchors = tr.xpath('.//a[@href]')
        detail_href = None
        detail_score = -1
        for anchor in anchors:
            href = anchor.attrib.get('href', '') or ''
            if not _is_38_detail_href(href):
                continue
            score = _score_38_detail_href(href)
            if score > detail_score:
                detail_href = href
                detail_score = score
        if not detail_href:
            continue
        texts: list[str] = []
        for node in tr.xpath('.//th|.//td'):
            text = ' '.join(part.strip() for part in node.itertext() if part and str(part).strip())
            text = _clean_text_value(text) or ''
            if text:
                texts.append(text)
        if not texts:
            texts = [(_clean_text_value(' '.join(a.itertext())) or '') for a in anchors]
        company = None
        for candidate in texts:
            if not _is_probable_schedule_name(candidate):
                continue
            if any(token in candidate for token in ['분석', '공모주', 'IPO', '비상장', '청약일정']):
                continue
            company = candidate
            break
        if company is None:
            for anchor in anchors:
                candidate = _clean_text_value(' '.join(anchor.itertext())) or ''
                if _is_probable_schedule_name(candidate):
                    company = candidate
                    break
        if company is None:
            continue
        name_key = normalize_name_key(company)
        if not name_key:
            continue
        rows.append({'name_key': name_key, 'detail_url': urljoin(base_url, detail_href)})

    out = pd.DataFrame(rows)
    if out.empty:
        return pd.DataFrame(columns=['name_key', 'detail_url'])
    return out.drop_duplicates(subset=['name_key'], keep='first').reset_index(drop=True)


def _extract_38_detail_links(html: str) -> pd.DataFrame:
    out = _extract_schedule_row_links(html, base_url=THIRTYEIGHT_BASE_URL, mobile=False)
    if not out.empty:
        return out
    try:
        root = lxml_html.fromstring(html)
    except Exception:
        return pd.DataFrame(columns=['name_key', 'detail_url'])
    rows: list[dict[str, str]] = []
    for anchor in root.xpath('//a[@href]'):
        href = anchor.attrib.get('href', '') or ''
        if not _is_38_detail_href(href):
            continue
        text = ' '.join(anchor.itertext()).strip()
        if not text:
            continue
        name_key = normalize_name_key(text)
        if not name_key or not _is_probable_schedule_name(text):
            continue
        rows.append({'name_key': name_key, 'detail_url': urljoin(THIRTYEIGHT_BASE_URL, href)})
    out = pd.DataFrame(rows)
    if out.empty:
        return pd.DataFrame(columns=['name_key', 'detail_url'])
    return out.drop_duplicates(subset=['name_key'], keep='first').reset_index(drop=True)


def _extract_38_mobile_detail_links(html: str) -> pd.DataFrame:
    out = _extract_schedule_row_links(html, base_url=THIRTYEIGHT_MOBILE_BASE_URL, mobile=True)
    if not out.empty:
        return out
    try:
        root = lxml_html.fromstring(html)
    except Exception:
        return pd.DataFrame(columns=['name_key', 'detail_url'])
    rows: list[dict[str, str]] = []
    for anchor in root.xpath('//a[@href]'):
        href = anchor.attrib.get('href', '') or ''
        if not _is_38_detail_href(href):
            continue
        text = ' '.join(anchor.itertext()).strip()
        if not text:
            continue
        name_key = normalize_name_key(text)
        if not name_key or not _is_probable_schedule_name(text):
            continue
        rows.append({'name_key': name_key, 'detail_url': urljoin(THIRTYEIGHT_MOBILE_BASE_URL, href)})
    out = pd.DataFrame(rows)
    if out.empty:
        return pd.DataFrame(columns=['name_key', 'detail_url'])
    return out.drop_duplicates(subset=['name_key'], keep='first').reset_index(drop=True)


def fetch_kind_listing_table(timeout: int = 15) -> pd.DataFrame:
    response = _http_get(KIND_LISTING_URL, timeout=timeout, referer=KIND_LISTING_URL)
    table = _read_best_table(response.text, ["회사명", "상장일", "상장유형", "증권구분", "상장주선인", "지정자문인"])
    if table.empty:
        table = _extract_kind_listing_summary_fallback(response.text)
    if table.empty:
        return pd.DataFrame()
    rename_map = {}
    for col in table.columns:
        col_str = str(col)
        if "회사명" in col_str:
            rename_map[col] = "회사명"
        elif "상장일" in col_str:
            rename_map[col] = "상장일"
        elif "상장유형" in col_str:
            rename_map[col] = "상장유형"
        elif "상장주선인" in col_str or "지정자문인" in col_str:
            rename_map[col] = "주관사"
        elif "업종" in col_str:
            rename_map[col] = "업종"
        elif "증권구분" in col_str or "시장구분" in col_str:
            rename_map[col] = "시장구분"
        elif "종목코드" in col_str or "단축코드" in col_str:
            rename_map[col] = "종목코드"
        elif "공모가" in col_str and "희망" not in col_str:
            rename_map[col] = "공모가"
    return table.rename(columns=rename_map)


def fetch_kind_public_offering_table(timeout: int = 15) -> pd.DataFrame:
    errors: list[str] = []
    for url in KIND_PUBLIC_OFFER_FALLBACK_URLS:
        try:
            response = _http_get(url, timeout=timeout, referer=url)
            table = _read_best_table(response.text, ["회사명", "신고서제출일", "수요예측일정", "청약일정", "확정공모가", "상장예정일", "상장주선인", "지정자문인"])
            if table.empty:
                table = _extract_kind_public_summary_fallback(response.text)
            if table.empty:
                errors.append(f"{url}: parsed table empty")
                continue
            rename_map = {}
            for col in table.columns:
                col_str = str(col)
                if "회사명" in col_str:
                    rename_map[col] = "회사명"
                elif "신고서제출일" in col_str:
                    rename_map[col] = "신고서제출일"
                elif "청약" in col_str and "일정" in col_str:
                    rename_map[col] = "청약일정"
                elif "수요예측" in col_str and "일정" in col_str:
                    rename_map[col] = "수요예측일정"
                elif "납입일" in col_str:
                    rename_map[col] = "납입일"
                elif "확정공모가" in col_str or ("공모가" in col_str and "희망" not in col_str):
                    rename_map[col] = "확정공모가"
                elif "상장예정일" in col_str or ("상장" in col_str and "예정" in col_str):
                    rename_map[col] = "상장예정일"
                elif "상장주선인" in col_str or "지정자문인" in col_str:
                    rename_map[col] = "주관사"
                elif "시장구분" in col_str or "증권구분" in col_str:
                    rename_map[col] = "시장구분"
                elif "종목코드" in col_str or "단축코드" in col_str:
                    rename_map[col] = "종목코드"
                elif "업종" in col_str:
                    rename_map[col] = "업종"
            out = table.rename(columns=rename_map)
            if not out.empty:
                return out
        except Exception as exc:
            errors.append(f"{url}: {exc}")
    raise RuntimeError("KIND public offering fetch failed; " + " | ".join(errors[:6]))


def fetch_kind_pubprice_table(timeout: int = 15) -> pd.DataFrame:
    response = _http_get(KIND_PUB_PRICE_URL, timeout=timeout, referer=KIND_PUB_PRICE_URL)
    table = _read_best_table(response.text, ["회사명", "주관사", "상장일", "공모가", "수정공모가", "최근거래일", "등락률"])
    if table.empty:
        return pd.DataFrame()
    return table


def fetch_38_schedule(timeout: int = 15, include_detail_links: bool = True) -> pd.DataFrame:
    errors: list[str] = []
    candidates = [
        (THIRTYEIGHT_MOBILE_SCHEDULE_URL, _extract_38_mobile_detail_links),
        (THIRTYEIGHT_SCHEDULE_URL, _extract_38_detail_links),
    ]
    for url, link_extractor in candidates:
        try:
            response = _http_get(url, timeout=timeout)
            table = _read_best_table(response.text, ["기업명", "종목명", "공모일정", "주간사", "주관사"])
            if table.empty:
                errors.append(f"{url}: parsed table empty")
                continue
            rename_map = {}
            for col in table.columns:
                col_str = str(col)
                if "기업명" in col_str or "종목명" in col_str or "회사명" in col_str:
                    rename_map[col] = "기업명"
                elif "공모" in col_str and "일정" in col_str:
                    rename_map[col] = "공모일정"
                elif "청약" in col_str and "경쟁" in col_str:
                    rename_map[col] = "청약경쟁률"
                elif "기관" in col_str and "경쟁" in col_str:
                    rename_map[col] = "기관경쟁률"
                elif "주간사" in col_str or "주관사" in col_str:
                    rename_map[col] = "주간사"
                elif "확정" in col_str and "공모가" in col_str:
                    rename_map[col] = "확정공모가"
                elif ("희망" in col_str and "공모가" in col_str) or "공모희망가" in col_str:
                    rename_map[col] = "희망공모가"
                elif ("공모가" in col_str) and (col not in rename_map):
                    rename_map[col] = "공모가"
                elif "상장예정일" in col_str or "상장일" in col_str:
                    rename_map[col] = "상장일"
            out = table.rename(columns=rename_map)
            if include_detail_links:
                links = link_extractor(response.text)
                if not links.empty and any(c in out.columns for c in ["기업명", "종목명", "회사명"]):
                    merged = out.copy()
                    name_col = next(c for c in ["기업명", "종목명", "회사명"] if c in merged.columns)
                    merged["name_key"] = merged[name_col].map(normalize_name_key)
                    merged = merged.merge(links, on="name_key", how="left")
                    out = merged.drop(columns=["name_key"])
            if not out.empty:
                return out
            errors.append(f"{url}: renamed table empty")
        except Exception as exc:
            errors.append(f"{url}: {exc}")
    raise RuntimeError("38 schedule fetch failed; " + " | ".join(errors[:6]))




def fetch_38_new_listing_table(timeout: int = 15, max_pages: int = 4) -> pd.DataFrame:
    errors: list[str] = []
    frames: list[pd.DataFrame] = []
    for page in range(1, max_pages + 1):
        url = THIRTYEIGHT_NEW_LISTING_URL if page == 1 else f"{THIRTYEIGHT_NEW_LISTING_URL}&page={page}"
        try:
            response = _http_get(url, timeout=timeout)
            table = _read_best_table(response.text, ["기업명", "신규상장일", "현재가", "공모가"])
            if table.empty:
                errors.append(f"{url}: parsed table empty")
                continue
            rename_map = {}
            for col in table.columns:
                col_str = str(col)
                if "기업명" in col_str or "종목명" in col_str or "회사명" in col_str:
                    rename_map[col] = "기업명"
                elif "신규상장일" in col_str or "상장일" in col_str:
                    rename_map[col] = "신규상장일"
                elif ("현재가" in col_str or "종가" in col_str) and "전일" not in col_str:
                    rename_map[col] = "현재가"
                elif "전일비" in col_str or "등락률" in col_str:
                    rename_map[col] = "전일비"
                elif "공모가" in col_str:
                    rename_map[col] = "공모가"
            out = table.rename(columns=rename_map)
            if not out.empty:
                out["_page"] = page
                frames.append(out)
        except Exception as exc:
            errors.append(f"{url}: {exc}")
    if not frames:
        raise RuntimeError("38 new-listing fetch failed; " + " | ".join(errors[:6]))
    merged = pd.concat(frames, ignore_index=True)
    if "기업명" in merged.columns:
        merged["name_key"] = merged["기업명"].map(normalize_name_key)
        merged = merged.sort_values([c for c in ["신규상장일", "name_key", "_page"] if c in merged.columns], na_position="last")
        merged = merged.drop_duplicates(subset=["name_key"], keep="last").drop(columns=["name_key", "_page"], errors="ignore")
    return merged.reset_index(drop=True)


def standardize_38_new_listing_table(df: pd.DataFrame, *, today: pd.Timestamp | None = None) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=STANDARD_ISSUE_COLUMNS)
    today = today or today_kst()
    work = _normalize_columns(df)

    name_col = pick_first_present(work, ["기업명", "종목명", "회사명"])
    listing_col = pick_first_present(work, ["신규상장일", "상장일"])
    price_col = pick_first_present(work, ["현재가", "최근거래일 종가", "종가"])
    offer_col = pick_first_present(work, ["공모가", "확정공모가"])
    change_col = pick_first_present(work, ["전일비", "등락률", "전일비(%),", "전일비(%)"])

    rows: list[dict[str, Any]] = []
    for _, record in work.iterrows():
        name = record.get(name_col or "")
        if pd.isna(name) or str(name).strip() == "":
            continue
        listing_date = parse_date_text(_scalarize_cell(record.get(listing_col or "")), default_year=today.year) if listing_col else None
        row = build_blank_issue_row()
        row.update({
            "ipo_id": f"38_NW_{normalize_name_key(name)}",
            "name": str(name).strip(),
            "name_key": normalize_name_key(name),
            "stage": infer_issue_stage(None, None, listing_date, today=today, fallback="상장후"),
            "listing_date": listing_date,
            "offer_price": _normalize_price_text(record.get(offer_col or "")),
            "current_price": _normalize_price_text(record.get(price_col or "")),
            "day_change_pct": _normalize_ratio_text(record.get(change_col or "")),
            "source": "38",
            "source_detail": "new-listing-table",
            "last_refresh_ts": today,
        })
        rows.append(row)
    return standardize_issue_frame(pd.DataFrame(rows))


def standardize_38_seed_table(df: pd.DataFrame, *, today: pd.Timestamp | None = None) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=STANDARD_ISSUE_COLUMNS)
    today = today or today_kst()
    work = _normalize_columns(df)
    name_col = pick_first_present(work, ["name", "기업명", "종목명", "회사명"])
    key_col = pick_first_present(work, ["name_key", "기업명key", "종목key"])
    listing_col = pick_first_present(work, ["listing_date", "신규상장일", "상장일"])
    offer_col = pick_first_present(work, ["ipo_price", "공모가", "확정공모가"])
    kinds_col = pick_first_present(work, ["source_kinds", "sourcekind", "kind"])

    rows: list[dict[str, Any]] = []
    for _, record in work.iterrows():
        name = record.get(name_col or "")
        if pd.isna(name) or str(name).strip() == "":
            continue
        name_key = normalize_name_key(record.get(key_col or "") or name)
        listing_date = parse_date_text(record.get(listing_col or ""), default_year=today.year) if listing_col else None
        source_kinds = str(record.get(kinds_col or "") or "").strip()
        fallback_stage = "청약예정" if "subscription" in source_kinds and listing_date is None else "상장예정"
        row = build_blank_issue_row()
        row.update({
            "ipo_id": f"38_SEED_{name_key}",
            "name": str(name).strip(),
            "name_key": name_key,
            "stage": infer_issue_stage(None, None, listing_date, today=today, fallback=fallback_stage),
            "listing_date": listing_date,
            "offer_price": _normalize_price_text(record.get(offer_col or "")),
            "source": "38-seed",
            "source_detail": f"ipo-master-38:{source_kinds or 'seed'}",
            "last_refresh_ts": today,
        })
        rows.append(row)
    return standardize_issue_frame(pd.DataFrame(rows))


def build_blank_issue_row() -> dict[str, Any]:
    row = {col: pd.NA for col in STANDARD_ISSUE_COLUMNS}
    row["source"] = pd.NA
    return row




def _candidate_38_demand_result_urls(page: int) -> list[str]:
    candidates = []
    if page <= 1:
        candidates.extend([
            THIRTYEIGHT_DEMAND_RESULT_URL,
            "https://www.38.co.kr/html/fund/index.htm?o=r1",
            "https://forum.38.co.kr/html/fund/index.htm?l=&o=r1",
        ])
    else:
        candidates.extend([
            f"{THIRTYEIGHT_DEMAND_RESULT_URL}&page={page}",
            f"https://www.38.co.kr/html/fund/index.htm?o=r1&page={page}",
            f"https://forum.38.co.kr/html/fund/index.htm?l=&o=r1&page={page}",
        ])
    seen: set[str] = set()
    out: list[str] = []
    for url in candidates:
        if url not in seen:
            seen.add(url)
            out.append(url)
    return out


def _candidate_38_ir_urls(page: int) -> list[str]:
    candidates = []
    if page <= 1:
        candidates.extend([
            THIRTYEIGHT_IR_DATA_URL,
            f"{THIRTYEIGHT_IR_DATA_URL}?Array=&page=1&s%5Bsc_string%5D=",
            f"{THIRTYEIGHT_IR_DATA_URL}?page=1",
        ])
    else:
        candidates.extend([
            f"{THIRTYEIGHT_IR_DATA_URL}?Array=&page={page}&s%5Bsc_string%5D=",
            f"{THIRTYEIGHT_IR_DATA_URL}?page={page}",
        ])
    seen: set[str] = set()
    out: list[str] = []
    for url in candidates:
        if url not in seen:
            seen.add(url)
            out.append(url)
    return out


def _extract_row_cell_texts(tr: Any) -> list[str]:
    texts: list[str] = []
    for cell in tr.xpath('./th|./td'):
        text = _clean_text_value(" ".join(t.strip() for t in cell.xpath('.//text()') if str(t).strip()))
        if text:
            texts.append(text)
    return texts


def _pick_company_name_from_texts(texts: Sequence[str]) -> str | None:
    skip_tokens = ["IR자료", "PDF", "다운", "보기", "38커뮤니케이션", "등록일", "번호"]
    for text in texts:
        cleaned = _clean_text_value(text) or ""
        if not cleaned:
            continue
        if re.fullmatch(r"[0-9]+", cleaned):
            continue
        if any(token.lower() in cleaned.lower() for token in skip_tokens):
            continue
        if parse_date_text(cleaned) is not None:
            continue
        if _is_probable_schedule_name(cleaned):
            return cleaned
    return None


def parse_38_demand_result_html(html: str, *, url: str = "", today: pd.Timestamp | None = None) -> pd.DataFrame:
    today = today or today_kst()
    table = _read_best_table(html, ["기업명", "예측일", "공모희망가", "공모가", "기관 경쟁률", "의무보유 확약", "주간사"])
    if table.empty:
        return pd.DataFrame(columns=STANDARD_ISSUE_COLUMNS)
    work = _normalize_columns(table)
    name_col = pick_first_present(work, ["기업명", "종목명", "회사명"])
    forecast_col = pick_first_present(work, ["예측일", "수요예측일", "기관수요예측일"])
    offer_col = pick_first_present(work, ["공모가", "확정공모가"])
    band_col = pick_first_present(work, ["공모희망가", "희망공모가", "희망가", "공모가"])
    broker_col = pick_first_present(work, ["주간사", "주관사"])
    inst_col = next((col for col in work.columns if "기관" in str(col) and "경쟁" in str(col)), None)
    if inst_col is None:
        inst_col = pick_first_present(work, ["기관 경쟁률", "기관경쟁률", "수요예측경쟁률"])
    lock_col = next((col for col in work.columns if ("의무보유" in str(col)) or ("확약" in str(col))), None)
    if lock_col is None:
        lock_col = pick_first_present(work, ["의무보유 확약", "의무보유확약", "확약"])
    rows: list[dict[str, Any]] = []
    for _, record in work.iterrows():
        name = record.get(name_col or "")
        if not _is_probable_schedule_name(name):
            continue
        band_low, band_high = _normalize_band_pair(record.get(band_col or ""))
        offer_price = _normalize_price_text(record.get(offer_col or ""))
        inst_ratio = _normalize_ratio_text(record.get(inst_col or ""))
        lock_ratio = _normalize_ratio_text(record.get(lock_col or ""))
        forecast_date = parse_date_text(_scalarize_cell(record.get(forecast_col or "")), default_year=today.year) if forecast_col else None
        has_signal = any(value is not None and not pd.isna(value) for value in [forecast_date, band_low, band_high, offer_price, inst_ratio, lock_ratio])
        if not has_signal:
            continue
        row = build_blank_issue_row()
        row.update({
            "ipo_id": f"38_result_{normalize_name_key(name)}",
            "name": str(name).strip(),
            "name_key": normalize_name_key(name),
            "underwriters": _clean_underwriter_value(_scalarize_cell(record.get(broker_col or ""))),
            "forecast_date": forecast_date,
            "price_band_low": band_low,
            "price_band_high": band_high,
            "offer_price": offer_price,
            "institutional_competition_ratio": inst_ratio,
            "lockup_commitment_ratio": lock_ratio,
            "source": "38",
            "source_detail": f"38-demand:{url}" if url else "38-demand",
            "last_refresh_ts": today,
        })
        rows.append(row)
    out = pd.DataFrame(rows)
    if out.empty:
        return pd.DataFrame(columns=STANDARD_ISSUE_COLUMNS)
    return clean_issue_frame(out)


def fetch_38_demand_results(timeout: int = 15, max_pages: int = 6) -> pd.DataFrame:
    errors: list[str] = []
    frames: list[pd.DataFrame] = []
    empty_streak = 0
    for page in range(1, max_pages + 1):
        page_frame = pd.DataFrame()
        for url in _candidate_38_demand_result_urls(page):
            try:
                response = _http_get(url, timeout=timeout)
                page_frame = parse_38_demand_result_html(response.text, url=url)
                if not page_frame.empty:
                    break
            except Exception as exc:
                errors.append(f"{url}: {exc}")
        if page_frame.empty:
            empty_streak += 1
            if page >= 2 and empty_streak >= 2:
                break
            continue
        empty_streak = 0
        page_frame = page_frame.copy()
        page_frame["_page"] = page
        frames.append(page_frame)
    if not frames:
        if errors:
            raise RuntimeError("38 demand result fetch failed; " + " | ".join(errors[:6]))
        return pd.DataFrame(columns=STANDARD_ISSUE_COLUMNS)
    out = pd.concat(frames, ignore_index=True)
    out = out.sort_values([c for c in ["forecast_date", "_page", "name_key"] if c in out.columns], ascending=[False, True, True], na_position="last")
    if "name_key" in out.columns:
        out = out.drop_duplicates(subset=["name_key"], keep="first")
    return clean_issue_frame(out.drop(columns=["_page"], errors="ignore")).reset_index(drop=True)


def parse_38_ir_html(html: str, *, url: str = "", today: pd.Timestamp | None = None) -> pd.DataFrame:
    today = today or today_kst()
    try:
        root = lxml_html.fromstring(html)
    except Exception:
        return pd.DataFrame(columns=STANDARD_ISSUE_COLUMNS)
    rows: list[dict[str, Any]] = []
    for tr in root.xpath('//tr'):
        texts = _extract_row_cell_texts(tr)
        if not texts:
            continue
        anchors = []
        for a in tr.xpath('.//a[@href]'):
            href = str(a.get('href') or '').strip()
            if not href:
                continue
            full_url = urljoin(url or THIRTYEIGHT_BASE_URL, href)
            anchor_text = _clean_text_value(" ".join(t.strip() for t in a.xpath('.//text()') if str(t).strip())) or ""
            anchors.append((full_url, anchor_text))
        if not anchors:
            continue
        pdf_url = None
        for href, anchor_text in anchors:
            lowered = href.lower()
            if re.search(r'\.pdf(?:$|[?#])', lowered) or 'pdf' in anchor_text.lower() or 'download' in lowered or '/down' in lowered or '/file/' in lowered:
                pdf_url = href
                break
        if not pdf_url:
            continue
        company = _pick_company_name_from_texts(texts)
        if not company:
            continue
        date_value = None
        for text in texts:
            date_value = parse_date_text(text, default_year=today.year)
            if date_value is not None:
                break
        title_candidates: list[str] = []
        for text in texts:
            cleaned = _clean_text_value(text) or ""
            if not cleaned or cleaned == company:
                continue
            if parse_date_text(cleaned, default_year=today.year) is not None:
                continue
            if cleaned.lower() in {"pdf", "다운로드", "보기"}:
                continue
            if any(token in cleaned for token in ["38커뮤니케이션", "IPO IR자료"]):
                continue
            title_candidates.append(cleaned)
        title = max(title_candidates, key=len) if title_candidates else f"{company} IR자료"
        row = build_blank_issue_row()
        row.update({
            "ipo_id": f"38_ir_{normalize_name_key(company)}",
            "name": company,
            "name_key": normalize_name_key(company),
            "ir_title": title,
            "ir_date": date_value,
            "ir_pdf_url": pdf_url,
            "ir_source_page": url or THIRTYEIGHT_IR_DATA_URL,
            "source": "38",
            "source_detail": f"38-ir:{url}" if url else "38-ir",
            "last_refresh_ts": today,
        })
        rows.append(row)
    if not rows:
        return pd.DataFrame(columns=STANDARD_ISSUE_COLUMNS)
    out = pd.DataFrame(rows)
    out = out.sort_values([c for c in ["ir_date", "name_key"] if c in out.columns], ascending=[False, True], na_position="last")
    out = out.drop_duplicates(subset=["name_key"], keep="first")
    return clean_issue_frame(out).reset_index(drop=True)


def fetch_38_ir_links(timeout: int = 15, max_pages: int = 8) -> pd.DataFrame:
    errors: list[str] = []
    frames: list[pd.DataFrame] = []
    empty_streak = 0
    for page in range(1, max_pages + 1):
        page_frame = pd.DataFrame()
        for url in _candidate_38_ir_urls(page):
            try:
                response = _http_get(url, timeout=timeout)
                page_frame = parse_38_ir_html(response.text, url=url)
                if not page_frame.empty:
                    break
            except Exception as exc:
                errors.append(f"{url}: {exc}")
        if page_frame.empty:
            empty_streak += 1
            if page >= 2 and empty_streak >= 2:
                break
            continue
        empty_streak = 0
        page_frame = page_frame.copy()
        page_frame["_page"] = page
        frames.append(page_frame)
    if not frames:
        if errors:
            raise RuntimeError("38 IR fetch failed; " + " | ".join(errors[:6]))
        return pd.DataFrame(columns=STANDARD_ISSUE_COLUMNS)
    out = pd.concat(frames, ignore_index=True)
    out = out.sort_values([c for c in ["ir_date", "_page", "name_key"] if c in out.columns], ascending=[False, True, True], na_position="last")
    out = out.drop_duplicates(subset=["name_key"], keep="first")
    return clean_issue_frame(out.drop(columns=["_page"], errors="ignore")).reset_index(drop=True)


def parse_seibro_release_html(html: str, *, url: str = "", today: pd.Timestamp | None = None) -> pd.DataFrame:
    today = today or today_kst()
    table = _read_best_table(html, ["해제일", "기업명", "해제주식수", "예수잔량", "시장구분"])
    work = _normalize_columns(table) if not table.empty else pd.DataFrame()
    rows: list[dict[str, Any]] = []
    if not work.empty:
        date_col = pick_first_present(work, ["해제일"])
        name_col = pick_first_present(work, ["기업명", "회사명", "종목명"])
        shares_col = next((col for col in work.columns if "해제주식수" in str(col)), None) or pick_first_present(work, ["해제주식수"])
        remain_col = next((col for col in work.columns if "예수잔량" in str(col)), None) or pick_first_present(work, ["예수잔량"])
        market_col = next((col for col in work.columns if "시장" in str(col)), None) or pick_first_present(work, ["시장구분", "시장"])
        for _, record in work.iterrows():
            name = _clean_text_value(record.get(name_col or ""))
            if not _is_probable_schedule_name(name):
                continue
            release_date = pd.to_datetime(_scalarize_cell(record.get(date_col or "")), errors='coerce')
            release_shares = safe_float(record.get(shares_col or ""))
            remaining = safe_float(record.get(remain_col or ""))
            market = _clean_market_value(record.get(market_col or ""))
            if pd.isna(release_date) or release_shares is None:
                continue
            rows.append({
                "name": name,
                "name_key": normalize_name_key(name),
                "release_date": pd.Timestamp(release_date).normalize(),
                "release_shares": float(release_shares),
                "remaining_locked_shares": None if remaining is None else float(remaining),
                "market": market,
                "source": "Seibro",
                "source_detail": url or SEIBRO_RELEASE_URL,
                "last_refresh_ts": today,
            })
    if not rows:
        try:
            text_blob = re.sub(r'\s+', ' ', lxml_html.fromstring(html).text_content()) if html else ''
        except Exception:
            text_blob = ''
        pattern = re.compile(r'(20\d{2}/\d{2}/\d{2})\s+([가-힣A-Za-z0-9()·.\-]+)\s+([0-9][0-9,]*)\s+([0-9][0-9,]*)\s+(코스닥시장|유가증권시장|코넥스|K-OTC|기타비상장)')
        for date_text, name, shares_text, remain_text, market_text in pattern.findall(text_blob):
            if not _is_probable_schedule_name(name):
                continue
            release_date = pd.to_datetime(date_text, errors='coerce')
            release_shares = safe_float(shares_text)
            remaining = safe_float(remain_text)
            if pd.isna(release_date) or release_shares is None:
                continue
            rows.append({
                "name": name,
                "name_key": normalize_name_key(name),
                "release_date": pd.Timestamp(release_date).normalize(),
                "release_shares": float(release_shares),
                "remaining_locked_shares": None if remaining is None else float(remaining),
                "market": _clean_market_value(market_text),
                "source": "Seibro",
                "source_detail": url or SEIBRO_RELEASE_URL,
                "last_refresh_ts": today,
            })
    if not rows:
        return pd.DataFrame(columns=["name", "name_key", "release_date", "release_shares", "remaining_locked_shares", "market", "source", "source_detail", "last_refresh_ts"])
    out = pd.DataFrame(rows)
    out = out.groupby(["name_key", "release_date", "market"], dropna=False, as_index=False).agg({
        "name": "first",
        "release_shares": "sum",
        "remaining_locked_shares": "sum",
        "source": "first",
        "source_detail": "first",
        "last_refresh_ts": "max",
    })
    return out.sort_values(["release_date", "name"], ascending=[True, True], na_position="last").reset_index(drop=True)


def fetch_seibro_release_schedule(timeout: int = 15) -> pd.DataFrame:
    response = _http_get(SEIBRO_RELEASE_URL, timeout=timeout)
    return parse_seibro_release_html(response.text, url=SEIBRO_RELEASE_URL)
_KIND_COMPANY_MATCHERS = {
    "name": ["회사명", "종목명", "기업명", "name"],
    "listing_date": ["상장일", "신규상장일", "listing_date"],
    "underwriters": ["주관사", "상장주선인", "지정자문인", "lead_manager"],
    "offer_price": ["공모가", "확정공모가", "수정공모가", "공모가격", "ipo_price"],
    "market": ["시장구분", "증권구분", "시장", "market"],
    "sector": ["업종", "sector"],
    "symbol": ["종목코드", "단축코드", "code", "symbol"],
    "post_listing_total_shares": ["상장주식수", "상장후총주식수", "listed_shares"],
}




def _normalize_kind_download_columns(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()
    work = df.copy().dropna(axis=1, how="all")
    work.columns = [str(c).replace("\xa0", " ").strip() or f"col_{i}" for i, c in enumerate(work.columns)]
    work.columns = _dedupe_labels(list(work.columns))

    header_tokens = ["회사명", "종목명", "기업명", "종목코드", "단축코드", "신규상장일", "상장일", "공모가", "확정공모가", "수정공모가", "시장구분", "주관사", "상장주선인", "최초상장주식수"]

    def score_labels(labels: Sequence[Any]) -> int:
        blob = " ".join(clean_column_label(label) for label in labels)
        return sum(token.lower().replace(" ", "") in blob for token in [clean_column_label(x) for x in header_tokens])

    if score_labels(work.columns) < 2:
        best_idx = None
        best_score = 0
        for i in range(min(len(work), 8)):
            row_labels = [str(v).replace("\xa0", " ").strip() for v in work.iloc[i].tolist()]
            score = score_labels(row_labels)
            if score > best_score:
                best_score = score
                best_idx = i
        if best_idx is not None and best_score >= 2:
            new_cols = [str(v).replace("\xa0", " ").strip() or f"col_{j}" for j, v in enumerate(work.iloc[best_idx].tolist())]
            work = work.iloc[best_idx + 1:].copy()
            work.columns = _dedupe_labels(new_cols)

    work = work.dropna(axis=1, how="all").reset_index(drop=True)
    return work


def _first_series_by_matchers(work: pd.DataFrame, matchers: Sequence) -> pd.Series | None:
    labels = [clean_column_label(c) for c in work.columns]
    chosen: pd.Series | None = None
    for matcher in matchers:
        cur: pd.Series | None = None
        for idx, (raw, norm) in enumerate(zip(work.columns, labels)):
            try:
                matched = matcher(str(raw), norm)
            except Exception:
                matched = False
            if not matched:
                continue
            series = work.iloc[:, idx]
            if isinstance(series, pd.DataFrame):
                series = series.iloc[:, 0]
            series = series.reset_index(drop=True)
            cur = series if cur is None else cur.combine_first(series)
        if cur is not None:
            chosen = cur if chosen is None else chosen.combine_first(cur)
    return chosen


def standardize_kind_corp_download_table(df: pd.DataFrame, *, source_url: str = "", today: pd.Timestamp | None = None) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=STANDARD_ISSUE_COLUMNS)
    today = today or today_kst()
    work = _normalize_kind_download_columns(df)
    if work.empty:
        return pd.DataFrame(columns=STANDARD_ISSUE_COLUMNS)

    def has_any(norm: str, keywords: Sequence[str]) -> bool:
        return any(clean_column_label(k) in norm for k in keywords)

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
    ipo_s = _first_series_by_matchers(work, [
        lambda raw, norm: (("공모가" in raw) or ("공모가" in norm)) and not any(x in raw for x in ["수정", "확정", "희망", "대비", "주가", "금액"]),
        lambda raw, norm: ("확정공모가" in raw) or ("확정공모가" in norm),
        lambda raw, norm: any(x in raw or x in norm for x in ["수정공모가", "(수정)공모가"]),
        lambda raw, norm: ("ipo_price" in norm) or ("ipoprice" in norm),
    ])
    market_s = _first_series_by_matchers(work, [
        lambda raw, norm: has_any(norm, ["시장구분"]) or (norm == clean_column_label("시장")),
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
    sector_s = _first_series_by_matchers(work, [
        lambda raw, norm: has_any(norm, ["업종", "industry"]),
    ])

    if company_s is None or listing_s is None:
        return pd.DataFrame(columns=STANDARD_ISSUE_COLUMNS)

    rows: list[dict[str, Any]] = []
    for idx in range(len(work)):
        name = company_s.iloc[idx] if idx < len(company_s) else None
        if is_missing(name) or looks_like_junk_issue_name(name):
            continue
        name_text = str(name).strip()
        if not name_text:
            continue
        listing_date = parse_date_text(listing_s.iloc[idx] if idx < len(listing_s) else None, default_year=today.year)
        if listing_date is None or pd.isna(listing_date):
            continue
        row = build_blank_issue_row()
        row.update({
            "ipo_id": f"KIND_CORP_{normalize_name_key(name_text)}",
            "name": name_text,
            "name_key": normalize_name_key(name_text),
            "market": _clean_market_value(market_s.iloc[idx] if market_s is not None and idx < len(market_s) else None) or pd.NA,
            "symbol": _clean_symbol_value(symbol_s.iloc[idx] if symbol_s is not None and idx < len(symbol_s) else None) or pd.NA,
            "sector": _clean_sector_value(sector_s.iloc[idx] if sector_s is not None and idx < len(sector_s) else None),
            "stage": infer_issue_stage(None, None, listing_date, today=today, fallback="상장예정"),
            "underwriters": _clean_underwriter_value(mgr_s.iloc[idx] if mgr_s is not None and idx < len(mgr_s) else None),
            "listing_date": listing_date,
            "offer_price": _normalize_price_text(ipo_s.iloc[idx] if ipo_s is not None and idx < len(ipo_s) else None),
            "post_listing_total_shares": _parse_share_count_text(shares_s.iloc[idx] if shares_s is not None and idx < len(shares_s) else None),
            "kind_url": source_url or KIND_CORP_DOWNLOAD_URLS[0],
            "source": "KIND-corpList",
            "source_detail": "corp-download",
            "last_refresh_ts": today,
        })
        rows.append(row)

    if not rows:
        return pd.DataFrame(columns=STANDARD_ISSUE_COLUMNS)
    out = standardize_issue_frame(pd.DataFrame(rows))
    out = out.sort_values(["listing_date", "name_key"]).drop_duplicates(subset=["name_key"], keep="last")
    return out.reset_index(drop=True)


def fetch_kind_corp_download_table(timeout: int = 15) -> pd.DataFrame:
    errors: list[str] = []
    today = today_kst()
    for url in KIND_CORP_DOWNLOAD_URLS:
        try:
            response = _http_get(url, timeout=timeout)
            tables = pd.read_html(StringIO(response.text), displayed_only=False)
            frames = [standardize_kind_corp_download_table(table, source_url=url, today=today) for table in tables]
            frames = [frame for frame in frames if frame is not None and not frame.empty]
            if not frames:
                errors.append(f"{url}: parsed table empty")
                continue
            out = clean_issue_frame(pd.concat(frames, ignore_index=True))
            if not out.empty:
                out = out.sort_values(["listing_date", "name_key"], na_position="last").drop_duplicates(subset=["name_key"], keep="last").reset_index(drop=True)
                return out
            errors.append(f"{url}: standardized rows empty")
        except Exception as exc:
            errors.append(f"{url}: {exc}")
    raise RuntimeError("KIND corp download fetch failed; " + " | ".join(errors[:6]))


def standardize_kind_listing_table(df: pd.DataFrame, *, source: str = "KIND", today: pd.Timestamp | None = None) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=STANDARD_ISSUE_COLUMNS)
    today = today or today_kst()
    work = _normalize_columns(df)

    mapped: dict[str, str] = {}
    for target, matchers in _KIND_COMPANY_MATCHERS.items():
        col = pick_first_present(work, matchers)
        if col:
            mapped[target] = col

    rows: list[dict[str, Any]] = []
    for _, record in work.iterrows():
        name = record.get(mapped.get("name", ""))
        if pd.isna(name) or str(name).strip() == "":
            continue
        listing_date = parse_date_text(record.get(mapped.get("listing_date", "")), default_year=today.year)
        offer_price = _normalize_price_text(record.get(mapped.get("offer_price", "")))
        stage = infer_issue_stage(None, None, listing_date, today=today, fallback="상장예정")
        row = build_blank_issue_row()
        row.update(
            {
                "ipo_id": f"KIND_{normalize_name_key(name)}",
                "name": str(name).strip(),
                "name_key": normalize_name_key(name),
                "market": _clean_market_value(_scalarize_cell(record.get(mapped.get("market", "")))) or pd.NA,
                "symbol": _clean_symbol_value(_scalarize_cell(record.get(mapped.get("symbol", ""))) or "") or pd.NA,
                "sector": _clean_sector_value(_scalarize_cell(record.get(mapped.get("sector", "")))),
                "stage": stage,
                "underwriters": _clean_underwriter_value(_scalarize_cell(record.get(mapped.get("underwriters", "")))),
                "listing_date": listing_date,
                "offer_price": offer_price,
                "post_listing_total_shares": _parse_share_count_text(_scalarize_cell(record.get(mapped.get("post_listing_total_shares", "")))),
                "kind_url": KIND_LISTING_URL,
                "ir_url": KIND_IR_ROOM_URL,
                "source": source,
                "source_detail": "live-listing-table" if source == "KIND" else "local-kind-export",
                "last_refresh_ts": today,
            }
        )
        rows.append(row)

    return standardize_issue_frame(pd.DataFrame(rows))


def standardize_kind_public_offering_table(df: pd.DataFrame, *, today: pd.Timestamp | None = None) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=STANDARD_ISSUE_COLUMNS)
    today = today or today_kst()
    work = _normalize_columns(df)

    name_col = pick_first_present(work, ["회사명", "종목명", "기업명"])
    filing_col = pick_first_present(work, ["신고서제출일", "증권신고서제출일"])
    forecast_col = pick_first_present(work, ["수요예측일정", "수요예측"])
    date_col = pick_first_present(work, ["청약일정", "공모일정"])
    listing_col = pick_first_present(work, ["상장예정일", "상장일"])
    broker_col = pick_first_present(work, ["상장주선인", "지정자문인", "주관사", "주간사"])
    offer_price_col = pick_first_present(work, ["확정공모가", "공모가"])
    market_col = pick_first_present(work, ["시장구분", "증권구분", "시장"])
    symbol_col = pick_first_present(work, ["종목코드", "단축코드", "code"])
    sector_col = pick_first_present(work, ["업종"])

    rows: list[dict[str, Any]] = []
    for _, record in work.iterrows():
        name = record.get(name_col or "")
        if pd.isna(name) or str(name).strip() == "":
            continue
        forecast_start, _ = parse_date_range_text(_scalarize_cell(record.get(forecast_col or "")), default_year=today.year)
        sub_start, sub_end = parse_date_range_text(_scalarize_cell(record.get(date_col or "")), default_year=today.year)
        listing_date = parse_date_text(_scalarize_cell(record.get(listing_col or "")), default_year=today.year) if listing_col else None
        filing_date = parse_date_text(_scalarize_cell(record.get(filing_col or "")), default_year=today.year) if filing_col else None
        row = build_blank_issue_row()
        row.update(
            {
                "ipo_id": f"KIND_OFFER_{normalize_name_key(name)}",
                "name": str(name).strip(),
                "name_key": normalize_name_key(name),
                "market": _clean_market_value(_scalarize_cell(record.get(market_col or ""))) or pd.NA,
                "symbol": _clean_symbol_value(_scalarize_cell(record.get(symbol_col or "")) or "") or pd.NA,
                "sector": _clean_sector_value(_scalarize_cell(record.get(sector_col or ""))),
                "stage": infer_issue_stage(sub_start, sub_end, listing_date, today=today, fallback="청약예정"),
                "underwriters": _clean_underwriter_value(_scalarize_cell(record.get(broker_col or ""))),
                "subscription_start": sub_start,
                "subscription_end": sub_end,
                "forecast_date": forecast_start,
                "dart_filing_date": filing_date,
                "listing_date": listing_date,
                "offer_price": _normalize_price_text(record.get(offer_price_col or "")),
                "kind_url": KIND_PUBLIC_OFFER_URL,
                "source": "KIND-공모기업",
                "source_detail": "public-offering-table",
                "last_refresh_ts": today,
            }
        )
        rows.append(row)

    return standardize_issue_frame(pd.DataFrame(rows))


def standardize_kind_pubprice_table(df: pd.DataFrame, *, today: pd.Timestamp | None = None) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=STANDARD_ISSUE_COLUMNS)
    today = today or today_kst()
    work = _normalize_columns(df)

    name_col = pick_first_present(work, ["회사명", "종목명", "기업명"])
    broker_col = pick_first_present(work, ["주관사", "상장주선인", "지정자문인"])
    listing_col = pick_first_present(work, ["상장일", "신규상장일"])
    market_col = pick_first_present(work, ["시장구분", "증권구분", "시장"])
    offer_price_col = pick_first_present(work, ["수정공모가", "(수정)공모가", "확정공모가", "공모가"])
    current_price_col = pick_first_present(work, ["최근거래일종가", "최근거래일 종가", "최근거래일주가", "최근종가", "현재가", "종가"])
    change_col = pick_first_present(work, ["최근거래일등락률", "최근거래일 등락률", "최근등락률", "등락률"])

    rows: list[dict[str, Any]] = []
    for _, record in work.iterrows():
        name = record.get(name_col or "")
        if pd.isna(name) or str(name).strip() == "":
            continue
        row = build_blank_issue_row()
        row.update(
            {
                "ipo_id": f"KIND_PUBPRICE_{normalize_name_key(name)}",
                "name": str(name).strip(),
                "name_key": normalize_name_key(name),
                "market": _clean_market_value(_scalarize_cell(record.get(market_col or ""))) or pd.NA,
                "stage": infer_issue_stage(None, None, parse_date_text(record.get(listing_col or ""), default_year=today.year), today=today, fallback="상장후"),
                "underwriters": _clean_underwriter_value(_scalarize_cell(record.get(broker_col or ""))),
                "listing_date": parse_date_text(record.get(listing_col or ""), default_year=today.year),
                "offer_price": _normalize_price_text(record.get(offer_price_col or "")),
                "current_price": _normalize_price_text(record.get(current_price_col or "")),
                "day_change_pct": _normalize_ratio_text(record.get(change_col or "")),
                "kind_url": KIND_PUB_PRICE_URL,
                "source": "KIND-공모가비교",
                "source_detail": "pubprice-compare-table",
                "last_refresh_ts": today,
            }
        )
        rows.append(row)

    return standardize_issue_frame(pd.DataFrame(rows))


def _flatten_pair_tables(html_text: str, *, allowed_labels: Iterable[str] | None = None) -> dict[str, str]:
    flat: dict[str, str] = {}
    allowed_keys = {_compact_label(label) for label in (allowed_labels or []) if _compact_label(label)}
    try:
        tables = pd.read_html(StringIO(html_text), displayed_only=False)
    except Exception:
        tables = []
    for table in tables:
        work = _normalize_columns(table).fillna("")
        if work.empty:
            continue
        for _, row in work.iterrows():
            cells = [_clean_text_value(x) or "" for x in row.tolist()]
            if len(cells) < 2:
                continue
            for i in range(0, len(cells) - 1, 2):
                raw_key = cells[i]
                raw_val = cells[i + 1]
                key = clean_column_label(raw_key)
                compact = _compact_label(raw_key)
                if not key or key.startswith("unnamed") or key in {"", "col", "내용", "항목"}:
                    continue
                if allowed_keys and compact not in allowed_keys:
                    continue
                if not raw_val or raw_val.lower() in {"nan", "none"} or _looks_like_menu_blob(raw_val):
                    continue
                flat.setdefault(key, raw_val)

        if len(work) == 1:
            row0 = work.iloc[0]
            for col in work.columns:
                raw_key = str(col)
                compact = _compact_label(raw_key)
                key = clean_column_label(raw_key)
                if not key or key.startswith("unnamed"):
                    continue
                if allowed_keys and compact not in allowed_keys:
                    continue
                raw_val = _clean_text_value(row0[col]) or ""
                if not raw_val or raw_val.lower() in {"nan", "none"} or _looks_like_menu_blob(raw_val):
                    continue
                flat.setdefault(key, raw_val)
    return flat


def _lookup_flat(flat: dict[str, str], *candidates: str) -> str | None:
    normalized_items = [(_compact_label(existing), value) for existing, value in flat.items()]
    for candidate in candidates:
        wanted = _compact_label(candidate)
        if not wanted:
            continue
        for existing, value in normalized_items:
            if not existing:
                continue
            exact = existing == wanted
            near = existing.startswith(wanted) or wanted.startswith(existing)
            if not exact and not near:
                continue
            text = _clean_text_value(value)
            if text and not _looks_like_menu_blob(text):
                return text
    return None


def parse_38_detail_html(html_text: str, *, url: str = "") -> dict[str, Any]:
    flat = _flatten_pair_tables(html_text, allowed_labels=_ALLOWED_38_DETAIL_LABELS)
    try:
        root = lxml_html.fromstring(html_text)
        text_blob = re.sub(r"\s+", " ", root.text_content())
    except Exception:
        text_blob = re.sub(r"\s+", " ", str(html_text or ""))

    subscription_text = _lookup_flat(flat, "공모청약일", "청약일정", "공모일정") or _extract_38_text_fallback(text_blob, r"공모청약일\s*[:：]?\s*([0-9./~\- ]{5,40})", r"청약일정\s*[:：]?\s*([0-9./~\- ]{5,40})")
    sub_start, sub_end = parse_date_range_text(subscription_text) if subscription_text else (None, None)
    forecast_text = _lookup_flat(flat, "수요예측일정", "기관수요예측일", "수요예측일", "예측일") or _extract_38_text_fallback(text_blob, r"(?:수요예측일정|기관수요예측일|수요예측일|예측일)\s*[:：]?\s*([0-9./~\- ]{5,40})")
    forecast_start, _ = parse_date_range_text(forecast_text) if forecast_text else (None, None)
    listing_text = _lookup_flat(flat, "신규상장일", "상장일", "상장예정일") or _extract_38_text_fallback(text_blob, r"(?:신규상장일|상장예정일|상장일)\s*[:：]?\s*([0-9./-]{6,20})")
    band_text = _lookup_flat(flat, "희망공모가액", "희망공모가", "공모희망가") or _extract_38_text_fallback(text_blob, r"희망공모가(?:액)?\s*[:：]?\s*([0-9,]+\s*[~〜-]\s*[0-9,]+)")
    current_text = _lookup_flat(flat, "현재가", "주가") or _extract_38_text_fallback(text_blob, r"현재가\s*[:：]?\s*([0-9,]+)")
    total_offer_text = _lookup_flat(flat, "총공모주식수", "총공모주식") or _extract_38_text_fallback(text_blob, r"(?:총공모주식수|공모주식수)\s*[:：]?\s*([0-9,]+)\s*주")
    offer_structure_text = _lookup_flat(flat, "상장공모") or _extract_38_text_fallback(text_blob, r"상장공모\s*[:：]?\s*([^\n]{1,140})")
    new_shares_text = _lookup_flat(flat, "신주모집") or _extract_38_text_fallback(text_blob, r"신주모집\s*[:：]?\s*([0-9,]+)\s*주")
    selling_shares_text = _lookup_flat(flat, "구주매출") or _extract_38_text_fallback(text_blob, r"구주매출\s*[:：]?\s*([0-9,]+)\s*주")
    post_listing_text = _lookup_flat(flat, "상장후총주식수", "상장후주식수", "상장주식수") or _extract_38_text_fallback(text_blob, r"(?:상장후총주식수|상장후주식수|상장주식수)\s*[:：]?\s*([0-9,]+)\s*주")

    market = _clean_market_value(_lookup_flat(flat, "시장구분", "시장"))
    if market is None:
        market = _clean_market_value(_extract_38_text_fallback(text_blob, r"(코스닥)\s*상장", r"(유가증권)\s*상장", r"(코넥스)\s*상장", r"시장구분\s*[:：]?\s*((?:코스닥|유가증권|코넥스|거래소))"))
    total_offer_shares = _parse_share_count_text(total_offer_text)
    structure_counts = _extract_offer_structure_counts(offer_structure_text)
    new_shares = structure_counts.get("new_shares") or _parse_share_count_text(new_shares_text)
    selling_shares = structure_counts.get("selling_shares") or _parse_share_count_text(selling_shares_text)
    if total_offer_shares is not None and new_shares is None and selling_shares is not None:
        new_shares = max(float(total_offer_shares) - float(selling_shares), 0.0)
    if total_offer_shares is not None and selling_shares is None and new_shares is not None:
        selling_shares = max(float(total_offer_shares) - float(new_shares), 0.0)
    secondary_sale_ratio = None
    if total_offer_shares and selling_shares is not None and float(total_offer_shares) > 0:
        secondary_sale_ratio = round(float(selling_shares) / float(total_offer_shares) * 100, 4)

    name = _clean_text_value(_lookup_flat(flat, "종목명", "회사명", "기업명"))
    if not name:
        title_match = re.search(r"([가-힣A-Za-z0-9()·.\-]+)\s*(?:기업개요|IPO공모|소액주주토론방|상장예비심사)", text_blob)
        if title_match:
            name = _clean_text_value(title_match.group(1))
    sector = _clean_sector_value(_lookup_flat(flat, "업종"))
    if sector is None:
        sector = _clean_sector_value(_extract_38_text_fallback(text_blob, r"업종\s*[:：]?\s*([^,;|]{2,80})"))

    result = {
        "name": name,
        "market": market,
        "symbol": _clean_symbol_value(_lookup_flat(flat, "종목코드", "단축코드")) or _clean_symbol_value(_extract_38_text_fallback(text_blob, r"(?:종목코드|단축코드)\s*[:：]?\s*([0-9A-Z]{4,6})(?![A-Za-z0-9])")),
        "sector": sector,
        "underwriters": _clean_underwriter_value(_lookup_flat(flat, "주간사", "주관사")) or _clean_underwriter_value(_extract_38_text_fallback(text_blob, r"(?:주간사|주관사)\s*[:：]?\s*([가-힣A-Za-z0-9,&·\s]+(?:증권|투자증권)[가-힣A-Za-z0-9,&·\s]*)")),
        "subscription_start": sub_start,
        "subscription_end": sub_end,
        "listing_date": parse_date_text(listing_text) if listing_text else None,
        "offer_price": _normalize_price_text(_lookup_flat(flat, "확정공모가", "공모가")) or _normalize_price_text(_extract_38_text_fallback(text_blob, r"확정공모가\s*[:：]?\s*([0-9,]+)")),
        "price_band_low": _normalize_band_pair(band_text)[0] if band_text else None,
        "price_band_high": _normalize_band_pair(band_text)[1] if band_text else None,
        "retail_competition_ratio_live": _normalize_ratio_text(_lookup_flat(flat, "청약경쟁률", "일반청약경쟁률")) or _normalize_ratio_text(_extract_38_text_fallback(text_blob, r"(?:청약경쟁률|일반청약경쟁률)\s*[:：]?\s*([0-9,.:]+)")),
        "institutional_competition_ratio": _normalize_ratio_text(_lookup_flat(flat, "기관경쟁률", "수요예측경쟁률")) or _normalize_ratio_text(_extract_38_text_fallback(text_blob, r"(?:기관경쟁률|수요예측경쟁률)\s*[:：]?\s*([0-9,.:]+)")),
        "forecast_date": forecast_start,
        "lockup_commitment_ratio": _normalize_ratio_text(_lookup_flat(flat, "의무보유확약", "확약")) or _normalize_ratio_text(_extract_38_text_fallback(text_blob, r"(?:의무보유확약|확약)\s*[:：]?\s*([0-9.,]+%)")),
        "current_price": _normalize_price_text(current_text),
        "total_offer_shares": total_offer_shares,
        "new_shares": new_shares,
        "selling_shares": selling_shares,
        "secondary_sale_ratio": secondary_sale_ratio,
        "post_listing_total_shares": _parse_share_count_text(post_listing_text),
        "source_detail": f"38-detail:{url}" if url else "38-detail",
    }
    return {k: v for k, v in result.items() if not is_missing(v)}


def enrich_38_schedule_with_details(
    df: pd.DataFrame,
    *,
    timeout: int = 10,
    max_rows: int = 80,
    today: pd.Timestamp | None = None,
) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=STANDARD_ISSUE_COLUMNS)
    if "detail_url" not in df.columns:
        return clean_issue_frame(df)

    today = today or today_kst()
    work = df.copy()
    cache: dict[str, dict[str, Any]] = {}
    enriched_rows: list[dict[str, Any]] = []
    target_cols = [
        "market",
        "symbol",
        "sector",
        "listing_date",
        "institutional_competition_ratio",
        "forecast_date",
        "lockup_commitment_ratio",
        "current_price",
        "total_offer_shares",
        "new_shares",
        "selling_shares",
        "secondary_sale_ratio",
        "post_listing_total_shares",
    ]

    for i, (_, row) in enumerate(work.iterrows()):
        current = row.to_dict()
        detail_url = str(current.get("detail_url") or "").strip()
        need_detail = any(is_missing(current.get(col)) for col in target_cols)
        if detail_url and need_detail and i < max_rows:
            if detail_url not in cache:
                try:
                    html_text = _http_get(detail_url, timeout=timeout).text
                    cache[detail_url] = parse_38_detail_html(html_text, url=detail_url)
                except Exception:
                    cache[detail_url] = {}
            detail = cache.get(detail_url, {})
            if detail and _row_name_matches(detail, current.get("name")):
                for col in STANDARD_ISSUE_COLUMNS:
                    if col == "name":
                        continue
                    current[col] = coalesce(detail.get(col), current.get(col))
                current["stage"] = infer_issue_stage(
                    current.get("subscription_start"),
                    current.get("subscription_end"),
                    current.get("listing_date"),
                    today=today,
                    fallback=current.get("stage"),
                )
                current["source"] = coalesce(current.get("source"), "38")
                current["source_detail"] = coalesce(detail.get("source_detail"), current.get("source_detail"))
        enriched_rows.append(current)
    enriched = pd.DataFrame(enriched_rows)
    return clean_issue_frame(enriched.drop(columns=["detail_url"], errors="ignore"))


def standardize_38_schedule_table(
    df: pd.DataFrame,
    *,
    today: pd.Timestamp | None = None,
    fetch_details: bool = False,
    detail_timeout: int = 10,
    detail_max_rows: int = 80,
) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=STANDARD_ISSUE_COLUMNS)
    today = today or today_kst()
    work = _normalize_columns(df)

    name_col = pick_first_present(work, ["기업명", "종목명", "회사명"])
    date_col = pick_first_present(work, ["공모일정", "청약일정", "공모주일정", "일정"])
    listing_col = pick_first_present(work, ["상장일", "상장예정일", "신규상장일"])
    broker_col = pick_first_present(work, ["주간사", "주관사"])
    retail_ratio_col = pick_first_present(work, ["청약경쟁률", "일반청약경쟁률", "경쟁률"])
    inst_ratio_col = pick_first_present(work, ["기관경쟁률", "수요예측경쟁률"])
    price_col, band_col = _resolve_38_price_columns(work)
    detail_col = pick_first_present(work, ["detail_url"])

    rows: list[dict[str, Any]] = []
    for _, record in work.iterrows():
        name = record.get(name_col or "")
        if not _is_probable_schedule_name(name):
            continue
        sub_start, sub_end = parse_date_range_text(_scalarize_cell(record.get(date_col or "")), default_year=today.year)
        listing_date = parse_date_text(_scalarize_cell(record.get(listing_col or "")), default_year=today.year) if listing_col else None
        price_low, price_high = _normalize_band_pair(record.get(band_col or ""))
        offer_price = _normalize_price_text(record.get(price_col or ""))
        underwriters = _clean_underwriter_value(_scalarize_cell(record.get(broker_col or "")))
        has_core_signal = any(value is not None and not pd.isna(value) for value in [sub_start, sub_end, listing_date, price_low, price_high, offer_price, underwriters])
        if not has_core_signal:
            continue
        row = build_blank_issue_row()
        row.update(
            {
                "ipo_id": f"38_{normalize_name_key(name)}",
                "name": str(name).strip(),
                "name_key": normalize_name_key(name),
                "stage": infer_issue_stage(sub_start, sub_end, listing_date, today=today, fallback="청약예정"),
                "underwriters": underwriters,
                "subscription_start": sub_start,
                "subscription_end": sub_end,
                "listing_date": None if pd.isna(listing_date) else listing_date,
                "price_band_low": price_low,
                "price_band_high": price_high,
                "offer_price": offer_price,
                "retail_competition_ratio_live": _normalize_ratio_text(record.get(retail_ratio_col or "")),
                "institutional_competition_ratio": _normalize_ratio_text(record.get(inst_ratio_col or "")),
                "source": "38",
                "source_detail": "ipo-schedule",
                "last_refresh_ts": today,
                "detail_url": str(_scalarize_cell(record.get(detail_col or "")) or "").strip() or pd.NA,
            }
        )
        rows.append(row)

    out = pd.DataFrame(rows)
    if out.empty:
        return pd.DataFrame(columns=STANDARD_ISSUE_COLUMNS)
    if fetch_details:
        return enrich_38_schedule_with_details(out, timeout=detail_timeout, max_rows=detail_max_rows, today=today)
    return clean_issue_frame(out)


def load_kind_export_from_path(path: str | Path, *, today: pd.Timestamp | None = None) -> pd.DataFrame:
    if not path:
        return pd.DataFrame(columns=STANDARD_ISSUE_COLUMNS)
    frames = read_tabular_file(path)
    return standardize_kind_local_export_frames(frames, source="local-kind", today=today)


def load_kind_export_from_bytes(payload: bytes, filename: str, *, today: pd.Timestamp | None = None) -> pd.DataFrame:
    frames = read_tabular_file(payload, filename=filename)
    return standardize_kind_local_export_frames(frames, source="local-kind", today=today)


def standardize_kind_local_export_frames(
    frames: list[pd.DataFrame],
    *,
    source: str = "local-kind",
    today: pd.Timestamp | None = None,
) -> pd.DataFrame:
    today = today or today_kst()
    if not frames:
        return pd.DataFrame(columns=STANDARD_ISSUE_COLUMNS)

    normalized_frames: list[pd.DataFrame] = []
    for frame in frames:
        if frame is None or frame.empty:
            continue
        work = _normalize_columns(frame)
        labels = [clean_column_label(c) for c in work.columns]
        joined = " ".join(labels)
        if any(token in joined for token in [clean_column_label("최근거래일 종가"), clean_column_label("등락률"), clean_column_label("최근종가")]):
            parsed = standardize_kind_pubprice_table(work, today=today)
        elif any(token in joined for token in [clean_column_label("청약일정"), clean_column_label("상장예정일")]):
            parsed = standardize_kind_public_offering_table(work, today=today)
        else:
            parsed = standardize_kind_listing_table(work, source=source, today=today)
        if parsed is not None and not parsed.empty:
            normalized_frames.append(parsed)

    if not normalized_frames:
        return pd.DataFrame(columns=STANDARD_ISSUE_COLUMNS)

    merged = normalized_frames[0]
    for frame in normalized_frames[1:]:
        merged = _overlay_issue_frames(merged, frame)
    return clean_issue_frame(merged)


def _overlay_issue_frames(base: pd.DataFrame, updates: pd.DataFrame) -> pd.DataFrame:
    if base.empty:
        return standardize_issue_frame(updates)
    if updates.empty:
        return standardize_issue_frame(base)
    base = standardize_issue_frame(base)
    updates = standardize_issue_frame(updates)
    base["name_key"] = base["name_key"].map(normalize_name_key)
    updates["name_key"] = updates["name_key"].map(normalize_name_key)
    update_map = updates.set_index("name_key")

    rows: list[dict[str, Any]] = []
    seen: set[str] = set()
    for _, row in base.iterrows():
        key = row["name_key"]
        current = row.to_dict()
        if key in update_map.index:
            upd = update_map.loc[key]
            if isinstance(upd, pd.DataFrame):
                upd = upd.iloc[0]
            for col in STANDARD_ISSUE_COLUMNS:
                current[col] = coalesce(upd.get(col), current.get(col))
            current["stage"] = infer_issue_stage(
                current.get("subscription_start"),
                current.get("subscription_end"),
                current.get("listing_date"),
                fallback=current.get("stage"),
            )
        rows.append(current)
        seen.add(key)

    new_rows = updates[~updates["name_key"].isin(seen)]
    if not new_rows.empty:
        rows.extend(new_rows.to_dict(orient="records"))
    return standardize_issue_frame(pd.DataFrame(rows))


def merge_live_sources(
    kind_df: pd.DataFrame,
    schedule_df: pd.DataFrame,
    *,
    kind_public_df: pd.DataFrame | None = None,
    kind_pubprice_df: pd.DataFrame | None = None,
    kind_corp_df: pd.DataFrame | None = None,
) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for frame in [kind_corp_df, kind_df, kind_public_df, schedule_df, kind_pubprice_df]:
        if frame is not None and not frame.empty:
            frames.append(standardize_issue_frame(frame))
    if not frames:
        return pd.DataFrame(columns=STANDARD_ISSUE_COLUMNS)

    merged = frames[0]
    for frame in frames[1:]:
        merged = _overlay_issue_frames(merged, frame)
    if "source" in merged.columns:
        merged["source"] = merged["source"].fillna("merged-live")
    if "source_detail" in merged.columns:
        merged["source_detail"] = merged["source_detail"].fillna("multi-source")
    return clean_issue_frame(merged)


def find_local_kind_export(default_root: Path | None = None) -> Path | None:
    roots = [
        default_root or Path.cwd(),
        Path.cwd().parent,
        Path.home() / "Downloads",
        Path.home() / "Desktop",
    ]
    patterns = ["*신규상장*.xlsx", "*신규상장*.xls", "*신규상장*.csv", "*공모가대비주가*.xlsx", "*공모가대비주가*.csv"]
    for root in roots:
        if root is None or not Path(root).exists():
            continue
        root = Path(root)
        for pattern in patterns:
            matches = sorted(root.glob(pattern), key=lambda p: p.stat().st_mtime, reverse=True)
            if matches:
                return matches[0]
    return None
