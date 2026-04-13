from __future__ import annotations

import os
import urllib.parse
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd
import requests

from src.utils import ensure_dir, normalize_name_key, normalize_symbol_text, parse_date_columns, safe_float, today_kst

KSD_STOCK_BASE_URL = "http://api.seibro.or.kr/openapi/service/StockSvc"
KSD_CORP_BASE_URL = "http://api.seibro.or.kr/openapi/service/CorpSvc"


MARKET_CODE_TO_LABEL = {
    "11": "유가증권",
    "12": "코스닥",
    "13": "K-OTC",
    "14": "코넥스",
    "50": "기타",
    "110": "유가증권",
    "120": "코스닥",
    "130": "K-OTC",
    "140": "코넥스",
    "211": "예탁지정",
    "212": "등록지정",
    "221": "예탁지정취소",
    "222": "등록지정취소",
    "230": "보호예수",
    "241": "권리",
    "242": "등록적격",
    "250": "KSM",
    "910": "상장폐지",
    "990": "기타",
}


SHAREHOLDER_GROUP_PATTERNS = {
    "major": ["최대주주", "대주주", "특수관계"],
    "retail": ["개인", "소액주주"],
    "foreign": ["외국인"],
    "employee": ["우리사주", "임직원"],
    "institution": ["기관", "투자신탁", "은행", "보험", "연기금", "증권회사", "금융"],
    "corporate": ["법인", "정부관리회사", "정부"],
}


@dataclass(slots=True)
class XMLResult:
    ok: bool
    code: str
    message: str
    items: list[dict[str, Any]]
    url: str
    params: dict[str, Any]


class PublicDataAPIError(RuntimeError):
    pass


class KSDPublicDataClient:
    def __init__(self, service_key: str, *, timeout: int = 12, cache_dir: str | Path | None = None) -> None:
        decoded = urllib.parse.unquote(str(service_key or "").strip())
        self.service_key = decoded or str(service_key or "").strip()
        self.timeout = timeout
        self.cache_dir = ensure_dir(cache_dir or Path.cwd() / "data" / "cache")
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36",
                "Accept": "application/xml,text/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
            }
        )

    @classmethod
    def from_env(cls, *, cache_dir: str | Path | None = None, timeout: int = 12) -> "KSDPublicDataClient | None":
        candidates = [
            os.getenv("KSD_PUBLIC_DATA_SERVICE_KEY", "").strip(),
            os.getenv("PUBLIC_DATA_SERVICE_KEY", "").strip(),
            os.getenv("DATA_GO_SERVICE_KEY", "").strip(),
            os.getenv("SEIBRO_SERVICE_KEY", "").strip(),
        ]
        key = next((value for value in candidates if value), "")
        if not key:
            return None
        return cls(key, timeout=timeout, cache_dir=cache_dir)

    def _request_xml(self, base_url: str, endpoint: str, params: dict[str, Any] | None = None) -> XMLResult:
        query = {k: v for k, v in (params or {}).items() if v not in {None, ""}}
        query.setdefault("numOfRows", 9999)
        query.setdefault("pageNo", 1)
        attempts = [
            {"ServiceKey": self.service_key, **query},
            {"serviceKey": self.service_key, **query},
        ]
        base_urls = [base_url]
        if str(base_url).startswith("http://"):
            base_urls.append("https://" + str(base_url)[len("http://"):])
        last_result: XMLResult | None = None
        errors: list[str] = []
        for resolved_base in base_urls:
            for candidate in attempts:
                url = f"{resolved_base.rstrip('/')}/{endpoint.lstrip('/')}"
                try:
                    response = self.session.get(url, params=candidate, timeout=self.timeout)
                    response.raise_for_status()
                except Exception as exc:
                    errors.append(f"{url}: {exc}")
                    continue
                try:
                    parsed = self._parse_xml(response.content, url=response.url, params=candidate)
                except Exception as exc:
                    errors.append(f"{url}: parse failed: {exc}")
                    continue
                last_result = parsed
                if parsed.ok and parsed.items:
                    return parsed
                # 공공데이터 에러 코드나 키 미등록 메시지가 있으면 즉시 중단한다.
                message = f"{parsed.code} {parsed.message}".strip().lower()
                if any(token in message for token in ["service key", "not registered", "invalid", "error"]) and not parsed.items:
                    return parsed
        if last_result is not None:
            return last_result
        raise PublicDataAPIError("; ".join(errors) or f"public data request failed: {base_url}/{endpoint}")

    def _parse_xml(self, payload: bytes, *, url: str, params: dict[str, Any]) -> XMLResult:
        try:
            root = ET.fromstring(payload)
        except Exception as exc:
            raise PublicDataAPIError(f"XML parse failed: {exc}") from exc
        code = self._find_text(root, ["resultCode"]) or ""
        message = self._find_text(root, ["resultMsg"]) or ""
        items_parent = root.find(".//items")
        items: list[dict[str, Any]] = []
        if items_parent is not None:
            for item in items_parent.findall("item"):
                items.append(self._element_to_dict(item))
        else:
            body_items = root.findall(".//body/item")
            for item in body_items:
                items.append(self._element_to_dict(item))
        ok = (code in {"", "00"}) and (not message or "service" in message.lower() or "ok" in message.lower() or "normal" in message.lower())
        return XMLResult(ok=ok, code=code, message=message, items=items, url=url, params=params)

    @staticmethod
    def _element_to_dict(element: ET.Element) -> dict[str, Any]:
        row: dict[str, Any] = {}
        for child in list(element):
            key = child.tag
            value = (child.text or "").strip()
            row[key] = value if value != "" else pd.NA
        return row

    @staticmethod
    def _find_text(root: ET.Element, tags: list[str]) -> str | None:
        for tag in tags:
            element = root.find(f".//{tag}")
            if element is not None and element.text:
                text = element.text.strip()
                if text:
                    return text
        return None

    # ---------- StockSvc ----------

    def lookup_stock_by_name(self, name: str, *, num_rows: int = 50) -> pd.DataFrame:
        target = str(name or "").strip()
        if not target:
            return self._empty_name_lookup()
        result = self._request_xml(KSD_STOCK_BASE_URL, "getStkIsinByNmN1", {"secnNm": target, "numOfRows": num_rows, "pageNo": 1})
        rows: list[dict[str, Any]] = []
        for item in result.items:
            kor_name = str(item.get("korSecnNm") or "").strip()
            if not kor_name:
                continue
            symbol = normalize_symbol_text(item.get("shotnIsin"))
            isin = str(item.get("isin") or "").strip() or pd.NA
            issuco = str(item.get("issucoCustno") or "").strip() or pd.NA
            name_key = normalize_name_key(kor_name)
            row = {
                "query_name": target,
                "query_name_key": normalize_name_key(target),
                "name": kor_name,
                "name_key": name_key,
                "symbol": symbol,
                "isin": isin,
                "issuco_custno": issuco,
                "share_type": str(item.get("secnKacdNm") or "").strip() or pd.NA,
                "issue_date": self._parse_date(item.get("issuDt")),
                "source": "KSD-종목명조회",
                "source_detail": result.url,
                "last_refresh_ts": today_kst(),
            }
            row["_exact_name"] = 1 if normalize_name_key(target) == name_key else 0
            row["_common_share"] = 1 if str(row.get("share_type") or "").startswith("보통") else 0
            rows.append(row)
        if not rows:
            return self._empty_name_lookup()
        out = pd.DataFrame(rows)
        out = out.sort_values(["_exact_name", "_common_share", "name", "symbol"], ascending=[False, False, True, True], na_position="last")
        return out.drop(columns=["_exact_name", "_common_share"], errors="ignore").reset_index(drop=True)

    def lookup_stock_by_short_code(self, short_code: str) -> pd.DataFrame:
        symbol = normalize_symbol_text(short_code)
        if not symbol:
            return self._empty_name_lookup()
        result = self._request_xml(KSD_STOCK_BASE_URL, "getStkIsinByShortIsinN1", {"shortIsin": symbol, "numOfRows": 5, "pageNo": 1})
        rows: list[dict[str, Any]] = []
        for item in result.items:
            rows.append(
                {
                    "query_name": pd.NA,
                    "query_name_key": pd.NA,
                    "name": str(item.get("korSecnNm") or "").strip() or pd.NA,
                    "name_key": normalize_name_key(item.get("korSecnNm")),
                    "symbol": normalize_symbol_text(item.get("shotnIsin") or symbol),
                    "isin": str(item.get("isin") or "").strip() or pd.NA,
                    "issuco_custno": str(item.get("issucoCustno") or "").strip() or pd.NA,
                    "share_type": str(item.get("secnKacdNm") or "").strip() or pd.NA,
                    "issue_date": self._parse_date(item.get("issuDt")),
                    "source": "KSD-단축코드조회",
                    "source_detail": result.url,
                    "last_refresh_ts": today_kst(),
                }
            )
        return pd.DataFrame(rows) if rows else self._empty_name_lookup()

    def list_market_short_codes(self, market_code: str) -> pd.DataFrame:
        code = str(market_code or "").strip()
        if not code:
            return self._empty_market_codes()
        # 문서와 실제 파라미터명이 혼재되어 있어 둘 다 시도한다.
        result = self._request_xml(KSD_STOCK_BASE_URL, "getShotnByMartN1", {"martTpcd": code, "numOfRows": 9999, "pageNo": 1})
        if not result.items:
            result = self._request_xml(KSD_STOCK_BASE_URL, "getShotnByMartN1", {"mart_tpcd": code, "numOfRows": 9999, "pageNo": 1})
        rows: list[dict[str, Any]] = []
        for item in result.items:
            symbol = normalize_symbol_text(item.get("shotnIsin") or item.get("shortIsin"))
            name = str(item.get("korSecnNm") or item.get("secnNm") or "").strip()
            if not symbol and not name:
                continue
            rows.append(
                {
                    "market_code": code,
                    "market": MARKET_CODE_TO_LABEL.get(code, code),
                    "name": name or pd.NA,
                    "name_key": normalize_name_key(name),
                    "symbol": symbol,
                    "isin": str(item.get("isin") or "").strip() or pd.NA,
                    "source": "KSD-시장단축코드",
                    "source_detail": result.url,
                    "last_refresh_ts": today_kst(),
                }
            )
        if not rows:
            return self._empty_market_codes()
        out = pd.DataFrame(rows).drop_duplicates(subset=[c for c in ["market_code", "symbol", "name_key"] if c in rows[0]], keep="first")
        return out.reset_index(drop=True)

    def get_listing_info(self, isin: str) -> pd.DataFrame:
        isin_value = str(isin or "").strip()
        if not isin_value:
            return self._empty_listing_info()
        result = self._request_xml(KSD_STOCK_BASE_URL, "getStkListInfoN1", {"isin": isin_value, "numOfRows": 5, "pageNo": 1})
        rows: list[dict[str, Any]] = []
        for item in result.items:
            list_code = str(item.get("listTpcd") or "").strip()
            delisting = self._parse_date(item.get("dlistDt"))
            if delisting is not None and str(item.get("dlistDt")) in {"99991231", "9999-12-31", "9999/12/31"}:
                delisting = pd.NaT
            rows.append(
                {
                    "isin": isin_value,
                    "name": str(item.get("korSecnNm") or "").strip() or pd.NA,
                    "name_key": normalize_name_key(item.get("korSecnNm")),
                    "listing_date": self._parse_date(item.get("apliDt")),
                    "delisting_date": delisting,
                    "listing_status_code": list_code or pd.NA,
                    "listing_status": MARKET_CODE_TO_LABEL.get(list_code, list_code or pd.NA),
                    "market": MARKET_CODE_TO_LABEL.get(list_code, pd.NA),
                    "expiry_date": self._parse_date(item.get("xpitDt")),
                    "source": "KSD-상장정보",
                    "source_detail": result.url,
                    "last_refresh_ts": today_kst(),
                }
            )
        return parse_date_columns(pd.DataFrame(rows), ["listing_date", "delisting_date", "expiry_date", "last_refresh_ts"]) if rows else self._empty_listing_info()

    # ---------- CorpSvc ----------

    def lookup_corp_by_name(self, name: str, *, num_rows: int = 200) -> pd.DataFrame:
        target = str(name or "").strip()
        if not target:
            return self._empty_corp_lookup()
        result = self._request_xml(KSD_CORP_BASE_URL, "getIssucoCustnoByNm", {"issucoNm": target, "numOfRows": num_rows, "pageNo": 1})
        rows: list[dict[str, Any]] = []
        for item in result.items:
            corp_name = str(item.get("issucoNm") or "").strip()
            if not corp_name:
                continue
            rows.append(
                {
                    "query_name": target,
                    "query_name_key": normalize_name_key(target),
                    "name": corp_name,
                    "name_key": normalize_name_key(corp_name),
                    "issuco_custno": str(item.get("issucoCustno") or "").strip() or pd.NA,
                    "bizno": str(item.get("bizno") or "").strip() or pd.NA,
                    "source": "KSD-회사번호조회",
                    "source_detail": result.url,
                    "last_refresh_ts": today_kst(),
                }
            )
        if not rows:
            return self._empty_corp_lookup()
        out = pd.DataFrame(rows)
        out["_exact_name"] = out["name_key"].eq(normalize_name_key(target)).astype(int)
        out = out.sort_values(["_exact_name", "name"], ascending=[False, True])
        return out.drop(columns=["_exact_name"], errors="ignore").reset_index(drop=True)

    def get_corp_basic_info(self, issuco_custno: str) -> pd.DataFrame:
        code = str(issuco_custno or "").strip()
        if not code:
            return self._empty_corp_basic()
        result = self._request_xml(KSD_CORP_BASE_URL, "getIssucoBasicInfo", {"issucoCustno": code})
        rows: list[dict[str, Any]] = []
        for item in result.items:
            total_shares = self._parse_number(item.get("totalStkCnt"))
            rows.append(
                {
                    "issuco_custno": code,
                    "name": str(item.get("issucoNm") or item.get("korSecnNm") or "").strip() or pd.NA,
                    "name_key": normalize_name_key(item.get("issucoNm") or item.get("korSecnNm")),
                    "listing_date": self._parse_date(item.get("apliDt") or item.get("apliDtY")),
                    "homep_url": str(item.get("homepAddr") or "").strip() or pd.NA,
                    "ceo_name": str(item.get("ceoNm") or "").strip() or pd.NA,
                    "bizno": str(item.get("bizno") or "").strip() or pd.NA,
                    "face_value": self._parse_number(item.get("pval")),
                    "post_listing_total_shares": total_shares,
                    "source": "KSD-기업개요",
                    "source_detail": result.url,
                    "last_refresh_ts": today_kst(),
                }
            )
        return parse_date_columns(pd.DataFrame(rows), ["listing_date", "last_refresh_ts"]) if rows else self._empty_corp_basic()

    def get_distribution_dates(self, issuco_custno: str) -> pd.DataFrame:
        code = str(issuco_custno or "").strip()
        if not code:
            return self._empty_distribution_dates()
        result = self._request_xml(KSD_CORP_BASE_URL, "getStkDistributionRgtStdDt", {"issucoCustno": code, "numOfRows": 500, "pageNo": 1})
        rows: list[dict[str, Any]] = []
        for item in result.items:
            rows.append(
                {
                    "issuco_custno": code,
                    "distribution_date": self._parse_date(item.get("rgtStdDt")),
                    "source": "KSD-분포기준일",
                    "source_detail": result.url,
                    "last_refresh_ts": today_kst(),
                }
            )
        return parse_date_columns(pd.DataFrame(rows), ["distribution_date", "last_refresh_ts"]).dropna(subset=["distribution_date"]).drop_duplicates() if rows else self._empty_distribution_dates()

    def get_shareholder_distribution(self, issuco_custno: str, distribution_date: str | pd.Timestamp) -> pd.DataFrame:
        code = str(issuco_custno or "").strip()
        if not code:
            return self._empty_shareholder_distribution()
        if isinstance(distribution_date, pd.Timestamp):
            date_text = distribution_date.strftime("%Y%m%d")
        else:
            ts = self._parse_date(distribution_date)
            date_text = ts.strftime("%Y%m%d") if pd.notna(ts) else str(distribution_date or "").strip()
        if not date_text:
            return self._empty_shareholder_distribution()
        result = self._request_xml(KSD_CORP_BASE_URL, "getStkDistributionShareholderStatus", {"issucoCustno": code, "rgtStdDt": date_text, "numOfRows": 200, "pageNo": 1})
        rows: list[dict[str, Any]] = []
        for item in result.items:
            group_name = str(item.get("stkDistbutTpnm") or "").strip()
            rows.append(
                {
                    "issuco_custno": code,
                    "distribution_date": self._parse_date(item.get("rgtStdDt") or date_text),
                    "shareholder_group": group_name or pd.NA,
                    "shareholder_count": self._parse_number(item.get("shrs")),
                    "shareholder_count_ratio": self._parse_number(item.get("shrsRatio")),
                    "share_count": self._parse_number(item.get("stkqty")),
                    "share_count_ratio": self._parse_number(item.get("stkqtyRatio")),
                    "source": "KSD-주주분포",
                    "source_detail": result.url,
                    "last_refresh_ts": today_kst(),
                }
            )
        return parse_date_columns(pd.DataFrame(rows), ["distribution_date", "last_refresh_ts"]) if rows else self._empty_shareholder_distribution()

    def summarize_shareholder_distribution(self, distribution_df: pd.DataFrame) -> pd.DataFrame:
        if distribution_df is None or distribution_df.empty:
            return self._empty_shareholder_summary()
        work = distribution_df.copy()
        work["shareholder_group"] = work.get("shareholder_group", pd.Series(dtype="object")).astype(str).str.strip()
        work["share_count_ratio"] = pd.to_numeric(work.get("share_count_ratio"), errors="coerce")
        work["shareholder_count_ratio"] = pd.to_numeric(work.get("shareholder_count_ratio"), errors="coerce")
        groups = {
            label: self._sum_matching_ratio(work, patterns)
            for label, patterns in SHAREHOLDER_GROUP_PATTERNS.items()
        }
        base = work.sort_values([c for c in ["distribution_date", "share_count_ratio"] if c in work.columns], ascending=[False, False], na_position="last").iloc[0].to_dict()
        row = {
            "issuco_custno": base.get("issuco_custno"),
            "distribution_date": base.get("distribution_date"),
            "major_shareholder_ratio": groups.get("major"),
            "retail_shareholder_ratio": groups.get("retail"),
            "foreign_shareholder_ratio": groups.get("foreign"),
            "institution_shareholder_ratio": groups.get("institution"),
            "corporate_shareholder_ratio": groups.get("corporate"),
            "employee_shareholder_ratio": groups.get("employee"),
            "shareholder_distribution_note": self._build_distribution_note(groups),
            "source": "KSD-주주분포요약",
            "source_detail": base.get("source_detail"),
            "last_refresh_ts": today_kst(),
        }
        return parse_date_columns(pd.DataFrame([row]), ["distribution_date", "last_refresh_ts"])

    @staticmethod
    def _sum_matching_ratio(df: pd.DataFrame, patterns: list[str]) -> float | None:
        if df.empty:
            return None
        text = df.get("shareholder_group", pd.Series(dtype="object")).fillna("").astype(str)
        mask = pd.Series(False, index=df.index)
        for pattern in patterns:
            mask = mask | text.str.contains(pattern, na=False)
        if not mask.any():
            return None
        value = pd.to_numeric(df.loc[mask, "share_count_ratio"], errors="coerce").sum(min_count=1)
        return float(value) if pd.notna(value) else None

    @staticmethod
    def _build_distribution_note(groups: dict[str, float | None]) -> str:
        parts: list[str] = []
        label_map = {
            "major": "최대주주",
            "retail": "개인",
            "foreign": "외국인",
            "institution": "기관",
            "corporate": "법인",
            "employee": "우리사주",
        }
        for key in ["major", "retail", "foreign", "institution", "corporate", "employee"]:
            value = groups.get(key)
            if value is None:
                continue
            parts.append(f"{label_map[key]} {value:.1f}%")
        return " · ".join(parts)

    @staticmethod
    def _parse_number(value: Any) -> float | None:
        if value is None:
            return None
        text = str(value).strip()
        if not text or text.lower() in {"nan", "none", "null", "nat", "-", "--"}:
            return None
        cleaned = text.replace(",", "").replace("%", "").replace("주", "").replace("원", "").strip()
        try:
            number = float(cleaned)
        except Exception:
            return None
        return float(int(number)) if number.is_integer() else float(number)

    @staticmethod
    def _parse_date(value: Any) -> pd.Timestamp | pd.NaT:
        if value is None:
            return pd.NaT
        text = str(value).strip()
        if not text or text.lower() in {"nan", "none", "null", "nat", "-", "--", "99991231"}:
            return pd.NaT
        try:
            return pd.to_datetime(text, errors="coerce")
        except Exception:
            return pd.NaT

    @staticmethod
    def _empty_name_lookup() -> pd.DataFrame:
        return pd.DataFrame(columns=["query_name", "query_name_key", "name", "name_key", "symbol", "isin", "issuco_custno", "share_type", "issue_date", "source", "source_detail", "last_refresh_ts"])

    @staticmethod
    def _empty_market_codes() -> pd.DataFrame:
        return pd.DataFrame(columns=["market_code", "market", "name", "name_key", "symbol", "isin", "source", "source_detail", "last_refresh_ts"])

    @staticmethod
    def _empty_listing_info() -> pd.DataFrame:
        return pd.DataFrame(columns=["isin", "name", "name_key", "listing_date", "delisting_date", "listing_status_code", "listing_status", "market", "expiry_date", "source", "source_detail", "last_refresh_ts"])

    @staticmethod
    def _empty_corp_lookup() -> pd.DataFrame:
        return pd.DataFrame(columns=["query_name", "query_name_key", "name", "name_key", "issuco_custno", "bizno", "source", "source_detail", "last_refresh_ts"])

    @staticmethod
    def _empty_corp_basic() -> pd.DataFrame:
        return pd.DataFrame(columns=["issuco_custno", "name", "name_key", "listing_date", "homep_url", "ceo_name", "bizno", "face_value", "post_listing_total_shares", "source", "source_detail", "last_refresh_ts"])

    @staticmethod
    def _empty_distribution_dates() -> pd.DataFrame:
        return pd.DataFrame(columns=["issuco_custno", "distribution_date", "source", "source_detail", "last_refresh_ts"])

    @staticmethod
    def _empty_shareholder_distribution() -> pd.DataFrame:
        return pd.DataFrame(columns=["issuco_custno", "distribution_date", "shareholder_group", "shareholder_count", "shareholder_count_ratio", "share_count", "share_count_ratio", "source", "source_detail", "last_refresh_ts"])

    @staticmethod
    def _empty_shareholder_summary() -> pd.DataFrame:
        return pd.DataFrame(columns=["issuco_custno", "distribution_date", "major_shareholder_ratio", "retail_shareholder_ratio", "foreign_shareholder_ratio", "institution_shareholder_ratio", "corporate_shareholder_ratio", "employee_shareholder_ratio", "shareholder_distribution_note", "source", "source_detail", "last_refresh_ts"])


__all__ = [
    "KSDPublicDataClient",
    "MARKET_CODE_TO_LABEL",
    "KSD_STOCK_BASE_URL",
    "KSD_CORP_BASE_URL",
]
