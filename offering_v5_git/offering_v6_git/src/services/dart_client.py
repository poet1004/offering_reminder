from __future__ import annotations

import os
import zipfile
from io import BytesIO
from pathlib import Path
from typing import Any
from xml.etree import ElementTree as ET

import pandas as pd
import requests

from src.utils import cache_dir, ensure_dir, normalize_name_key


class DartClient:
    def __init__(self, api_key: str, session: requests.Session | None = None) -> None:
        self.api_key = api_key
        self.session = session or requests.Session()
        self.base_url = "https://opendart.fss.or.kr/api"

    @classmethod
    def from_env(cls) -> "DartClient | None":
        api_key = os.getenv("DART_API_KEY", "").strip()
        if not api_key:
            return None
        return cls(api_key=api_key)

    def is_configured(self) -> bool:
        return bool(self.api_key)

    def _get_json(self, path: str, params: dict[str, Any]) -> dict[str, Any]:
        query = {"crtfc_key": self.api_key, **params}
        response = self.session.get(f"{self.base_url}{path}", params=query, timeout=20)
        response.raise_for_status()
        data = response.json()
        status = data.get("status")
        if status not in (None, "000"):
            raise RuntimeError(data.get("message", data))
        return data

    def _get_binary(self, path: str, params: dict[str, Any]) -> bytes:
        query = {"crtfc_key": self.api_key, **params}
        response = self.session.get(f"{self.base_url}{path}", params=query, timeout=30)
        response.raise_for_status()
        return response.content

    def search_filings(
        self,
        corp_code: str | None = None,
        bgn_de: str | None = None,
        end_de: str | None = None,
        page_no: int = 1,
        page_count: int = 10,
        pblntf_ty: str | None = None,
        last_reprt_at: str | None = None,
        corp_cls: str | None = None,
    ) -> list[dict[str, Any]]:
        params: dict[str, Any] = {"page_no": page_no, "page_count": page_count}
        if corp_code:
            params["corp_code"] = corp_code
        if bgn_de:
            params["bgn_de"] = bgn_de
        if end_de:
            params["end_de"] = end_de
        if pblntf_ty:
            params["pblntf_ty"] = pblntf_ty
        if last_reprt_at:
            params["last_reprt_at"] = last_reprt_at
        if corp_cls:
            params["corp_cls"] = corp_cls
        data = self._get_json("/list.json", params)
        return data.get("list", [])

    def equity_registration_statement(self, corp_code: str, bgn_de: str, end_de: str) -> dict[str, pd.DataFrame]:
        data = self._get_json(
            "/estkRs.json",
            {
                "corp_code": corp_code,
                "bgn_de": bgn_de,
                "end_de": end_de,
            },
        )
        groups = data.get("group", [])
        if isinstance(groups, dict):
            groups = [groups]
        result: dict[str, pd.DataFrame] = {}
        for idx, group in enumerate(groups):
            title = str(group.get("title") or f"group_{idx + 1}")
            rows = group.get("list", [])
            if isinstance(rows, dict):
                rows = [rows]
            result[title] = pd.DataFrame(rows)
        return result

    def document_cache_path(self, rcept_no: str, base_dir: Path | None = None) -> Path:
        root = ensure_dir((base_dir or cache_dir()) / "dart_documents")
        return root / f"{rcept_no}.zip"

    def download_document_to_path(self, rcept_no: str, base_dir: Path | None = None, force: bool = False) -> Path:
        path = self.document_cache_path(rcept_no, base_dir=base_dir)
        if path.exists() and not force:
            return path
        payload = self._get_binary("/document.xml", {"rcept_no": rcept_no})
        path.write_bytes(payload)
        return path

    def extract_document_files(self, rcept_no: str, base_dir: Path | None = None, force: bool = False) -> list[dict[str, Any]]:
        path = self.download_document_to_path(rcept_no, base_dir=base_dir, force=force)
        zf = zipfile.ZipFile(path)
        items: list[dict[str, Any]] = []
        for info in zf.infolist():
            if info.is_dir():
                continue
            raw = zf.read(info.filename)
            text = None
            for enc in ["utf-8-sig", "utf-8", "cp949", "euc-kr", "utf-16", "latin1"]:
                try:
                    text = raw.decode(enc)
                    break
                except Exception:
                    continue
            if text is None:
                continue
            items.append({"name": info.filename, "text": text, "size": info.file_size})
        return items

    def company_overview(self, corp_code: str) -> dict[str, Any]:
        return self._get_json("/company.json", {"corp_code": corp_code})

    def viewer_url(self, rcept_no: str) -> str:
        return f"https://dart.fss.or.kr/dsaf001/main.do?rcpNo={rcept_no}"

    def document_download_url(self, rcept_no: str) -> str:
        return f"{self.base_url}/document.xml?crtfc_key={self.api_key}&rcept_no={rcept_no}"

    def corp_code_cache_path(self, base_dir: Path | None = None) -> Path:
        return ensure_dir(base_dir or cache_dir()) / "dart_corp_codes.csv"

    def download_corp_codes(self, base_dir: Path | None = None, force: bool = False) -> pd.DataFrame:
        cache_path = self.corp_code_cache_path(base_dir)
        if cache_path.exists() and not force:
            return pd.read_csv(cache_path, dtype=str)

        payload = self._get_binary("/corpCode.xml", {})
        zf = zipfile.ZipFile(BytesIO(payload))
        xml_name = next((name for name in zf.namelist() if name.lower().endswith(".xml")), None)
        if xml_name is None:
            raise RuntimeError("DART corpCode.xml 압축 파일에서 XML을 찾지 못했습니다.")
        xml_bytes = zf.read(xml_name)
        root = ET.fromstring(xml_bytes)
        rows: list[dict[str, str]] = []
        for item in root.findall("list"):
            row = {
                "corp_code": (item.findtext("corp_code") or "").strip(),
                "corp_name": (item.findtext("corp_name") or "").strip(),
                "corp_eng_name": (item.findtext("corp_eng_name") or "").strip(),
                "stock_code": (item.findtext("stock_code") or "").strip(),
                "modify_date": (item.findtext("modify_date") or "").strip(),
            }
            rows.append(row)
        df = pd.DataFrame(rows)
        df["name_key"] = df["corp_name"].map(normalize_name_key)
        df.to_csv(cache_path, index=False, encoding="utf-8-sig")
        return df

    def load_corp_codes(self, base_dir: Path | None = None) -> pd.DataFrame:
        cache_path = self.corp_code_cache_path(base_dir)
        if cache_path.exists():
            return pd.read_csv(cache_path, dtype=str)
        return self.download_corp_codes(base_dir=base_dir, force=False)

    def lookup_company(
        self,
        *,
        stock_code: str | None = None,
        corp_name: str | None = None,
        base_dir: Path | None = None,
    ) -> dict[str, Any] | None:
        table = self.load_corp_codes(base_dir=base_dir)
        if table.empty:
            return None
        if stock_code:
            stock_code = str(stock_code).strip()
            if stock_code.isdigit():
                stock_code = stock_code.zfill(6)
                matched = table[table["stock_code"].fillna("").str.zfill(6) == stock_code]
                if not matched.empty:
                    return matched.iloc[0].to_dict()
        if corp_name:
            corp_name = str(corp_name).strip()
            key = normalize_name_key(corp_name)
            matched = table[table["name_key"] == key]
            if not matched.empty:
                return matched.iloc[0].to_dict()

            alias_candidates = [corp_name]
            stripped = corp_name.replace("(유가)", "").replace("(코)", "").strip()
            stripped = stripped.replace("㈜", "").replace("(주)", "").replace("주식회사", "").strip()
            alias_candidates.append(stripped)
            for candidate in list(alias_candidates):
                normalized = normalize_name_key(candidate)
                if normalized:
                    alias_candidates.append(normalized)
            seen: set[str] = set()
            alias_candidates = [x for x in alias_candidates if x and not (x in seen or seen.add(x))]

            for candidate in alias_candidates:
                normalized = normalize_name_key(candidate)
                if normalized:
                    matched = table[table["name_key"].fillna("").eq(normalized)]
                    if not matched.empty:
                        return matched.iloc[0].to_dict()
                    name_keys = table["name_key"].fillna("")
                    fuzzy_norm = table[name_keys.map(lambda x: bool(x) and (normalized in x or x in normalized))]
                    if not fuzzy_norm.empty:
                        fuzzy_norm = fuzzy_norm.assign(_len_gap=(fuzzy_norm["name_key"].fillna("").str.len() - len(normalized)).abs())
                        return fuzzy_norm.sort_values(["_len_gap", "corp_name"]).iloc[0].drop(labels=["_len_gap"], errors="ignore").to_dict()
                fuzzy = table[table["corp_name"].fillna("").str.contains(str(candidate), regex=False)]
                if not fuzzy.empty:
                    return fuzzy.iloc[0].to_dict()
        return None

    def latest_company_filings(
        self,
        *,
        stock_code: str | None = None,
        corp_name: str | None = None,
        bgn_de: str | None = None,
        end_de: str | None = None,
        page_count: int = 20,
        base_dir: Path | None = None,
    ) -> pd.DataFrame:
        company = self.lookup_company(stock_code=stock_code, corp_name=corp_name, base_dir=base_dir)
        if company is None:
            return pd.DataFrame()
        corp_code = str(company.get("corp_code") or "").strip()
        filings = self.search_filings(
            corp_code=corp_code,
            bgn_de=bgn_de,
            end_de=end_de,
            page_no=1,
            page_count=page_count,
            last_reprt_at="Y",
        )
        if not filings:
            return pd.DataFrame()
        df = pd.DataFrame(filings)
        if df.empty:
            return df
        if "rcept_no" in df.columns:
            df["viewer_url"] = df["rcept_no"].map(self.viewer_url)
        if "rcept_dt" in df.columns:
            df["rcept_dt"] = pd.to_datetime(df["rcept_dt"], format="%Y%m%d", errors="coerce")
        keywords = ["증권신고서", "투자설명서", "증권발행실적보고서", "주요사항보고서", "사업보고서", "반기보고서", "분기보고서"]
        if "report_nm" in df.columns:
            mask = pd.Series(False, index=df.index)
            for keyword in keywords:
                mask = mask | df["report_nm"].fillna("").str.contains(keyword, regex=False)
            filtered = df[mask].copy()
            if not filtered.empty:
                df = filtered
        return df.sort_values(by=["rcept_dt", "report_nm"], ascending=[False, True]).reset_index(drop=True)

    def latest_company_financials(
        self,
        *,
        corp_code: str,
        bsns_year: int,
        reprt_code: str = "11011",
    ) -> pd.DataFrame:
        data = self._get_json(
            "/fnlttSinglAcnt.json",
            {
                "corp_code": corp_code,
                "bsns_year": str(bsns_year),
                "reprt_code": reprt_code,
            },
        )
        return pd.DataFrame(data.get("list", []))
