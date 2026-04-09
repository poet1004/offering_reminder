from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import pandas as pd
import requests

from src.services.ipo_scrapers import fetch_38_new_listing_table, standardize_38_new_listing_table
from src.utils import normalize_name_key, normalize_symbol_text, safe_float, today_kst


class PublicQuoteService:
    """최근 상장 종목 현재가를 보강하기 위한 경량 서비스.

    우선순위
    1) 네이버증권 종목 페이지(code=xxxxxx / 0001A0 등 6자리 영숫자 허용)
    2) 38 신규상장 표(기업명 매칭)
    3) 마지막 성공 캐시(public_quotes_latest.csv)
    """

    def __init__(self, data_dir: str | Path):
        self.data_dir = Path(data_dir)
        self.cache_path = self.data_dir / "cache" / "public_quotes_latest.csv"
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36",
                "Referer": "https://finance.naver.com/",
                "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
            }
        )

    def get_quotes(self, req_df: pd.DataFrame, max_items: int = 60) -> pd.DataFrame:
        if req_df is None or req_df.empty:
            return self._empty()
        work = req_df.copy()
        work["name"] = work.get("name", pd.Series(dtype="object")).astype(str).str.strip()
        work["name_key"] = work["name"].map(normalize_name_key)
        work["symbol"] = work.get("symbol", pd.Series(dtype="object")).map(normalize_symbol_text)
        work = work[(work["name_key"].astype(str).str.len() > 0) | work["symbol"].notna()].copy()
        if work.empty:
            return self._empty()

        cache = self._read_cache()
        rows: list[dict[str, Any]] = []
        recent_38: pd.DataFrame | None = None
        for _, row in work.drop_duplicates(subset=["name_key", "symbol"], keep="first").head(max_items).iterrows():
            symbol = normalize_symbol_text(row.get("symbol"))
            name = str(row.get("name") or "").strip()
            name_key = normalize_name_key(name)
            payload: dict[str, Any] | None = None
            if symbol:
                payload = self._fetch_naver_quote(symbol, name=name, name_key=name_key)
            if payload is None:
                if recent_38 is None:
                    recent_38 = self._fetch_recent_38_table()
                payload = self._lookup_recent_38(recent_38, name=name, name_key=name_key, symbol=symbol)
            if payload is None and not cache.empty:
                payload = self._lookup_cache(cache, name_key=name_key, symbol=symbol)
            if payload is not None:
                rows.append(payload)

        out = pd.DataFrame(rows)
        if out.empty:
            return self._empty()
        out = out.drop_duplicates(subset=[c for c in ["name_key", "symbol"] if c in out.columns], keep="first")
        self._write_cache(out)
        return out

    def _empty(self) -> pd.DataFrame:
        return pd.DataFrame(columns=["name_key", "symbol", "market", "current_price", "day_change_pct", "quote_asof", "quote_provider"])

    def _read_cache(self) -> pd.DataFrame:
        if not self.cache_path.exists():
            return self._empty()
        try:
            df = pd.read_csv(self.cache_path)
        except Exception:
            return self._empty()
        if df.empty:
            return self._empty()
        if "name_key" not in df.columns and "name" in df.columns:
            df["name_key"] = df["name"].map(normalize_name_key)
        if "symbol" in df.columns:
            df["symbol"] = df["symbol"].map(normalize_symbol_text)
        return df

    def _write_cache(self, df: pd.DataFrame) -> None:
        try:
            self.cache_path.parent.mkdir(parents=True, exist_ok=True)
            existing = self._read_cache()
            merged = pd.concat([df, existing], ignore_index=True)
            if "name_key" not in merged.columns and "name" in merged.columns:
                merged["name_key"] = merged["name"].map(normalize_name_key)
            keep_cols = [c for c in ["name", "name_key", "symbol", "market", "current_price", "day_change_pct", "quote_asof", "quote_provider"] if c in merged.columns]
            merged = merged[keep_cols].copy()
            if "quote_asof" in merged.columns:
                merged["quote_asof"] = pd.to_datetime(merged["quote_asof"], errors="coerce")
                merged = merged.sort_values(["quote_asof", "name_key"], ascending=[False, True], na_position="last")
            merged = merged.drop_duplicates(subset=[c for c in ["name_key", "symbol"] if c in merged.columns], keep="first")
            merged.to_csv(self.cache_path, index=False)
        except Exception:
            return

    def _lookup_cache(self, cache: pd.DataFrame, *, name_key: str, symbol: str | None) -> dict[str, Any] | None:
        subset = pd.DataFrame()
        if symbol and "symbol" in cache.columns:
            subset = cache[cache["symbol"].map(normalize_symbol_text) == symbol].copy()
        if subset.empty and name_key and "name_key" in cache.columns:
            subset = cache[cache["name_key"] == name_key].copy()
        if subset.empty:
            return None
        row = subset.iloc[0].to_dict()
        return {
            "name": row.get("name"),
            "name_key": name_key or row.get("name_key"),
            "symbol": normalize_symbol_text(row.get("symbol")) or symbol,
            "market": row.get("market"),
            "current_price": self._to_number(row.get("current_price")),
            "day_change_pct": self._to_number(row.get("day_change_pct")),
            "quote_asof": row.get("quote_asof"),
            "quote_provider": row.get("quote_provider") or "cache",
        }

    def _fetch_recent_38_table(self) -> pd.DataFrame:
        try:
            df = standardize_38_new_listing_table(fetch_38_new_listing_table(timeout=10, max_pages=5))
        except Exception:
            return self._empty()
        if df.empty:
            return self._empty()
        df = df.copy()
        df["name_key"] = df.get("name", pd.Series(dtype="object")).map(normalize_name_key)
        return df

    def _lookup_recent_38(self, frame: pd.DataFrame | None, *, name: str, name_key: str, symbol: str | None) -> dict[str, Any] | None:
        if frame is None or frame.empty:
            return None
        subset = pd.DataFrame()
        if symbol and "symbol" in frame.columns:
            subset = frame[frame["symbol"].map(normalize_symbol_text) == symbol].copy()
        if subset.empty and name_key:
            subset = frame[frame["name_key"] == name_key].copy()
        if subset.empty and name:
            subset = frame[frame.get("name", pd.Series(dtype="object")).astype(str).str.contains(name, na=False)].copy()
        if subset.empty:
            return None
        row = subset.sort_values([c for c in ["listing_date", "name_key"] if c in subset.columns], ascending=[False, True], na_position="last").iloc[0].to_dict()
        return {
            "name": row.get("name"),
            "name_key": name_key or row.get("name_key"),
            "symbol": normalize_symbol_text(row.get("symbol")) or symbol,
            "market": row.get("market"),
            "current_price": self._to_number(row.get("current_price")),
            "day_change_pct": self._to_number(row.get("day_change_pct")),
            "quote_asof": row.get("listing_date") or today_kst().normalize(),
            "quote_provider": row.get("source") or "38-new-listing",
        }

    def _fetch_naver_quote(self, symbol: str, *, name: str = "", name_key: str = "") -> dict[str, Any] | None:
        code = normalize_symbol_text(symbol)
        if not code:
            return None
        url = f"https://finance.naver.com/item/main.naver?code={code}"
        resp = self.session.get(url, timeout=8)
        resp.raise_for_status()
        text = resp.text
        price = None
        change_pct = None

        patterns = [
            r'<p[^>]*class="no_today"[^>]*>.*?<span[^>]*class="blind"[^>]*>([0-9,]+)</span>',
            r'현재가\s*</em>\s*<span[^>]*class="blind"[^>]*>([0-9,]+)</span>',
            r'"nv"\s*:\s*"?([0-9,]+)"?',
        ]
        for pat in patterns:
            m = re.search(pat, text, re.S)
            if m:
                price = self._to_number(m.group(1))
                if price is not None:
                    break
        change_patterns = [
            r'<p[^>]*class="no_exday"[^>]*>.*?<span[^>]*class="blind"[^>]*>[가-힣]+</span>.*?<span[^>]*class="blind"[^>]*>[0-9,]+</span>.*?<span[^>]*class="blind"[^>]*>([+-]?[0-9.]+)%</span>',
            r'등락률\s*</em>\s*<span[^>]*class="blind"[^>]*>([+-]?[0-9.]+)%</span>',
        ]
        for pat in change_patterns:
            m = re.search(pat, text, re.S)
            if m:
                change_pct = self._to_number(m.group(1))
                break
        if price is None:
            return None
        return {
            "name": name or None,
            "name_key": name_key or normalize_name_key(name),
            "symbol": code,
            "market": None,
            "current_price": price,
            "day_change_pct": change_pct,
            "quote_asof": today_kst().normalize(),
            "quote_provider": "NaverFinance",
        }

    @staticmethod
    def _to_number(value: Any) -> float | None:
        number = safe_float(value)
        if number is not None:
            return float(number)
        text = str(value or "").replace(",", "").replace("%", "").strip()
        try:
            return float(text)
        except Exception:
            return None
