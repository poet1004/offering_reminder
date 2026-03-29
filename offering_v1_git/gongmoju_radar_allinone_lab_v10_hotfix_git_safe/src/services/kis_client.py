
from __future__ import annotations

import os
import time
from dataclasses import dataclass
from typing import Any

import pandas as pd
import requests

from src.utils import safe_float


@dataclass
class KISConfig:
    app_key: str
    app_secret: str
    env: str = "real"

    @property
    def base_url(self) -> str:
        if self.env == "demo":
            return "https://openapivts.koreainvestment.com:29443"
        return "https://openapi.koreainvestment.com:9443"


class KISClient:
    """
    공식 KIS Open API 샘플 코드 구조를 참고한 경량 클라이언트.
    앱에서는 현재가/일봉 차트 정도만 쓰는 MVP 용도로 최소 구현했다.
    """

    def __init__(self, config: KISConfig, session: requests.Session | None = None) -> None:
        self.config = config
        self.session = session or requests.Session()
        self._access_token: str | None = None
        self._token_expiry_ts: float = 0.0

    @classmethod
    def from_env(cls) -> "KISClient | None":
        app_key = os.getenv("KIS_APP_KEY", "").strip()
        app_secret = os.getenv("KIS_APP_SECRET", "").strip()
        env = os.getenv("KIS_ENV", "real").strip().lower() or "real"
        if not app_key or not app_secret:
            return None
        return cls(KISConfig(app_key=app_key, app_secret=app_secret, env=env))

    def is_configured(self) -> bool:
        return bool(self.config.app_key and self.config.app_secret)

    def _ensure_token(self) -> str:
        if self._access_token and time.time() < self._token_expiry_ts - 120:
            return self._access_token

        url = f"{self.config.base_url}/oauth2/tokenP"
        payload = {
            "grant_type": "client_credentials",
            "appkey": self.config.app_key,
            "appsecret": self.config.app_secret,
        }
        headers = {
            "content-type": "application/json",
        }
        response = self.session.post(url, json=payload, headers=headers, timeout=15)
        response.raise_for_status()
        data = response.json()
        token = data.get("access_token")
        if not token:
            raise RuntimeError(f"KIS token 발급 실패: {data}")
        expires_in = int(data.get("expires_in", 60 * 60 * 23))
        self._access_token = token
        self._token_expiry_ts = time.time() + expires_in
        return token

    def _get(self, path: str, tr_id: str, params: dict[str, Any]) -> dict[str, Any]:
        token = self._ensure_token()
        headers = {
            "content-type": "application/json",
            "authorization": f"Bearer {token}",
            "appkey": self.config.app_key,
            "appsecret": self.config.app_secret,
            "tr_id": tr_id,
            "custtype": "P",
        }
        url = f"{self.config.base_url}{path}"
        response = self.session.get(url, headers=headers, params=params, timeout=15)
        response.raise_for_status()
        data = response.json()
        rt_cd = data.get("rt_cd")
        if rt_cd not in (None, "0"):
            raise RuntimeError(f"KIS API 실패: {data.get('msg1', data)}")
        return data

    def get_stock_price(self, symbol: str, market_div: str = "J") -> dict[str, Any]:
        data = self._get(
            "/uapi/domestic-stock/v1/quotations/inquire-price",
            "FHKST01010100",
            {
                "FID_COND_MRKT_DIV_CODE": market_div,
                "FID_INPUT_ISCD": str(symbol).zfill(6),
            },
        )
        output = data.get("output", {})
        return {
            "symbol": str(symbol).zfill(6),
            "name": output.get("hts_kor_isnm") or output.get("bstp_kor_isnm") or "",
            "price": safe_float(output.get("stck_prpr")),
            "change_pct": safe_float(output.get("prdy_ctrt")),
            "change": safe_float(output.get("prdy_vrss")),
            "raw": output,
        }

    def get_stock_history(self, symbol: str, start_date: str, end_date: str, market_div: str = "J") -> pd.DataFrame:
        data = self._get(
            "/uapi/domestic-stock/v1/quotations/inquire-daily-itemchartprice",
            "FHKST03010100",
            {
                "FID_COND_MRKT_DIV_CODE": market_div,
                "FID_INPUT_ISCD": str(symbol).zfill(6),
                "FID_INPUT_DATE_1": start_date,
                "FID_INPUT_DATE_2": end_date,
                "FID_PERIOD_DIV_CODE": "D",
                "FID_ORG_ADJ_PRC": "0",
            },
        )
        output = data.get("output2", [])
        df = pd.DataFrame(output)
        return self._normalize_daily_history(df)

    def get_stock_history_chunked(
        self,
        symbol: str,
        start_date: str,
        end_date: str,
        market_div: str = "J",
        step_days: int = 90,
    ) -> pd.DataFrame:
        start_ts = pd.Timestamp(start_date)
        end_ts = pd.Timestamp(end_date)
        chunks: list[pd.DataFrame] = []
        current = start_ts
        while current <= end_ts:
            chunk_end = min(current + pd.Timedelta(days=step_days - 1), end_ts)
            try:
                chunk = self.get_stock_history(
                    symbol=str(symbol),
                    start_date=current.strftime("%Y%m%d"),
                    end_date=chunk_end.strftime("%Y%m%d"),
                    market_div=market_div,
                )
                if not chunk.empty:
                    chunks.append(chunk)
            except Exception:
                pass
            current = chunk_end + pd.Timedelta(days=1)

        if not chunks:
            return pd.DataFrame(columns=["date", "open", "high", "low", "close", "volume"])
        out = (
            pd.concat(chunks, ignore_index=True)
            .drop_duplicates(subset=["date"], keep="last")
            .sort_values("date")
            .reset_index(drop=True)
        )
        return out

    def get_index_price(self, index_code: str = "0001") -> dict[str, Any]:
        data = self._get(
            "/uapi/domestic-stock/v1/quotations/inquire-index-price",
            "FHPUP02100000",
            {
                "FID_COND_MRKT_DIV_CODE": "U",
                "FID_INPUT_ISCD": index_code,
            },
        )
        output = data.get("output", {})
        # 응답 키가 다양할 수 있어 대표 후보를 순차 탐색한다.
        price = (
            safe_float(output.get("bstp_nmix_prpr"))
            or safe_float(output.get("bstp_cls_prc"))
            or safe_float(output.get("stck_prpr"))
        )
        change_pct = safe_float(output.get("prdy_ctrt")) or safe_float(output.get("bstp_nmix_prdy_ctrt"))
        return {"index_code": index_code, "price": price, "change_pct": change_pct, "raw": output}

    def get_index_history(self, index_code: str, start_date: str, end_date: str) -> pd.DataFrame:
        data = self._get(
            "/uapi/domestic-stock/v1/quotations/inquire-daily-indexchartprice",
            "FHKUP03500100",
            {
                "FID_COND_MRKT_DIV_CODE": "U",
                "FID_INPUT_ISCD": index_code,
                "FID_INPUT_DATE_1": start_date,
                "FID_INPUT_DATE_2": end_date,
                "FID_PERIOD_DIV_CODE": "D",
            },
        )
        out = pd.DataFrame(data.get("output2", []))
        if out.empty:
            return pd.DataFrame(columns=["date", "close"])
        date_col = next((c for c in out.columns if "date" in c.lower() or "bsop" in c.lower()), None)
        close_col = next((c for c in out.columns if "clpr" in c.lower() or "prpr" in c.lower()), None)
        if date_col is None or close_col is None:
            return pd.DataFrame(columns=["date", "close"])
        result = pd.DataFrame({
            "date": pd.to_datetime(out[date_col], errors="coerce"),
            "close": pd.to_numeric(out[close_col], errors="coerce"),
        }).dropna()
        return result.sort_values("date").reset_index(drop=True)

    def get_index_history_chunked(
        self,
        index_code: str,
        start_date: str,
        end_date: str,
        *,
        step_days: int = 45,
    ) -> pd.DataFrame:
        start_ts = pd.Timestamp(start_date)
        end_ts = pd.Timestamp(end_date)
        chunks: list[pd.DataFrame] = []
        current = start_ts
        while current <= end_ts:
            chunk_end = min(current + pd.Timedelta(days=step_days - 1), end_ts)
            try:
                chunk = self.get_index_history(
                    index_code=index_code,
                    start_date=current.strftime("%Y%m%d"),
                    end_date=chunk_end.strftime("%Y%m%d"),
                )
                if not chunk.empty:
                    chunks.append(chunk)
            except Exception:
                pass
            current = chunk_end + pd.Timedelta(days=1)
        if not chunks:
            return pd.DataFrame(columns=["date", "close"])
        return (
            pd.concat(chunks, ignore_index=True)
            .drop_duplicates(subset=["date"], keep="last")
            .sort_values("date")
            .reset_index(drop=True)
        )

    def get_domestic_futures_price(self, futures_code: str) -> dict[str, Any]:
        data = self._get(
            "/uapi/domestic-futureoption/v1/quotations/inquire-price",
            "FHMIF10000000",
            {
                "FID_COND_MRKT_DIV_CODE": "F",
                "FID_INPUT_ISCD": futures_code,
            },
        )
        return {
            "code": futures_code,
            "output1": data.get("output1", {}),
            "output2": data.get("output2", {}),
            "output3": data.get("output3", {}),
        }

    @staticmethod
    def _normalize_daily_history(df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return pd.DataFrame(columns=["date", "open", "high", "low", "close", "volume"])
        mapping = {}
        candidates = {
            "date": ["stck_bsop_date", "date", "xymd"],
            "open": ["stck_oprc", "open", "oprc"],
            "high": ["stck_hgpr", "high", "hgpr"],
            "low": ["stck_lwpr", "low", "lwpr"],
            "close": ["stck_clpr", "stck_prpr", "close", "clpr", "prpr"],
            "volume": ["acml_vol", "volume", "vol", "cntg_vol"],
        }
        lowered = {str(c).lower(): c for c in df.columns}
        for target, aliases in candidates.items():
            for alias in aliases:
                if alias.lower() in lowered:
                    mapping[lowered[alias.lower()]] = target
                    break
        out = df.rename(columns=mapping).copy()
        for column in ["date", "open", "high", "low", "close", "volume"]:
            if column not in out.columns:
                out[column] = pd.NA
        out["date"] = pd.to_datetime(out["date"], errors="coerce")
        for column in ["open", "high", "low", "close", "volume"]:
            out[column] = pd.to_numeric(out[column], errors="coerce")
        out = out.dropna(subset=["date", "close"]).sort_values("date").reset_index(drop=True)
        return out[["date", "open", "high", "low", "close", "volume"]]
