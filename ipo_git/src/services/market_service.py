from __future__ import annotations

import re
from pathlib import Path
from typing import Any
from urllib.parse import quote

import pandas as pd
import requests

from src.services.kis_client import KISClient
from src.services.live_cache import LiveCacheStore
from src.utils import data_dir, safe_float, today_kst


MARKET_SPECS: list[dict[str, Any]] = [
    {"name": "KOSPI", "ticker": "^KS11", "group": "국내지수", "providers": ["kis_domestic_index", "yahoo_http"], "kis_index_code": "0001"},
    {"name": "KOSDAQ", "ticker": "^KQ11", "group": "국내지수", "providers": ["kis_domestic_index", "yahoo_http"], "kis_index_code": "1001"},
    {"name": "USD/KRW", "ticker": "KRW=X", "group": "환율", "providers": ["yahoo_http"]},
    {"name": "S&P 500", "ticker": "^GSPC", "group": "해외지수", "providers": ["yahoo_http"]},
    {"name": "NASDAQ", "ticker": "^IXIC", "group": "해외지수", "providers": ["yahoo_http"]},
    {"name": "S&P500 Futures", "ticker": "ES=F", "group": "선물", "providers": ["yahoo_http"]},
    {"name": "NASDAQ100 Futures", "ticker": "NQ=F", "group": "선물", "providers": ["yahoo_http"]},
    {"name": "WTI", "ticker": "CL=F", "group": "원자재", "providers": ["yahoo_http"]},
    {"name": "Gold", "ticker": "GC=F", "group": "원자재", "providers": ["yahoo_http"]},
]

SNAPSHOT_COLUMNS = ["name", "group", "ticker", "last", "change_pct", "asof", "provider"]
HISTORY_COLUMNS = ["date", "close", "ticker", "provider"]
DIAG_COLUMNS = ["category", "name", "ticker", "provider", "ok", "detail", "rows", "asof"]

YAHOO_CHART_URL = "https://query1.finance.yahoo.com/v8/finance/chart/{ticker}"
YAHOO_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
    )
}


class MarketService:
    def __init__(self, base_dir: Path | str | None = None, kis_client: KISClient | None = None) -> None:
        self.base_dir = Path(base_dir) if base_dir is not None else data_dir()
        self.kis_client = kis_client
        self.cache = LiveCacheStore(self.base_dir / "cache")
        self.http = requests.Session()
        self.http.headers.update(YAHOO_HEADERS)

    def load_sample_snapshot(self) -> pd.DataFrame:
        return pd.read_csv(self.base_dir / "sample_market_snapshot.csv")

    def load_sample_history(self) -> pd.DataFrame:
        df = pd.read_csv(self.base_dir / "sample_market_history.csv")
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        return df

    def get_market_snapshot(self, prefer_live: bool = False, allow_sample_fallback: bool = True) -> tuple[pd.DataFrame, str]:
        bundle = self.get_market_snapshot_bundle(prefer_live=prefer_live, allow_sample_fallback=allow_sample_fallback)
        return bundle["frame"], bundle["source"]

    def get_market_snapshot_bundle(self, prefer_live: bool = False, allow_sample_fallback: bool = True) -> dict[str, Any]:
        diagnostics: list[dict[str, Any]] = []
        cached = self._read_cached_frame("market_snapshot_last_success", SNAPSHOT_COLUMNS)
        cached_meta = self.cache.read_meta("market_snapshot_last_success")

        if prefer_live:
            live_frame, providers, live_diag = self._fetch_live_snapshot()
            diagnostics.extend(live_diag)
            self._write_diagnostics("market_snapshot_diag_latest", live_diag)
            if not live_frame.empty:
                merged_live = self._merge_snapshot_with_cached(live_frame, cached)
                used_cache_rows = bool(not cached.empty and len(merged_live) >= len(live_frame) and not merged_live.equals(live_frame))
                source = f"live({'+'.join(sorted(providers))})" if providers else "live"
                if used_cache_rows:
                    source = f"{source}+cache_fresher"
                self.cache.write_frame(
                    "market_snapshot_last_success",
                    merged_live,
                    meta={
                        "source": source,
                        "provider_count": len(providers),
                        "notes": "last successful live market snapshot",
                    },
                )
                return {
                    "frame": merged_live,
                    "source": source,
                    "diagnostics": pd.DataFrame(live_diag, columns=DIAG_COLUMNS),
                    "cached_used": used_cache_rows,
                    "sample_used": False,
                }
            if not cached.empty:
                return {
                    "frame": cached,
                    "source": self._cache_source_label(cached_meta, default="cache(last_success)"),
                    "diagnostics": pd.DataFrame(live_diag, columns=DIAG_COLUMNS),
                    "cached_used": True,
                    "sample_used": False,
                }

        if not prefer_live and not cached.empty:
            diagnostics_df = self.read_diagnostics("market_snapshot_diag_latest")
            return {
                "frame": cached,
                "source": self._cache_source_label(cached_meta, default="cache(last_success)"),
                "diagnostics": diagnostics_df,
                "cached_used": True,
                "sample_used": False,
            }

        if allow_sample_fallback:
            return {
                "frame": self.load_sample_snapshot(),
                "source": "sample",
                "diagnostics": pd.DataFrame(diagnostics, columns=DIAG_COLUMNS),
                "cached_used": False,
                "sample_used": True,
            }
        return {
            "frame": pd.DataFrame(columns=SNAPSHOT_COLUMNS),
            "source": "unavailable",
            "diagnostics": pd.DataFrame(diagnostics, columns=DIAG_COLUMNS),
            "cached_used": False,
            "sample_used": False,
        }

    def get_market_history(self, ticker: str, prefer_live: bool = False, period: str = "6mo", allow_sample_fallback: bool = True) -> tuple[pd.DataFrame, str]:
        bundle = self.get_market_history_bundle(ticker=ticker, prefer_live=prefer_live, period=period, allow_sample_fallback=allow_sample_fallback)
        return bundle["frame"], bundle["source"]

    def get_market_history_bundle(
        self,
        ticker: str,
        prefer_live: bool = False,
        period: str = "6mo",
        allow_sample_fallback: bool = True,
    ) -> dict[str, Any]:
        spec = self._find_spec_by_ticker(ticker)
        history_key = self._history_cache_key(ticker, period)
        cached = self._read_cached_frame(history_key, HISTORY_COLUMNS)
        cached_meta = self.cache.read_meta(history_key)
        diagnostics: list[dict[str, Any]] = []

        if prefer_live:
            live_frame, provider, live_diag = self._fetch_live_history(spec, period=period)
            diagnostics.extend(live_diag)
            self._write_diagnostics(f"{history_key}_diag_latest", live_diag)
            if not live_frame.empty:
                source = f"live({provider})" if provider else "live"
                self.cache.write_frame(
                    history_key,
                    live_frame,
                    meta={
                        "source": source,
                        "notes": f"last successful market history for {ticker} {period}",
                    },
                )
                return {
                    "frame": live_frame,
                    "source": source,
                    "diagnostics": pd.DataFrame(live_diag, columns=DIAG_COLUMNS),
                    "cached_used": False,
                    "sample_used": False,
                }
            if not cached.empty:
                return {
                    "frame": cached,
                    "source": self._cache_source_label(cached_meta, default="cache(last_success)"),
                    "diagnostics": pd.DataFrame(live_diag, columns=DIAG_COLUMNS),
                    "cached_used": True,
                    "sample_used": False,
                }

        if not prefer_live and not cached.empty:
            diagnostics_df = self.read_diagnostics(f"{history_key}_diag_latest")
            return {
                "frame": cached,
                "source": self._cache_source_label(cached_meta, default="cache(last_success)"),
                "diagnostics": diagnostics_df,
                "cached_used": True,
                "sample_used": False,
            }

        if allow_sample_fallback:
            sample = self.load_sample_history()
            out = sample[sample["ticker"] == ticker].copy().sort_values("date").reset_index(drop=True)
            out = self._ensure_history_columns(out)
            return {
                "frame": out,
                "source": "sample",
                "diagnostics": pd.DataFrame(diagnostics, columns=DIAG_COLUMNS),
                "cached_used": False,
                "sample_used": True,
            }
        return {
            "frame": pd.DataFrame(columns=HISTORY_COLUMNS),
            "source": "unavailable",
            "diagnostics": pd.DataFrame(diagnostics, columns=DIAG_COLUMNS),
            "cached_used": False,
            "sample_used": False,
        }

    def read_diagnostics(self, name: str = "market_snapshot_diag_latest") -> pd.DataFrame:
        df = self._read_cached_frame(name, DIAG_COLUMNS)
        if df.empty:
            return pd.DataFrame(columns=DIAG_COLUMNS)
        if "ok" in df.columns:
            df["ok"] = df["ok"].map(lambda x: str(x).strip().lower() in {"1", "true", "t", "yes"})
        return df

    def refresh_market_cache(self, periods: list[str] | None = None) -> dict[str, Any]:
        periods = periods or ["1mo", "3mo", "6mo", "1y"]
        snapshot_bundle = self.get_market_snapshot_bundle(prefer_live=True, allow_sample_fallback=False)
        diagnostics = snapshot_bundle.get("diagnostics", pd.DataFrame())
        failed_count = 0
        if isinstance(diagnostics, pd.DataFrame) and not diagnostics.empty and "ok" in diagnostics.columns:
            failed_count = int((~diagnostics["ok"].fillna(False)).sum())
        report: dict[str, Any] = {
            "snapshot_source": snapshot_bundle["source"],
            "snapshot_rows": int(len(snapshot_bundle["frame"])),
            "snapshot_cached_used": bool(snapshot_bundle.get("cached_used")),
            "snapshot_failures": failed_count,
            "histories": [],
        }
        if snapshot_bundle["frame"].empty:
            return report
        for ticker in snapshot_bundle["frame"].get("ticker", pd.Series(dtype="object")).dropna().astype(str).tolist():
            for period in periods:
                history_bundle = self.get_market_history_bundle(ticker=ticker, prefer_live=True, period=period, allow_sample_fallback=False)
                report["histories"].append(
                    {
                        "ticker": ticker,
                        "period": period,
                        "source": history_bundle["source"],
                        "rows": int(len(history_bundle["frame"])),
                        "cached_used": bool(history_bundle.get("cached_used")),
                    }
                )
        return report

    def get_stock_signal_from_kis(self, symbol: str, days: int = 130) -> dict[str, Any] | None:
        if self.kis_client is None:
            return None
        end = pd.Timestamp.now(tz="Asia/Seoul").tz_localize(None).normalize()
        start = end - pd.Timedelta(days=days)
        try:
            history = self.kis_client.get_stock_history_chunked(
                symbol=symbol,
                start_date=start.strftime("%Y%m%d"),
                end_date=end.strftime("%Y%m%d"),
                market_div="J",
            )
            if history.empty:
                return None
            from src.services.calculations import latest_signal_from_history

            result = latest_signal_from_history(history)
            result["history"] = history
            return result
        except Exception:
            return None

    def market_mood(self, snapshot: pd.DataFrame) -> dict[str, Any]:
        if snapshot.empty:
            return {"label": "데이터없음", "score": None}
        score = 0.0
        for _, row in snapshot.iterrows():
            change_pct = safe_float(row.get("change_pct"), 0.0) or 0.0
            group = str(row.get("group") or "")
            weight = 1.0
            if group == "국내지수":
                weight = 2.0
            elif group == "선물":
                weight = 1.5
            score += change_pct * weight
        if score >= 2:
            label = "강세"
        elif score >= 0.5:
            label = "우호적"
        elif score <= -2:
            label = "약세"
        elif score <= -0.5:
            label = "경계"
        else:
            label = "중립"
        return {"label": label, "score": round(score, 2)}


    def _merge_snapshot_with_cached(self, live_frame: pd.DataFrame, cached_frame: pd.DataFrame) -> pd.DataFrame:
        if live_frame.empty:
            return cached_frame.copy()
        if cached_frame.empty:
            return live_frame.copy()

        live_work = live_frame.copy()
        cached_work = cached_frame.copy()
        live_work["ticker"] = live_work.get("ticker", pd.Series(dtype="object")).astype(str)
        cached_work["ticker"] = cached_work.get("ticker", pd.Series(dtype="object")).astype(str)

        rows: list[dict[str, Any]] = []
        for ticker in sorted(set(live_work["ticker"]).union(set(cached_work["ticker"]))):
            live_row = live_work[live_work["ticker"] == ticker].tail(1)
            cached_row = cached_work[cached_work["ticker"] == ticker].tail(1)
            chosen = self._pick_fresher_snapshot_row(
                live_row.iloc[0] if not live_row.empty else None,
                cached_row.iloc[0] if not cached_row.empty else None,
            )
            if chosen is None:
                continue
            rows.append(dict(chosen))
        out = pd.DataFrame(rows, columns=SNAPSHOT_COLUMNS)
        if out.empty:
            return out
        return out.sort_values(["group", "name"]).reset_index(drop=True)

    @staticmethod
    def _pick_fresher_snapshot_row(live_row: pd.Series | None, cached_row: pd.Series | None) -> pd.Series | None:
        if live_row is None:
            return cached_row
        if cached_row is None:
            return live_row

        live_asof = pd.to_datetime(live_row.get("asof"), errors="coerce")
        cached_asof = pd.to_datetime(cached_row.get("asof"), errors="coerce")
        live_last = safe_float(live_row.get("last"))
        cached_last = safe_float(cached_row.get("last"))

        if live_last is None and cached_last is not None:
            return cached_row
        if cached_last is None and live_last is not None:
            return live_row
        if pd.isna(live_asof) and not pd.isna(cached_asof):
            return cached_row
        if pd.isna(cached_asof) and not pd.isna(live_asof):
            return live_row
        if not pd.isna(cached_asof) and not pd.isna(live_asof) and cached_asof > live_asof:
            return cached_row
        return live_row

    def _fetch_live_snapshot(self) -> tuple[pd.DataFrame, set[str], list[dict[str, Any]]]:
        diagnostics: list[dict[str, Any]] = []
        providers_used: set[str] = set()
        now = pd.Timestamp.now(tz="Asia/Seoul").tz_localize(None)
        row_map: dict[str, dict[str, Any]] = {}

        for spec in MARKET_SPECS:
            if "kis_domestic_index" not in spec.get("providers", []):
                continue
            if self.kis_client is None:
                diagnostics.append(self._diag_row("snapshot", spec, "KIS", False, "KIS 환경변수가 없어 국내지수 실시간 조회를 건너뜀"))
                continue
            try:
                quote_payload = self.kis_client.get_index_price(str(spec.get("kis_index_code")))
                price = safe_float(quote_payload.get("price"))
                if price is None:
                    raise RuntimeError("price is empty")
                row_map[spec["ticker"]] = {
                    "name": spec["name"],
                    "group": spec["group"],
                    "ticker": spec["ticker"],
                    "last": round(price, 2),
                    "change_pct": round(safe_float(quote_payload.get("change_pct"), 0.0) or 0.0, 2),
                    "asof": now,
                    "provider": "KIS",
                }
                diagnostics.append(self._diag_row("snapshot", spec, "KIS", True, "OK", rows=1))
                providers_used.add("KIS")
            except Exception as exc:
                diagnostics.append(self._diag_row("snapshot", spec, "KIS", False, str(exc)))

        for spec in MARKET_SPECS:
            if "yahoo_http" not in spec.get("providers", []) or spec["ticker"] in row_map:
                continue
            try:
                hist = self._fetch_yahoo_chart_frame(spec["ticker"], period="5d")
                if hist.empty:
                    raise RuntimeError("empty history")
                last_close = hist["close"].dropna()
                if last_close.empty:
                    raise RuntimeError("close is empty")
                last = float(last_close.iloc[-1])
                prev = float(last_close.iloc[-2]) if len(last_close) >= 2 else last
                change_pct = ((last / prev) - 1.0) * 100 if prev else 0.0
                asof = pd.to_datetime(hist["date"].dropna().iloc[-1], errors="coerce") if not hist["date"].dropna().empty else now
                if pd.isna(asof):
                    asof_value = now
                elif getattr(asof, "tzinfo", None):
                    asof_value = asof.tz_localize(None)
                else:
                    asof_value = asof
                row_map[spec["ticker"]] = {
                    "name": spec["name"],
                    "group": spec["group"],
                    "ticker": spec["ticker"],
                    "last": round(last, 2),
                    "change_pct": round(change_pct, 2),
                    "asof": asof_value,
                    "provider": "YahooHTTP",
                }
                diagnostics.append(self._diag_row("snapshot", spec, "YahooHTTP", True, "OK", rows=int(len(hist))))
                providers_used.add("YahooHTTP")
            except Exception as exc:
                diagnostics.append(self._diag_row("snapshot", spec, "YahooHTTP", False, str(exc)))

        out = pd.DataFrame(list(row_map.values()), columns=SNAPSHOT_COLUMNS)
        if not out.empty:
            out = out.sort_values(["group", "name"]).reset_index(drop=True)
        return out, providers_used, diagnostics

    def _fetch_live_history(self, spec: dict[str, Any] | None, *, period: str) -> tuple[pd.DataFrame, str | None, list[dict[str, Any]]]:
        if spec is None:
            diag = [
                {
                    "category": "history",
                    "name": "",
                    "ticker": "",
                    "provider": "resolver",
                    "ok": False,
                    "detail": "unknown ticker",
                    "rows": 0,
                    "asof": today_kst(),
                }
            ]
            return pd.DataFrame(columns=HISTORY_COLUMNS), None, diag

        diagnostics: list[dict[str, Any]] = []
        if "kis_domestic_index" in spec.get("providers", []) and self.kis_client is not None:
            try:
                end = pd.Timestamp.now(tz="Asia/Seoul").tz_localize(None).normalize()
                start = end - pd.Timedelta(days=self._period_days(period))
                history = self.kis_client.get_index_history_chunked(
                    index_code=str(spec.get("kis_index_code")),
                    start_date=start.strftime("%Y%m%d"),
                    end_date=end.strftime("%Y%m%d"),
                )
                if history.empty:
                    raise RuntimeError("empty history")
                out = history.copy()
                out["ticker"] = spec["ticker"]
                out["provider"] = "KIS"
                out = self._ensure_history_columns(out)
                diagnostics.append(self._diag_row("history", spec, "KIS", True, "OK", rows=int(len(out))))
                return out, "KIS", diagnostics
            except Exception as exc:
                diagnostics.append(self._diag_row("history", spec, "KIS", False, str(exc)))
        elif "kis_domestic_index" in spec.get("providers", []):
            diagnostics.append(self._diag_row("history", spec, "KIS", False, "KIS 환경변수가 없어 국내지수 이력 조회를 건너뜀"))

        if "yahoo_http" in spec.get("providers", []):
            try:
                out = self._fetch_yahoo_chart_frame(spec["ticker"], period=period)
                if out.empty:
                    raise RuntimeError("empty history")
                out["ticker"] = spec["ticker"]
                out["provider"] = "YahooHTTP"
                out = self._ensure_history_columns(out.dropna().sort_values("date").reset_index(drop=True))
                diagnostics.append(self._diag_row("history", spec, "YahooHTTP", True, "OK", rows=int(len(out))))
                return out, "YahooHTTP", diagnostics
            except Exception as exc:
                diagnostics.append(self._diag_row("history", spec, "YahooHTTP", False, str(exc)))

        return pd.DataFrame(columns=HISTORY_COLUMNS), None, diagnostics

    def _fetch_yahoo_chart_frame(self, ticker: str, *, period: str) -> pd.DataFrame:
        range_value = self._yahoo_range(period)
        url = YAHOO_CHART_URL.format(ticker=quote(str(ticker), safe=""))
        response = self.http.get(
            url,
            params={
                "range": range_value,
                "interval": "1d",
                "includePrePost": "false",
                "events": "div,splits",
            },
            timeout=15,
        )
        response.raise_for_status()
        payload = response.json()
        chart = payload.get("chart", {}) if isinstance(payload, dict) else {}
        error = chart.get("error")
        if error:
            raise RuntimeError(str(error))
        result_list = chart.get("result") or []
        if not result_list:
            raise RuntimeError("empty result")
        result = result_list[0]
        timestamps = result.get("timestamp") or []
        indicators = result.get("indicators") or {}
        quote_list = indicators.get("quote") or []
        quote_row = quote_list[0] if quote_list else {}
        close_values = self._yahoo_series_with_fallback(indicators, quote_row, "close")
        open_values = quote_row.get("open") or []
        high_values = quote_row.get("high") or []
        low_values = quote_row.get("low") or []
        volume_values = quote_row.get("volume") or []

        rows: list[dict[str, Any]] = []
        for idx, ts in enumerate(timestamps):
            close = safe_float(self._safe_list_get(close_values, idx))
            if close is None:
                continue
            rows.append(
                {
                    "date": pd.to_datetime(ts, unit="s", utc=True, errors="coerce").tz_localize(None),
                    "open": safe_float(self._safe_list_get(open_values, idx)),
                    "high": safe_float(self._safe_list_get(high_values, idx)),
                    "low": safe_float(self._safe_list_get(low_values, idx)),
                    "close": close,
                    "volume": safe_float(self._safe_list_get(volume_values, idx)),
                }
            )
        if not rows:
            raise RuntimeError("no usable rows")
        out = pd.DataFrame(rows)
        return out.sort_values("date").reset_index(drop=True)

    @staticmethod
    def _safe_list_get(values: Any, idx: int) -> Any:
        if not isinstance(values, list):
            return None
        if idx < 0 or idx >= len(values):
            return None
        return values[idx]

    @staticmethod
    def _yahoo_series_with_fallback(indicators: dict[str, Any], quote_row: dict[str, Any], field: str) -> list[Any]:
        primary = quote_row.get(field)
        if isinstance(primary, list) and primary:
            return primary
        if field == "close":
            adj_list = indicators.get("adjclose") or []
            adj_row = adj_list[0] if adj_list else {}
            adj_values = adj_row.get("adjclose")
            if isinstance(adj_values, list):
                return adj_values
        return []

    def _read_cached_frame(self, name: str, columns: list[str]) -> pd.DataFrame:
        df = self.cache.read_frame(name)
        if df.empty:
            return pd.DataFrame(columns=columns)
        for col in columns:
            if col not in df.columns:
                df[col] = pd.NA
        return df[columns].copy()

    def _write_diagnostics(self, name: str, rows: list[dict[str, Any]]) -> None:
        df = pd.DataFrame(rows, columns=DIAG_COLUMNS)
        self.cache.write_frame(name, df, meta={"source": "live-diagnostics", "notes": name})

    @staticmethod
    def _cache_source_label(meta: dict[str, Any], default: str = "cache") -> str:
        saved_at = str(meta.get("saved_at") or "").strip()
        source = str(meta.get("source") or default)
        return f"{source} @ {saved_at}" if saved_at else source

    @staticmethod
    def _history_cache_key(ticker: str, period: str) -> str:
        slug = re.sub(r"[^A-Za-z0-9]+", "_", str(ticker)).strip("_").lower() or "ticker"
        return f"market_history_{slug}_{period}"

    @staticmethod
    def _period_days(period: str) -> int:
        mapping = {"1mo": 35, "3mo": 100, "6mo": 200, "1y": 370, "2y": 740}
        return mapping.get(period, 200)

    @staticmethod
    def _yahoo_range(period: str) -> str:
        mapping = {"1mo": "1mo", "3mo": "3mo", "6mo": "6mo", "1y": "1y", "2y": "2y", "5d": "5d", "5y": "5y", "max": "max"}
        return mapping.get(period, "6mo")

    @staticmethod
    def _diag_row(category: str, spec: dict[str, Any], provider: str, ok: bool, detail: str, rows: int = 0) -> dict[str, Any]:
        return {
            "category": category,
            "name": spec.get("name", ""),
            "ticker": spec.get("ticker", ""),
            "provider": provider,
            "ok": bool(ok),
            "detail": str(detail),
            "rows": int(rows),
            "asof": pd.Timestamp.now(tz="Asia/Seoul").tz_localize(None),
        }

    @staticmethod
    def _find_spec_by_ticker(ticker: str) -> dict[str, Any] | None:
        for spec in MARKET_SPECS:
            if str(spec.get("ticker")) == str(ticker):
                return spec
        return None

    @staticmethod
    def _ensure_history_columns(df: pd.DataFrame) -> pd.DataFrame:
        out = df.copy()
        for col in HISTORY_COLUMNS:
            if col not in out.columns:
                out[col] = pd.NA
        return out[HISTORY_COLUMNS].copy()
