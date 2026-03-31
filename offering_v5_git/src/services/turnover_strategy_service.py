from __future__ import annotations

import json
import sqlite3
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Callable, Iterable

import pandas as pd

from integrated_lab.ipo_lockup_unified_lab.ipo_lockup_program import CostConfig
from integrated_lab.ipo_lockup_unified_lab.turnover_daily_backtest import DEFAULT_HOLD_BY_TERM, backtest_turnover_signals
from integrated_lab.ipo_lockup_unified_lab.turnover_signal_engine import DEFAULT_PRICE_FILTERS, build_turnover_signals
from src.services.kis_client import KISClient
from src.services.unified_lab_bridge import UnifiedLabBridgeService, UnifiedLabPaths
from src.utils import parse_date_columns, today_kst


DailyHistoryProvider = Callable[[str, str, str], pd.DataFrame]


@dataclass(frozen=True)
class TurnoverStrategyParams:
    interval_min: int = 5
    multiples: tuple[float, ...] = (1.0,)
    price_filters: tuple[str, ...] = ("reclaim_open_or_vwap",)
    max_days_after: int = 5
    aggregate_by: str = "type"
    cum_scope: str = "through_window"
    unlock_terms: tuple[str, ...] = ()
    unlock_types: tuple[str, ...] = ()
    unlock_start_date: str | None = None
    unlock_end_date: str | None = None
    max_events: int | None = 40
    min_prev_close_vs_ipo: float | None = None
    max_prev_close_vs_ipo: float | None = None
    buy_cost: float = 0.00015
    sell_cost: float = 0.00215
    hold_days_by_term: dict[str, int] = field(default_factory=lambda: dict(DEFAULT_HOLD_BY_TERM))

    def normalized(self) -> "TurnoverStrategyParams":
        filters = tuple(sorted({str(v).strip() for v in self.price_filters if str(v).strip()})) or ("reclaim_open_or_vwap",)
        invalid = [v for v in filters if v not in DEFAULT_PRICE_FILTERS]
        if invalid:
            raise ValueError(f"지원하지 않는 price_filter: {invalid}")
        multiples = tuple(sorted({round(float(v), 4) for v in self.multiples if float(v) > 0})) or (1.0,)
        hold_map = {str(k).upper(): int(v) for k, v in (self.hold_days_by_term or {}).items() if pd.notna(v)}
        merged_hold = dict(DEFAULT_HOLD_BY_TERM)
        merged_hold.update(hold_map)
        return TurnoverStrategyParams(
            interval_min=max(1, int(self.interval_min)),
            multiples=multiples,
            price_filters=filters,
            max_days_after=max(0, int(self.max_days_after)),
            aggregate_by=str(self.aggregate_by or "type"),
            cum_scope=str(self.cum_scope or "through_window"),
            unlock_terms=tuple(sorted({str(v).upper() for v in self.unlock_terms if str(v).strip()})),
            unlock_types=tuple(sorted({str(v) for v in self.unlock_types if str(v).strip()})),
            unlock_start_date=str(self.unlock_start_date) if self.unlock_start_date else None,
            unlock_end_date=str(self.unlock_end_date) if self.unlock_end_date else None,
            max_events=None if self.max_events in (None, 0) else max(1, int(self.max_events)),
            min_prev_close_vs_ipo=None if self.min_prev_close_vs_ipo in (None, "") else float(self.min_prev_close_vs_ipo),
            max_prev_close_vs_ipo=None if self.max_prev_close_vs_ipo in (None, "") else float(self.max_prev_close_vs_ipo),
            buy_cost=float(self.buy_cost),
            sell_cost=float(self.sell_cost),
            hold_days_by_term=merged_hold,
        )

    def cache_key(self) -> str:
        payload = asdict(self.normalized())
        return json.dumps(payload, sort_keys=True, ensure_ascii=False)


class TurnoverDailyPriceProvider:
    def __init__(
        self,
        *,
        cache_dir: Path,
        kis_client: KISClient | None = None,
        minute_db_path: Path | None = None,
        history_provider: DailyHistoryProvider | None = None,
    ) -> None:
        self.cache_dir = cache_dir
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.kis_client = kis_client
        self.minute_db_path = Path(minute_db_path) if minute_db_path else None
        self.history_provider = history_provider
        self._diagnostic_rows: list[dict[str, Any]] = []

    def fetch_daily_bars(self, symbol: str, start_date: str, end_date: str, adj_price: bool = True, use_cache: bool = True) -> pd.DataFrame:
        symbol = str(symbol).zfill(6)
        cache_path = self.cache_dir / f"{symbol}_{start_date}_{end_date}_{'adj' if adj_price else 'raw'}.csv"
        if use_cache and cache_path.exists():
            try:
                cached = pd.read_csv(cache_path, parse_dates=["date"])
                if not cached.empty:
                    self._diagnostic_rows.append({
                        "symbol": symbol,
                        "source": "daily_cache",
                        "rows": int(len(cached)),
                        "detail": str(cache_path),
                    })
                    return cached
            except Exception:
                pass

        frame = pd.DataFrame(columns=["date", "open", "high", "low", "close", "volume"])
        source = "empty"
        detail = ""
        if self.history_provider is not None:
            try:
                frame = self._normalize_daily_frame(self.history_provider(symbol, start_date, end_date))
                source = "history_provider"
            except Exception as exc:  # noqa: BLE001
                detail = f"history_provider_failed: {type(exc).__name__}: {exc}"

        if frame.empty and self.kis_client is not None:
            try:
                frame = self._normalize_daily_frame(self.kis_client.get_stock_history_chunked(symbol, start_date, end_date))
                source = "KIS"
                detail = ""
            except Exception as exc:  # noqa: BLE001
                detail = f"KIS_failed: {type(exc).__name__}: {exc}"

        if frame.empty and self.minute_db_path and self.minute_db_path.exists():
            try:
                frame = self._daily_from_minute_db(symbol, start_date, end_date)
                if not frame.empty:
                    source = "minute_db"
                    detail = ""
            except Exception as exc:  # noqa: BLE001
                if not detail:
                    detail = f"minute_db_failed: {type(exc).__name__}: {exc}"

        frame = self._normalize_daily_frame(frame)
        self._diagnostic_rows.append(
            {
                "symbol": symbol,
                "source": source,
                "rows": int(len(frame)),
                "detail": detail,
                "start_date": start_date,
                "end_date": end_date,
            }
        )
        if use_cache and not frame.empty:
            frame.to_csv(cache_path, index=False, encoding="utf-8-sig")
        return frame

    def diagnostics_frame(self) -> pd.DataFrame:
        if not self._diagnostic_rows:
            return pd.DataFrame(columns=["symbol", "source", "rows", "detail", "start_date", "end_date"])
        out = pd.DataFrame(self._diagnostic_rows)
        return out.drop_duplicates(subset=[c for c in ["symbol", "source", "start_date", "end_date"] if c in out.columns], keep="last").reset_index(drop=True)

    def _daily_from_minute_db(self, symbol: str, start_date: str, end_date: str) -> pd.DataFrame:
        if not self.minute_db_path or not self.minute_db_path.exists():
            return pd.DataFrame(columns=["date", "open", "high", "low", "close", "volume"])
        start_text = pd.Timestamp(start_date).strftime("%Y-%m-%d")
        end_text = pd.Timestamp(end_date).strftime("%Y-%m-%d")
        conn = sqlite3.connect(self.minute_db_path)
        try:
            bars = pd.read_sql_query(
                """
                SELECT trade_date, ts, open, high, low, close, volume
                FROM minute_bars
                WHERE symbol = ?
                  AND trade_date >= ?
                  AND trade_date <= ?
                ORDER BY ts
                """,
                conn,
                params=(symbol, start_text, end_text),
                parse_dates=["ts"],
            )
        finally:
            conn.close()
        if bars.empty:
            return pd.DataFrame(columns=["date", "open", "high", "low", "close", "volume"])
        bars["trade_date"] = pd.to_datetime(bars["trade_date"], errors="coerce")
        daily = (
            bars.groupby("trade_date", dropna=True)
            .agg(
                open=("open", "first"),
                high=("high", "max"),
                low=("low", "min"),
                close=("close", "last"),
                volume=("volume", "sum"),
            )
            .reset_index()
            .rename(columns={"trade_date": "date"})
        )
        return daily

    @staticmethod
    def _normalize_daily_frame(frame: pd.DataFrame | None) -> pd.DataFrame:
        if not isinstance(frame, pd.DataFrame) or frame.empty:
            return pd.DataFrame(columns=["date", "open", "high", "low", "close", "volume"])
        out = frame.copy()
        if "date" not in out.columns:
            if "trade_date" in out.columns:
                out = out.rename(columns={"trade_date": "date"})
            else:
                return pd.DataFrame(columns=["date", "open", "high", "low", "close", "volume"])
        out["date"] = pd.to_datetime(out["date"], errors="coerce")
        for col in ["open", "high", "low", "close", "volume"]:
            if col in out.columns:
                out[col] = pd.to_numeric(out[col], errors="coerce")
            else:
                out[col] = pd.NA
        out = out.dropna(subset=["date", "close"]).copy()
        if out.empty:
            return pd.DataFrame(columns=["date", "open", "high", "low", "close", "volume"])
        return out[["date", "open", "high", "low", "close", "volume"]].sort_values("date").drop_duplicates(subset=["date"], keep="last").reset_index(drop=True)


class TurnoverStrategyService:
    def __init__(
        self,
        base_dir: Path | str | None = None,
        *,
        kis_client: KISClient | None = None,
        history_provider: DailyHistoryProvider | None = None,
    ) -> None:
        self.base_dir = Path(base_dir) if base_dir is not None else None
        self.kis_client = kis_client
        self.history_provider = history_provider
        self.bridge = UnifiedLabBridgeService(self.base_dir)

    def run_workspace_research(
        self,
        workspace_hint: str | Path | None,
        params: TurnoverStrategyParams,
        *,
        allow_packaged_sample: bool = False,
    ) -> dict[str, Any]:
        normalized = params.normalized()
        paths = self.bridge.resolve_paths(workspace_hint, allow_packaged_sample=allow_packaged_sample)
        unlocks = self._load_unlocks(paths.unlock_csv)
        diagnostics_rows: list[dict[str, Any]] = [
            {
                "stage": "workspace",
                "ok": paths.workspace is not None,
                "detail": str(paths.workspace or ""),
                "rows": 0,
            },
            {
                "stage": "unlock_csv",
                "ok": paths.unlock_csv is not None and Path(paths.unlock_csv).exists(),
                "detail": str(paths.unlock_csv or ""),
                "rows": int(len(unlocks)),
            },
            {
                "stage": "minute_db",
                "ok": paths.minute_db_path is not None and Path(paths.minute_db_path).exists(),
                "detail": str(paths.minute_db_path or ""),
                "rows": 0,
            },
            {
                "stage": "daily_source",
                "ok": self.kis_client is not None or self.history_provider is not None or (paths.minute_db_path is not None and Path(paths.minute_db_path).exists()),
                "detail": "KIS" if self.kis_client is not None else ("history_provider" if self.history_provider is not None else "minute_db_fallback"),
                "rows": 0,
            },
        ]
        filtered_unlocks = self.filter_unlocks(unlocks, normalized)
        diagnostics_rows.append({
            "stage": "unlock_filter",
            "ok": True,
            "detail": normalized.cache_key(),
            "rows": int(len(filtered_unlocks)),
        })
        if filtered_unlocks.empty:
            return {
                "params": asdict(normalized),
                "paths": self._paths_dict(paths),
                "unlocks": filtered_unlocks,
                "signals": pd.DataFrame(),
                "misses": pd.DataFrame(),
                "trades": pd.DataFrame(),
                "summary": pd.DataFrame(),
                "annual": pd.DataFrame(),
                "skip_summary": pd.DataFrame(),
                "skip_reasons": pd.DataFrame(),
                "diagnostics": pd.DataFrame(diagnostics_rows),
            }

        if paths.minute_db_path is None or not Path(paths.minute_db_path).exists():
            diagnostics_rows.append({"stage": "signal_build", "ok": False, "detail": "minute_db_missing", "rows": 0})
            return {
                "params": asdict(normalized),
                "paths": self._paths_dict(paths),
                "unlocks": filtered_unlocks,
                "signals": pd.DataFrame(),
                "misses": pd.DataFrame(),
                "trades": pd.DataFrame(),
                "summary": pd.DataFrame(),
                "annual": pd.DataFrame(),
                "skip_summary": pd.DataFrame(),
                "skip_reasons": pd.DataFrame(),
                "diagnostics": pd.DataFrame(diagnostics_rows),
            }

        signals, misses = self._build_signals(filtered_unlocks, Path(paths.minute_db_path), normalized)
        diagnostics_rows.append({
            "stage": "signal_build",
            "ok": True,
            "detail": ", ".join(normalized.price_filters),
            "rows": int(len(signals)),
        })
        diagnostics_rows.append({
            "stage": "signal_miss",
            "ok": True,
            "detail": "generated",
            "rows": int(len(misses)),
        })

        provider = TurnoverDailyPriceProvider(
            cache_dir=self._daily_cache_dir(paths),
            kis_client=self.kis_client,
            minute_db_path=paths.minute_db_path,
            history_provider=self.history_provider,
        )
        costs = CostConfig(buy_cost=normalized.buy_cost, sell_cost=normalized.sell_cost)
        if signals.empty:
            trades = pd.DataFrame()
            summary = pd.DataFrame()
            annual = pd.DataFrame()
            skip_reasons = pd.DataFrame()
            skip_summary = pd.DataFrame()
        else:
            trades, summary, annual, skip_reasons, skip_summary = backtest_turnover_signals(
                signals=signals,
                bt=provider,
                costs=costs,
                hold_map=normalized.hold_days_by_term,
                filters={
                    "min_prev_close_vs_ipo": normalized.min_prev_close_vs_ipo,
                    "max_prev_close_vs_ipo": normalized.max_prev_close_vs_ipo,
                },
            )
        daily_diag = provider.diagnostics_frame()
        if not daily_diag.empty:
            daily_diag = daily_diag.copy()
            daily_diag["stage"] = "daily_fetch"
            diagnostics_rows.extend(daily_diag.to_dict(orient="records"))

        trades = parse_date_columns(trades, ["listing_date", "unlock_date", "entry_dt", "exit_dt", "prev_close_date"])
        signals = parse_date_columns(signals, ["listing_date", "unlock_date", "entry_ts", "entry_trade_date"])
        misses = parse_date_columns(misses, ["listing_date", "unlock_date"])
        skip_reasons = parse_date_columns(skip_reasons, ["listing_date", "unlock_date", "entry_trade_date"])

        return {
            "params": asdict(normalized),
            "paths": self._paths_dict(paths),
            "unlocks": filtered_unlocks.reset_index(drop=True),
            "signals": signals.reset_index(drop=True),
            "misses": misses.reset_index(drop=True),
            "trades": trades.reset_index(drop=True),
            "summary": summary.reset_index(drop=True),
            "annual": annual.reset_index(drop=True),
            "skip_summary": skip_summary.reset_index(drop=True),
            "skip_reasons": skip_reasons.reset_index(drop=True),
            "diagnostics": pd.DataFrame(diagnostics_rows).reset_index(drop=True),
        }

    def summarize_existing_workspace_results(
        self,
        trades: pd.DataFrame,
        *,
        multiples: Iterable[float] | None = None,
        price_filters: Iterable[str] | None = None,
        terms: Iterable[str] | None = None,
        unlock_types: Iterable[str] | None = None,
        min_prev_close_vs_ipo: float | None = None,
        max_prev_close_vs_ipo: float | None = None,
    ) -> dict[str, pd.DataFrame]:
        work = trades.copy() if isinstance(trades, pd.DataFrame) else pd.DataFrame()
        if work.empty:
            return {"trades": pd.DataFrame(), "summary": pd.DataFrame(), "annual": pd.DataFrame()}
        if multiples:
            wanted = {round(float(v), 4) for v in multiples}
            work = work[pd.to_numeric(work.get("multiple"), errors="coerce").round(4).isin(wanted)].copy()
        if price_filters:
            work = work[work.get("price_filter", pd.Series(dtype="object")).astype(str).isin([str(v) for v in price_filters])].copy()
        if terms:
            work = work[work.get("term", pd.Series(dtype="object")).astype(str).str.upper().isin([str(v).upper() for v in terms])].copy()
        if unlock_types:
            work = work[work.get("unlock_type", pd.Series(dtype="object")).astype(str).isin([str(v) for v in unlock_types])].copy()
        prev_ratio = pd.to_numeric(work.get("prev_close_vs_ipo"), errors="coerce")
        if min_prev_close_vs_ipo is not None:
            work = work[prev_ratio >= float(min_prev_close_vs_ipo)].copy()
            prev_ratio = pd.to_numeric(work.get("prev_close_vs_ipo"), errors="coerce")
        if max_prev_close_vs_ipo is not None:
            work = work[prev_ratio <= float(max_prev_close_vs_ipo)].copy()

        if work.empty:
            return {"trades": work, "summary": pd.DataFrame(), "annual": pd.DataFrame()}

        grouped = (
            work.groupby(["signal_name", "term", "unlock_type", "multiple", "price_filter", "hold_days_after_entry"], dropna=False)
            .agg(
                trades=("symbol", "size"),
                win_rate=("net_ret", lambda x: float((pd.to_numeric(x, errors="coerce") > 0).mean())),
                avg_ret=("net_ret", lambda x: float(pd.to_numeric(x, errors="coerce").mean())),
                median_ret=("net_ret", lambda x: float(pd.to_numeric(x, errors="coerce").median())),
                sum_ret=("net_ret", lambda x: float(pd.to_numeric(x, errors="coerce").sum())),
                compound_ret=("net_ret", lambda x: float((1 + pd.to_numeric(x, errors="coerce")).prod() - 1)),
                min_ret=("net_ret", lambda x: float(pd.to_numeric(x, errors="coerce").min())),
                max_ret=("net_ret", lambda x: float(pd.to_numeric(x, errors="coerce").max())),
            )
            .reset_index()
        )
        annual = pd.DataFrame()
        if "entry_dt" in work.columns:
            temp = work.copy()
            temp["year"] = pd.to_datetime(temp["entry_dt"], errors="coerce").dt.year
            temp = temp.dropna(subset=["year"]).copy()
            if not temp.empty:
                annual = (
                    temp.groupby(["year", "signal_name", "term", "unlock_type", "multiple", "price_filter", "hold_days_after_entry"], dropna=False)
                    .agg(
                        trades=("symbol", "size"),
                        win_rate=("net_ret", lambda x: float((pd.to_numeric(x, errors="coerce") > 0).mean())),
                        avg_ret=("net_ret", lambda x: float(pd.to_numeric(x, errors="coerce").mean())),
                        median_ret=("net_ret", lambda x: float(pd.to_numeric(x, errors="coerce").median())),
                        sum_ret=("net_ret", lambda x: float(pd.to_numeric(x, errors="coerce").sum())),
                        compound_ret=("net_ret", lambda x: float((1 + pd.to_numeric(x, errors="coerce")).prod() - 1)),
                        min_ret=("net_ret", lambda x: float(pd.to_numeric(x, errors="coerce").min())),
                        max_ret=("net_ret", lambda x: float(pd.to_numeric(x, errors="coerce").max())),
                    )
                    .reset_index()
                )
        return {
            "trades": work.reset_index(drop=True),
            "summary": grouped.reset_index(drop=True),
            "annual": annual.reset_index(drop=True),
        }

    def filter_unlocks(self, unlocks: pd.DataFrame, params: TurnoverStrategyParams) -> pd.DataFrame:
        work = unlocks.copy() if isinstance(unlocks, pd.DataFrame) else pd.DataFrame()
        if work.empty:
            return pd.DataFrame()
        if "unlock_date" in work.columns:
            work["unlock_date"] = pd.to_datetime(work["unlock_date"], errors="coerce")
            work = work.dropna(subset=["unlock_date"]).copy()
        if params.unlock_terms:
            work = work[work.get("term", pd.Series(dtype="object")).astype(str).str.upper().isin(params.unlock_terms)].copy()
        if params.unlock_types:
            work = work[work.get("unlock_type", pd.Series(dtype="object")).astype(str).isin(params.unlock_types)].copy()
        if params.unlock_start_date:
            work = work[work["unlock_date"] >= pd.Timestamp(params.unlock_start_date)].copy()
        if params.unlock_end_date:
            work = work[work["unlock_date"] <= pd.Timestamp(params.unlock_end_date)].copy()
        work = work.sort_values([c for c in ["unlock_date", "listing_date", "symbol", "term"] if c in work.columns], ascending=[False, False, True, True]).reset_index(drop=True)
        if params.max_events:
            work = work.head(int(params.max_events)).copy()
            work = work.sort_values([c for c in ["unlock_date", "listing_date", "symbol", "term"] if c in work.columns]).reset_index(drop=True)
        return work

    def _build_signals(self, unlocks: pd.DataFrame, minute_db_path: Path, params: TurnoverStrategyParams) -> tuple[pd.DataFrame, pd.DataFrame]:
        signal_frames: list[pd.DataFrame] = []
        miss_frames: list[pd.DataFrame] = []
        conn = sqlite3.connect(minute_db_path)
        try:
            for price_filter in params.price_filters:
                hits, misses = build_turnover_signals(
                    unlock_df=unlocks,
                    conn=conn,
                    interval_min=params.interval_min,
                    multiples=list(params.multiples),
                    price_filter=price_filter,
                    max_days_after=params.max_days_after,
                    aggregate_by=params.aggregate_by,
                    cum_scope=params.cum_scope,
                )
                if not hits.empty:
                    signal_frames.append(hits)
                if not misses.empty:
                    miss_frames.append(misses)
        finally:
            conn.close()
        hits_out = pd.concat(signal_frames, ignore_index=True) if signal_frames else pd.DataFrame()
        misses_out = pd.concat(miss_frames, ignore_index=True) if miss_frames else pd.DataFrame()
        return hits_out, misses_out

    @staticmethod
    def _load_unlocks(path: Path | None) -> pd.DataFrame:
        if path is None or not Path(path).exists():
            return pd.DataFrame()
        df = pd.read_csv(path, dtype={"symbol": str})
        return parse_date_columns(df, ["listing_date", "unlock_date", "lockup_end_date"])

    @staticmethod
    def _paths_dict(paths: UnifiedLabPaths) -> dict[str, str]:
        return {
            "workspace": str(paths.workspace or ""),
            "unlock_csv": str(paths.unlock_csv or ""),
            "minute_db_path": str(paths.minute_db_path or ""),
            "signals_csv": str(paths.signals_csv or ""),
            "turnover_backtest_dir": str(paths.turnover_backtest_dir or ""),
        }

    def _daily_cache_dir(self, paths: UnifiedLabPaths) -> Path:
        base = self.base_dir or Path.cwd() / "data"
        if paths.workspace is not None:
            tag = paths.workspace.name
        else:
            tag = today_kst().strftime("%Y%m%d")
        return Path(base) / "cache" / "turnover_daily" / tag
