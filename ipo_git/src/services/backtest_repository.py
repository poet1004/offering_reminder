from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd

from src.utils import data_dir, parse_date_columns


DATE_COLS = ["listing_date", "unlock_date", "entry_dt", "exit_dt", "prev_close_date"]
TRADE_NUMERIC_COLS = ["entry_price", "exit_price", "hold_days_after_entry", "ipo_price", "prev_close_vs_ipo", "entry_price_vs_ipo", "gross_ret", "net_ret"]
PRESET_THRESHOLD_TO_VERSION = {
    1.0: "1.0",
    1.2: "1.2",
    1.5: "1.5",
    2.0: "2.0",
}


class BacktestRepository:
    def __init__(self, base_dir: Path | str | None = None) -> None:
        base = Path(base_dir) if base_dir is not None else data_dir()
        self.base_dir = base / "backtest"

    def available_versions(self) -> list[str]:
        versions = []
        for path in sorted(self.base_dir.glob("v*_summary_all_pretty.csv"), reverse=True):
            stem = path.stem.replace("_summary_all_pretty", "")
            version = stem.replace("v", "", 1).replace("_", ".")
            versions.append(version)
        return versions or ["2.0", "1.5", "1.2", "1.0"]

    def _path(self, version: str, suffix: str) -> Path:
        return self.base_dir / f"v{version.replace('.', '_')}_{suffix}"

    def versions_summary(self) -> pd.DataFrame:
        path = self.base_dir / "versions_summary_pretty.csv"
        if not path.exists():
            return pd.DataFrame()
        return pd.read_csv(path)

    def load_summary(self, version: str, pretty: bool = True) -> pd.DataFrame:
        suffix = "summary_all_pretty.csv" if pretty else "summary_all.csv"
        path = self._path(version, suffix)
        if not path.exists():
            return pd.DataFrame()
        return pd.read_csv(path)

    def load_annual(self, version: str, pretty: bool = True) -> pd.DataFrame:
        suffix = "annual_all_pretty.csv" if pretty else "annual_all.csv"
        path = self._path(version, suffix)
        if not path.exists():
            return pd.DataFrame()
        return pd.read_csv(path)

    def load_trades(self, version: str) -> pd.DataFrame:
        path = self._path(version, "all_trades.csv")
        if not path.exists():
            return pd.DataFrame()
        df = pd.read_csv(path)
        return self._coerce_trades(df)

    def load_skip_summary(self, version: str) -> pd.DataFrame:
        path = self._path(version, "backtest_skip_summary.csv")
        if not path.exists():
            return pd.DataFrame()
        return pd.read_csv(path)

    def load_skip_reasons(self, version: str) -> pd.DataFrame:
        path = self._path(version, "backtest_skip_reasons.csv")
        if not path.exists():
            return pd.DataFrame()
        df = pd.read_csv(path)
        return parse_date_columns(df, ["listing_date", "unlock_date"])

    def best_term_edge(self, version: str) -> pd.DataFrame:
        summary = self.load_summary(version, pretty=True)
        if summary.empty or "term" not in summary.columns:
            return pd.DataFrame()
        numeric_cols = [c for c in ["trades", "win_rate", "avg_ret", "median_ret", "sum_ret", "compound_ret", "max_ret", "min_ret"] if c in summary.columns]
        grouped = summary.groupby("term", as_index=False)[numeric_cols].mean(numeric_only=True)
        if not grouped.empty and "compound_ret" in grouped.columns:
            grouped = grouped.sort_values("compound_ret", ascending=False).reset_index(drop=True)
        return grouped

    def preset_version_for_threshold(self, min_prev_close_vs_ipo: float) -> str | None:
        rounded = round(float(min_prev_close_vs_ipo), 2)
        return PRESET_THRESHOLD_TO_VERSION.get(rounded)

    def custom_threshold_view(self, min_prev_close_vs_ipo: float, base_version: str = "1.0") -> dict[str, Any]:
        base_trades = self.load_trades(base_version)
        filtered = self.filter_trades_by_min_prev_close_ratio(min_prev_close_vs_ipo, source_version=base_version, base_trades=base_trades)
        excluded = self.excluded_trade_summary(min_prev_close_vs_ipo, source_version=base_version, base_trades=base_trades, filtered_trades=filtered)
        metrics = {
            "base_trade_count": int(len(base_trades)),
            "filtered_trade_count": int(len(filtered)),
            "excluded_trade_count": int(max(len(base_trades) - len(filtered), 0)),
            "matching_preset_version": self.preset_version_for_threshold(min_prev_close_vs_ipo),
        }
        return {
            "summary": self.summarize_trades(filtered),
            "annual": self.summarize_trades(filtered, by_year=True),
            "trades": filtered,
            "excluded_summary": excluded,
            "metrics": metrics,
        }

    def filter_trades_by_min_prev_close_ratio(
        self,
        min_prev_close_vs_ipo: float,
        *,
        source_version: str = "1.0",
        base_trades: pd.DataFrame | None = None,
    ) -> pd.DataFrame:
        trades = self._coerce_trades(base_trades if base_trades is not None else self.load_trades(source_version))
        if trades.empty:
            return trades
        work = trades.copy()
        ratio = pd.to_numeric(work.get("prev_close_vs_ipo"), errors="coerce")
        return work.loc[ratio >= float(min_prev_close_vs_ipo)].reset_index(drop=True)

    def excluded_trade_summary(
        self,
        min_prev_close_vs_ipo: float,
        *,
        source_version: str = "1.0",
        base_trades: pd.DataFrame | None = None,
        filtered_trades: pd.DataFrame | None = None,
    ) -> pd.DataFrame:
        base = self._coerce_trades(base_trades if base_trades is not None else self.load_trades(source_version))
        filtered = self._coerce_trades(filtered_trades) if filtered_trades is not None else self.filter_trades_by_min_prev_close_ratio(min_prev_close_vs_ipo, source_version=source_version, base_trades=base)
        if base.empty:
            return pd.DataFrame()
        ratio = pd.to_numeric(base.get("prev_close_vs_ipo"), errors="coerce")
        excluded = base.loc[ratio < float(min_prev_close_vs_ipo)].copy()
        if excluded.empty:
            return pd.DataFrame(columns=["term", "strategy_name", "entry_mode", "count", "min_prev_close_vs_ipo", "min_ratio", "max_ratio", "avg_ratio"])
        grouped = (
            excluded.groupby(["term", "strategy_name", "entry_mode"], dropna=False)
            .agg(
                count=("name", "size"),
                min_ratio=("prev_close_vs_ipo", "min"),
                max_ratio=("prev_close_vs_ipo", "max"),
                avg_ratio=("prev_close_vs_ipo", "mean"),
            )
            .reset_index()
        )
        grouped["min_prev_close_vs_ipo"] = round(float(min_prev_close_vs_ipo), 2)
        grouped["avg_ratio"] = grouped["avg_ratio"].round(3)
        grouped["min_ratio"] = grouped["min_ratio"].round(3)
        grouped["max_ratio"] = grouped["max_ratio"].round(3)
        grouped = grouped[["term", "strategy_name", "entry_mode", "count", "min_prev_close_vs_ipo", "min_ratio", "max_ratio", "avg_ratio"]]
        return grouped.sort_values(["count", "term", "strategy_name"], ascending=[False, True, True]).reset_index(drop=True)

    def summarize_trades(self, trades: pd.DataFrame, *, by_year: bool = False) -> pd.DataFrame:
        work = self._coerce_trades(trades)
        if work.empty:
            return pd.DataFrame()

        group_cols = ["strategy_name", "term", "entry_mode", "hold_days_after_entry"]
        if by_year:
            if "entry_dt" not in work.columns:
                return pd.DataFrame()
            work = work.copy()
            work["year"] = pd.to_datetime(work["entry_dt"], errors="coerce").dt.year
            work = work.dropna(subset=["year"]).copy()
            if work.empty:
                return pd.DataFrame()
            work["year"] = work["year"].astype(int)
            group_cols = ["year"] + group_cols

        records: list[dict[str, Any]] = []
        for keys, group in work.groupby(group_cols, dropna=False):
            if not isinstance(keys, tuple):
                keys = (keys,)
            record = dict(zip(group_cols, keys))
            returns = pd.to_numeric(group.get("net_ret"), errors="coerce").dropna()
            if returns.empty:
                continue
            record["trades"] = int(len(returns))
            record["win_rate"] = round(float((returns > 0).mean() * 100.0), 2)
            record["avg_ret"] = round(float(returns.mean() * 100.0), 2)
            record["median_ret"] = round(float(returns.median() * 100.0), 2)
            record["sum_ret"] = round(float(returns.sum() * 100.0), 2)
            record["compound_ret"] = round(float(((1.0 + returns).prod() - 1.0) * 100.0), 2)
            record["min_ret"] = round(float(returns.min() * 100.0), 2)
            record["max_ret"] = round(float(returns.max() * 100.0), 2)
            records.append(record)

        out = pd.DataFrame(records)
        if out.empty:
            return out
        sort_cols = ["hold_days_after_entry", "strategy_name"]
        if by_year:
            sort_cols = ["year", *sort_cols]
        out = out.sort_values(sort_cols, ascending=True, na_position="last").reset_index(drop=True)
        return out

    def _coerce_trades(self, trades: pd.DataFrame) -> pd.DataFrame:
        if not isinstance(trades, pd.DataFrame):
            return pd.DataFrame()
        work = trades.copy()
        work = parse_date_columns(work, DATE_COLS)
        for col in TRADE_NUMERIC_COLS:
            if col in work.columns:
                work[col] = pd.to_numeric(work[col], errors="coerce")
        return work
