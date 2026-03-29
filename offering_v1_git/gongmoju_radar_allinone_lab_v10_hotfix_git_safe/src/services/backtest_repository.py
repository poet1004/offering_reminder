from __future__ import annotations

from pathlib import Path

import pandas as pd

from src.utils import data_dir, parse_date_columns


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
        return parse_date_columns(df, ["listing_date", "unlock_date", "entry_dt", "exit_dt", "prev_close_date"])

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
