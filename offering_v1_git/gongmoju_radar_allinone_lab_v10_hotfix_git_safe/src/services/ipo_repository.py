from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd

from src.services.ipo_scrapers import find_local_kind_export
from src.utils import (
    clean_issue_frame,
    data_dir,
    detect_existing_file,
    normalize_name_key,
    normalize_symbol_text,
    parse_date_columns,
    safe_bool,
    safe_float,
    standardize_issue_frame,
    today_kst,
)


class IPORepository:
    def __init__(self, base_dir: Path | str | None = None) -> None:
        self.base_dir = Path(base_dir) if base_dir is not None else data_dir()

    def load_sample_issues(self) -> pd.DataFrame:
        path = self.base_dir / "sample_ipo_events.csv"
        df = pd.read_csv(path)
        df = parse_date_columns(df)
        df["name_key"] = df.get("name_key", pd.Series(dtype="object"))
        df["name_key"] = df["name_key"].fillna(df["name"].map(normalize_name_key))
        df["source"] = df.get("source", "sample")
        df["source_detail"] = df.get("source_detail", "packaged-sample")
        return standardize_issue_frame(df)

    def auto_detect_external_unlock_dataset(self, allow_packaged_sample: bool = False) -> Path | None:
        roots = [Path.cwd(), Path.cwd().parent, self.base_dir.parent]
        candidates: list[Path] = []
        for root in roots:
            candidates.extend(
                [
                    root / "dataset_out" / "synthetic_ipo_events.csv",
                    root / "workspace" / "unlock_out" / "unlock_events_backtest_input.csv",
                    root / "ipo_lockup_unified_lab" / "workspace" / "unlock_out" / "unlock_events_backtest_input.csv",
                    root / "integrated_lab" / "ipo_lockup_unified_lab" / "workspace" / "unlock_out" / "unlock_events_backtest_input.csv",
                    root / "integrated_lab" / "workspace" / "unlock_out" / "unlock_events_backtest_input.csv",
                    root / "lab" / "ipo_lockup_unified_lab" / "workspace" / "unlock_out" / "unlock_events_backtest_input.csv",
                    root / "ipo_lockup_unified_lab" / "dataset_out" / "synthetic_ipo_events.csv",
                    root / "ipo_lockup_runner_fastdaily" / "dataset_out" / "synthetic_ipo_events.csv",
                ]
            )

        desktop = Path.home() / "Desktop"
        downloads = Path.home() / "Downloads"
        for root in [desktop / "한국투자증권", desktop, downloads]:
            candidates.extend(
                [
                    root / "dataset_out" / "synthetic_ipo_events.csv",
                    root / "workspace" / "unlock_out" / "unlock_events_backtest_input.csv",
                    root / "ipo_lockup_unified_lab" / "workspace" / "unlock_out" / "unlock_events_backtest_input.csv",
                    root / "integrated_lab" / "ipo_lockup_unified_lab" / "workspace" / "unlock_out" / "unlock_events_backtest_input.csv",
                    root / "ipo_lockup_unified_lab" / "dataset_out" / "synthetic_ipo_events.csv",
                ]
            )

        if allow_packaged_sample:
            candidates.extend(
                [
                    self.base_dir / "sample_unified_lab_workspace" / "unlock_out" / "unlock_events_backtest_input.csv",
                    self.base_dir.parent / "data" / "sample_unified_lab_workspace" / "unlock_out" / "unlock_events_backtest_input.csv",
                ]
            )
        return detect_existing_file(candidates)

    def auto_detect_local_kind_export(self, include_home_dirs: bool = False) -> Path | None:
        candidates = [
            self.base_dir / "uploads" / "kind_latest.xlsx",
            self.base_dir / "uploads" / "kind_latest.csv",
            self.base_dir.parent / "kind_latest.xlsx",
            self.base_dir.parent / "kind_latest.csv",
            self.base_dir.parent / "integrated_lab" / "ipo_lockup_unified_lab" / "workspace" / "dataset_out" / "kind_ipo_master.csv",
            self.base_dir.parent / "integrated_lab" / "ipo_lockup_unified_lab" / "workspace" / "dataset_out" / "live_issue_seed.csv",
            self.base_dir.parent / "integrated_lab" / "ipo_lockup_unified_lab" / "kind_master.csv",
            self.base_dir.parent / "ipo_lockup_unified_lab" / "workspace" / "dataset_out" / "kind_ipo_master.csv",
            self.base_dir.parent / "ipo_lockup_unified_lab" / "workspace" / "dataset_out" / "live_issue_seed.csv",
            self.base_dir.parent / "ipo_lockup_unified_lab" / "kind_master.csv",
        ]
        detected = detect_existing_file(candidates)
        if detected is not None:
            return detected
        if include_home_dirs:
            return find_local_kind_export(self.base_dir.parent)
        return None

    def auto_detect_dart_enriched_export(self) -> Path | None:
        candidates = [
            self.base_dir / "uploads" / "dart_enriched_latest.csv",
            self.base_dir.parent / "dart_enriched_latest.csv",
        ]
        return detect_existing_file(candidates)

    def load_dart_enriched_export(self, dataset_path: str | Path | None = None) -> pd.DataFrame:
        if dataset_path:
            path = Path(dataset_path).expanduser().resolve()
        else:
            path = self.auto_detect_dart_enriched_export()
        if path is None or not path.exists():
            return pd.DataFrame()
        df = pd.read_csv(path)
        df = parse_date_columns(df)
        return standardize_issue_frame(df)

    def load_external_unlock_events(self, dataset_path: str | Path | None = None, allow_packaged_sample: bool = False) -> pd.DataFrame:
        if dataset_path:
            path = Path(dataset_path).expanduser().resolve()
        else:
            path = self.auto_detect_external_unlock_dataset(allow_packaged_sample=allow_packaged_sample)
        if path is None or not path.exists():
            return pd.DataFrame(columns=["name", "symbol", "listing_date", "unlock_date", "term", "ipo_price"])
        df = pd.read_csv(path)
        df = parse_date_columns(df, ["listing_date", "unlock_date"])
        required = ["name", "symbol", "listing_date", "unlock_date", "term"]
        for col in required:
            if col not in df.columns:
                df[col] = pd.NA
        if "ipo_price" not in df.columns:
            if "offer_price" in df.columns:
                df["ipo_price"] = df["offer_price"]
            else:
                df["ipo_price"] = pd.NA
        df["name_key"] = df.get("name_key", pd.Series(dtype="object"))
        df["name_key"] = df["name_key"].fillna(df["name"].map(normalize_name_key))
        return df[required + ["ipo_price", "name_key"]].copy()

    def unlock_calendar_from_issues(self, issues: pd.DataFrame) -> pd.DataFrame:
        term_map = {
            "unlock_date_15d": "15D",
            "unlock_date_1m": "1M",
            "unlock_date_3m": "3M",
            "unlock_date_6m": "6M",
            "unlock_date_1y": "1Y",
        }
        rows: list[dict[str, Any]] = []
        for _, row in issues.iterrows():
            for col, term in term_map.items():
                unlock_date = row.get(col)
                if pd.isna(unlock_date):
                    continue
                rows.append(
                    {
                        "name": row.get("name"),
                        "name_key": normalize_name_key(row.get("name")),
                        "symbol": row.get("symbol"),
                        "market": row.get("market"),
                        "listing_date": row.get("listing_date"),
                        "unlock_date": unlock_date,
                        "term": term,
                        "offer_price": row.get("offer_price"),
                        "current_price": row.get("current_price"),
                        "stage": row.get("stage"),
                        "source": row.get("source", "sample"),
                    }
                )
        out = pd.DataFrame(rows)
        if out.empty:
            return out
        out = parse_date_columns(out, ["listing_date", "unlock_date"])
        return out.sort_values(["unlock_date", "name"]).reset_index(drop=True)

    def upcoming_subscriptions(self, issues: pd.DataFrame, today: pd.Timestamp, window_days: int = 30) -> pd.DataFrame:
        out = clean_issue_frame(issues.copy())
        mask = (
            out["subscription_start"].notna()
            & (out["subscription_start"] >= today - pd.Timedelta(days=7))
            & (out["subscription_start"] <= today + pd.Timedelta(days=window_days))
        )
        out = out.loc[mask].sort_values(["subscription_start", "name"]).reset_index(drop=True)
        return out

    def upcoming_listings(self, issues: pd.DataFrame, today: pd.Timestamp, window_days: int = 30) -> pd.DataFrame:
        out = clean_issue_frame(issues.copy())
        mask = (
            out["listing_date"].notna()
            & (out["listing_date"] >= today - pd.Timedelta(days=15))
            & (out["listing_date"] <= today + pd.Timedelta(days=window_days))
        )
        out = out.loc[mask].sort_values(["listing_date", "name"]).reset_index(drop=True)
        return out

    def upcoming_unlocks(
        self,
        unlocks: pd.DataFrame,
        today: pd.Timestamp,
        window_days: int = 45,
    ) -> pd.DataFrame:
        if unlocks.empty:
            return unlocks.copy()
        out = unlocks.copy()
        mask = (
            out["unlock_date"].notna()
            & (out["unlock_date"] >= today - pd.Timedelta(days=7))
            & (out["unlock_date"] <= today + pd.Timedelta(days=window_days))
        )
        out = out.loc[mask].sort_values(["unlock_date", "name"]).reset_index(drop=True)
        return out

    def alert_candidates(
        self,
        issues: pd.DataFrame,
        unlocks: pd.DataFrame,
        today: pd.Timestamp,
        unlock_alert_days: int = 7,
        move_threshold_pct: float = 5.0,
        volume_spike_ratio: float = 3.0,
    ) -> pd.DataFrame:
        rows: list[dict[str, Any]] = []
        if not unlocks.empty:
            unlock_window = unlocks[
                unlocks["unlock_date"].between(today, today + pd.Timedelta(days=unlock_alert_days), inclusive="both")
            ]
            for _, row in unlock_window.iterrows():
                rows.append(
                    {
                        "alert_type": "보호예수 해제 임박",
                        "name": row.get("name"),
                        "symbol": row.get("symbol"),
                        "when": row.get("unlock_date"),
                        "detail": f"{row.get('term')} 해제 예정",
                    }
                )
        if not issues.empty:
            for _, row in issues.iterrows():
                day_change = abs(safe_float(row.get("day_change_pct"), 0.0) or 0.0)
                spike = safe_float(row.get("volume_spike_ratio"), 0.0) or 0.0
                flag = safe_bool(row.get("unusual_move_flag"), False)
                if flag or day_change >= move_threshold_pct or spike >= volume_spike_ratio:
                    rows.append(
                        {
                            "alert_type": "이례적 가격변동",
                            "name": row.get("name"),
                            "symbol": row.get("symbol"),
                            "when": today,
                            "detail": f"등락률 {day_change:.2f}%, 거래량 배수 {spike:.2f}",
                        }
                    )
        out = pd.DataFrame(rows)
        if out.empty:
            return out
        out = parse_date_columns(out, ["when"])
        return out.sort_values(["when", "alert_type", "name"]).reset_index(drop=True)

    def dashboard_metrics(
        self,
        issues: pd.DataFrame,
        unlocks: pd.DataFrame,
        today: pd.Timestamp,
    ) -> dict[str, int]:
        return {
            "subscription_count": int(len(self.upcoming_subscriptions(issues, today, window_days=14))),
            "listing_count": int(len(self.upcoming_listings(issues, today, window_days=14))),
            "unlock_count": int(len(self.upcoming_unlocks(unlocks, today, window_days=30))),
            "alert_count": int(len(self.alert_candidates(issues, unlocks, today))),
        }

    def build_timeline(self, issues: pd.DataFrame, unlocks: pd.DataFrame, today: pd.Timestamp, window_days: int = 30) -> pd.DataFrame:
        rows: list[dict[str, Any]] = []
        for _, row in self.upcoming_subscriptions(issues, today, window_days).iterrows():
            rows.append({"date": row.get("subscription_start"), "event_type": "청약시작", "name": row.get("name"), "detail": row.get("underwriters")})
            rows.append({"date": row.get("subscription_end"), "event_type": "청약종료", "name": row.get("name"), "detail": row.get("underwriters")})
        for _, row in self.upcoming_listings(issues, today, window_days).iterrows():
            rows.append({"date": row.get("listing_date"), "event_type": "상장", "name": row.get("name"), "detail": row.get("market")})
        for _, row in self.upcoming_unlocks(unlocks, today, window_days).iterrows():
            rows.append({"date": row.get("unlock_date"), "event_type": "보호예수해제", "name": row.get("name"), "detail": row.get("term")})
        out = pd.DataFrame(rows)
        if out.empty:
            return out
        out = parse_date_columns(out, ["date"])
        return out.sort_values(["date", "event_type", "name"]).reset_index(drop=True)

    def latest_data_timestamp(self, issues: pd.DataFrame) -> pd.Timestamp | None:
        if issues.empty or "last_refresh_ts" not in issues.columns:
            return None
        ts = pd.to_datetime(issues["last_refresh_ts"], errors="coerce")
        if ts.dropna().empty:
            return None
        return ts.max()

    def merge_price_snapshot(self, issues: pd.DataFrame, price_df: pd.DataFrame) -> pd.DataFrame:
        if issues.empty or price_df.empty:
            return issues.copy()
        out = issues.copy()
        if "symbol" not in price_df.columns:
            return out
        price_df = price_df.copy()
        price_df["symbol"] = price_df["symbol"].map(normalize_symbol_text)
        out["symbol"] = out["symbol"].map(normalize_symbol_text)
        price_df = price_df.dropna(subset=["symbol"]).copy()
        if price_df.empty:
            return out
        merged = out.merge(
            price_df[["symbol", "price", "change_pct"]].rename(columns={"price": "live_current_price", "change_pct": "live_day_change_pct"}),
            on="symbol",
            how="left",
        )
        merged["current_price"] = merged["live_current_price"].combine_first(merged["current_price"])
        merged["day_change_pct"] = merged["live_day_change_pct"].combine_first(merged["day_change_pct"])
        return merged.drop(columns=[c for c in ["live_current_price", "live_day_change_pct"] if c in merged.columns])
