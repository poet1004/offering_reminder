from __future__ import annotations

import math
import re
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

import pandas as pd
from pandas.errors import EmptyDataError

from src.utils import detect_existing_file, normalize_name_key, parse_date_columns, safe_float, today_kst


DATE_COLUMNS_COMMON = [
    "listing_date",
    "unlock_date",
    "lockup_end_date",
    "entry_ts",
    "entry_trade_date",
    "entry_dt",
    "exit_dt",
    "prev_close_date",
    "min_ts",
    "max_ts",
    "start_ts",
    "end_ts",
    "created_at",
    "updated_at",
]


@dataclass
class UnifiedLabPaths:
    workspace: Path | None
    unlock_csv: Path | None
    signals_csv: Path | None
    misses_csv: Path | None
    minute_db_path: Path | None
    turnover_backtest_dir: Path | None
    turnover_summary_csv: Path | None
    turnover_summary_pretty_csv: Path | None
    turnover_annual_csv: Path | None
    turnover_annual_pretty_csv: Path | None
    turnover_trades_csv: Path | None
    turnover_skip_summary_csv: Path | None
    turnover_skip_reasons_csv: Path | None
    beta_summary_csv: Path | None
    beta_trades_csv: Path | None


@dataclass
class UnifiedLabBundle:
    paths: UnifiedLabPaths
    unlocks: pd.DataFrame
    signals: pd.DataFrame
    misses: pd.DataFrame
    turnover_summary_raw: pd.DataFrame
    turnover_summary_pretty: pd.DataFrame
    turnover_annual_raw: pd.DataFrame
    turnover_annual_pretty: pd.DataFrame
    turnover_trades: pd.DataFrame
    turnover_skip_summary: pd.DataFrame
    turnover_skip_reasons: pd.DataFrame
    beta_summary: pd.DataFrame
    beta_trades: pd.DataFrame
    minute_job_counts: pd.DataFrame
    minute_jobs: pd.DataFrame
    minute_job_preview: pd.DataFrame
    minute_bar_stats: pd.DataFrame
    minute_unlock_events: pd.DataFrame
    minute_symbol_coverage: pd.DataFrame
    source_status: pd.DataFrame


class UnifiedLabBridgeService:
    def __init__(self, base_dir: Path | str | None = None) -> None:
        self.base_dir = Path(base_dir) if base_dir is not None else None

    def auto_detect_workspace(self, workspace_hint: str | Path | None = None, allow_packaged_sample: bool = False) -> Path | None:
        if workspace_hint:
            path = Path(workspace_hint).expanduser().resolve()
            if self._looks_like_workspace(path):
                return path
            nested = self._search_workspace_under(path)
            if nested is not None:
                return nested

        manifest_path = None
        if self.base_dir is not None:
            runtime_dir = self.base_dir / "runtime"
            manifests = sorted(runtime_dir.glob("import_unified_lab_*.json"), key=lambda p: p.stat().st_mtime, reverse=True) if runtime_dir.exists() else []
            if manifests:
                manifest_path = manifests[0]
        if manifest_path is not None:
            try:
                import json

                payload = json.loads(manifest_path.read_text(encoding="utf-8"))
                for key in ["detected_workspace", "extracted_root", "out_dir"]:
                    candidate_text = str(payload.get(key) or "").strip()
                    if not candidate_text:
                        continue
                    candidate = Path(candidate_text).expanduser()
                    if self._looks_like_workspace(candidate):
                        return candidate.resolve()
                    nested = self._search_workspace_under(candidate)
                    if nested is not None:
                        return nested
            except Exception:
                pass

        candidates: list[Path] = []
        roots = [Path.cwd()]
        if self.base_dir is not None:
            roots.extend([self.base_dir, self.base_dir.parent, self.base_dir.parent.parent])
        for root in roots:
            root_candidates = [
                root / "workspace",
                root / "ipo_lockup_unified_lab" / "workspace",
                root / "ipo_lockup_runner_fastdaily" / "workspace",
                root / "integrated_lab" / "workspace",
                root / "integrated_lab" / "ipo_lockup_unified_lab" / "workspace",
                root / "lab" / "workspace",
                root / "lab" / "ipo_lockup_unified_lab" / "workspace",
                root / "ipo_lockup_unified_lab_workspace",
                root / "ipo_lockup_workspace",
                root / "unified_lab_workspace",
                root / "data" / "imports",
            ]
            if allow_packaged_sample:
                root_candidates = [
                    root / "data" / "sample_unified_lab_workspace",
                    root / "sample_unified_lab_workspace",
                    *root_candidates,
                ]
            candidates.extend(root_candidates)

        home = Path.home()
        candidates.extend(
            [
                home / "Desktop" / "한국투자증권" / "workspace",
                home / "Desktop" / "한국투자증권" / "ipo_lockup_unified_lab" / "workspace",
                home / "Desktop" / "workspace",
                home / "Desktop" / "ipo_lockup_unified_lab" / "workspace",
                home / "Downloads" / "workspace",
                home / "Downloads" / "ipo_lockup_unified_lab" / "workspace",
            ]
        )
        for candidate in candidates:
            if self._looks_like_workspace(candidate):
                return candidate.resolve()
            nested = self._search_workspace_under(candidate, max_depth=6)
            if nested is not None:
                return nested
        return None

    def resolve_paths(self, workspace_hint: str | Path | None = None, allow_packaged_sample: bool = False) -> UnifiedLabPaths:
        workspace = self.auto_detect_workspace(workspace_hint, allow_packaged_sample=allow_packaged_sample)
        if workspace is None:
            return UnifiedLabPaths(
                workspace=None,
                unlock_csv=None,
                signals_csv=None,
                misses_csv=None,
                minute_db_path=None,
                turnover_backtest_dir=None,
                turnover_summary_csv=None,
                turnover_summary_pretty_csv=None,
                turnover_annual_csv=None,
                turnover_annual_pretty_csv=None,
                turnover_trades_csv=None,
                turnover_skip_summary_csv=None,
                turnover_skip_reasons_csv=None,
                beta_summary_csv=None,
                beta_trades_csv=None,
            )

        turnover_backtest_dir = self._first_existing_dir(
            [
                workspace / "turnover_backtest_out",
                workspace / "backtest_out" / "turnover",
                workspace / "turnover_backtest",
            ]
        )
        beta_summary_csv = detect_existing_file(
            [
                workspace / "analysis_out" / "trade_window_beta_summary.csv",
                workspace / "trade_window_beta_summary.csv",
            ]
        )
        beta_trades_csv = detect_existing_file(
            [
                workspace / "analysis_out" / "trade_window_beta_summary_trades.csv",
                workspace / "trade_window_beta_summary_trades.csv",
                workspace / "analysis_out" / "trade_window_beta_trades.csv",
            ]
        )
        if beta_trades_csv is None and beta_summary_csv is not None:
            stem_alt = beta_summary_csv.with_name(beta_summary_csv.stem + "_trades.csv")
            if stem_alt.exists():
                beta_trades_csv = stem_alt

        return UnifiedLabPaths(
            workspace=workspace,
            unlock_csv=detect_existing_file(
                [
                    workspace / "unlock_out" / "unlock_events_backtest_input.csv",
                    workspace / "dataset_out" / "unlock_events_backtest_input.csv",
                    workspace / "unlock_events_backtest_input.csv",
                    workspace / "dataset_out" / "synthetic_ipo_events.csv",
                ]
            ),
            signals_csv=detect_existing_file(
                [
                    workspace / "signal_out" / "turnover_signals.csv",
                    workspace / "turnover_signals.csv",
                ]
            ),
            misses_csv=detect_existing_file(
                [
                    workspace / "signal_out" / "turnover_signals_misses.csv",
                    workspace / "turnover_signals_misses.csv",
                ]
            ),
            minute_db_path=detect_existing_file(
                [
                    workspace / "data" / "curated" / "lockup_minute.db",
                    workspace / "lockup_minute.db",
                ]
            ),
            turnover_backtest_dir=turnover_backtest_dir,
            turnover_summary_csv=(turnover_backtest_dir / "summary_all.csv") if turnover_backtest_dir and (turnover_backtest_dir / "summary_all.csv").exists() else None,
            turnover_summary_pretty_csv=(turnover_backtest_dir / "summary_all_pretty.csv") if turnover_backtest_dir and (turnover_backtest_dir / "summary_all_pretty.csv").exists() else None,
            turnover_annual_csv=(turnover_backtest_dir / "annual_all.csv") if turnover_backtest_dir and (turnover_backtest_dir / "annual_all.csv").exists() else None,
            turnover_annual_pretty_csv=(turnover_backtest_dir / "annual_all_pretty.csv") if turnover_backtest_dir and (turnover_backtest_dir / "annual_all_pretty.csv").exists() else None,
            turnover_trades_csv=(turnover_backtest_dir / "all_trades.csv") if turnover_backtest_dir and (turnover_backtest_dir / "all_trades.csv").exists() else None,
            turnover_skip_summary_csv=(turnover_backtest_dir / "backtest_skip_summary.csv") if turnover_backtest_dir and (turnover_backtest_dir / "backtest_skip_summary.csv").exists() else None,
            turnover_skip_reasons_csv=(turnover_backtest_dir / "backtest_skip_reasons.csv") if turnover_backtest_dir and (turnover_backtest_dir / "backtest_skip_reasons.csv").exists() else None,
            beta_summary_csv=beta_summary_csv,
            beta_trades_csv=beta_trades_csv,
        )

    def load_bundle(self, workspace_hint: str | Path | None = None, allow_packaged_sample: bool = False) -> UnifiedLabBundle:
        paths = self.resolve_paths(workspace_hint, allow_packaged_sample=allow_packaged_sample)
        source_rows: list[dict[str, Any]] = []

        unlocks = self._read_csv(paths.unlock_csv, parse_dates=["listing_date", "unlock_date", "lockup_end_date"]) if paths.unlock_csv else pd.DataFrame()
        signals = self._read_csv(paths.signals_csv, parse_dates=["listing_date", "unlock_date", "entry_ts", "entry_trade_date"]) if paths.signals_csv else pd.DataFrame()
        misses = self._read_csv(paths.misses_csv, parse_dates=["listing_date", "unlock_date"]) if paths.misses_csv else pd.DataFrame()
        turnover_summary_raw = self._read_csv(paths.turnover_summary_csv) if paths.turnover_summary_csv else pd.DataFrame()
        turnover_summary_pretty = self._read_csv(paths.turnover_summary_pretty_csv) if paths.turnover_summary_pretty_csv else pd.DataFrame()
        turnover_annual_raw = self._read_csv(paths.turnover_annual_csv) if paths.turnover_annual_csv else pd.DataFrame()
        turnover_annual_pretty = self._read_csv(paths.turnover_annual_pretty_csv) if paths.turnover_annual_pretty_csv else pd.DataFrame()
        turnover_trades = self._read_csv(paths.turnover_trades_csv, parse_dates=["listing_date", "unlock_date", "entry_dt", "exit_dt", "prev_close_date"]) if paths.turnover_trades_csv else pd.DataFrame()
        turnover_skip_summary = self._read_csv(paths.turnover_skip_summary_csv) if paths.turnover_skip_summary_csv else pd.DataFrame()
        turnover_skip_reasons = self._read_csv(paths.turnover_skip_reasons_csv, parse_dates=["listing_date", "unlock_date"]) if paths.turnover_skip_reasons_csv else pd.DataFrame()
        beta_summary = self._read_csv(paths.beta_summary_csv) if paths.beta_summary_csv else pd.DataFrame()
        beta_trades = self._read_csv(paths.beta_trades_csv, parse_dates=["entry_dt", "exit_dt"]) if paths.beta_trades_csv else pd.DataFrame()

        unlocks = self._standardize_keys(unlocks)
        signals = self._standardize_keys(signals)
        misses = self._standardize_keys(misses)
        turnover_trades = self._standardize_keys(turnover_trades)
        turnover_skip_reasons = self._standardize_keys(turnover_skip_reasons)
        beta_trades = self._standardize_keys(beta_trades)

        if turnover_summary_pretty.empty and not turnover_summary_raw.empty:
            turnover_summary_pretty = self._pretty_pct_frame(turnover_summary_raw)
        if turnover_annual_pretty.empty and not turnover_annual_raw.empty:
            turnover_annual_pretty = self._pretty_pct_frame(turnover_annual_raw)

        for label, path, df in [
            ("unlock csv", paths.unlock_csv, unlocks),
            ("signals csv", paths.signals_csv, signals),
            ("misses csv", paths.misses_csv, misses),
            ("turnover summary", paths.turnover_summary_csv, turnover_summary_raw),
            ("turnover trades", paths.turnover_trades_csv, turnover_trades),
            ("turnover skip summary", paths.turnover_skip_summary_csv, turnover_skip_summary),
            ("beta summary", paths.beta_summary_csv, beta_summary),
        ]:
            source_rows.append({"source": label, "ok": path is not None and path.exists(), "rows": int(len(df)), "detail": str(path or "")})

        minute = self._load_minute_db(paths.minute_db_path)
        source_rows.extend(minute["status_rows"])

        return UnifiedLabBundle(
            paths=paths,
            unlocks=unlocks,
            signals=signals,
            misses=misses,
            turnover_summary_raw=turnover_summary_raw,
            turnover_summary_pretty=turnover_summary_pretty,
            turnover_annual_raw=turnover_annual_raw,
            turnover_annual_pretty=turnover_annual_pretty,
            turnover_trades=turnover_trades,
            turnover_skip_summary=turnover_skip_summary,
            turnover_skip_reasons=turnover_skip_reasons,
            beta_summary=beta_summary,
            beta_trades=beta_trades,
            minute_job_counts=minute["job_counts"],
            minute_jobs=minute["jobs"],
            minute_job_preview=minute["job_preview"],
            minute_bar_stats=minute["bar_stats"],
            minute_unlock_events=minute["unlock_events"],
            minute_symbol_coverage=minute["coverage"],
            source_status=pd.DataFrame(source_rows),
        )

    def signal_summary(self, signals: pd.DataFrame, misses: pd.DataFrame) -> pd.DataFrame:
        keys = [c for c in ["term", "multiple", "price_filter"] if (c in signals.columns or c in misses.columns)]
        if not keys:
            return pd.DataFrame()
        hit_base = signals.copy() if not signals.empty else pd.DataFrame(columns=keys)
        miss_base = misses.copy() if not misses.empty else pd.DataFrame(columns=keys)
        if hit_base.empty and miss_base.empty:
            return pd.DataFrame()

        hit_group = (
            hit_base.groupby(keys, dropna=False)
            .agg(
                hits=(keys[0], "size"),
                avg_turnover_ratio=("turnover_ratio", "mean") if "turnover_ratio" in hit_base.columns else (keys[0], "size"),
                avg_days_from_unlock=("days_from_unlock", "mean") if "days_from_unlock" in hit_base.columns else (keys[0], "size"),
            )
            .reset_index()
            if not hit_base.empty
            else pd.DataFrame(columns=keys + ["hits", "avg_turnover_ratio", "avg_days_from_unlock"])
        )
        miss_group = (
            miss_base.groupby(keys, dropna=False)
            .agg(misses=(keys[0], "size"))
            .reset_index()
            if not miss_base.empty
            else pd.DataFrame(columns=keys + ["misses"])
        )
        out = hit_group.merge(miss_group, on=keys, how="outer")
        out["hits"] = pd.to_numeric(out.get("hits"), errors="coerce").fillna(0).astype(int)
        out["misses"] = pd.to_numeric(out.get("misses"), errors="coerce").fillna(0).astype(int)
        total = out["hits"] + out["misses"]
        out["total"] = total
        out["hit_rate"] = out.apply(lambda r: (float(r["hits"]) / float(r["total"]) * 100.0) if r["total"] else math.nan, axis=1)
        if "avg_turnover_ratio" in out.columns and not hit_base.empty and "turnover_ratio" not in hit_base.columns:
            out["avg_turnover_ratio"] = math.nan
        if "avg_days_from_unlock" in out.columns and not hit_base.empty and "days_from_unlock" not in hit_base.columns:
            out["avg_days_from_unlock"] = math.nan
        return out.sort_values([c for c in ["term", "multiple", "price_filter"] if c in out.columns]).reset_index(drop=True)

    def turnover_term_summary(self, summary_raw: pd.DataFrame) -> pd.DataFrame:
        if summary_raw.empty or "term" not in summary_raw.columns:
            return pd.DataFrame()
        numeric_cols = [
            c
            for c in ["trades", "win_rate", "avg_ret", "median_ret", "sum_ret", "compound_ret", "min_ret", "max_ret"]
            if c in summary_raw.columns
        ]
        if not numeric_cols:
            return pd.DataFrame()
        out = summary_raw.groupby("term", as_index=False)[numeric_cols].mean(numeric_only=True)
        pct_cols = [c for c in ["win_rate", "avg_ret", "median_ret", "sum_ret", "compound_ret", "min_ret", "max_ret"] if c in out.columns]
        for col in pct_cols:
            out[col] = pd.to_numeric(out[col], errors="coerce") * 100.0
        return out.sort_values(["compound_ret", "win_rate"], ascending=[False, False]).reset_index(drop=True)

    def enrich_strategy_board(self, board: pd.DataFrame, bundle: UnifiedLabBundle, today: pd.Timestamp | None = None) -> pd.DataFrame:
        if board.empty:
            return board.copy()
        today = pd.Timestamp(today or today_kst()).normalize()
        out = board.copy()
        out["name_key"] = out.get("name_key", pd.Series(dtype="object")).fillna(out.get("name", pd.Series(dtype="object")).map(normalize_name_key))

        signal_lookup = self._indexed(bundle.signals)
        trade_lookup = self._indexed(bundle.turnover_trades)
        miss_lookup = self._indexed(bundle.misses)
        coverage_lookup = self._indexed(bundle.minute_symbol_coverage)
        job_lookup = self._indexed_jobs(bundle.minute_jobs)
        beta_term_lookup = self._term_lookup(bundle.beta_summary)

        bridge_rows: list[dict[str, Any]] = []
        for _, candidate in out.iterrows():
            key = self._candidate_key(candidate)
            signals = signal_lookup.get(key, pd.DataFrame())
            trades = trade_lookup.get(key, pd.DataFrame())
            misses = miss_lookup.get(key, pd.DataFrame())
            coverage = coverage_lookup.get(key, pd.DataFrame())
            jobs = job_lookup.get((str(candidate.get("symbol", "")).zfill(6), self._norm_date(candidate.get("unlock_date"))), pd.DataFrame())

            first_signal = signals.sort_values("entry_ts").iloc[0] if not signals.empty and "entry_ts" in signals.columns else None
            best_signal = signals.sort_values(["turnover_ratio", "entry_ts"], ascending=[False, True]).iloc[0] if not signals.empty and "turnover_ratio" in signals.columns else first_signal
            avg_net_ret = pd.to_numeric(trades.get("net_ret"), errors="coerce").mean() if not trades.empty and "net_ret" in trades.columns else math.nan
            win_rate = None
            if not trades.empty and "net_ret" in trades.columns:
                net = pd.to_numeric(trades["net_ret"], errors="coerce").dropna()
                win_rate = float((net > 0).mean() * 100.0) if not net.empty else math.nan
            job_status = self._job_status(jobs)
            bars = pd.to_numeric(coverage.get("bars"), errors="coerce").sum() if not coverage.empty and "bars" in coverage.columns else math.nan
            beta_proxy = beta_term_lookup.get(str(candidate.get("term") or ""), {}).get("beta_proxy")
            alpha_proxy = beta_term_lookup.get(str(candidate.get("term") or ""), {}).get("alpha_proxy")
            bridge_status = self._bridge_status(
                unlock_date=candidate.get("unlock_date"),
                today=today,
                signal_hits=len(signals),
                job_status=job_status,
                bars=bars,
            )
            bridge_rows.append(
                {
                    "turnover_signal_hits": int(len(signals)),
                    "turnover_miss_count": int(len(misses)),
                    "turnover_first_signal_ts": first_signal.get("entry_ts") if first_signal is not None else pd.NaT,
                    "turnover_first_entry_price": first_signal.get("entry_price") if first_signal is not None else pd.NA,
                    "turnover_best_multiple": best_signal.get("multiple") if best_signal is not None else pd.NA,
                    "turnover_best_price_filter": best_signal.get("price_filter") if best_signal is not None else pd.NA,
                    "turnover_best_ratio": best_signal.get("turnover_ratio") if best_signal is not None else pd.NA,
                    "turnover_backtest_trades": int(len(trades)),
                    "turnover_backtest_avg_net_ret_pct": round(float(avg_net_ret) * 100.0, 2) if pd.notna(avg_net_ret) else pd.NA,
                    "turnover_backtest_win_rate": round(float(win_rate), 2) if win_rate is not None and pd.notna(win_rate) else pd.NA,
                    "turnover_beta_proxy": beta_proxy,
                    "turnover_alpha_proxy": alpha_proxy,
                    "minute_job_status": job_status,
                    "minute_job_count": int(len(jobs)),
                    "minute_bars_loaded": int(bars) if pd.notna(bars) else pd.NA,
                    "bridge_status": bridge_status,
                }
            )
        bridge_df = pd.DataFrame(bridge_rows)
        base = out.drop(columns=[c for c in bridge_df.columns if c in out.columns], errors="ignore").reset_index(drop=True)
        merged = pd.concat([base, bridge_df], axis=1)
        return parse_date_columns(merged, ["turnover_first_signal_ts"])

    def candidate_context(self, candidate: pd.Series | dict[str, Any], bundle: UnifiedLabBundle) -> dict[str, pd.DataFrame | dict[str, Any]]:
        key = self._candidate_key(candidate)
        signals = self._indexed(bundle.signals).get(key, pd.DataFrame())
        trades = self._indexed(bundle.turnover_trades).get(key, pd.DataFrame())
        misses = self._indexed(bundle.misses).get(key, pd.DataFrame())
        coverage = self._indexed(bundle.minute_symbol_coverage).get(key, pd.DataFrame())
        jobs = self._indexed_jobs(bundle.minute_jobs).get((str(candidate.get("symbol", "")).zfill(6), self._norm_date(candidate.get("unlock_date"))), pd.DataFrame())
        beta = self._term_lookup(bundle.beta_summary).get(str(candidate.get("term") or ""), {})
        return {
            "signals": signals,
            "trades": trades,
            "misses": misses,
            "coverage": coverage,
            "jobs": jobs,
            "beta": beta,
        }

    def build_execution_bridge_export(
        self,
        board: pd.DataFrame,
        bundle: UnifiedLabBundle,
        *,
        today: pd.Timestamp | None = None,
        min_decision_rank: int = 2,
    ) -> pd.DataFrame:
        enriched = board.copy()
        if not {"bridge_status", "turnover_signal_hits", "minute_job_status"}.issubset(set(enriched.columns)):
            enriched = self.enrich_strategy_board(enriched, bundle, today=today)
        if enriched.empty:
            return enriched
        out = enriched.copy()
        if "decision_rank" in out.columns:
            out = out[pd.to_numeric(out["decision_rank"], errors="coerce").fillna(99) <= int(min_decision_rank)].copy()
        if out.empty:
            return out
        cols = [
            c
            for c in [
                "strategy_version",
                "symbol",
                "name",
                "market",
                "term",
                "unlock_date",
                "decision",
                "priority_tier",
                "planned_check_date",
                "planned_entry_date",
                "planned_exit_date",
                "entry_rule",
                "suggested_weight_pct_of_base",
                "bridge_status",
                "minute_job_status",
                "minute_job_count",
                "minute_bars_loaded",
                "turnover_signal_hits",
                "turnover_first_signal_ts",
                "turnover_first_entry_price",
                "turnover_best_multiple",
                "turnover_best_price_filter",
                "turnover_best_ratio",
                "turnover_backtest_trades",
                "turnover_backtest_avg_net_ret_pct",
                "turnover_backtest_win_rate",
                "turnover_beta_proxy",
                "turnover_alpha_proxy",
                "rationale",
            ]
            if c in out.columns
        ]
        return out[cols].reset_index(drop=True)

    @staticmethod
    def _pretty_pct_frame(df: pd.DataFrame) -> pd.DataFrame:
        out = df.copy()
        pct_cols = [c for c in ["win_rate", "avg_ret", "median_ret", "sum_ret", "compound_ret", "min_ret", "max_ret", "alpha_proxy", "avg_bench_ret"] if c in out.columns]
        for col in pct_cols:
            out[col] = pd.to_numeric(out[col], errors="coerce") * 100.0
        return out

    @staticmethod
    def _standardize_keys(df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return df.copy()
        out = df.copy()
        if "symbol" in out.columns:
            out["symbol"] = out["symbol"].astype(str).str.extract(r"(\d+)", expand=False).fillna(out["symbol"].astype(str)).str.zfill(6)
        if "name" in out.columns:
            out["name_key"] = out.get("name_key", pd.Series(dtype="object"))
            out["name_key"] = out["name_key"].fillna(out["name"].map(normalize_name_key))
        elif "corp_name" in out.columns:
            out["name"] = out["corp_name"]
            out["name_key"] = out["name"].map(normalize_name_key)
        for col in DATE_COLUMNS_COMMON:
            if col in out.columns:
                out[col] = pd.to_datetime(out[col], errors="coerce")
        return out

    @staticmethod
    def _read_csv(path: Path | None, parse_dates: Iterable[str] | None = None) -> pd.DataFrame:
        if path is None or not path.exists():
            return pd.DataFrame()
        try:
            if path.stat().st_size == 0:
                return pd.DataFrame()
        except OSError:
            return pd.DataFrame()
        try:
            raw = path.read_bytes()
            if not raw.strip(b"\xef\xbb\xbf\r\n\t ,;"):
                return pd.DataFrame()
        except OSError:
            return pd.DataFrame()
        try:
            df = pd.read_csv(path, dtype={"symbol": str})
        except (EmptyDataError, pd.errors.ParserError, UnicodeDecodeError, OSError, ValueError):
            return pd.DataFrame()
        except Exception as exc:
            if "No columns to parse" in str(exc):
                return pd.DataFrame()
            raise
        if parse_dates and not df.empty:
            df = parse_date_columns(df, parse_dates)
        return df

    @staticmethod
    def _first_existing_dir(candidates: Iterable[str | Path]) -> Path | None:
        for candidate in candidates:
            if not candidate:
                continue
            path = Path(candidate).expanduser().resolve()
            if path.exists() and path.is_dir():
                return path
        return None

    def _search_workspace_under(self, root: Path | None, max_depth: int = 5) -> Path | None:
        if root is None:
            return None
        root = Path(root).expanduser()
        if not root.exists() or not root.is_dir():
            return None
        marker_patterns = [
            "unlock_out",
            "signal_out",
            "turnover_backtest_out",
            "data/curated/lockup_minute.db",
            "unlock_events_backtest_input.csv",
            "turnover_signals.csv",
            "dataset_out/synthetic_ipo_events.csv",
            "dataset_out/live_issue_seed.csv",
        ]
        for pattern in marker_patterns:
            for match in root.rglob(pattern):
                try:
                    rel_parts = match.relative_to(root).parts
                except Exception:
                    rel_parts = ()
                if len(rel_parts) > max_depth + 2:
                    continue
                for candidate in [match, *match.parents]:
                    if candidate == candidate.parent:
                        break
                    try:
                        rel = candidate.relative_to(root)
                        if len(rel.parts) > max_depth:
                            continue
                    except Exception:
                        pass
                    if self._looks_like_workspace(candidate):
                        return candidate.resolve()
        return None

    @staticmethod
    def _looks_like_workspace(path: Path | None) -> bool:
        if path is None:
            return False
        path = Path(path).expanduser()
        if not path.exists() or not path.is_dir():
            return False
        direct_markers = [
            path / "unlock_out",
            path / "signal_out",
            path / "turnover_backtest_out",
            path / "data" / "curated" / "lockup_minute.db",
            path / "dataset_out" / "synthetic_ipo_events.csv",
            path / "dataset_out" / "live_issue_seed.csv",
        ]
        if any(marker.exists() for marker in direct_markers):
            return True
        root_file_markers = [
            path / "unlock_events_backtest_input.csv",
            path / "turnover_signals.csv",
        ]
        return sum(marker.exists() for marker in root_file_markers) >= 2

    @staticmethod
    def _table_exists(conn: sqlite3.Connection, table: str) -> bool:
        row = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table,)).fetchone()
        return row is not None

    def _load_minute_db(self, db_path: Path | None) -> dict[str, Any]:
        empty = {
            "job_counts": pd.DataFrame(),
            "jobs": pd.DataFrame(),
            "job_preview": pd.DataFrame(),
            "bar_stats": pd.DataFrame(),
            "unlock_events": pd.DataFrame(),
            "coverage": pd.DataFrame(),
            "status_rows": [{"source": "minute db", "ok": False, "rows": 0, "detail": str(db_path or "") }],
        }
        if db_path is None or not db_path.exists():
            return empty
        conn = sqlite3.connect(db_path)
        try:
            jobs = pd.read_sql_query("SELECT * FROM minute_jobs", conn) if self._table_exists(conn, "minute_jobs") else pd.DataFrame()
            if not jobs.empty:
                jobs = self._standardize_keys(jobs)
                jobs = self._parse_job_metadata(jobs)
            job_counts = (
                jobs.groupby("status", as_index=False).size().rename(columns={"size": "jobs"}).sort_values("status")
                if not jobs.empty and "status" in jobs.columns
                else pd.DataFrame()
            )
            job_preview = jobs.sort_values([c for c in ["status", "priority", "created_at"] if c in jobs.columns]).head(200).reset_index(drop=True) if not jobs.empty else pd.DataFrame()
            bar_stats = pd.read_sql_query(
                "SELECT symbol, interval_min, MIN(ts) AS min_ts, MAX(ts) AS max_ts, COUNT(*) AS bars FROM minute_bars GROUP BY symbol, interval_min ORDER BY bars DESC, symbol LIMIT 200",
                conn,
            ) if self._table_exists(conn, "minute_bars") else pd.DataFrame()
            if not bar_stats.empty:
                bar_stats = self._standardize_keys(bar_stats)
            unlock_events = pd.read_sql_query("SELECT * FROM unlock_events ORDER BY unlock_date, symbol", conn) if self._table_exists(conn, "unlock_events") else pd.DataFrame()
            if not unlock_events.empty:
                unlock_events = self._standardize_keys(unlock_events.rename(columns={"corp_name": "name", "unlock_type": "term"}))
                unlock_events = unlock_events.rename(columns={"term": "unlock_type"})
            coverage = pd.read_sql_query(
                """
                SELECT
                    ue.symbol,
                    ue.corp_name AS name,
                    ue.unlock_date,
                    ue.unlock_type,
                    ue.unlock_shares,
                    COUNT(mb.ts) AS bars,
                    MIN(mb.ts) AS min_ts,
                    MAX(mb.ts) AS max_ts
                FROM unlock_events ue
                LEFT JOIN minute_bars mb
                  ON ue.symbol = mb.symbol
                GROUP BY ue.symbol, ue.corp_name, ue.unlock_date, ue.unlock_type, ue.unlock_shares
                ORDER BY ue.unlock_date, ue.symbol
                """,
                conn,
            ) if self._table_exists(conn, "unlock_events") and self._table_exists(conn, "minute_bars") else pd.DataFrame()
            if not coverage.empty:
                coverage = self._standardize_keys(coverage.rename(columns={"unlock_type": "term"}))
            status_rows = [
                {"source": "minute db", "ok": True, "rows": int(len(jobs)), "detail": str(db_path)},
                {"source": "minute jobs", "ok": not jobs.empty, "rows": int(len(jobs)), "detail": str(db_path)},
                {"source": "minute bar stats", "ok": not bar_stats.empty, "rows": int(len(bar_stats)), "detail": str(db_path)},
                {"source": "minute unlock coverage", "ok": not coverage.empty, "rows": int(len(coverage)), "detail": str(db_path)},
            ]
            return {
                "job_counts": job_counts,
                "jobs": jobs,
                "job_preview": job_preview,
                "bar_stats": bar_stats,
                "unlock_events": unlock_events,
                "coverage": coverage,
                "status_rows": status_rows,
            }
        finally:
            conn.close()

    @staticmethod
    def _parse_job_metadata(df: pd.DataFrame) -> pd.DataFrame:
        if df.empty or "job_id" not in df.columns:
            return df
        out = df.copy()
        parsed = out["job_id"].astype(str).str.extract(r"^(?P<symbol>\d{6})_(?P<job_unlock_date>\d{4}-\d{2}-\d{2})_(?P<job_unlock_type>.+?)_(?P<interval>\d+)m$")
        for col in parsed.columns:
            out[col] = parsed[col]
        if "job_unlock_date" in out.columns:
            out["job_unlock_date"] = pd.to_datetime(out["job_unlock_date"], errors="coerce")
        if "symbol_x" in out.columns:
            out = out.drop(columns=["symbol_x"])
        return out

    @staticmethod
    def _norm_date(value: Any) -> str:
        ts = pd.to_datetime(value, errors="coerce")
        if pd.isna(ts):
            return ""
        return ts.normalize().strftime("%Y-%m-%d")

    @staticmethod
    def _candidate_key(candidate: pd.Series | dict[str, Any]) -> tuple[str, str, str]:
        symbol = str(candidate.get("symbol", "") or "").zfill(6)
        name_key = normalize_name_key(candidate.get("name"))
        term = str(candidate.get("term", "") or "")
        unlock_date = UnifiedLabBridgeService._norm_date(candidate.get("unlock_date"))
        if symbol == "000000":
            symbol = ""
        return (symbol or name_key, term, unlock_date)

    def _indexed(self, df: pd.DataFrame) -> dict[tuple[str, str, str], pd.DataFrame]:
        if df.empty:
            return {}
        work = self._standardize_keys(df)
        rows: dict[tuple[str, str, str], list[int]] = {}
        for idx, row in work.iterrows():
            key = self._candidate_key(row)
            rows.setdefault(key, []).append(idx)
            alt_symbol = str(row.get("symbol", "") or "").zfill(6)
            name_key = normalize_name_key(row.get("name"))
            if alt_symbol:
                rows.setdefault((name_key, str(row.get("term", "") or ""), self._norm_date(row.get("unlock_date"))), []).append(idx)
        out: dict[tuple[str, str, str], pd.DataFrame] = {}
        for key, idxs in rows.items():
            out[key] = work.loc[sorted(set(idxs))].reset_index(drop=True)
        return out

    @staticmethod
    def _indexed_jobs(df: pd.DataFrame) -> dict[tuple[str, str], pd.DataFrame]:
        if df.empty:
            return {}
        work = df.copy()
        if "job_unlock_date" not in work.columns:
            work = UnifiedLabBridgeService._parse_job_metadata(work)
        result: dict[tuple[str, str], pd.DataFrame] = {}
        for (symbol, unlock_date), grp in work.groupby([work.get("symbol", pd.Series(dtype="object")).astype(str).str.zfill(6), work.get("job_unlock_date", pd.Series(dtype="datetime64[ns]")).map(UnifiedLabBridgeService._norm_date)], dropna=False):
            result[(symbol, unlock_date)] = grp.reset_index(drop=True)
        return result

    @staticmethod
    def _term_lookup(df: pd.DataFrame) -> dict[str, dict[str, Any]]:
        if df.empty or "term" not in df.columns:
            return {}
        work = df.copy()
        return work.groupby("term", dropna=False).first().to_dict(orient="index")

    @staticmethod
    def _job_status(jobs: pd.DataFrame) -> str:
        if jobs.empty or "status" not in jobs.columns:
            return "미설정"
        statuses = set(jobs["status"].astype(str))
        for status in ["running", "queued", "failed", "done"]:
            if status in statuses:
                return status
        return sorted(statuses)[0] if statuses else "미설정"

    @staticmethod
    def _bridge_status(*, unlock_date: Any, today: pd.Timestamp, signal_hits: int, job_status: str, bars: Any) -> str:
        unlock_ts = pd.to_datetime(unlock_date, errors="coerce")
        bars_num = safe_float(bars)
        if signal_hits > 0:
            return "신호발생"
        if job_status == "running":
            return "수집중"
        if job_status == "queued":
            return "수집대기"
        if job_status == "failed":
            return "수집실패"
        if bars_num not in {None, 0}:
            if pd.notna(unlock_ts) and unlock_ts.normalize() < today.normalize():
                return "신호없음"
            return "데이터적재"
        if pd.notna(unlock_ts) and unlock_ts.normalize() >= today.normalize():
            return "큐미설정"
        return "미연결"
