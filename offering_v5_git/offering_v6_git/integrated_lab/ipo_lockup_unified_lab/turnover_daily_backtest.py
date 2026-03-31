from __future__ import annotations

import argparse
import json
import math
from pathlib import Path
from typing import Any, Dict, Optional, Sequence

import numpy as np
import pandas as pd

try:
    from .ipo_lockup_program import (
        CostConfig,
        DailyBacktester,
        _first_trade_idx_on_or_after,
        make_broker,
        make_pretty_pct,
        save_csv,
    )
except ImportError:  # script execution
    from ipo_lockup_program import (
        CostConfig,
        DailyBacktester,
        _first_trade_idx_on_or_after,
        make_broker,
        make_pretty_pct,
        save_csv,
    )

DEFAULT_HOLD_BY_TERM = {
    "15D": 5,
    "1M": 21,
    "3M": 32,
    "6M": 63,
    "1Y": 126,
    "2Y": 252,
}


def load_turnover_config(path: str | Path) -> tuple[CostConfig, dict[str, int], dict[str, float | None]]:
    data = json.loads(Path(path).read_text(encoding="utf-8"))
    costs = CostConfig(**data.get("costs", {}))
    hold_map = DEFAULT_HOLD_BY_TERM.copy()
    hold_map.update({str(k).upper(): int(v) for k, v in data.get("hold_days_by_term", {}).items()})
    filters = data.get("filters", {})
    return costs, hold_map, {
        "min_prev_close_vs_ipo": filters.get("min_prev_close_vs_ipo"),
        "max_prev_close_vs_ipo": filters.get("max_prev_close_vs_ipo"),
    }


def _duration_metrics(series: pd.Series, hold_days: pd.Series) -> dict[str, float]:
    s = pd.to_numeric(series, errors="coerce")
    h = pd.to_numeric(hold_days, errors="coerce")
    mask = s.notna() & h.notna() & (h > 0) & (s > -1)
    if not mask.any():
        return {"avg_log_ret_per_day": math.nan, "bp_per_day": math.nan, "geo_ann": math.nan}
    daily_log = np.log1p(s[mask]) / h[mask]
    avg_log = float(daily_log.mean())
    return {
        "avg_log_ret_per_day": avg_log,
        "bp_per_day": avg_log * 10000,
        "geo_ann": float(np.expm1(avg_log * 252)),
    }


def summarize_trades(trades: pd.DataFrame, group_cols: list[str]) -> pd.DataFrame:
    if trades.empty:
        return pd.DataFrame()
    g = trades.groupby(group_cols, dropna=False)

    summary = g.apply(
        lambda x: pd.Series(
            {
                "trades": len(x),
                "win_rate": float((pd.to_numeric(x["net_ret"], errors="coerce") > 0).mean()),
                "avg_ret": float(pd.to_numeric(x["net_ret"], errors="coerce").mean()),
                "median_ret": float(pd.to_numeric(x["net_ret"], errors="coerce").median()),
                "sum_ret": float(pd.to_numeric(x["net_ret"], errors="coerce").sum()),
                "compound_ret": float((1 + pd.to_numeric(x["net_ret"], errors="coerce")).prod() - 1),
                "min_ret": float(pd.to_numeric(x["net_ret"], errors="coerce").min()),
                "max_ret": float(pd.to_numeric(x["net_ret"], errors="coerce").max()),
                **_duration_metrics(pd.to_numeric(x["net_ret"], errors="coerce"), pd.to_numeric(x["hold_days_after_entry"], errors="coerce")),
            }
        )
    ).reset_index()
    return summary


def backtest_turnover_signals(
    signals: pd.DataFrame,
    bt: DailyBacktester,
    costs: CostConfig,
    hold_map: dict[str, int],
    filters: dict[str, float | None],
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    all_trades: list[dict[str, Any]] = []
    skip_rows: list[dict[str, Any]] = []

    signals = signals.copy()
    signals["entry_trade_date"] = pd.to_datetime(signals["entry_trade_date"], errors="coerce")
    signals["unlock_date"] = pd.to_datetime(signals["unlock_date"], errors="coerce")
    signals["listing_date"] = pd.to_datetime(signals.get("listing_date"), errors="coerce")

    symbol_windows: dict[str, dict[str, pd.Timestamp]] = {}
    for _, row in signals.iterrows():
        symbol = str(row.get("symbol", "")).zfill(6)
        entry_dt = pd.Timestamp(row["entry_trade_date"]).normalize()
        term = str(row.get("term", "")).upper()
        hold_days = int(hold_map.get(term, hold_map.get("1M", 21)))
        start_ts = entry_dt - pd.Timedelta(days=25)
        end_ts = entry_dt + pd.Timedelta(days=max(40, hold_days * 2 + 30))
        win = symbol_windows.setdefault(symbol, {"min": start_ts, "max": end_ts})
        if start_ts < win["min"]:
            win["min"] = start_ts
        if end_ts > win["max"]:
            win["max"] = end_ts

    symbol_daily: dict[str, pd.DataFrame] = {}
    symbol_errors: dict[str, str] = {}
    total_symbols = len(symbol_windows)
    for idx, (symbol, win) in enumerate(sorted(symbol_windows.items()), start=1):
        start_date = win["min"].strftime("%Y%m%d")
        end_date = win["max"].strftime("%Y%m%d")
        if idx == 1 or idx % 20 == 0 or idx == total_symbols:
            print(f"[turnover preload] {idx}/{total_symbols} {symbol} {start_date}~{end_date}")
        try:
            symbol_daily[symbol] = bt.fetch_daily_bars(symbol, start_date, end_date, adj_price=True, use_cache=True)
        except Exception as exc:  # noqa: BLE001
            symbol_errors[symbol] = f"{type(exc).__name__}: {str(exc)[:200]}"
            symbol_daily[symbol] = pd.DataFrame(columns=["date", "open", "high", "low", "close", "volume"])

    for _, row in signals.iterrows():
        symbol = str(row.get("symbol", "")).zfill(6)
        term = str(row.get("term", "")).upper()
        hold_days = int(hold_map.get(term, hold_map.get("1M", 21)))
        entry_dt = pd.Timestamp(row["entry_trade_date"]).normalize()
        entry_price = float(pd.to_numeric(pd.Series([row.get("entry_price")]), errors="coerce").iloc[0] or math.nan)

        def add_skip(reason: str, **extra: Any) -> None:
            srow = {
                "signal_name": row.get("signal_name"),
                "symbol": symbol,
                "name": row.get("name"),
                "term": term,
                "unlock_date": row.get("unlock_date"),
                "entry_trade_date": entry_dt,
                "reason": reason,
            }
            srow.update(extra)
            skip_rows.append(srow)

        if symbol_errors.get(symbol):
            add_skip("daily_fetch_error", error_text=symbol_errors[symbol])
            continue

        daily = symbol_daily.get(symbol)
        if daily is None or daily.empty:
            add_skip("daily_empty_symbol")
            continue

        entry_idx = _first_trade_idx_on_or_after(daily, entry_dt)
        if entry_idx is None:
            add_skip("entry_not_found")
            continue

        if not math.isfinite(entry_price) or entry_price <= 0:
            add_skip("entry_price_invalid", entry_price=entry_price)
            continue

        ipo_price = pd.to_numeric(pd.Series([row.get("ipo_price")]), errors="coerce").iloc[0]
        prev_close_vs_ipo = math.nan
        prev_close_date = pd.NaT
        if entry_idx > 0 and pd.notna(ipo_price) and float(ipo_price) > 0:
            prev_row = daily.iloc[entry_idx - 1]
            prev_close = float(prev_row["close"])
            prev_close_date = pd.Timestamp(prev_row["date"])
            if prev_close > 0:
                prev_close_vs_ipo = prev_close / float(ipo_price)

        min_prev = filters.get("min_prev_close_vs_ipo")
        max_prev = filters.get("max_prev_close_vs_ipo")
        if min_prev is not None:
            if pd.isna(ipo_price) or float(ipo_price) <= 0:
                add_skip("ipo_price_missing_for_prev_filter")
                continue
            if pd.isna(prev_close_vs_ipo) or prev_close_vs_ipo < float(min_prev):
                add_skip("prev_close_vs_ipo_below_min", prev_close_vs_ipo=prev_close_vs_ipo, threshold=float(min_prev))
                continue
        if max_prev is not None:
            if pd.isna(ipo_price) or float(ipo_price) <= 0:
                add_skip("ipo_price_missing_for_prev_filter")
                continue
            if pd.isna(prev_close_vs_ipo) or prev_close_vs_ipo > float(max_prev):
                add_skip("prev_close_vs_ipo_above_max", prev_close_vs_ipo=prev_close_vs_ipo, threshold=float(max_prev))
                continue

        exit_idx = entry_idx + hold_days
        if exit_idx >= len(daily):
            add_skip("exit_out_of_range", entry_idx=int(entry_idx), exit_idx=int(exit_idx), daily_rows=int(len(daily)))
            continue

        exit_row = daily.iloc[exit_idx]
        exit_price = float(exit_row["close"])
        exit_dt = pd.Timestamp(exit_row["date"])
        if not math.isfinite(exit_price) or exit_price <= 0:
            add_skip("exit_price_invalid", exit_price=exit_price)
            continue

        gross_ret = exit_price / entry_price - 1.0
        net_ret = (exit_price * (1 - costs.sell_cost)) / (entry_price * (1 + costs.buy_cost)) - 1.0

        all_trades.append(
            {
                "signal_name": row.get("signal_name"),
                "symbol": symbol,
                "name": row.get("name"),
                "term": term,
                "unlock_type": row.get("unlock_type"),
                "multiple": row.get("multiple"),
                "price_filter": row.get("price_filter"),
                "aggregate_by": row.get("aggregate_by"),
                "cum_scope": row.get("cum_scope"),
                "listing_date": row.get("listing_date"),
                "unlock_date": row.get("unlock_date"),
                "entry_dt": entry_dt,
                "entry_price": entry_price,
                "exit_dt": exit_dt,
                "exit_price": exit_price,
                "hold_days_after_entry": hold_days,
                "ipo_price": ipo_price,
                "ipo_price_source": row.get("ipo_price_source"),
                "prev_close_vs_ipo": prev_close_vs_ipo,
                "prev_close_date": prev_close_date,
                "turnover_ratio": row.get("turnover_ratio"),
                "cum_volume": row.get("cum_volume"),
                "gross_ret": gross_ret,
                "net_ret": net_ret,
            }
        )

    trades = pd.DataFrame(all_trades)
    skips = pd.DataFrame(skip_rows)

    skip_summary = pd.DataFrame()
    if not skips.empty:
        skip_summary = (
            skips.groupby(["signal_name", "term", "reason"], dropna=False)
            .size()
            .rename("count")
            .reset_index()
            .sort_values(["signal_name", "count", "reason"], ascending=[True, False, True])
            .reset_index(drop=True)
        )

    if trades.empty:
        return trades, pd.DataFrame(), pd.DataFrame(), skips, skip_summary

    group_cols = ["signal_name", "term", "unlock_type", "multiple", "price_filter", "hold_days_after_entry"]
    summary = summarize_trades(trades, group_cols=group_cols)
    annual = summarize_trades(trades.assign(year=pd.to_datetime(trades["entry_dt"]).dt.year), group_cols=["year"] + group_cols)
    return trades, summary, annual, skips, skip_summary


def cli_main(argv: Optional[Sequence[str]] = None) -> None:
    parser = argparse.ArgumentParser(description="Backtest turnover-entry signals with KIS daily exits")
    parser.add_argument("--signals-csv", required=True)
    parser.add_argument("--config", required=True)
    parser.add_argument("--key-file", required=True)
    parser.add_argument("--cache-dir", required=True)
    parser.add_argument("--out-dir", required=True)
    parser.add_argument("--mock", action="store_true")
    args = parser.parse_args(argv)

    signals = pd.read_csv(args.signals_csv, parse_dates=["listing_date", "unlock_date", "entry_trade_date"])
    costs, hold_map, filters = load_turnover_config(args.config)

    broker = make_broker(args.key_file, mock=args.mock)
    bt = DailyBacktester(broker, cache_dir=args.cache_dir)

    trades, summary, annual, skips, skip_summary = backtest_turnover_signals(
        signals=signals,
        bt=bt,
        costs=costs,
        hold_map=hold_map,
        filters=filters,
    )

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    save_csv(trades, out_dir / "all_trades.csv")
    save_csv(summary, out_dir / "summary_all.csv")
    save_csv(annual, out_dir / "annual_all.csv")
    save_csv(make_pretty_pct(summary), out_dir / "summary_all_pretty.csv")
    save_csv(make_pretty_pct(annual), out_dir / "annual_all_pretty.csv")
    save_csv(skips, out_dir / "backtest_skip_reasons.csv")
    save_csv(skip_summary, out_dir / "backtest_skip_summary.csv")

    print(f"[DONE] turnover backtest saved: {out_dir}")
    if not summary.empty:
        print(make_pretty_pct(summary).to_string(index=False))
    else:
        print("summary empty. check backtest_skip_summary.csv")


if __name__ == "__main__":
    cli_main()
