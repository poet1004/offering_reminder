from __future__ import annotations

import argparse
import math
from pathlib import Path
from typing import Optional, Sequence

import pandas as pd


def _load_benchmark_csv(path: str | Path) -> pd.DataFrame:
    df = pd.read_csv(path)
    cols = {c.lower(): c for c in df.columns}
    date_col = cols.get("date") or cols.get("tradedate") or cols.get("dt")
    close_col = cols.get("close") or cols.get("adjclose") or cols.get("adj_close")
    if not date_col or not close_col:
        raise ValueError("benchmark csv에는 date, close 컬럼이 필요합니다.")
    out = df[[date_col, close_col]].copy()
    out.columns = ["date", "close"]
    out["date"] = pd.to_datetime(out["date"], errors="coerce")
    out["close"] = pd.to_numeric(out["close"], errors="coerce")
    out = out.dropna(subset=["date", "close"]).sort_values("date").drop_duplicates("date", keep="last").reset_index(drop=True)
    return out


def _benchmark_window_ret(bench: pd.DataFrame, start_dt: pd.Timestamp, end_dt: pd.Timestamp) -> float:
    start_dt = pd.Timestamp(start_dt).normalize()
    end_dt = pd.Timestamp(end_dt).normalize()
    s = bench[bench["date"] >= start_dt]
    e = bench[bench["date"] <= end_dt]
    if s.empty or e.empty:
        return math.nan
    start_close = float(s.iloc[0]["close"])
    end_close = float(e.iloc[-1]["close"])
    if start_close <= 0:
        return math.nan
    return end_close / start_close - 1.0


def compute_trade_window_beta_proxy(trades: pd.DataFrame, bench: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    work = trades.copy()
    work["entry_dt"] = pd.to_datetime(work["entry_dt"], errors="coerce")
    work["exit_dt"] = pd.to_datetime(work["exit_dt"], errors="coerce")
    work["net_ret"] = pd.to_numeric(work["net_ret"], errors="coerce")
    work["bench_window_ret"] = work.apply(lambda r: _benchmark_window_ret(bench, r["entry_dt"], r["exit_dt"]), axis=1)
    work = work.dropna(subset=["net_ret", "bench_window_ret"]).reset_index(drop=True)

    group_candidates = ["strategy_name", "signal_name", "term"]
    group_cols = [c for c in group_candidates if c in work.columns]
    if not group_cols:
        group_cols = ["term"] if "term" in work.columns else []

    if not group_cols:
        group_keys = [("__all__", work)]
        results = []
        for _, grp in group_keys:
            beta = grp["net_ret"].cov(grp["bench_window_ret"]) / grp["bench_window_ret"].var() if grp["bench_window_ret"].var() not in (0, None) else math.nan
            corr = grp["net_ret"].corr(grp["bench_window_ret"])
            alpha = grp["net_ret"].mean() - beta * grp["bench_window_ret"].mean() if pd.notna(beta) else math.nan
            results.append({"bucket": "__all__", "trades": len(grp), "beta_proxy": beta, "corr": corr, "alpha_proxy": alpha})
        summary = pd.DataFrame(results)
        return work, summary

    rows = []
    for keys, grp in work.groupby(group_cols, dropna=False):
        if not isinstance(keys, tuple):
            keys = (keys,)
        bench_var = grp["bench_window_ret"].var()
        beta = grp["net_ret"].cov(grp["bench_window_ret"]) / bench_var if pd.notna(bench_var) and bench_var != 0 else math.nan
        corr = grp["net_ret"].corr(grp["bench_window_ret"])
        alpha = grp["net_ret"].mean() - beta * grp["bench_window_ret"].mean() if pd.notna(beta) else math.nan
        row = {col: val for col, val in zip(group_cols, keys)}
        row.update(
            {
                "trades": len(grp),
                "avg_ret": float(grp["net_ret"].mean()),
                "avg_bench_ret": float(grp["bench_window_ret"].mean()),
                "beta_proxy": float(beta) if pd.notna(beta) else math.nan,
                "corr": float(corr) if pd.notna(corr) else math.nan,
                "alpha_proxy": float(alpha) if pd.notna(alpha) else math.nan,
            }
        )
        rows.append(row)
    summary = pd.DataFrame(rows).sort_values(["trades"], ascending=[False]).reset_index(drop=True)
    return work, summary


def cli_main(argv: Optional[Sequence[str]] = None) -> None:
    parser = argparse.ArgumentParser(description="Trade-window beta proxy vs benchmark csv")
    parser.add_argument("--trades-csv", required=True)
    parser.add_argument("--benchmark-csv", required=True)
    parser.add_argument("--out-summary-csv", required=True)
    parser.add_argument("--out-trades-csv", default=None)
    args = parser.parse_args(argv)

    trades = pd.read_csv(args.trades_csv, parse_dates=["entry_dt", "exit_dt"])
    bench = _load_benchmark_csv(args.benchmark_csv)
    enriched, summary = compute_trade_window_beta_proxy(trades, bench)

    out_summary = Path(args.out_summary_csv)
    out_summary.parent.mkdir(parents=True, exist_ok=True)
    summary.to_csv(out_summary, index=False, encoding="utf-8-sig")
    out_trades = Path(args.out_trades_csv) if args.out_trades_csv else out_summary.with_name(out_summary.stem + "_trades.csv")
    enriched.to_csv(out_trades, index=False, encoding="utf-8-sig")

    print(f"[DONE] summary -> {out_summary}")
    print(f"[DONE] enriched trades -> {out_trades}")


if __name__ == "__main__":
    cli_main()
