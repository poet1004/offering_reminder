from __future__ import annotations

import argparse
import math
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence

import pandas as pd


@dataclass(frozen=True)
class SignalSpec:
    multiple: float
    price_filter: str


DEFAULT_PRICE_FILTERS = {
    "none",
    "reclaim_open",
    "reclaim_vwap",
    "reclaim_open_or_vwap",
    "range_top40",
    "open_and_vwap",
}


def _load_unlock_csv(path: str | Path) -> pd.DataFrame:
    df = pd.read_csv(path, dtype={"symbol": str})
    if df.empty:
        return df
    df["symbol"] = df["symbol"].astype(str).str.zfill(6)
    for c in ["listing_date", "unlock_date", "lockup_end_date"]:
        if c in df.columns:
            df[c] = pd.to_datetime(df[c], errors="coerce")
    if "term" not in df.columns and "lockup_term" in df.columns:
        df["term"] = df["lockup_term"]
    if "name" not in df.columns and "corp_name" in df.columns:
        df["name"] = df["corp_name"]
    return df


def aggregate_unlock_events(df: pd.DataFrame, aggregate_by: str) -> pd.DataFrame:
    if df.empty:
        return df
    keys_common = ["symbol", "name", "listing_date", "unlock_date", "term"]
    if aggregate_by == "none":
        out = df.copy()
        out["event_key"] = out.index.astype(str)
        out["unlock_label"] = out.get("unlock_type", out.get("term", "unknown")).astype(str)
        return out

    if aggregate_by == "day":
        keys = [c for c in ["symbol", "name", "listing_date", "unlock_date"] if c in df.columns]
        label = "day_total"
    elif aggregate_by == "term":
        keys = [c for c in keys_common if c in df.columns]
        label = "term_total"
    else:  # type
        keys = [c for c in keys_common + ["unlock_type"] if c in df.columns]
        label = "type_total"

    agg_map = {
        "unlock_shares": "sum",
        "unlock_ratio": "sum",
        "source_rcept_no": lambda x: "|".join(sorted({str(v) for v in x if str(v) and str(v).lower() != "nan"})),
        "source_report_nm": lambda x: "|".join(sorted({str(v) for v in x if str(v) and str(v).lower() != "nan"})),
        "holder_group": lambda x: "|".join(sorted({str(v) for v in x if str(v) and str(v).lower() != "nan"})),
        "parse_confidence": lambda x: "|".join(sorted({str(v) for v in x if str(v) and str(v).lower() != "nan"})),
        "ipo_price": "first",
        "market": "first",
        "lead_manager": "first",
        "listed_shares": "first",
        "ipo_price_source": "first",
    }
    present_agg = {k: v for k, v in agg_map.items() if k in df.columns}
    out = df.groupby(keys, dropna=False).agg(present_agg).reset_index()
    out["event_key"] = out[keys].astype(str).agg("|".join, axis=1)
    if "unlock_type" in out.columns:
        out["unlock_label"] = out["unlock_type"].astype(str)
    elif "term" in out.columns:
        out["unlock_label"] = out["term"].astype(str)
    else:
        out["unlock_label"] = label
    return out


def _fetch_bars(conn: sqlite3.Connection, symbol: str, interval_min: int, start_date: str, end_date: str) -> pd.DataFrame:
    return pd.read_sql_query(
        """
        SELECT symbol, interval_min, ts, trade_date, open, high, low, close, volume, amount, adjusted_flag, source
        FROM minute_bars
        WHERE symbol = ?
          AND interval_min = ?
          AND trade_date >= ?
          AND trade_date <= ?
        ORDER BY ts
        """,
        conn,
        params=(symbol, int(interval_min), start_date, end_date),
        parse_dates=["ts"],
    )


def _prepare_intraday_features(df: pd.DataFrame, cum_scope: str) -> pd.DataFrame:
    work = df.copy()
    work["trade_date"] = pd.to_datetime(work["trade_date"], errors="coerce").dt.date.astype(str)
    work["ts"] = pd.to_datetime(work["ts"], errors="coerce")
    if "amount" in work.columns and not work["amount"].isna().all():
        work["pv"] = pd.to_numeric(work["amount"], errors="coerce").fillna(0)
    else:
        work["pv"] = work["close"] * work["volume"]
    work["session_open"] = work.groupby("trade_date")["open"].transform("first")
    work["cum_day_volume"] = work.groupby("trade_date")["volume"].cumsum()
    work["cum_day_pv"] = work.groupby("trade_date")["pv"].cumsum()
    work["cum_vwap"] = work["cum_day_pv"] / work["cum_day_volume"].replace(0, pd.NA)
    work["high_so_far"] = work.groupby("trade_date")["high"].cummax()
    work["low_so_far"] = work.groupby("trade_date")["low"].cummin()
    if cum_scope == "same_day":
        work["cum_scope_volume"] = work["cum_day_volume"]
    else:
        work["cum_scope_volume"] = work["volume"].cumsum()
    return work


def _passes_price_filter(row: pd.Series, price_filter: str) -> bool:
    close = float(row["close"])
    session_open = float(row["session_open"])
    cum_vwap = float(row["cum_vwap"]) if pd.notna(row["cum_vwap"]) else math.nan
    low_so_far = float(row["low_so_far"])
    high_so_far = float(row["high_so_far"])

    if price_filter == "none":
        return True
    if price_filter == "reclaim_open":
        return close >= session_open
    if price_filter == "reclaim_vwap":
        return math.isfinite(cum_vwap) and close >= cum_vwap
    if price_filter == "reclaim_open_or_vwap":
        return close >= session_open or (math.isfinite(cum_vwap) and close >= cum_vwap)
    if price_filter == "open_and_vwap":
        return close >= session_open and (math.isfinite(cum_vwap) and close >= cum_vwap)
    if price_filter == "range_top40":
        denom = high_so_far - low_so_far
        if denom <= 0:
            return False
        return (close - low_so_far) / denom >= 0.6
    raise ValueError(f"unknown price_filter: {price_filter}")


def build_turnover_signals(
    unlock_df: pd.DataFrame,
    conn: sqlite3.Connection,
    interval_min: int,
    multiples: list[float],
    price_filter: str,
    max_days_after: int,
    aggregate_by: str,
    cum_scope: str,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    events = aggregate_unlock_events(unlock_df, aggregate_by=aggregate_by)
    hit_rows: list[dict] = []
    miss_rows: list[dict] = []

    for _, ev in events.iterrows():
        symbol = str(ev.get("symbol", "")).zfill(6)
        unlock_date = pd.Timestamp(ev["unlock_date"]).normalize()
        end_date = unlock_date + pd.Timedelta(days=int(max_days_after))
        bars = _fetch_bars(
            conn=conn,
            symbol=symbol,
            interval_min=interval_min,
            start_date=str(unlock_date.date()),
            end_date=str(end_date.date()),
        )
        if bars.empty:
            for multiple in multiples:
                miss_rows.append(
                    {
                        "symbol": symbol,
                        "name": ev.get("name"),
                        "unlock_date": unlock_date,
                        "term": ev.get("term"),
                        "unlock_label": ev.get("unlock_label"),
                        "multiple": multiple,
                        "price_filter": price_filter,
                        "reason": "bars_missing",
                    }
                )
            continue

        bars = _prepare_intraday_features(bars, cum_scope=cum_scope)
        unlock_shares = float(pd.to_numeric(pd.Series([ev.get("unlock_shares")]), errors="coerce").iloc[0] or 0)
        if unlock_shares <= 0:
            for multiple in multiples:
                miss_rows.append(
                    {
                        "symbol": symbol,
                        "name": ev.get("name"),
                        "unlock_date": unlock_date,
                        "term": ev.get("term"),
                        "unlock_label": ev.get("unlock_label"),
                        "multiple": multiple,
                        "price_filter": price_filter,
                        "reason": "unlock_shares_invalid",
                    }
                )
            continue

        for multiple in multiples:
            threshold = unlock_shares * float(multiple)
            found = None
            for idx, row in bars.iterrows():
                if float(row["cum_scope_volume"]) >= threshold and _passes_price_filter(row, price_filter):
                    found = row
                    break
            if found is None:
                miss_rows.append(
                    {
                        "symbol": symbol,
                        "name": ev.get("name"),
                        "unlock_date": unlock_date,
                        "term": ev.get("term"),
                        "unlock_label": ev.get("unlock_label"),
                        "multiple": multiple,
                        "price_filter": price_filter,
                        "reason": "threshold_not_reached_or_filter_failed",
                        "max_cum_volume": float(bars["cum_scope_volume"].max()),
                        "unlock_shares": unlock_shares,
                    }
                )
                continue

            entry_ts = pd.Timestamp(found["ts"])
            entry_ts_naive = entry_ts.tz_localize(None) if getattr(entry_ts, 'tzinfo', None) is not None else entry_ts
            hit_rows.append(
                {
                    "signal_name": f"{ev.get('unlock_label','unknown')}_{multiple}x_{price_filter}",
                    "symbol": symbol,
                    "name": ev.get("name"),
                    "listing_date": ev.get("listing_date"),
                    "unlock_date": unlock_date,
                    "entry_ts": entry_ts.isoformat(),
                    "entry_trade_date": entry_ts_naive.normalize(),
                    "entry_price": float(found["close"]),
                    "term": ev.get("term"),
                    "unlock_type": ev.get("unlock_type", ev.get("unlock_label")),
                    "unlock_label": ev.get("unlock_label"),
                    "unlock_shares": unlock_shares,
                    "multiple": float(multiple),
                    "threshold_shares": threshold,
                    "cum_volume": float(found["cum_scope_volume"]),
                    "turnover_ratio": float(found["cum_scope_volume"]) / unlock_shares if unlock_shares > 0 else math.nan,
                    "session_open": float(found["session_open"]),
                    "cum_vwap": float(found["cum_vwap"]) if pd.notna(found["cum_vwap"]) else math.nan,
                    "high_so_far": float(found["high_so_far"]),
                    "low_so_far": float(found["low_so_far"]),
                    "price_filter": price_filter,
                    "cum_scope": cum_scope,
                    "aggregate_by": aggregate_by,
                    "interval_min": int(interval_min),
                    "bars_seen": int(idx + 1),
                    "days_from_unlock": int((entry_ts_naive.normalize() - unlock_date).days),
                    "ipo_price": ev.get("ipo_price"),
                    "market": ev.get("market"),
                    "lead_manager": ev.get("lead_manager"),
                    "listed_shares": ev.get("listed_shares"),
                    "ipo_price_source": ev.get("ipo_price_source"),
                    "unlock_ratio": ev.get("unlock_ratio"),
                    "source_rcept_no": ev.get("source_rcept_no"),
                    "source_report_nm": ev.get("source_report_nm"),
                    "holder_group": ev.get("holder_group"),
                    "parse_confidence": ev.get("parse_confidence"),
                }
            )

    return pd.DataFrame(hit_rows), pd.DataFrame(miss_rows)


def cli_main(argv: Optional[Sequence[str]] = None) -> None:
    parser = argparse.ArgumentParser(description="Build turnover entry signals from minute DB and unlock events")
    parser.add_argument("--unlock-csv", required=True)
    parser.add_argument("--db-path", required=True)
    parser.add_argument("--out-csv", required=True)
    parser.add_argument("--miss-csv", default=None)
    parser.add_argument("--interval-min", type=int, default=5)
    parser.add_argument("--multiples", default="1,2")
    parser.add_argument("--price-filter", default="reclaim_open_or_vwap", choices=sorted(DEFAULT_PRICE_FILTERS))
    parser.add_argument("--max-days-after", type=int, default=5)
    parser.add_argument("--aggregate-by", default="type", choices=["none", "type", "term", "day"])
    parser.add_argument("--cum-scope", default="through_window", choices=["through_window", "same_day"])
    args = parser.parse_args(argv)

    unlock_df = _load_unlock_csv(args.unlock_csv)
    conn = sqlite3.connect(args.db_path)
    try:
        multiples = [float(x.strip()) for x in str(args.multiples).split(",") if x.strip()]
        hits, misses = build_turnover_signals(
            unlock_df=unlock_df,
            conn=conn,
            interval_min=args.interval_min,
            multiples=multiples,
            price_filter=args.price_filter,
            max_days_after=args.max_days_after,
            aggregate_by=args.aggregate_by,
            cum_scope=args.cum_scope,
        )
    finally:
        conn.close()

    out_path = Path(args.out_csv)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    hits.to_csv(out_path, index=False, encoding="utf-8-sig")
    miss_path = Path(args.miss_csv) if args.miss_csv else out_path.with_name(out_path.stem + "_misses.csv")
    misses.to_csv(miss_path, index=False, encoding="utf-8-sig")

    print(f"[DONE] signals={len(hits)} -> {out_path}")
    print(f"[DONE] misses={len(misses)} -> {miss_path}")


if __name__ == "__main__":
    cli_main()
