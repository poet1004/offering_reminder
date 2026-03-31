from __future__ import annotations

import sqlite3
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
WORKSPACE = DATA_DIR / "sample_unified_lab_workspace"


def make_pretty_pct(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for col in [c for c in ["win_rate", "avg_ret", "median_ret", "sum_ret", "compound_ret", "min_ret", "max_ret", "avg_bench_ret", "alpha_proxy"] if c in out.columns]:
        out[col] = pd.to_numeric(out[col], errors="coerce") * 100.0
    return out


def ensure_dirs() -> dict[str, Path]:
    parts = {
        "unlock_out": WORKSPACE / "unlock_out",
        "signal_out": WORKSPACE / "signal_out",
        "turnover_backtest_out": WORKSPACE / "turnover_backtest_out",
        "analysis_out": WORKSPACE / "analysis_out",
        "minute_db_dir": WORKSPACE / "data" / "curated",
    }
    for path in parts.values():
        path.mkdir(parents=True, exist_ok=True)
    return parts


def build_unlock_events(sample: pd.DataFrame) -> pd.DataFrame:
    sample = sample.copy()
    sample["symbol"] = sample["symbol"].astype(str).str.zfill(6)
    issue_map = sample.set_index("symbol").to_dict(orient="index")
    rows = [
        {"symbol": "480050", "term": "1M", "unlock_date_col": "unlock_date_1m", "unlock_shares": 820_000, "unlock_type": "inst_1m", "holder_group": "기관", "holder_name": "A Growth Fund", "relation": "기관투자자", "unlock_ratio": 8.20, "source_rcept_no": "20260318000001", "source_report_nm": "투자설명서", "parse_confidence": "high", "note": "sample workspace historical"},
        {"symbol": "480040", "term": "15D", "unlock_date_col": "unlock_date_15d", "unlock_shares": 450_000, "unlock_type": "inst_15d", "holder_group": "기관", "holder_name": "B Partners", "relation": "기관투자자", "unlock_ratio": 4.50, "source_rcept_no": "20260325000002", "source_report_nm": "증권신고서", "parse_confidence": "high", "note": "sample workspace historical"},
        {"symbol": "480080", "term": "6M", "unlock_date_col": "unlock_date_6m", "unlock_shares": 1_200_000, "unlock_type": "inst_6m", "holder_group": "기관", "holder_name": "C Ventures", "relation": "기관투자자", "unlock_ratio": 12.00, "source_rcept_no": "20250920000003", "source_report_nm": "증권신고서", "parse_confidence": "mid", "note": "sample workspace historical miss"},
        {"symbol": "480060", "term": "1M", "unlock_date_col": "unlock_date_1m", "unlock_shares": 380_000, "unlock_type": "inst_1m", "holder_group": "기관", "holder_name": "D Macro", "relation": "기관투자자", "unlock_ratio": 3.80, "source_rcept_no": "20260220000007", "source_report_nm": "투자설명서", "parse_confidence": "high", "note": "sample workspace historical"},
        {"symbol": "480020", "term": "15D", "unlock_date_col": "unlock_date_15d", "unlock_shares": 400_000, "unlock_type": "inst_15d", "holder_group": "기관", "holder_name": "D Capital", "relation": "기관투자자", "unlock_ratio": 4.00, "source_rcept_no": "20260401000004", "source_report_nm": "투자설명서", "parse_confidence": "high", "note": "future queued job"},
        {"symbol": "480030", "term": "15D", "unlock_date_col": "unlock_date_15d", "unlock_shares": 500_000, "unlock_type": "inst_15d", "holder_group": "기관", "holder_name": "E Asset", "relation": "기관투자자", "unlock_ratio": 5.00, "source_rcept_no": "20260328000005", "source_report_nm": "증권신고서", "parse_confidence": "high", "note": "future queued job"},
        {"symbol": "480010", "term": "15D", "unlock_date_col": "unlock_date_15d", "unlock_shares": 350_000, "unlock_type": "inst_15d", "holder_group": "기관", "holder_name": "F PE", "relation": "기관투자자", "unlock_ratio": 3.50, "source_rcept_no": "20260402000006", "source_report_nm": "투자설명서", "parse_confidence": "mid", "note": "future queued job"},
    ]
    out_rows = []
    for row in rows:
        issue = issue_map[row["symbol"]]
        out_rows.append(
            {
                "symbol": row["symbol"],
                "name": issue["name"],
                "listing_date": issue["listing_date"],
                "unlock_date": issue[row["unlock_date_col"]],
                "term": row["term"],
                "ipo_price": issue["offer_price"],
                "market": issue["market"],
                "lead_manager": issue["underwriters"].split(",")[0].strip(),
                "listed_shares": 10_000_000,
                "ipo_price_source": "sample_master",
                "unlock_type": row["unlock_type"],
                "holder_group": row["holder_group"],
                "holder_name": row["holder_name"],
                "relation": row["relation"],
                "unlock_shares": row["unlock_shares"],
                "unlock_ratio": row["unlock_ratio"],
                "lockup_end_date": issue[row["unlock_date_col"]],
                "source_report_nm": row["source_report_nm"],
                "source_rcept_no": row["source_rcept_no"],
                "source_section": "의무보유확약",
                "parse_confidence": row["parse_confidence"],
                "note": row["note"],
            }
        )
    out = pd.DataFrame(out_rows)
    out["listing_date"] = pd.to_datetime(out["listing_date"])
    out["unlock_date"] = pd.to_datetime(out["unlock_date"])
    out["lockup_end_date"] = pd.to_datetime(out["lockup_end_date"])
    return out.sort_values(["unlock_date", "symbol"]).reset_index(drop=True)


def build_signals(sample: pd.DataFrame, unlocks: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    sample = sample.copy()
    sample["symbol"] = sample["symbol"].astype(str).str.zfill(6)
    issue_map = sample.set_index("symbol").to_dict(orient="index")
    unlock_map = {(row.symbol, row.term): row for row in unlocks.itertuples(index=False)}

    hit_rows = []
    miss_rows = []

    def add_hit(symbol: str, term: str, entry_ts: str, multiple: float, price_filter: str, entry_price: float, turnover_ratio: float, days_from_unlock: int, signal_name: str) -> None:
        issue = issue_map[symbol]
        ev = unlock_map[(symbol, term)]
        hit_rows.append(
            {
                "signal_name": signal_name,
                "symbol": symbol,
                "name": issue["name"],
                "listing_date": ev.listing_date,
                "unlock_date": ev.unlock_date,
                "entry_ts": pd.Timestamp(entry_ts),
                "entry_trade_date": pd.Timestamp(entry_ts).normalize(),
                "entry_price": entry_price,
                "term": term,
                "unlock_type": ev.unlock_type,
                "unlock_label": ev.unlock_type,
                "unlock_shares": ev.unlock_shares,
                "multiple": multiple,
                "threshold_shares": ev.unlock_shares * multiple,
                "cum_volume": ev.unlock_shares * turnover_ratio,
                "turnover_ratio": turnover_ratio,
                "session_open": round(entry_price * 0.985, 2),
                "cum_vwap": round(entry_price * 0.992, 2),
                "high_so_far": round(entry_price * 1.012, 2),
                "low_so_far": round(entry_price * 0.971, 2),
                "price_filter": price_filter,
                "cum_scope": "through_window",
                "aggregate_by": "type",
                "interval_min": 5,
                "bars_seen": 6 + int(multiple * 3),
                "days_from_unlock": days_from_unlock,
                "ipo_price": ev.ipo_price,
                "market": ev.market,
                "lead_manager": ev.lead_manager,
                "listed_shares": ev.listed_shares,
                "ipo_price_source": ev.ipo_price_source,
                "unlock_ratio": ev.unlock_ratio,
                "source_rcept_no": ev.source_rcept_no,
                "source_report_nm": ev.source_report_nm,
                "holder_group": ev.holder_group,
                "parse_confidence": ev.parse_confidence,
            }
        )

    def add_miss(symbol: str, term: str, multiple: float, price_filter: str, reason: str, max_cum_volume: float) -> None:
        issue = issue_map[symbol]
        ev = unlock_map[(symbol, term)]
        miss_rows.append(
            {
                "symbol": symbol,
                "name": issue["name"],
                "unlock_date": ev.unlock_date,
                "term": term,
                "unlock_label": ev.unlock_type,
                "multiple": multiple,
                "price_filter": price_filter,
                "reason": reason,
                "max_cum_volume": max_cum_volume,
                "unlock_shares": ev.unlock_shares,
            }
        )

    add_hit("480050", "1M", "2026-03-20 13:55:00+09:00", 1.0, "reclaim_open_or_vwap", 34_800, 1.12, 0, "inst_1m_1.0x_reclaim_open_or_vwap")
    add_hit("480040", "15D", "2026-03-27 10:15:00+09:00", 1.0, "reclaim_open_or_vwap", 18_100, 1.05, 0, "inst_15d_1.0x_reclaim_open_or_vwap")
    add_hit("480040", "15D", "2026-03-28 09:40:00+09:00", 2.0, "reclaim_open_or_vwap", 18_420, 2.01, 1, "inst_15d_2.0x_reclaim_open_or_vwap")
    add_hit("480080", "6M", "2026-03-23 11:20:00+09:00", 1.0, "range_top40", 19_480, 1.08, 1, "inst_6m_1.0x_range_top40")
    add_hit("480060", "1M", "2026-02-28 10:25:00+09:00", 1.0, "reclaim_open", 9_720, 1.03, 0, "inst_1m_1.0x_reclaim_open")

    add_miss("480050", "1M", 2.0, "reclaim_open_or_vwap", "threshold_not_reached_or_filter_failed", 1_450_000)
    add_miss("480080", "6M", 2.0, "range_top40", "threshold_not_reached_or_filter_failed", 1_520_000)
    add_miss("480020", "15D", 1.0, "reclaim_open_or_vwap", "bars_missing", 0)
    add_miss("480030", "15D", 1.0, "reclaim_open_or_vwap", "bars_missing", 0)

    hits = pd.DataFrame(hit_rows)
    misses = pd.DataFrame(miss_rows)
    return hits.sort_values(["unlock_date", "entry_ts", "symbol"]).reset_index(drop=True), misses.sort_values(["unlock_date", "symbol"]).reset_index(drop=True)


def build_turnover_backtest(signals: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    hold_days = {"15D": 5, "1M": 15, "6M": 30}
    trades_rows = []
    for row in signals.itertuples(index=False):
        if row.symbol == "480080" and float(row.multiple) == 1.0:
            exit_price = 18_910
        elif row.symbol == "480050":
            exit_price = 36_600
        elif row.symbol == "480040" and float(row.multiple) == 2.0:
            exit_price = 17_980
        elif row.symbol == "480040":
            exit_price = 18_980
        else:
            exit_price = 9_540
        entry_price = float(row.entry_price)
        gross_ret = exit_price / entry_price - 1.0
        net_ret = gross_ret - 0.003
        entry_dt = pd.Timestamp(row.entry_ts).tz_localize(None)
        exit_dt = entry_dt.normalize() + pd.tseries.offsets.BDay(hold_days.get(str(row.term), 10))
        trades_rows.append(
            {
                "signal_name": row.signal_name,
                "symbol": row.symbol,
                "name": row.name,
                "term": row.term,
                "unlock_type": row.unlock_type,
                "multiple": row.multiple,
                "price_filter": row.price_filter,
                "aggregate_by": row.aggregate_by,
                "cum_scope": row.cum_scope,
                "listing_date": row.listing_date,
                "unlock_date": row.unlock_date,
                "entry_dt": entry_dt,
                "entry_price": entry_price,
                "exit_dt": exit_dt,
                "exit_price": exit_price,
                "hold_days_after_entry": hold_days.get(str(row.term), 10),
                "ipo_price": row.ipo_price,
                "ipo_price_source": row.ipo_price_source,
                "prev_close_vs_ipo": (entry_price * 0.97) / float(row.ipo_price) if float(row.ipo_price) else None,
                "prev_close_date": entry_dt.normalize() - pd.tseries.offsets.BDay(1),
                "turnover_ratio": row.turnover_ratio,
                "cum_volume": row.cum_volume,
                "gross_ret": gross_ret,
                "net_ret": net_ret,
            }
        )
    trades = pd.DataFrame(trades_rows)
    group_cols = ["signal_name", "term", "unlock_type", "multiple", "price_filter", "hold_days_after_entry"]
    summary = (
        trades.groupby(group_cols, as_index=False)
        .agg(
            trades=("symbol", "size"),
            win_rate=("net_ret", lambda s: (s > 0).mean()),
            avg_ret=("net_ret", "mean"),
            median_ret=("net_ret", "median"),
            sum_ret=("net_ret", "sum"),
            compound_ret=("net_ret", lambda s: (1.0 + s).prod() - 1.0),
            min_ret=("net_ret", "min"),
            max_ret=("net_ret", "max"),
        )
        .sort_values(["term", "multiple", "price_filter", "signal_name"])
        .reset_index(drop=True)
    )
    annual = (
        trades.assign(year=pd.to_datetime(trades["entry_dt"]).dt.year)
        .groupby(["year"] + group_cols, as_index=False)
        .agg(
            trades=("symbol", "size"),
            win_rate=("net_ret", lambda s: (s > 0).mean()),
            avg_ret=("net_ret", "mean"),
            median_ret=("net_ret", "median"),
            sum_ret=("net_ret", "sum"),
            compound_ret=("net_ret", lambda s: (1.0 + s).prod() - 1.0),
            min_ret=("net_ret", "min"),
            max_ret=("net_ret", "max"),
        )
        .reset_index(drop=True)
    )
    skip_reasons = pd.DataFrame(
        [
            {"signal_name": "inst_1m_2.0x_reclaim_open_or_vwap", "term": "1M", "reason": "threshold_not_reached_or_filter_failed", "symbol": "480050", "name": "퀀텀로보틱스", "unlock_date": "2026-03-20", "prev_close_vs_ipo": 1.05, "threshold": 2.0},
            {"signal_name": "inst_6m_2.0x_range_top40", "term": "6M", "reason": "threshold_not_reached_or_filter_failed", "symbol": "480080", "name": "스마트그리드솔루션", "unlock_date": "2026-03-22", "prev_close_vs_ipo": 0.94, "threshold": 2.0},
            {"signal_name": "inst_15d_1.0x_reclaim_open_or_vwap", "term": "15D", "reason": "bars_missing", "symbol": "480020", "name": "에이아이센서", "unlock_date": "2026-04-18", "prev_close_vs_ipo": None, "threshold": 1.0},
        ]
    )
    skip_summary = (
        skip_reasons.groupby(["signal_name", "term", "reason"], as_index=False)
        .size()
        .rename(columns={"size": "count"})
    )
    return trades, summary, annual, skip_reasons, skip_summary


def build_beta_summary(trades: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    enriched = trades.copy()
    bench_returns = {
        "480050": 0.021,
        "480040": 0.009,
        "480080": -0.011,
        "480060": -0.004,
    }
    enriched["bench_window_ret"] = enriched["symbol"].map(bench_returns).astype(float)
    enriched["strategy_name"] = "turnover_unlock"
    rows = []
    for keys, grp in enriched.groupby(["strategy_name", "signal_name", "term"], dropna=False):
        strategy_name, signal_name, term = keys
        beta_proxy = grp["net_ret"].cov(grp["bench_window_ret"]) / grp["bench_window_ret"].var() if len(grp) > 1 and grp["bench_window_ret"].var() not in (0, None) else 0.75
        corr = grp["net_ret"].corr(grp["bench_window_ret"]) if len(grp) > 1 else 0.25
        avg_ret = float(grp["net_ret"].mean())
        avg_bench_ret = float(grp["bench_window_ret"].mean())
        alpha_proxy = avg_ret - float(beta_proxy) * avg_bench_ret
        rows.append(
            {
                "strategy_name": strategy_name,
                "signal_name": signal_name,
                "term": term,
                "trades": len(grp),
                "avg_ret": avg_ret,
                "avg_bench_ret": avg_bench_ret,
                "beta_proxy": float(beta_proxy),
                "corr": float(corr) if pd.notna(corr) else None,
                "alpha_proxy": float(alpha_proxy),
            }
        )
    summary = pd.DataFrame(rows).sort_values(["term", "signal_name"]).reset_index(drop=True)
    return enriched, summary


def build_minute_db(unlocks: pd.DataFrame, signals: pd.DataFrame, parts: dict[str, Path]) -> None:
    db_path = parts["minute_db_dir"] / "lockup_minute.db"
    if db_path.exists():
        db_path.unlink()
    conn = sqlite3.connect(db_path)
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS unlock_events (
            symbol TEXT NOT NULL,
            corp_name TEXT,
            unlock_date TEXT NOT NULL,
            unlock_type TEXT NOT NULL,
            unlock_shares INTEGER NOT NULL,
            source_rcp_no TEXT,
            PRIMARY KEY (symbol, unlock_date, unlock_type)
        );
        CREATE TABLE IF NOT EXISTS minute_jobs (
            job_id TEXT PRIMARY KEY,
            symbol TEXT NOT NULL,
            interval_min INTEGER NOT NULL,
            start_ts TEXT NOT NULL,
            end_ts TEXT NOT NULL,
            reason TEXT NOT NULL,
            priority INTEGER NOT NULL,
            status TEXT NOT NULL DEFAULT 'queued',
            retry_count INTEGER NOT NULL DEFAULT 0,
            last_error TEXT,
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            updated_at TEXT NOT NULL DEFAULT (datetime('now'))
        );
        CREATE TABLE IF NOT EXISTS minute_bars (
            symbol TEXT NOT NULL,
            interval_min INTEGER NOT NULL,
            ts TEXT NOT NULL,
            trade_date TEXT NOT NULL,
            open INTEGER NOT NULL,
            high INTEGER NOT NULL,
            low INTEGER NOT NULL,
            close INTEGER NOT NULL,
            volume INTEGER NOT NULL,
            amount INTEGER,
            adjusted_flag TEXT,
            source TEXT NOT NULL,
            ingested_at TEXT NOT NULL DEFAULT (datetime('now')),
            PRIMARY KEY (symbol, interval_min, ts)
        );
        CREATE TABLE IF NOT EXISTS fetch_log (
            fetch_id INTEGER PRIMARY KEY AUTOINCREMENT,
            job_id TEXT,
            symbol TEXT,
            api_family TEXT,
            tr_code TEXT,
            request_started_at TEXT,
            request_finished_at TEXT,
            response_rows INTEGER,
            continued TEXT,
            status TEXT,
            error_code TEXT,
            error_message TEXT
        );
        """
    )
    unlock_rows = []
    for row in unlocks.itertuples(index=False):
        unlock_rows.append((row.symbol, row.name, str(pd.Timestamp(row.unlock_date).date()), row.unlock_type, int(row.unlock_shares), row.source_rcept_no))
    conn.executemany(
        "INSERT INTO unlock_events (symbol, corp_name, unlock_date, unlock_type, unlock_shares, source_rcp_no) VALUES (?, ?, ?, ?, ?, ?)",
        unlock_rows,
    )

    job_rows = []
    for row in unlocks.itertuples(index=False):
        unlock_date = pd.Timestamp(row.unlock_date)
        status = "done" if unlock_date <= pd.Timestamp("2026-03-28") else "queued"
        if row.symbol == "480030":
            status = "running"
        job_rows.append(
            (
                f"{row.symbol}_{unlock_date.date()}_{row.unlock_type}_5m",
                row.symbol,
                5,
                (unlock_date - pd.Timedelta(days=2)).strftime("%Y-%m-%dT00:00:00+09:00"),
                (unlock_date + pd.Timedelta(days=5)).strftime("%Y-%m-%dT23:59:59+09:00"),
                "unlock_event",
                100,
                status,
                0,
                None if status != "failed" else "network timeout",
                "2026-03-26 09:00:00",
                "2026-03-26 09:10:00",
            )
        )
    conn.executemany(
        "INSERT INTO minute_jobs (job_id, symbol, interval_min, start_ts, end_ts, reason, priority, status, retry_count, last_error, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        job_rows,
    )

    bars_rows = []
    hist_symbols = ["480050", "480040", "480080", "480060"]
    for symbol in hist_symbols:
        if symbol == "480060":
            unlock_date = pd.Timestamp("2026-02-28")
            base_price = 9700
        else:
            unlock_date = pd.to_datetime(unlocks.loc[unlocks["symbol"] == symbol, "unlock_date"].iloc[0])
            base_price = int(unlocks.loc[unlocks["symbol"] == symbol, "ipo_price"].iloc[0] * (1.02 if symbol != "480080" else 0.95))
        for day_offset in range(0, 2):
            trade_day = unlock_date + pd.Timedelta(days=day_offset)
            start = trade_day.normalize() + pd.Timedelta(hours=9)
            for idx in range(12):
                ts = start + pd.Timedelta(minutes=5 * idx)
                open_p = base_price + idx * 12 + day_offset * 25
                close_p = open_p + (18 if idx % 3 != 0 else -7)
                high_p = max(open_p, close_p) + 9
                low_p = min(open_p, close_p) - 11
                volume = 50_000 + idx * 9_000 + day_offset * 30_000
                amount = int(close_p * volume)
                bars_rows.append((symbol, 5, ts.strftime("%Y-%m-%dT%H:%M:%S+09:00"), str(ts.date()), int(open_p), int(high_p), int(low_p), int(close_p), int(volume), int(amount), None, "sample_csv_import"))
    conn.executemany(
        "INSERT INTO minute_bars (symbol, interval_min, ts, trade_date, open, high, low, close, volume, amount, adjusted_flag, source) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        bars_rows,
    )

    fetch_rows = [
        (None, "480050", "csv_import", "OPT10080", "2026-03-20T13:00:00+09:00", "2026-03-20T13:00:05+09:00", 24, "N", "done", None, None),
        (None, "480040", "csv_import", "OPT10080", "2026-03-27T10:00:00+09:00", "2026-03-27T10:00:04+09:00", 24, "N", "done", None, None),
    ]
    conn.executemany(
        "INSERT INTO fetch_log (job_id, symbol, api_family, tr_code, request_started_at, request_finished_at, response_rows, continued, status, error_code, error_message) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        fetch_rows,
    )
    conn.commit()
    conn.close()


def main() -> None:
    parts = ensure_dirs()
    sample = pd.read_csv(DATA_DIR / "sample_ipo_events.csv", dtype={"symbol": str})
    unlocks = build_unlock_events(sample)
    hits, misses = build_signals(sample, unlocks)
    trades, summary, annual, skip_reasons, skip_summary = build_turnover_backtest(hits)
    beta_trades, beta_summary = build_beta_summary(trades)
    build_minute_db(unlocks, hits, parts)

    unlocks.to_csv(parts["unlock_out"] / "unlock_events_backtest_input.csv", index=False, encoding="utf-8-sig")
    hits.to_csv(parts["signal_out"] / "turnover_signals.csv", index=False, encoding="utf-8-sig")
    misses.to_csv(parts["signal_out"] / "turnover_signals_misses.csv", index=False, encoding="utf-8-sig")
    trades.to_csv(parts["turnover_backtest_out"] / "all_trades.csv", index=False, encoding="utf-8-sig")
    summary.to_csv(parts["turnover_backtest_out"] / "summary_all.csv", index=False, encoding="utf-8-sig")
    make_pretty_pct(summary).to_csv(parts["turnover_backtest_out"] / "summary_all_pretty.csv", index=False, encoding="utf-8-sig")
    annual.to_csv(parts["turnover_backtest_out"] / "annual_all.csv", index=False, encoding="utf-8-sig")
    make_pretty_pct(annual).to_csv(parts["turnover_backtest_out"] / "annual_all_pretty.csv", index=False, encoding="utf-8-sig")
    skip_summary.to_csv(parts["turnover_backtest_out"] / "backtest_skip_summary.csv", index=False, encoding="utf-8-sig")
    skip_reasons.to_csv(parts["turnover_backtest_out"] / "backtest_skip_reasons.csv", index=False, encoding="utf-8-sig")
    beta_summary.to_csv(parts["analysis_out"] / "trade_window_beta_summary.csv", index=False, encoding="utf-8-sig")
    beta_trades.to_csv(parts["analysis_out"] / "trade_window_beta_summary_trades.csv", index=False, encoding="utf-8-sig")
    print(f"sample unified lab workspace generated -> {WORKSPACE}")


if __name__ == "__main__":
    main()
