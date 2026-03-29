from __future__ import annotations

import argparse
import math
import re
import sqlite3
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Iterable, Iterator, List, Optional, Sequence

import pandas as pd

SEOUL_TZ = "+09:00"
DEFAULT_DB_PATH = Path("data/curated/lockup_minute.db")


@dataclass(frozen=True)
class UnlockEvent:
    symbol: str
    corp_name: str
    unlock_date: str      # YYYY-MM-DD
    unlock_type: str      # inst_15d / inst_1m / inst_3m / inst_6m / ...
    unlock_shares: int
    source_rcp_no: str = ""


@dataclass(frozen=True)
class MinuteJob:
    job_id: str
    symbol: str
    interval_min: int
    start_ts: str         # ISO8601 Asia/Seoul
    end_ts: str           # ISO8601 Asia/Seoul
    reason: str           # unlock_event / refill_gap / live_watch
    priority: int = 100


@dataclass(frozen=True)
class MinuteBar:
    symbol: str
    interval_min: int
    ts: str               # ISO8601 Asia/Seoul
    trade_date: str       # YYYY-MM-DD
    open: int
    high: int
    low: int
    close: int
    volume: int
    amount: Optional[int] = None
    adjusted_flag: Optional[str] = None
    source: str = "csv_import"


def _read_csv_auto(path: Path) -> pd.DataFrame:
    last_err = None
    for enc in ("utf-8-sig", "utf-8", "cp949", "euc-kr", "latin1"):
        try:
            return pd.read_csv(path, encoding=enc)
        except Exception as exc:  # noqa: BLE001
            last_err = exc
    raise RuntimeError(f"CSV를 읽지 못했습니다: {path} / {last_err}")


def _norm_col(s: str) -> str:
    x = str(s or "").replace("\xa0", " ").strip().lower()
    x = re.sub(r"[\s_./\-]+", "", x)
    return x


def _parse_date(v: object) -> pd.Timestamp:
    s = str(v or "").strip()
    s = re.sub(r"[^0-9]", "", s)
    if len(s) == 8:
        return pd.to_datetime(s, format="%Y%m%d", errors="coerce")
    return pd.to_datetime(v, errors="coerce")


def _parse_time_str(v: object) -> str:
    s = re.sub(r"[^0-9]", "", str(v or "").strip())
    if s == "":
        return "00:00:00"
    if len(s) <= 4:
        s = s.zfill(4)
        return f"{s[:2]}:{s[2:4]}:00"
    if len(s) <= 6:
        s = s.zfill(6)
        return f"{s[:2]}:{s[2:4]}:{s[4:6]}"
    return "00:00:00"


def _safe_int(v: object) -> Optional[int]:
    s = str(v if v is not None else "").strip()
    if s == "" or s.lower() == "nan":
        return None
    s = s.replace(",", "")
    m = re.search(r"-?\d+(?:\.\d+)?", s)
    if not m:
        return None
    try:
        return int(float(m.group(0)))
    except Exception:
        return None


def _infer_symbol_from_path(path: Path) -> Optional[str]:
    m = re.search(r"(?<!\d)(\d{6})(?!\d)", path.stem)
    if m:
        return m.group(1)
    return None


def _detect_csv_columns(df: pd.DataFrame) -> dict:
    norm_map = {c: _norm_col(c) for c in df.columns}
    result = {}

    def first_match(candidates: Sequence[str]) -> Optional[str]:
        for raw, norm in norm_map.items():
            if norm in candidates or any(tok == norm for tok in candidates):
                return raw
        for raw, norm in norm_map.items():
            if any(tok in norm for tok in candidates):
                return raw
        return None

    result["symbol"] = first_match(["symbol", "종목코드", "단축코드", "code", "shcode"])
    result["datetime"] = first_match(["datetime", "일시", "체결시간", "dt", "timestamp", "ts"])
    result["date"] = first_match(["date", "일자", "영업일자", "tradedate", "ymd"])
    result["time"] = first_match(["time", "시간", "체결시각", "hhmmss"])
    result["open"] = first_match(["open", "시가"])
    result["high"] = first_match(["high", "고가"])
    result["low"] = first_match(["low", "저가"])
    result["close"] = first_match(["close", "종가", "현재가", "price", "종료가"])
    result["volume"] = first_match(["volume", "거래량", "누적거래량", "vol"])
    result["amount"] = first_match(["amount", "거래대금", "대금"])
    return result


def load_unlock_events_from_csv(path: str | Path) -> list[UnlockEvent]:
    df = pd.read_csv(path, dtype={"symbol": str})
    if df.empty:
        return []
    if "symbol" not in df.columns:
        raise ValueError("unlock csv에 symbol 컬럼이 필요합니다.")
    if "unlock_date" not in df.columns:
        raise ValueError("unlock csv에 unlock_date 컬럼이 필요합니다.")
    if "unlock_shares" not in df.columns:
        raise ValueError("unlock csv에 unlock_shares 컬럼이 필요합니다.")

    out: list[UnlockEvent] = []
    for _, r in df.iterrows():
        symbol = str(r.get("symbol", "")).zfill(6)
        if not re.fullmatch(r"\d{6}", symbol):
            continue
        unlock_shares = _safe_int(r.get("unlock_shares"))
        if unlock_shares is None or unlock_shares <= 0:
            continue
        out.append(
            UnlockEvent(
                symbol=symbol,
                corp_name=str(r.get("name", r.get("corp_name", ""))),
                unlock_date=str(pd.Timestamp(r["unlock_date"]).date()),
                unlock_type=str(r.get("unlock_type", r.get("term", "unknown"))),
                unlock_shares=unlock_shares,
                source_rcp_no=str(r.get("source_rcept_no", "")),
            )
        )
    return out


class SQLiteStore:
    def __init__(self, path: Path = DEFAULT_DB_PATH) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        self.path = path
        self.conn = sqlite3.connect(path)
        self.conn.execute("PRAGMA journal_mode=WAL;")
        self.conn.execute("PRAGMA synchronous=NORMAL;")
        self._init_schema()

    def close(self) -> None:
        self.conn.close()

    def _init_schema(self) -> None:
        self.conn.executescript(
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
        self.conn.commit()

    def upsert_unlock_events(self, events: Iterable[UnlockEvent]) -> None:
        rows = [
            (e.symbol, e.corp_name, e.unlock_date, e.unlock_type, e.unlock_shares, e.source_rcp_no)
            for e in events
        ]
        self.conn.executemany(
            """
            INSERT INTO unlock_events (symbol, corp_name, unlock_date, unlock_type, unlock_shares, source_rcp_no)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(symbol, unlock_date, unlock_type) DO UPDATE SET
                corp_name=excluded.corp_name,
                unlock_shares=excluded.unlock_shares,
                source_rcp_no=excluded.source_rcp_no
            """,
            rows,
        )
        self.conn.commit()

    def enqueue_jobs(self, jobs: Iterable[MinuteJob]) -> None:
        rows = [(j.job_id, j.symbol, j.interval_min, j.start_ts, j.end_ts, j.reason, j.priority) for j in jobs]
        self.conn.executemany(
            """
            INSERT OR IGNORE INTO minute_jobs (job_id, symbol, interval_min, start_ts, end_ts, reason, priority)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )
        self.conn.commit()

    def upsert_bars(self, bars: Iterable[MinuteBar]) -> int:
        rows = [
            (
                b.symbol,
                b.interval_min,
                b.ts,
                b.trade_date,
                b.open,
                b.high,
                b.low,
                b.close,
                b.volume,
                b.amount,
                b.adjusted_flag,
                b.source,
            )
            for b in bars
        ]
        if not rows:
            return 0
        self.conn.executemany(
            """
            INSERT INTO minute_bars
            (symbol, interval_min, ts, trade_date, open, high, low, close, volume, amount, adjusted_flag, source)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(symbol, interval_min, ts) DO UPDATE SET
                trade_date=excluded.trade_date,
                open=excluded.open,
                high=excluded.high,
                low=excluded.low,
                close=excluded.close,
                volume=excluded.volume,
                amount=excluded.amount,
                adjusted_flag=excluded.adjusted_flag,
                source=excluded.source,
                ingested_at=datetime('now')
            """,
            rows,
        )
        self.conn.commit()
        return len(rows)

    def queue_counts(self) -> pd.DataFrame:
        return pd.read_sql_query(
            """
            SELECT status, COUNT(*) AS jobs
            FROM minute_jobs
            GROUP BY status
            ORDER BY status
            """,
            self.conn,
        )

    def queue_preview(self, limit: int = 20) -> pd.DataFrame:
        return pd.read_sql_query(
            """
            SELECT job_id, symbol, interval_min, start_ts, end_ts, reason, priority, status, retry_count, last_error
            FROM minute_jobs
            ORDER BY status, priority, created_at
            LIMIT ?
            """,
            self.conn,
            params=(int(limit),),
        )

    def bar_stats(self) -> pd.DataFrame:
        return pd.read_sql_query(
            """
            SELECT symbol, interval_min, MIN(ts) AS min_ts, MAX(ts) AS max_ts, COUNT(*) AS bars
            FROM minute_bars
            GROUP BY symbol, interval_min
            ORDER BY bars DESC, symbol
            LIMIT 50
            """,
            self.conn,
        )


class RequestThrottler:
    def __init__(self, min_interval_sec: float = 0.35) -> None:
        self.min_interval_sec = min_interval_sec
        self._last_ts = 0.0

    def wait(self) -> None:
        now = time.monotonic()
        delta = now - self._last_ts
        if delta < self.min_interval_sec:
            time.sleep(self.min_interval_sec - delta)
        self._last_ts = time.monotonic()


class KiwoomOpenAPISession:
    """Windows OpenAPI+ adapter placeholder.

    실제 구현이 필요한 부분:
    - QAxWidget 로그인
    - SetInputValue/CommRqData("OPT10080")
    - nPrevNext=2 연속조회
    - GetCommDataEx("OPT10080", "주식분봉차트조회") 파싱
    """

    def login(self) -> None:
        raise NotImplementedError("Windows + Kiwoom OpenAPI+ 환경에서 구현하세요.")

    def fetch_minute_bars(
        self,
        symbol: str,
        interval_min: int,
        start_ts: str,
        end_ts: str,
    ) -> list[MinuteBar]:
        raise NotImplementedError("OPT10080 분봉 조회 구현이 필요합니다.")


def build_jobs_from_unlock_events(
    events: list[UnlockEvent],
    interval_min: int = 5,
    pre_days: int = 2,
    post_days: int = 5,
) -> list[MinuteJob]:
    jobs: list[MinuteJob] = []
    for e in events:
        unlock_dt = datetime.fromisoformat(e.unlock_date + "T00:00:00")
        start_dt = unlock_dt - timedelta(days=int(pre_days))
        end_dt = unlock_dt + timedelta(days=int(post_days))
        job_id = f"{e.symbol}_{e.unlock_date}_{e.unlock_type}_{interval_min}m"
        jobs.append(
            MinuteJob(
                job_id=job_id,
                symbol=e.symbol,
                interval_min=int(interval_min),
                start_ts=start_dt.strftime("%Y-%m-%dT00:00:00" + SEOUL_TZ),
                end_ts=end_dt.strftime("%Y-%m-%dT23:59:59" + SEOUL_TZ),
                reason="unlock_event",
                priority=100,
            )
        )
    return jobs


def import_minute_csv_to_bars(
    csv_path: str | Path,
    interval_min: int,
    symbol_override: Optional[str] = None,
    source_name: str = "csv_import",
) -> list[MinuteBar]:
    path = Path(csv_path)
    df = _read_csv_auto(path)
    cols = _detect_csv_columns(df)

    symbol_col = cols.get("symbol")
    datetime_col = cols.get("datetime")
    date_col = cols.get("date")
    time_col = cols.get("time")
    open_col = cols.get("open")
    high_col = cols.get("high")
    low_col = cols.get("low")
    close_col = cols.get("close")
    volume_col = cols.get("volume")
    amount_col = cols.get("amount")

    missing = [k for k, v in {
        "open": open_col,
        "high": high_col,
        "low": low_col,
        "close": close_col,
        "volume": volume_col,
    }.items() if v is None]
    if missing:
        raise RuntimeError(f"{path.name}: 필수 컬럼 누락 -> {missing}")

    if datetime_col is None and (date_col is None or time_col is None):
        raise RuntimeError(f"{path.name}: datetime 또는 date+time 컬럼을 찾지 못했습니다.")

    symbol_default = symbol_override or (
        str(df[symbol_col].iloc[0]).zfill(6) if symbol_col and not df.empty else _infer_symbol_from_path(path)
    )
    if not symbol_default or not re.fullmatch(r"\d{6}", str(symbol_default).zfill(6)):
        raise RuntimeError(f"{path.name}: 종목코드를 추론하지 못했습니다. --symbol을 지정하세요.")
    symbol_default = str(symbol_default).zfill(6)

    out: list[MinuteBar] = []
    for _, r in df.iterrows():
        symbol = symbol_default
        if symbol_col and pd.notna(r.get(symbol_col)):
            maybe_symbol = re.sub(r"\D", "", str(r.get(symbol_col)))
            if len(maybe_symbol) == 6:
                symbol = maybe_symbol

        if datetime_col is not None:
            ts = pd.to_datetime(r.get(datetime_col), errors="coerce")
        else:
            d = _parse_date(r.get(date_col))
            t = _parse_time_str(r.get(time_col))
            ts = pd.to_datetime(f"{d.date()} {t}", errors="coerce")

        if pd.isna(ts):
            continue
        ts = pd.Timestamp(ts)
        trade_date = str(ts.date())
        open_v = _safe_int(r.get(open_col))
        high_v = _safe_int(r.get(high_col))
        low_v = _safe_int(r.get(low_col))
        close_v = _safe_int(r.get(close_col))
        volume_v = _safe_int(r.get(volume_col))
        amount_v = _safe_int(r.get(amount_col)) if amount_col else None

        if any(v is None for v in [open_v, high_v, low_v, close_v, volume_v]):
            continue

        out.append(
            MinuteBar(
                symbol=symbol,
                interval_min=int(interval_min),
                ts=ts.strftime("%Y-%m-%dT%H:%M:%S" + SEOUL_TZ),
                trade_date=trade_date,
                open=int(open_v),
                high=int(high_v),
                low=int(low_v),
                close=int(close_v),
                volume=int(volume_v),
                amount=amount_v,
                source=source_name,
            )
        )
    return out


def run_collector(store: SQLiteStore, session: KiwoomOpenAPISession, min_interval_sec: float = 0.35) -> None:
    throttler = RequestThrottler(min_interval_sec=min_interval_sec)
    session.login()
    print("collector start")
    while True:
        row = store.conn.execute(
            """
            SELECT job_id, symbol, interval_min, start_ts, end_ts, reason, priority
            FROM minute_jobs
            WHERE status='queued'
            ORDER BY priority ASC, created_at ASC
            LIMIT 1
            """
        ).fetchone()
        if row is None:
            print("No queued jobs left.")
            break

        job = MinuteJob(*row)
        store.conn.execute(
            "UPDATE minute_jobs SET status='running', updated_at=datetime('now') WHERE job_id=?",
            (job.job_id,),
        )
        store.conn.commit()

        try:
            throttler.wait()
            bars = session.fetch_minute_bars(job.symbol, job.interval_min, job.start_ts, job.end_ts)
            inserted = store.upsert_bars(bars)
            store.conn.execute(
                "UPDATE minute_jobs SET status='done', updated_at=datetime('now') WHERE job_id=?",
                (job.job_id,),
            )
            store.conn.commit()
            print(f"DONE {job.job_id}: {inserted} bars")
        except Exception as exc:  # noqa: BLE001
            store.conn.execute(
                """
                UPDATE minute_jobs
                SET status='failed', retry_count=retry_count+1, last_error=?, updated_at=datetime('now')
                WHERE job_id=?
                """,
                (str(exc)[:500], job.job_id),
            )
            store.conn.commit()
            print(f"FAILED {job.job_id}: {exc}")


def cli_main(argv: Optional[Sequence[str]] = None) -> None:
    parser = argparse.ArgumentParser(description="Minute DB / queue helper for IPO lockup turnover research")
    parser.add_argument("--db-path", default=str(DEFAULT_DB_PATH))
    sub = parser.add_subparsers(dest="cmd", required=True)

    p1 = sub.add_parser("init-db")
    p2 = sub.add_parser("enqueue-from-unlock")
    p2.add_argument("--unlock-csv", required=True)
    p2.add_argument("--interval-min", type=int, default=5)
    p2.add_argument("--pre-days", type=int, default=2)
    p2.add_argument("--post-days", type=int, default=5)

    p3 = sub.add_parser("show-queue")
    p3.add_argument("--limit", type=int, default=20)

    p4 = sub.add_parser("import-minute-csv")
    p4.add_argument("--csv-path", required=True)
    p4.add_argument("--interval-min", type=int, default=5)
    p4.add_argument("--symbol", default=None)
    p4.add_argument("--source-name", default="csv_import")

    p5 = sub.add_parser("import-minute-glob")
    p5.add_argument("--glob", required=True)
    p5.add_argument("--interval-min", type=int, default=5)
    p5.add_argument("--source-name", default="csv_import")

    p6 = sub.add_parser("run-collector")
    p6.add_argument("--min-interval-sec", type=float, default=0.35)

    args = parser.parse_args(argv)
    store = SQLiteStore(Path(args.db_path))

    try:
        if args.cmd == "init-db":
            print(f"[DONE] DB initialized: {store.path}")

        elif args.cmd == "enqueue-from-unlock":
            events = load_unlock_events_from_csv(args.unlock_csv)
            store.upsert_unlock_events(events)
            jobs = build_jobs_from_unlock_events(
                events,
                interval_min=args.interval_min,
                pre_days=args.pre_days,
                post_days=args.post_days,
            )
            store.enqueue_jobs(jobs)
            print(f"[DONE] unlock_events={len(events)} jobs_enqueued={len(jobs)}")

        elif args.cmd == "show-queue":
            counts = store.queue_counts()
            preview = store.queue_preview(limit=args.limit)
            print("[queue counts]")
            print(counts.to_string(index=False) if not counts.empty else "(empty)")
            print("\n[queue preview]")
            print(preview.to_string(index=False) if not preview.empty else "(empty)")
            stats = store.bar_stats()
            print("\n[bar stats]")
            print(stats.to_string(index=False) if not stats.empty else "(empty)")

        elif args.cmd == "import-minute-csv":
            bars = import_minute_csv_to_bars(
                csv_path=args.csv_path,
                interval_min=args.interval_min,
                symbol_override=args.symbol,
                source_name=args.source_name,
            )
            inserted = store.upsert_bars(bars)
            print(f"[DONE] imported {inserted} bars from {args.csv_path}")

        elif args.cmd == "import-minute-glob":
            import glob
            total = 0
            files = sorted(glob.glob(args.glob))
            if not files:
                raise FileNotFoundError(f"glob 결과가 없습니다: {args.glob}")
            for fp in files:
                bars = import_minute_csv_to_bars(
                    csv_path=fp,
                    interval_min=args.interval_min,
                    symbol_override=None,
                    source_name=args.source_name,
                )
                inserted = store.upsert_bars(bars)
                total += inserted
                print(f"[OK] {Path(fp).name}: {inserted} bars")
            print(f"[DONE] total imported bars={total}")

        elif args.cmd == "run-collector":
            session = KiwoomOpenAPISession()
            run_collector(store, session=session, min_interval_sec=args.min_interval_sec)

        else:
            raise ValueError(args.cmd)
    finally:
        store.close()


if __name__ == "__main__":
    cli_main()
