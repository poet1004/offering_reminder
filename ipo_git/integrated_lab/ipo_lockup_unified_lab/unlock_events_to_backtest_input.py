from __future__ import annotations

import argparse
from pathlib import Path
import pandas as pd


def main() -> None:
    p = argparse.ArgumentParser(description="Convert unlock_events_dart.csv into current backtester input format")
    p.add_argument("--unlock-csv", required=True)
    p.add_argument("--master-csv", required=True)
    p.add_argument("--out-csv", required=True)
    args = p.parse_args()

    unlock = pd.read_csv(args.unlock_csv, parse_dates=["listing_date", "unlock_date", "lockup_end_date"])
    master = pd.read_csv(args.master_csv)

    cols = [c for c in ["symbol", "ipo_price", "market", "lead_manager", "listed_shares", "ipo_price_source"] if c in master.columns]
    merged = unlock.merge(master[cols].drop_duplicates(subset=["symbol"]), on="symbol", how="left")
    merged = merged.rename(columns={"lockup_term": "term"})

    keep = [
        "symbol",
        "name",
        "listing_date",
        "unlock_date",
        "term",
        "ipo_price",
        "market",
        "lead_manager",
        "listed_shares",
        "ipo_price_source",
        "unlock_type",
        "holder_group",
        "holder_name",
        "relation",
        "unlock_shares",
        "unlock_ratio",
        "lockup_end_date",
        "source_report_nm",
        "source_rcept_no",
        "source_section",
        "parse_confidence",
        "note",
    ]
    keep = [c for c in keep if c in merged.columns]
    out = merged[keep].copy()
    Path(args.out_csv).parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(args.out_csv, index=False, encoding="utf-8-sig")
    print(f"[DONE] rows={len(out)} -> {args.out_csv}")


if __name__ == "__main__":
    main()
