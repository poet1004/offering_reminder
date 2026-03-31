from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pandas as pd

from src.services.kis_client import KISClient
from src.services.turnover_strategy_service import TurnoverStrategyParams, TurnoverStrategyService


def main() -> None:
    parser = argparse.ArgumentParser(description="Run turnover strategy research from Unified Lab workspace and export CSVs")
    parser.add_argument("--workspace", default=None, help="Unified Lab workspace path")
    parser.add_argument("--allow-packaged-sample", action="store_true", help="Allow packaged demo workspace autodetection")
    parser.add_argument("--multiples", default="1.0,1.5,2.0")
    parser.add_argument("--price-filters", default="reclaim_open_or_vwap")
    parser.add_argument("--max-days-after", type=int, default=5)
    parser.add_argument("--aggregate-by", default="type", choices=["type", "term", "day", "none"])
    parser.add_argument("--cum-scope", default="through_window", choices=["through_window", "same_day"])
    parser.add_argument("--interval-min", type=int, default=5)
    parser.add_argument("--unlock-start-date", default=None)
    parser.add_argument("--unlock-end-date", default=None)
    parser.add_argument("--terms", default="")
    parser.add_argument("--unlock-types", default="")
    parser.add_argument("--max-events", type=int, default=40)
    parser.add_argument("--min-prev-close-vs-ipo", type=float, default=0.0)
    parser.add_argument("--max-prev-close-vs-ipo", type=float, default=0.0)
    parser.add_argument("--out-dir", default=str(ROOT / "data" / "exports" / "turnover_research_latest"))
    args = parser.parse_args()

    params = TurnoverStrategyParams(
        interval_min=args.interval_min,
        multiples=tuple(float(x.strip()) for x in str(args.multiples).split(",") if x.strip()),
        price_filters=tuple(str(x).strip() for x in str(args.price_filters).split(",") if x.strip()),
        max_days_after=args.max_days_after,
        aggregate_by=args.aggregate_by,
        cum_scope=args.cum_scope,
        unlock_terms=tuple(str(x).strip().upper() for x in str(args.terms).split(",") if x.strip()),
        unlock_types=tuple(str(x).strip() for x in str(args.unlock_types).split(",") if x.strip()),
        unlock_start_date=args.unlock_start_date,
        unlock_end_date=args.unlock_end_date,
        max_events=args.max_events,
        min_prev_close_vs_ipo=None if args.min_prev_close_vs_ipo <= 0 else args.min_prev_close_vs_ipo,
        max_prev_close_vs_ipo=None if args.max_prev_close_vs_ipo <= 0 else args.max_prev_close_vs_ipo,
    ).normalized()

    service = TurnoverStrategyService(ROOT / "data", kis_client=KISClient.from_env())
    result = service.run_workspace_research(args.workspace, params, allow_packaged_sample=args.allow_packaged_sample)

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    for key in ["unlocks", "signals", "misses", "trades", "summary", "annual", "skip_summary", "skip_reasons", "diagnostics"]:
        frame = result.get(key, pd.DataFrame())
        if isinstance(frame, pd.DataFrame):
            frame.to_csv(out_dir / f"{key}.csv", index=False, encoding="utf-8-sig")
    (out_dir / "params.json").write_text(json.dumps(result.get("params", {}), ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"[DONE] turnover research exported -> {out_dir}")
    print({key: len(result.get(key, [])) if isinstance(result.get(key), pd.DataFrame) else result.get(key) for key in ["unlocks", "signals", "misses", "trades"]})


if __name__ == "__main__":
    main()
