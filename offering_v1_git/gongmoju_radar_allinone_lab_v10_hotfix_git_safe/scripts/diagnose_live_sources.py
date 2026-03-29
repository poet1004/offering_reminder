from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.services.dart_client import DartClient
from src.services.ipo_pipeline import IPODataHub
from src.services.kis_client import KISClient
from src.services.market_service import MarketService
from src.utils import load_project_env, runtime_dir


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="실데이터 소스 실패 원인을 진단합니다.")
    parser.add_argument("--json-out", default=None, help="JSON 저장 경로")
    parser.add_argument("--skip-ipo-refresh", action="store_true", help="KIND/38 갱신은 건너뜀")
    parser.add_argument("--skip-market", action="store_true", help="시장 진단은 건너뜀")
    return parser.parse_args()


def df_to_records(df: pd.DataFrame, limit: int = 50) -> list[dict[str, Any]]:
    if df is None or df.empty:
        return []
    work = df.head(limit).copy()
    for col in work.columns:
        if pd.api.types.is_datetime64_any_dtype(work[col]):
            work[col] = pd.to_datetime(work[col], errors="coerce").dt.strftime("%Y-%m-%d %H:%M:%S")
    return work.to_dict(orient="records")


def main() -> int:
    args = parse_args()
    load_project_env()
    data_dir = ROOT / "data"
    report_path = Path(args.json_out).expanduser().resolve() if args.json_out else runtime_dir() / "live_source_diagnostic.json"

    report: dict[str, Any] = {
        "generated_at": pd.Timestamp.now(tz="Asia/Seoul").tz_localize(None).strftime("%Y-%m-%d %H:%M:%S"),
        "project_root": str(ROOT),
    }

    if not args.skip_market:
        market = MarketService(data_dir, kis_client=KISClient.from_env())
        snapshot_bundle = market.get_market_snapshot_bundle(prefer_live=True, allow_sample_fallback=False)
        report["market"] = {
            "source": snapshot_bundle.get("source"),
            "rows": int(len(snapshot_bundle.get("frame", pd.DataFrame()))),
            "cached_used": bool(snapshot_bundle.get("cached_used")),
            "diagnostics": df_to_records(snapshot_bundle.get("diagnostics", pd.DataFrame()), limit=100),
        }

    if not args.skip_ipo_refresh:
        hub = IPODataHub(data_dir, dart_client=DartClient.from_env())
        report["ipo_refresh"] = hub.refresh_live_cache(fetch_kind=True, fetch_38=True)

    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(report, ensure_ascii=False, indent=2))
    print(f"saved: {report_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
