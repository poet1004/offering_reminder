from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.services.dart_client import DartClient
from src.services.ipo_pipeline import IPODataHub
from src.services.kis_client import KISClient
from src.services.market_service import MarketService
from src.utils import load_project_env


def main() -> None:
    parser = argparse.ArgumentParser(description="KIND/38/시장 live cache refresh")
    parser.add_argument("--data-dir", default=str(ROOT / "data"))
    parser.add_argument("--skip-kind", action="store_true")
    parser.add_argument("--skip-38", action="store_true")
    parser.add_argument("--skip-market", action="store_true")
    parser.add_argument("--skip-official", action="store_true")
    parser.add_argument("--market-periods", nargs="*", default=["1mo", "3mo", "6mo", "1y"])
    parser.add_argument("--refresh-dart-corp", action="store_true")
    args = parser.parse_args()

    load_project_env()
    data_dir = Path(args.data_dir)
    hub = IPODataHub(data_dir, dart_client=DartClient.from_env())
    report = {
        "ipo": hub.refresh_live_cache(fetch_kind=not args.skip_kind, fetch_38=not args.skip_38),
    }
    if not args.skip_official:
        proc = subprocess.run(
            [sys.executable, str(ROOT / "scripts" / "refresh_official_api_cache.py"), "--data-dir", str(data_dir)],
            capture_output=True,
            text=True,
        )
        report["official"] = {
            "returncode": proc.returncode,
            "stdout": (proc.stdout or "").strip(),
            "stderr": (proc.stderr or "").strip(),
        }
    if not args.skip_market:
        market = MarketService(data_dir, kis_client=KISClient.from_env())
        report["market"] = market.refresh_market_cache(periods=args.market_periods)
    print(report)
    if args.refresh_dart_corp:
        dart = DartClient.from_env()
        if dart is None:
            print("DART_API_KEY not configured")
        else:
            table = dart.download_corp_codes(base_dir=data_dir / "cache", force=True)
            print({"dart_corp_rows": len(table)})


if __name__ == "__main__":
    main()
