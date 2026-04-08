from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.services.kis_client import KISClient
from src.services.market_service import MarketService
from src.utils import load_project_env


def main() -> None:
    parser = argparse.ArgumentParser(description="Refresh public market snapshot cache for mobile feed")
    parser.add_argument("--data-dir", default=str(ROOT / "data"))
    args = parser.parse_args()

    load_project_env()
    data_dir = Path(args.data_dir)
    market = MarketService(data_dir, kis_client=KISClient.from_env())
    bundle = market.get_market_snapshot_bundle(prefer_live=True, allow_sample_fallback=False)
    frame = bundle.get("frame")
    payload = {
        "source": bundle.get("source"),
        "saved_at": bundle.get("saved_at"),
        "rows": int(len(frame)) if frame is not None else 0,
        "cached_used": bool(bundle.get("cached_used")),
        "sample_used": bool(bundle.get("sample_used")),
    }
    print(json.dumps(payload, ensure_ascii=False))


if __name__ == "__main__":
    main()
