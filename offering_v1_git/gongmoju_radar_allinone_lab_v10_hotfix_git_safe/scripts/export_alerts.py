from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


import argparse
from pathlib import Path

import pandas as pd

from src.services.alert_engine import AlertEngine, AlertSettings
from src.services.ipo_pipeline import IPODataHub


def main() -> None:
    parser = argparse.ArgumentParser(description="Export upcoming unlock and alert candidates to CSV")
    parser.add_argument("--data-dir", default=str(Path(__file__).resolve().parents[1] / "data"))
    parser.add_argument("--external-unlock", default="")
    parser.add_argument("--local-kind", default="")
    parser.add_argument("--source-mode", default="캐시 우선", choices=["샘플만", "캐시 우선", "실데이터 시도"])
    parser.add_argument("--out", default=str(Path(__file__).resolve().parents[1] / "data" / "exports" / "alerts.csv"))
    args = parser.parse_args()

    hub = IPODataHub(Path(args.data_dir))
    bundle = hub.load_bundle(
        prefer_live=args.source_mode == "실데이터 시도",
        use_cache=args.source_mode != "샘플만",
        external_unlock_path=args.external_unlock or None,
        local_kind_export_path=args.local_kind or None,
    )
    engine = AlertEngine()
    alerts = engine.generate(bundle.issues, bundle.all_unlocks, pd.Timestamp.now(tz="Asia/Seoul").tz_localize(None).normalize(), AlertSettings())

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    alerts.to_csv(out_path, index=False, encoding="utf-8-sig")
    print({"out": str(out_path), "rows": len(alerts)})


if __name__ == "__main__":
    main()