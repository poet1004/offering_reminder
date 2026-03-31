from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.services.ipo_pipeline import IPODataHub
from src.services.dart_client import DartClient
from src.services.kis_client import KISClient
from src.services.market_service import MarketService
from src.services.shorts_service import ShortsStudioService
from src.services.scoring import IPOScorer


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate daily vertical shorts assets from IPO dashboard data")
    parser.add_argument("--source-mode", default="캐시 우선", choices=["실데이터 우선", "캐시 우선", "샘플만"])
    parser.add_argument("--allow-packaged-sample", action="store_true")
    parser.add_argument("--window-days", type=int, default=7)
    parser.add_argument("--title", default=None)
    parser.add_argument("--out-dir", default=str(ROOT / "data" / "exports" / "daily_shorts_latest"))
    parser.add_argument("--script-file", default=None, help="Optional edited narration script txt")
    parser.add_argument("--with-video", action="store_true", help="Also create MP4")
    args = parser.parse_args()

    data_dir = ROOT / "data"
    prefer_live = args.source_mode == "실데이터 우선"
    allow_sample_fallback = args.source_mode == "샘플만"

    hub = IPODataHub(data_dir, kis_client=KISClient.from_env(), dart_client=DartClient.from_env())
    bundle = hub.load_bundle(
        prefer_live=prefer_live,
        use_cache=args.source_mode != "샘플만",
        allow_sample_fallback=allow_sample_fallback,
        allow_packaged_sample_paths=args.allow_packaged_sample,
    )
    issues = IPOScorer().add_scores(bundle.issues)
    market = MarketService(data_dir, kis_client=KISClient.from_env()).get_market_snapshot_bundle(
        prefer_live=prefer_live,
        allow_sample_fallback=True,
    )

    studio = ShortsStudioService(data_dir)
    payload = studio.build_daily_payload(
        bundle,
        issues,
        window_days=args.window_days,
        source_label=args.source_mode,
        market_snapshot=market.get("frame"),
        market_source=str(market.get("source", "sample")),
    )
    script_text = None
    if args.script_file:
        script_text = Path(args.script_file).expanduser().read_text(encoding="utf-8")
    result = studio.generate_assets(
        payload,
        Path(args.out_dir),
        title=args.title,
        create_video=args.with_video,
        create_zip=True,
        script_text=script_text,
    )
    print(f"[DONE] shorts assets -> {args.out_dir}")
    if result.get("video_path") is not None:
        print(f"video: {result['video_path']}")
    if result.get("zip_path") is not None:
        print(f"zip: {result['zip_path']}")
    if result.get("script_path") is not None:
        print(f"script: {result['script_path']}")


if __name__ == "__main__":
    main()
