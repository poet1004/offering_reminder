from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


import argparse
import json
from pathlib import Path

import pandas as pd

from src.services.dart_client import DartClient
from src.services.dart_ipo_parser import DartIPOParser, snapshot_summary_text
from src.services.ipo_pipeline import IPODataHub
from src.utils import ensure_dir, standardize_issue_frame


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="DART 증권신고서/투자설명서 기반 IPO 지표 추출")
    parser.add_argument("--corp-name", help="법인명 또는 종목명")
    parser.add_argument("--stock-code", help="6자리 종목코드")
    parser.add_argument("--input", help="일괄 추출용 CSV 파일 경로")
    parser.add_argument("--output", help="출력 파일 경로")
    parser.add_argument("--max-items", type=int, default=10, help="일괄 추출 최대 종목 수")
    parser.add_argument("--only-missing", action="store_true", help="기존 값이 비어있는 종목만 추출")
    parser.add_argument("--force", action="store_true", help="캐시 무시하고 다시 추출")
    parser.add_argument("--days", type=int, default=540, help="DART 검색 기간(일)")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    root = Path(__file__).resolve().parents[1]
    data_dir = root / "data"
    exports_dir = ensure_dir(data_dir / "exports")

    dart = DartClient.from_env()
    if dart is None:
        raise SystemExit("DART_API_KEY 환경변수가 필요합니다.")

    if args.input:
        source_path = Path(args.input).expanduser().resolve()
        df = pd.read_csv(source_path)
        df = standardize_issue_frame(df)
        hub = IPODataHub(data_dir, dart_client=dart)
        result = hub.batch_enrich_issues_from_dart(
            df,
            max_items=args.max_items,
            only_missing=args.only_missing,
            force=args.force,
            days=args.days,
        )
        out_path = Path(args.output).expanduser().resolve() if args.output else exports_dir / "dart_batch_enriched.csv"
        result.to_csv(out_path, index=False, encoding="utf-8-sig")
        print(f"saved: {out_path}")
        print(result.head(10).to_string(index=False))
        return

    if not args.corp_name and not args.stock_code:
        raise SystemExit("--corp-name 또는 --stock-code, 혹은 --input 중 하나가 필요합니다.")

    parser = DartIPOParser(dart, base_dir=data_dir / "cache")
    snapshot = parser.analyze_company(
        stock_code=args.stock_code,
        corp_name=args.corp_name,
        force=args.force,
        days=args.days,
    )
    print(snapshot_summary_text(snapshot))
    out_path = Path(args.output).expanduser().resolve() if args.output else exports_dir / "dart_ipo_snapshot.json"
    out_path.write_text(json.dumps(snapshot, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"saved: {out_path}")


if __name__ == "__main__":
    main()