from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.services.ipo_pipeline import IPODataHub
from src.services.lockup_strategy_service import LockupStrategyService
from src.services.scoring import IPOScorer


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="락업 매수전략 실행 보드 / 주문시트 CSV export")
    parser.add_argument("--version", default="2.0", help="백테스트 버전. 기본값 2.0")
    parser.add_argument("--today", default=None, help="기준일 YYYY-MM-DD. 미입력 시 오늘")
    parser.add_argument("--horizon-days", type=int, default=60, help="앞으로 볼 보호예수 해제 범위 일수")
    parser.add_argument("--external-unlock", default=None, help="synthetic_ipo_events.csv 경로")
    parser.add_argument("--local-kind", default=None, help="로컬 KIND export 경로")
    parser.add_argument("--prefer-live", action="store_true", help="KIND/38 라이브 소스 시도")
    parser.add_argument("--use-cache", action="store_true", help="캐시 소스 사용")
    parser.add_argument("--min-decision-rank", type=int, default=2, help="주문시트 포함 최대 decision rank. 1=우선검토, 2=관찰강화 포함")
    parser.add_argument("--out-dir", default=None, help="출력 폴더. 기본은 data/exports")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    data_dir = ROOT / "data"
    out_dir = Path(args.out_dir).expanduser().resolve() if args.out_dir else data_dir / "exports"
    out_dir.mkdir(parents=True, exist_ok=True)

    today = pd.Timestamp(args.today).normalize() if args.today else pd.Timestamp.now(tz="Asia/Seoul").tz_localize(None).normalize()
    hub = IPODataHub(data_dir)
    bundle = hub.load_bundle(
        prefer_live=args.prefer_live,
        use_cache=args.use_cache,
        external_unlock_path=args.external_unlock,
        local_kind_export_path=args.local_kind,
    )
    issues = IPOScorer().add_scores(bundle.issues)

    service = LockupStrategyService(data_dir)
    board = service.build_strategy_board(bundle.all_unlocks, issues, today, args.version, horizon_days=args.horizon_days)
    if board.empty:
        raise SystemExit("전략 보드가 비어 있습니다. unlock 데이터와 기준일을 확인하세요.")
    board = board.copy()
    board["strategy_version"] = args.version
    order_sheet = service.build_order_sheet(board, min_decision_rank=args.min_decision_rank)

    stamp = today.strftime("%Y%m%d")
    board_path = out_dir / f"lockup_strategy_board_v{args.version.replace('.', '_')}_{stamp}.csv"
    board.to_csv(board_path, index=False, encoding="utf-8-sig")
    print(f"strategy board saved: {board_path}")

    if not order_sheet.empty:
        order_path = out_dir / f"lockup_order_sheet_v{args.version.replace('.', '_')}_{stamp}.csv"
        order_sheet.to_csv(order_path, index=False, encoding="utf-8-sig")
        print(f"order sheet saved: {order_path}")
    else:
        print("order sheet skipped: no candidates matched the decision rank threshold")


if __name__ == "__main__":
    main()
