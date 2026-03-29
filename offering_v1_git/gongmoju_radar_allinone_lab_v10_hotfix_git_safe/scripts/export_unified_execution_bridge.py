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
from src.services.unified_lab_bridge import UnifiedLabBridgeService


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="락업 전략 보드 + 5분봉 / turnover 연구결과를 합친 execution bridge CSV export")
    parser.add_argument("--version", default="2.0", help="락업 전략 기준 버전. 기본값 2.0")
    parser.add_argument("--today", default=None, help="기준일 YYYY-MM-DD. 미입력 시 오늘")
    parser.add_argument("--horizon-days", type=int, default=90, help="앞으로 볼 보호예수 해제 범위 일수")
    parser.add_argument("--external-unlock", default=None, help="synthetic_ipo_events.csv 또는 unlock_events_backtest_input.csv 경로")
    parser.add_argument("--local-kind", default=None, help="로컬 KIND export 경로")
    parser.add_argument("--workspace", default=None, help="5분봉 unified lab workspace 경로")
    parser.add_argument("--prefer-live", action="store_true", help="KIND/38 라이브 소스 시도")
    parser.add_argument("--use-cache", action="store_true", help="캐시 소스 사용")
    parser.add_argument("--min-decision-rank", type=int, default=2, help="export에 포함할 최대 decision rank. 1=우선검토, 2=관찰강화 포함")
    parser.add_argument("--out-dir", default=None, help="출력 폴더. 기본은 data/exports")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    data_dir = ROOT / "data"
    out_dir = Path(args.out_dir).expanduser().resolve() if args.out_dir else data_dir / "exports"
    out_dir.mkdir(parents=True, exist_ok=True)

    today = pd.Timestamp(args.today).normalize() if args.today else pd.Timestamp.now(tz="Asia/Seoul").tz_localize(None).normalize()
    bridge_service = UnifiedLabBridgeService(data_dir)
    unified_bundle = bridge_service.load_bundle(args.workspace)
    resolved_external_unlock = args.external_unlock or str(unified_bundle.paths.unlock_csv or "") or None

    hub = IPODataHub(data_dir)
    bundle = hub.load_bundle(
        prefer_live=args.prefer_live,
        use_cache=args.use_cache,
        external_unlock_path=resolved_external_unlock,
        local_kind_export_path=args.local_kind,
    )
    issues = IPOScorer().add_scores(bundle.issues)
    lockup_service = LockupStrategyService(data_dir)
    board = lockup_service.build_strategy_board(bundle.all_unlocks, issues, today, args.version, horizon_days=args.horizon_days)
    if board.empty:
        raise SystemExit("전략 보드가 비어 있습니다. unlock 데이터와 기준일을 확인하세요.")
    board = board.copy()
    board["strategy_version"] = args.version
    board = bridge_service.enrich_strategy_board(board, unified_bundle, today=today)

    execution_bridge = bridge_service.build_execution_bridge_export(
        board,
        unified_bundle,
        today=today,
        min_decision_rank=args.min_decision_rank,
    )
    order_sheet = lockup_service.build_order_sheet(board, min_decision_rank=args.min_decision_rank)

    stamp = today.strftime("%Y%m%d")
    board_path = out_dir / f"execution_bridge_board_v{args.version.replace('.', '_')}_{stamp}.csv"
    board.to_csv(board_path, index=False, encoding="utf-8-sig")
    print(f"execution bridge board saved: {board_path}")

    if not execution_bridge.empty:
        bridge_path = out_dir / f"execution_bridge_export_v{args.version.replace('.', '_')}_{stamp}.csv"
        execution_bridge.to_csv(bridge_path, index=False, encoding="utf-8-sig")
        print(f"execution bridge export saved: {bridge_path}")
    else:
        print("execution bridge export skipped: no candidates matched the decision threshold")

    if not order_sheet.empty:
        order_path = out_dir / f"execution_bridge_order_sheet_v{args.version.replace('.', '_')}_{stamp}.csv"
        order_sheet.to_csv(order_path, index=False, encoding="utf-8-sig")
        print(f"order sheet saved: {order_path}")


if __name__ == "__main__":
    main()
