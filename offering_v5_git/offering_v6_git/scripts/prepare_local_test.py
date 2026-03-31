from __future__ import annotations

import argparse
import json
import shutil
import subprocess
import sys
import zipfile
from pathlib import Path
from typing import Any

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.services.execution_runtime import ExecutionRuntimeService
from src.services.ipo_pipeline import IPODataHub
from src.services.lockup_strategy_service import LockupStrategyService
from src.services.scoring import IPOScorer
from src.services.unified_lab_bridge import UnifiedLabBridgeService
from src.utils import ensure_dir, load_project_env, runtime_dir


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="로컬 테스트 전에 필요한 준비 절차를 한 번에 수행합니다.")
    parser.add_argument("--workspace", default=None, help="Unified Lab workspace 경로")
    parser.add_argument("--workspace-zip", default=None, help="workspace zip 또는 unified lab zip 경로")
    parser.add_argument("--workspace-out", default=None, help="workspace zip 압축해제 경로")
    parser.add_argument("--version", default="2.0", help="전략 버전")
    parser.add_argument("--today", default=None, help="기준일 YYYY-MM-DD")
    parser.add_argument("--horizon-days", type=int, default=90, help="전략 후보 탐색 기간")
    parser.add_argument("--external-unlock", default=None, help="external unlock CSV 경로")
    parser.add_argument("--local-kind", default=None, help="로컬 KIND export 경로")
    parser.add_argument("--prefer-live", action="store_true", help="KIND/38 라이브 소스 시도")
    parser.add_argument("--use-cache", action="store_true", help="캐시 소스 사용")
    parser.add_argument("--min-decision-rank", type=int, default=2, help="실행 계획에 포함할 최대 decision rank")
    parser.add_argument("--budget-krw", type=float, default=10_000_000, help="총 운용 예산")
    parser.add_argument("--cash-reserve-pct", type=float, default=5.0, help="현금 보유 비중")
    parser.add_argument("--single-cap-pct", type=float, default=35.0, help="단일 종목 최대 비중")
    parser.add_argument("--lot-size", type=int, default=1, help="주문 수량 최소 단위")
    parser.add_argument("--check-api", action="store_true", help="preflight에서 KIS/DART API 호출까지 점검")
    parser.add_argument("--out-dir", default=None, help="출력 폴더. 기본은 data/runtime")
    parser.add_argument("--clean-import-dir", action="store_true", help="workspace zip 압축해제 대상 폴더를 비우고 시작")
    parser.add_argument("--strict-preflight", action="store_true", help="preflight 실패 시 즉시 종료")
    return parser.parse_args()


def strip_single_root(extract_dir: Path) -> Path:
    entries = [p for p in extract_dir.iterdir() if p.name != "__MACOSX"]
    if len(entries) == 1 and entries[0].is_dir():
        return entries[0]
    return extract_dir


def extract_workspace_zip(zip_path: Path, out_dir: Path, clean: bool = False) -> Path:
    if out_dir.exists() and clean:
        shutil.rmtree(out_dir)
    ensure_dir(out_dir)
    with zipfile.ZipFile(zip_path) as zf:
        zf.extractall(out_dir)
    return strip_single_root(out_dir)


def save_df(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False, encoding="utf-8-sig")


def main() -> int:
    args = parse_args()
    load_project_env()
    data_dir = ROOT / "data"
    out_dir = Path(args.out_dir).expanduser().resolve() if args.out_dir else runtime_dir()
    ensure_dir(out_dir)

    today = pd.Timestamp(args.today).normalize() if args.today else pd.Timestamp.now(tz="Asia/Seoul").tz_localize(None).normalize()
    stamp = today.strftime("%Y%m%d")
    version_tag = args.version.replace(".", "_")

    extracted_workspace: Path | None = None
    if args.workspace_zip:
        zip_path = Path(args.workspace_zip).expanduser().resolve()
        if not zip_path.exists():
            raise SystemExit(f"workspace zip을 찾지 못했습니다: {zip_path}")
        import_dir = Path(args.workspace_out).expanduser().resolve() if args.workspace_out else data_dir / "imports" / zip_path.stem
        extracted_workspace = extract_workspace_zip(zip_path, import_dir, clean=args.clean_import_dir)

    workspace_hint = args.workspace or (str(extracted_workspace) if extracted_workspace else None)

    preflight_json = out_dir / f"preflight_report_{stamp}.json"
    preflight_cmd = [sys.executable, str(ROOT / "scripts" / "preflight_check.py"), "--json-out", str(preflight_json)]
    if workspace_hint:
        preflight_cmd.extend(["--workspace", str(workspace_hint)])
    if args.external_unlock:
        preflight_cmd.extend(["--external-unlock", str(args.external_unlock)])
    if args.local_kind:
        preflight_cmd.extend(["--local-kind", str(args.local_kind)])
    if args.check_api:
        preflight_cmd.append("--check-api")
    preflight_proc = subprocess.run(preflight_cmd, cwd=str(ROOT), capture_output=True, text=True)
    if args.strict_preflight and preflight_proc.returncode != 0:
        print(preflight_proc.stdout)
        print(preflight_proc.stderr)
        raise SystemExit("preflight failed")

    smoke_proc = subprocess.run([sys.executable, str(ROOT / "scripts" / "smoke_test.py")], cwd=str(ROOT), capture_output=True, text=True)
    if smoke_proc.returncode != 0:
        print(smoke_proc.stdout)
        print(smoke_proc.stderr)
        raise SystemExit("smoke_test.py failed")

    bridge_service = UnifiedLabBridgeService(data_dir)
    unified_bundle = bridge_service.load_bundle(workspace_hint)
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
    execution_bridge = bridge_service.build_execution_bridge_export(board, unified_bundle, today=today, min_decision_rank=args.min_decision_rank)
    order_sheet = lockup_service.build_order_sheet(board, min_decision_rank=args.min_decision_rank)

    runtime_service = ExecutionRuntimeService(data_dir)
    runtime_bundle = runtime_service.build_runtime_plan(
        board,
        total_budget_krw=args.budget_krw,
        cash_reserve_pct=args.cash_reserve_pct,
        max_single_position_pct=args.single_cap_pct,
        min_decision_rank=args.min_decision_rank,
        lot_size=args.lot_size,
        today=today,
    )
    dry_run = runtime_service.dry_run(runtime_bundle.plan, today=today)
    runtime_paths = runtime_service.export_bundle(
        runtime_bundle,
        out_dir=out_dir,
        prefix=f"runtime_v{version_tag}",
        stamp=stamp,
        dry_run_df=dry_run,
    )

    board_path = out_dir / f"lockup_strategy_board_v{version_tag}_{stamp}.csv"
    execution_bridge_path = out_dir / f"execution_bridge_export_v{version_tag}_{stamp}.csv"
    order_sheet_path = out_dir / f"execution_bridge_order_sheet_v{version_tag}_{stamp}.csv"
    save_df(board, board_path)
    save_df(execution_bridge, execution_bridge_path)
    save_df(order_sheet, order_sheet_path)

    manifest: dict[str, Any] = {
        "generated_at": pd.Timestamp.now(tz="Asia/Seoul").tz_localize(None).strftime("%Y-%m-%d %H:%M:%S"),
        "project_root": str(ROOT),
        "today": today.strftime("%Y-%m-%d"),
        "version": args.version,
        "workspace_hint": str(workspace_hint or ""),
        "resolved_workspace": str(unified_bundle.paths.workspace or ""),
        "resolved_external_unlock": str(resolved_external_unlock or ""),
        "preflight_returncode": int(preflight_proc.returncode),
        "preflight_report": str(preflight_json),
        "smoke_output": (smoke_proc.stdout or smoke_proc.stderr).strip(),
        "rows": {
            "strategy_board": int(len(board)),
            "execution_bridge": int(len(execution_bridge)),
            "order_sheet": int(len(order_sheet)),
            "runtime_plan": int(len(runtime_bundle.plan)),
            "runtime_warnings": int(len(runtime_bundle.warnings)),
            "runtime_dry_run": int(len(dry_run)),
        },
        "runtime_summary": runtime_bundle.summary,
        "paths": {
            "board_csv": str(board_path),
            "execution_bridge_csv": str(execution_bridge_path),
            "order_sheet_csv": str(order_sheet_path),
            **{key: str(value) for key, value in runtime_paths.items()},
        },
    }
    manifest_path = out_dir / f"prepare_manifest_v{version_tag}_{stamp}.json"
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    latest_manifest = out_dir / "prepare_latest_manifest.json"
    latest_manifest.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")

    print(json.dumps(manifest, ensure_ascii=False, indent=2))
    print("prepare_local_test completed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
