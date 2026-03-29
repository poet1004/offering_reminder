from __future__ import annotations

import argparse
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_LAB_ROOT = ROOT / 'integrated_lab' / 'ipo_lockup_unified_lab'
WORKSPACE_DIRS = [
    'workspace',
    'workspace/dataset_out',
    'workspace/unlock_out',
    'workspace/data/curated',
    'workspace/signal_out',
    'workspace/backtest_out',
    'workspace/turnover_backtest_out',
    'workspace/analysis_out',
    'workspace/cache_dart',
    'workspace/cache_kis',
    'workspace/logs',
]
README_TEXT = '''통합 Lockup Lab workspace

이 폴더는 공모주 레이더 앱과 동일 프로젝트 안에서 공유되는 Unified Lab 작업공간입니다.

주요 출력 위치
- unlock_out/unlock_events_backtest_input.csv
- signal_out/turnover_signals.csv
- turnover_backtest_out/*
- data/curated/lockup_minute.db

권장 순서
1. 프로젝트 루트에서 python scripts/sync_env_to_lab_keys.py
2. run_integrated_lab_wizard.bat 또는 integrated_lab/ipo_lockup_unified_lab/run_lockup_lab_wizard.py 실행
3. 산출물이 생기면 Streamlit 앱이 이 workspace를 자동으로 연결합니다.
'''


def main() -> None:
    parser = argparse.ArgumentParser(description='통합 lab workspace 폴더를 미리 생성합니다.')
    parser.add_argument('--lab-root', default=str(DEFAULT_LAB_ROOT), help='통합 lab 루트 경로')
    args = parser.parse_args()
    lab_root = Path(args.lab_root).expanduser().resolve()
    created = []
    for rel in WORKSPACE_DIRS:
        p = lab_root / rel
        p.mkdir(parents=True, exist_ok=True)
        created.append(str(p))
    readme = lab_root / 'workspace' / 'README_ALLINONE.txt'
    readme.write_text(README_TEXT, encoding='utf-8')
    report = {
        'lab_root': str(lab_root),
        'workspace': str(lab_root / 'workspace'),
        'created': created,
        'readme': str(readme),
    }
    print(json.dumps(report, ensure_ascii=False, indent=2))


if __name__ == '__main__':
    main()
