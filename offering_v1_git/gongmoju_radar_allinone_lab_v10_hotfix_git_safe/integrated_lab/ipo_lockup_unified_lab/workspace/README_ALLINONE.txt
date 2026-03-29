통합 Lockup Lab workspace

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
