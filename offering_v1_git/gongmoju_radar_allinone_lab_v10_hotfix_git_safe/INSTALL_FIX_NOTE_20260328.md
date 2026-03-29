# 설치/실행 안정화 메모 (2026-03-28)

이번 정리에서는 설치 메시지와 통합 lab 실행 안정성을 함께 손봤다.

## 바뀐 점

- `run_app.bat` / `run_prepare_local_test.bat`가 더 이상 매 실행마다 `pip install -r requirements.txt`를 먼저 하지 않는다.
- `.venv`가 있으면 자동으로 `.venv` 안의 Python을 사용한다.
- top-level `requirements.txt`에서 `numpy>=1.26` 강제 지정 제거.
  - `pandas`가 현재 인터프리터에 맞는 `numpy`를 자동으로 의존성으로 가져오도록 변경.
- 통합 lab 내부 `requirements.txt`와 `00_install_packages.bat`도 같은 방향으로 정리.
- `run_lockup_lab_wizard.py`의 `ROOT_DATA` 초기화 순서 버그 수정.
- `run_ipo_lockup_wizard.py`, `turnover_daily_backtest.py`가 패키지 import / 스크립트 실행 양쪽 모두 동작하도록 수정.
- `scripts/diagnose_python_env.py` 추가.

## 권장 사용 순서 (Windows)

1. 처음 한 번만 `bootstrap_clean_venv.bat`
2. 이후 앱 실행은 `run_app.bat`
3. 통합 lab 실행은 `run_integrated_lab_wizard.bat`

## 빠른 진단

```bat
python scripts\diagnose_python_env.py
python scripts\preflight_check.py --skip-smoke
```
