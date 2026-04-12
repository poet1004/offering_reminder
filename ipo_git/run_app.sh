#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"
PY=python
if [ -x .venv/bin/python ]; then
  PY=.venv/bin/python
fi
"$PY" - <<'PYEOF'
import importlib.util, sys
if importlib.util.find_spec("streamlit") is None:
    print("[ERROR] streamlit 이 설치되어 있지 않습니다.")
    print("먼저 bash setup_py311.sh 를 실행하세요.")
    raise SystemExit(1)
PYEOF
"$PY" -m streamlit run app.py
