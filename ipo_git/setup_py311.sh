#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"

if ! command -v python3.11 >/dev/null 2>&1; then
  echo "[ERROR] python3.11 not found. Install Python 3.11 and rerun."
  exit 1
fi

if [ ! -x .venv/bin/python ]; then
  python3.11 -m venv .venv
fi

. .venv/bin/activate
python scripts/check_python_env.py || true
python -m pip install --upgrade pip setuptools wheel
python -m pip install --only-binary=:all: numpy==1.26.4 pandas==2.2.3
python -m pip install --prefer-binary -r requirements.txt

echo
echo "[INFO] Base web app dependencies are installed."
echo "[INFO] Optional KIS helpers: python -m pip install -r requirements-optional.txt"
echo "[INFO] Run the app with: bash run_app.sh"
