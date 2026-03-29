#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"
PY=python
if [ -x .venv/bin/python ]; then
  PY=.venv/bin/python
fi
"$PY" -m streamlit run app.py
