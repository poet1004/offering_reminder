#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")" && pwd)"
PY="python"
if [ -x "$ROOT/.venv/bin/python" ]; then
  PY="$ROOT/.venv/bin/python"
fi
LOG="$ROOT/data/runtime/integrated_lab_wizard_latest.log"
mkdir -p "$ROOT/data/runtime"
"$PY" "$ROOT/scripts/sync_env_to_lab_keys.py" >> "$LOG" 2>&1
"$PY" "$ROOT/scripts/prepare_integrated_lab_workspace.py" >> "$LOG" 2>&1
"$PY" "$ROOT/scripts/export_ipo_seed_to_lab.py" --lab-root "$ROOT/integrated_lab/ipo_lockup_unified_lab" >> "$LOG" 2>&1 || true
cd "$ROOT/integrated_lab/ipo_lockup_unified_lab"
exec "$PY" run_lockup_lab_wizard.py
