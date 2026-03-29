#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"
python scripts/diagnose_live_sources.py
