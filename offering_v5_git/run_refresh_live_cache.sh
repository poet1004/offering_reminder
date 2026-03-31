#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"
python scripts/refresh_live_cache.py
