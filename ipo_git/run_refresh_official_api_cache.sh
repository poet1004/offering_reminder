#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"
python scripts/refresh_official_api_cache.py
