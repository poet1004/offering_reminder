#!/usr/bin/env bash
set -euo pipefail
ROOT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$ROOT_DIR"
python scripts/build_pages_site.py --repo . --output _site
printf '\nBuilt _site\n'
