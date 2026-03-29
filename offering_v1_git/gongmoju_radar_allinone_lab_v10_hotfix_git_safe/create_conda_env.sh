#!/usr/bin/env bash
set -euo pipefail
conda env create -f environment.yml || conda env update -f environment.yml --prune
source "$(conda info --base)/etc/profile.d/conda.sh"
conda activate gongmoju-radar
python scripts/preflight_check.py
