#!/usr/bin/env bash
set -e
cd "$(dirname "$0")"
python scripts/sync_env_to_lab_keys.py
