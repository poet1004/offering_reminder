#!/usr/bin/env bash
set -euo pipefail
python scripts/export_mobile_feed.py --repo . --output data/mobile/mobile-feed.json
python scripts/export_mobile_feed.py --repo . --site-dir data/mobile/site --site-base-url https://example.invalid/mobile
python scripts/export_mobile_feed.py --repo . --site-dir mobile-feed --site-base-url https://cdn.jsdelivr.net/gh/poet1004/offering_reminder@main/mobile-feed
