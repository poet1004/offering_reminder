#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.utils import normalize_symbol_text


def read_json(path: Path, default: Any) -> Any:
    if not path.exists() or path.stat().st_size == 0:
        return default
    try:
        with path.open('r', encoding='utf-8') as fp:
            return json.load(fp)
    except Exception:
        return default


def read_csv_rows(path: Path) -> int | None:
    if not path.exists() or path.stat().st_size == 0:
        return None
    try:
        return len(pd.read_csv(path))
    except Exception:
        return None


def count_present(items: list[dict[str, Any]], field: str) -> int:
    total = 0
    for item in items:
        value = item.get(field)
        if value in (None, '', [], {}):
            continue
        total += 1
    return total


def main() -> None:
    parser = argparse.ArgumentParser(description='mobile-feed 산출물을 검증하고 요약 JSON을 생성합니다.')
    parser.add_argument('--repo', default='.')
    parser.add_argument('--feed', required=True)
    parser.add_argument('--output', required=True)
    args = parser.parse_args()

    repo = Path(args.repo).resolve()
    feed_path = Path(args.feed).resolve()
    output_path = Path(args.output).resolve()

    feed = read_json(feed_path, {})
    items = list(feed.get('items') or [])
    events = list(feed.get('events') or [])
    cache_inventory = list(feed.get('cacheInventory') or [])

    event_type_counts: dict[str, int] = {}
    for event in events:
        key = str(event.get('type') or '')
        if not key:
            continue
        event_type_counts[key] = event_type_counts.get(key, 0) + 1

    cache_rows: dict[str, Any] = {}
    for entry in cache_inventory:
        name = str(entry.get('name') or '').strip()
        if not name:
            continue
        cache_rows[name] = entry.get('rows')

    # fall back to actual cache/meta files when feed inventory is sparse
    for name in [
        'kind_listing_live',
        'kind_public_offering_live',
        'kind_pubprice_live',
        'schedule_38_live',
        'schedule_38_demand_live',
        'schedule_38_new_listing_live',
        'official_ksd_name_lookup_live',
        'official_ksd_market_codes_live',
        'official_ksd_listing_info_live',
        'official_ksd_corp_basic_live',
        'official_ksd_shareholder_summary_live',
        'public_quotes_latest',
        'public_quotes_pykrx_latest',
        'public_technical_latest',
        'dart_corp_codes',
    ]:
        if name in cache_rows and cache_rows[name] not in (None, ''):
            continue
        meta = read_json(repo / 'data' / 'cache' / f'{name}.meta.json', {})
        rows = meta.get('row_count')
        if rows is None:
            rows = read_csv_rows(repo / 'data' / 'cache' / f'{name}.csv')
        if rows is not None:
            cache_rows[name] = rows

    listing_items = [item for item in items if item.get('listingDate')]
    listed_with_symbol = sum(1 for item in listing_items if normalize_symbol_text(item.get('symbol')))

    warnings: list[str] = []
    if not items:
        warnings.append('feed items empty')
    if not events:
        warnings.append('feed events empty')
    if count_present(items, 'currentPrice') == 0:
        warnings.append('currentPrice coverage is zero')
    if count_present(items, 'institutionalCompetitionRatio') == 0:
        warnings.append('institutionalCompetitionRatio coverage is zero')
    if count_present(items, 'lockupCommitmentRatio') == 0:
        warnings.append('lockupCommitmentRatio coverage is zero')
    if count_present(items, 'ma20') == 0:
        warnings.append('technical coverage is zero')
    if count_present(items, 'returnPct') == 0:
        warnings.append('returnPct coverage is zero')
    if count_present(items, 'signal') == 0:
        warnings.append('signal coverage is zero')
    for cache_name in ['kind_listing_live', 'kind_public_offering_live', 'kind_pubprice_live']:
        rows = cache_rows.get(cache_name)
        if rows in (0, '0'):
            warnings.append(f'{cache_name} rows=0')

    report = {
        'ok': len(warnings) == 0,
        'generatedAt': feed.get('generatedAt'),
        'upstreamUpdatedAt': feed.get('upstreamUpdatedAt'),
        'schemaVersion': feed.get('schemaVersion'),
        'summary': feed.get('summary') or {},
        'itemCoverage': {
            'offerPrice': count_present(items, 'offerPrice'),
            'listingDate': count_present(items, 'listingDate'),
            'subscriptionStart': count_present(items, 'subscriptionStart'),
            'underwriters': sum(1 for item in items if item.get('underwriters')),
            'currentPrice': count_present(items, 'currentPrice'),
            'returnPct': count_present(items, 'returnPct'),
            'institutionalCompetitionRatio': count_present(items, 'institutionalCompetitionRatio'),
            'lockupCommitmentRatio': count_present(items, 'lockupCommitmentRatio'),
            'existingShareholderRatio': count_present(items, 'existingShareholderRatio'),
            'ma20': count_present(items, 'ma20'),
            'ma60': count_present(items, 'ma60'),
            'rsi14': count_present(items, 'rsi14'),
            'signal': count_present(items, 'signal'),
        },
        'listedItems': len(listing_items),
        'listedItemsWithSymbol': listed_with_symbol,
        'eventTypeCounts': event_type_counts,
        'cacheRows': cache_rows,
        'warnings': warnings,
    }

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open('w', encoding='utf-8') as fp:
        json.dump(report, fp, ensure_ascii=False, indent=2)
    print(json.dumps(report, ensure_ascii=False))


if __name__ == '__main__':
    main()
