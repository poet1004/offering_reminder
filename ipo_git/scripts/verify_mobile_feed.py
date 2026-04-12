#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

import pandas as pd


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='모바일 피드 JSON의 구조/커버리지/신선도를 점검합니다.')
    parser.add_argument('--path', default='data/mobile/mobile-feed.json', help='검증할 mobile-feed.json 경로')
    parser.add_argument('--json-out', default='', help='검증 리포트 JSON 저장 경로')
    parser.add_argument('--min-items', type=int, default=1, help='최소 종목 수')
    parser.add_argument('--min-events', type=int, default=1, help='최소 이벤트 수')
    parser.add_argument('--min-market', type=int, default=1, help='최소 시장지표 수')
    parser.add_argument('--warn-stale-days', type=int, default=7, help='원본 데이터 기준 시각 경고 일수')
    parser.add_argument('--require-schema-version', type=int, default=2, help='필수 스키마 버전 최소값')
    return parser.parse_args()


def as_timestamp(value: Any) -> pd.Timestamp | None:
    parsed = pd.to_datetime(value, errors='coerce')
    if pd.isna(parsed):
        return None
    if getattr(parsed, 'tzinfo', None) is not None:
        try:
            parsed = parsed.tz_convert('Asia/Seoul').tz_localize(None)
        except Exception:
            parsed = parsed.tz_localize(None)
    return pd.Timestamp(parsed)


def make_report(payload: dict[str, Any], *, warn_stale_days: int, min_items: int, min_events: int, min_market: int, require_schema_version: int) -> dict[str, Any]:
    items = payload.get('items') or []
    events = payload.get('events') or []
    market = payload.get('marketQuotes') or []
    cache_inventory = payload.get('cacheInventory') or []
    source_status = payload.get('sourceStatus') or []
    warnings_payload = payload.get('warnings') or []

    event_types = pd.Series([str((row or {}).get('type') or '') for row in events], dtype='string').value_counts(dropna=False).to_dict()
    listing_count = int(event_types.get('listing', 0))
    unlock_count = int(sum(v for k, v in event_types.items() if str(k).startswith('unlock_')))
    subscription_count = int(sum(v for k, v in event_types.items() if str(k).startswith('subscription_')))

    upstream_updated_at = as_timestamp(payload.get('upstreamUpdatedAt'))
    generated_at = as_timestamp(payload.get('generatedAt'))
    now = pd.Timestamp.now(tz='Asia/Seoul').tz_localize(None)
    stale_days = None if upstream_updated_at is None else int((now.normalize() - upstream_updated_at.normalize()).days)

    checks: list[dict[str, Any]] = []

    def add_check(name: str, ok: bool, detail: str, severity: str = 'warning') -> None:
        checks.append({'name': name, 'ok': bool(ok), 'detail': detail, 'severity': severity})

    schema_version = int(payload.get('schemaVersion') or 0)
    add_check('schemaVersion', schema_version >= require_schema_version, f'{schema_version}', severity='critical')
    add_check('itemCount', len(items) >= min_items, f'{len(items)}', severity='critical')
    add_check('eventCount', len(events) >= min_events, f'{len(events)}', severity='critical')
    add_check('marketCount', len(market) >= min_market, f'{len(market)}', severity='warning')
    add_check('listing events', listing_count > 0, f'{listing_count}', severity='critical')
    add_check('unlock events', unlock_count > 0, f'{unlock_count}', severity='critical')
    add_check('subscription events', subscription_count > 0, f'{subscription_count}', severity='warning')
    add_check('upstreamUpdatedAt', upstream_updated_at is not None, str(payload.get('upstreamUpdatedAt') or ''), severity='warning')
    if stale_days is None:
        add_check('freshness', False, '원본 데이터 기준 시각을 파싱하지 못했습니다.', severity='warning')
    else:
        add_check('freshness', stale_days <= warn_stale_days, f'{stale_days}일 경과', severity='warning')

    key_cache_rows = {}
    for row in cache_inventory:
        name = str((row or {}).get('name') or '')
        if not name:
            continue
        rows = row.get('rows')
        key_cache_rows[name] = rows
    for cache_name in ['kind_listing_live', 'kind_public_offering_live', 'kind_pubprice_live', 'schedule_38_live', 'market_snapshot_last_success']:
        rows = key_cache_rows.get(cache_name)
        ok = rows is not None and pd.to_numeric(pd.Series([rows]), errors='coerce').fillna(-1).iloc[0] > 0
        severity = 'warning' if cache_name.startswith('kind_') else 'info'
        add_check(f'cache:{cache_name}', bool(ok), f'rows={rows}', severity=severity)

    source_failures = [row for row in source_status if (row or {}).get('ok') is False]
    add_check('sourceStatus failures', len(source_failures) == 0, f'{len(source_failures)}', severity='warning')

    critical_failures = [row for row in checks if row['severity'] == 'critical' and not row['ok']]
    warnings = [row for row in checks if row['severity'] != 'critical' and not row['ok']]

    return {
        'ok': len(critical_failures) == 0,
        'generatedAt': generated_at.isoformat() if generated_at is not None else None,
        'upstreamUpdatedAt': upstream_updated_at.isoformat() if upstream_updated_at is not None else None,
        'staleDays': stale_days,
        'summary': {
            'itemCount': len(items),
            'eventCount': len(events),
            'marketCount': len(market),
            'listingCount': listing_count,
            'unlockCount': unlock_count,
            'subscriptionCount': subscription_count,
            'schemaVersion': schema_version,
        },
        'eventTypes': event_types,
        'keyCacheRows': key_cache_rows,
        'sourceFailures': source_failures,
        'feedWarnings': warnings_payload,
        'checks': checks,
        'criticalFailures': len(critical_failures),
        'warnings': len(warnings),
    }


def main() -> int:
    args = parse_args()
    path = Path(args.path).expanduser().resolve()
    payload = json.loads(path.read_text(encoding='utf-8'))
    report = make_report(
        payload,
        warn_stale_days=args.warn_stale_days,
        min_items=args.min_items,
        min_events=args.min_events,
        min_market=args.min_market,
        require_schema_version=args.require_schema_version,
    )

    if args.json_out:
        out = Path(args.json_out).expanduser().resolve()
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding='utf-8')

    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0 if report['ok'] else 1


if __name__ == '__main__':
    raise SystemExit(main())
