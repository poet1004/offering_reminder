#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import html
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd


DEFAULT_ISSUE_PATHS = [
    'data/cache/schedule_38_live.csv',
    'data/bootstrap_cache/schedule_38_live.csv',
    'data/sample_ipo_events.csv',
]
DEFAULT_MARKET_PATHS = [
    'data/cache/market_snapshot_last_success.csv',
    'data/bootstrap_cache/market_snapshot_last_success.csv',
    'data/sample_market_snapshot.csv',
]


EVENT_COLOR = {
    'subscription_start': '#2b6ef2',
    'subscription_end': '#0f9d8a',
    'listing': '#1a9c5b',
    'unlock_15d': '#d97706',
    'unlock_1m': '#f59e0b',
    'unlock_3m': '#ef4444',
    'unlock_6m': '#dc2626',
    'unlock_1y': '#991b1b',
}
EVENT_LABEL = {
    'subscription_start': '청약 시작',
    'subscription_end': '청약 마감',
    'listing': '상장',
    'unlock_15d': '보호예수 15일',
    'unlock_1m': '보호예수 1개월',
    'unlock_3m': '보호예수 3개월',
    'unlock_6m': '보호예수 6개월',
    'unlock_1y': '보호예수 1년',
}


def read_csv_safe(path: Path) -> pd.DataFrame:
    if not path.exists() or path.stat().st_size == 0:
        return pd.DataFrame()
    try:
        return pd.read_csv(path)
    except Exception:
        return pd.DataFrame()


def first_existing(repo: Path, rel_paths: list[str]) -> Path | None:
    for rel in rel_paths:
        path = repo / rel
        if path.exists() and path.stat().st_size > 0:
            return path
    return None


def clean(value: Any) -> Any:
    if value is None or pd.isna(value):
        return None
    if isinstance(value, float) and value.is_integer():
        return int(value)
    return value


def as_date_str(value: Any) -> str | None:
    if value is None or pd.isna(value):
        return None
    parsed = pd.to_datetime(value, errors='coerce')
    if pd.isna(parsed):
        return None
    return parsed.strftime('%Y-%m-%d')


def as_datetime_str(value: Any) -> str | None:
    if value is None or pd.isna(value):
        return None
    parsed = pd.to_datetime(value, errors='coerce')
    if pd.isna(parsed):
        return None
    return parsed.isoformat()


def parse_underwriters(value: Any) -> list[str]:
    text = str(value or '').strip()
    if not text:
        return []
    return [part.strip() for part in text.split(',') if part.strip()]


def build_item(row: pd.Series) -> dict[str, Any]:
    symbol = clean(row.get('symbol'))
    return {
        'id': str(clean(row.get('ipo_id') or row.get('id') or row.get('name') or row.get('name_key') or 'unknown')),
        'name': clean(row.get('name')) or '이름없음',
        'nameKey': clean(row.get('name_key')),
        'market': clean(row.get('market')),
        'symbol': str(symbol) if symbol is not None else None,
        'sector': clean(row.get('sector')),
        'stage': clean(row.get('stage')),
        'underwriters': parse_underwriters(row.get('underwriters')),
        'subscriptionStart': as_date_str(row.get('subscription_start') or row.get('subscription_start_date')),
        'subscriptionEnd': as_date_str(row.get('subscription_end') or row.get('subscription_end_date')),
        'listingDate': as_date_str(row.get('listing_date')),
        'priceBandLow': clean(row.get('price_band_low')),
        'priceBandHigh': clean(row.get('price_band_high')),
        'offerPrice': clean(row.get('offer_price')),
        'retailCompetitionRatio': clean(row.get('retail_competition_ratio_live')),
        'institutionalCompetitionRatio': clean(row.get('institutional_competition_ratio')),
        'allocationRatioRetail': clean(row.get('allocation_ratio_retail')),
        'allocationRatioProportional': clean(row.get('allocation_ratio_proportional')),
        'lockupCommitmentRatio': clean(row.get('lockup_commitment_ratio')),
        'employeeForfeitRatio': clean(row.get('employee_forfeit_ratio')),
        'circulatingSharesRatioOnListing': clean(row.get('circulating_shares_ratio_on_listing')),
        'existingShareholderRatio': clean(row.get('existing_shareholder_ratio')),
        'totalOfferShares': clean(row.get('total_offer_shares')),
        'postListingTotalShares': clean(row.get('post_listing_total_shares')),
        'currentPrice': clean(row.get('current_price')),
        'dayChangePct': clean(row.get('day_change_pct')),
        'source': str(clean(row.get('source'))) if clean(row.get('source')) is not None else None,
        'sourceDetail': clean(row.get('source_detail')),
        'notes': clean(row.get('notes')),
        'lastRefreshTs': as_datetime_str(row.get('last_refresh_ts')),
        'unlockSchedule': {
            '15d': as_date_str(row.get('unlock_date_15d')),
            '1m': as_date_str(row.get('unlock_date_1m')),
            '3m': as_date_str(row.get('unlock_date_3m')),
            '6m': as_date_str(row.get('unlock_date_6m')),
            '1y': as_date_str(row.get('unlock_date_1y')),
        },
    }


def build_events(item: dict[str, Any]) -> list[dict[str, Any]]:
    defs = [
        ('subscription_start', item.get('subscriptionStart')),
        ('subscription_end', item.get('subscriptionEnd')),
        ('listing', item.get('listingDate')),
        ('unlock_15d', (item.get('unlockSchedule') or {}).get('15d')),
        ('unlock_1m', (item.get('unlockSchedule') or {}).get('1m')),
        ('unlock_3m', (item.get('unlockSchedule') or {}).get('3m')),
        ('unlock_6m', (item.get('unlockSchedule') or {}).get('6m')),
        ('unlock_1y', (item.get('unlockSchedule') or {}).get('1y')),
    ]
    events: list[dict[str, Any]] = []
    for event_type, date_value in defs:
        if not date_value:
            continue
        events.append(
            {
                'id': f"{item['id']}::{event_type}",
                'ipoId': item['id'],
                'name': item['name'],
                'date': date_value,
                'type': event_type,
                'label': EVENT_LABEL[event_type],
                'color': EVENT_COLOR[event_type],
                'market': item.get('market'),
                'stage': item.get('stage'),
            }
        )
    return events


def build_quotes(df: pd.DataFrame) -> list[dict[str, Any]]:
    quotes: list[dict[str, Any]] = []
    if df.empty:
        return quotes
    for _, row in df.iterrows():
        quotes.append(
            {
                'name': clean(row.get('name')),
                'group': clean(row.get('group')),
                'ticker': clean(row.get('ticker')),
                'last': clean(row.get('last')),
                'changePct': clean(row.get('change_pct')),
                'asOf': as_datetime_str(row.get('asof')),
                'provider': clean(row.get('provider')),
            }
        )
    return quotes


def dedupe_items(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    seen: set[str] = set()
    ordered: list[dict[str, Any]] = []
    for item in items:
        key = item.get('id') or item.get('name')
        if not key or key in seen:
            continue
        seen.add(str(key))
        ordered.append(item)
    return ordered


def compute_counts(events: list[dict[str, Any]], today: pd.Timestamp, days: int = 30) -> dict[str, int]:
    future = pd.Timestamp(today.normalize())
    end = future + pd.Timedelta(days=days)
    counts = {'subscription': 0, 'listing': 0, 'unlock': 0}
    for event in events:
        date_value = pd.to_datetime(event.get('date'), errors='coerce')
        if pd.isna(date_value):
            continue
        if getattr(date_value, 'tzinfo', None) is not None:
            date_value = date_value.tz_localize(None) if hasattr(date_value, 'tz_localize') else date_value.tz_convert(None)
        date_value = pd.Timestamp(date_value).normalize()
        if not (future <= date_value <= end):
            continue
        event_type = str(event.get('type') or '')
        if event_type.startswith('subscription_'):
            counts['subscription'] += 1
        elif event_type == 'listing':
            counts['listing'] += 1
        elif event_type.startswith('unlock_'):
            counts['unlock'] += 1
    return counts


def build_feed(repo: Path) -> dict[str, Any]:
    issues_path = first_existing(repo, DEFAULT_ISSUE_PATHS)
    market_path = first_existing(repo, DEFAULT_MARKET_PATHS)
    if not issues_path:
        raise FileNotFoundError('공모주 일정 CSV를 찾지 못했습니다.')
    issues = read_csv_safe(issues_path)
    market = read_csv_safe(market_path) if market_path else pd.DataFrame()

    items = dedupe_items([build_item(row) for _, row in issues.iterrows()])
    events: list[dict[str, Any]] = []
    for item in items:
        events.extend(build_events(item))
    quotes = build_quotes(market)

    events.sort(key=lambda record: (record.get('date') or '', record.get('name') or ''))
    generated_at = datetime.now(timezone.utc).astimezone().isoformat()
    today = pd.Timestamp.now(tz='Asia/Seoul').tz_localize(None).normalize()

    return {
        'appName': '공모주 알리미',
        'generatedAt': generated_at,
        'source': str(repo),
        'sources': {
            'issuesCsv': str(issues_path.relative_to(repo)),
            'marketCsv': str(market_path.relative_to(repo)) if market_path else None,
        },
        'summary': {
            'itemCount': len(items),
            'eventCount': len(events),
            'marketCount': len(quotes),
            'next30d': compute_counts(events, today, days=30),
        },
        'marketQuotes': quotes,
        'items': items,
        'events': events,
    }


def write_json(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open('w', encoding='utf-8') as fp:
        json.dump(data, fp, ensure_ascii=False, indent=2)


def render_index_html(feed: dict[str, Any], public_base_url: str, fallback_base_url: str = '') -> str:
    generated_at = html.escape(feed.get('generatedAt') or '-')
    summary = feed.get('summary') or {}
    next30d = summary.get('next30d') or {}
    sources = feed.get('sources') or {}
    rows = [
        ('종목 수', summary.get('itemCount', 0)),
        ('이벤트 수', summary.get('eventCount', 0)),
        ('시장 지표 수', summary.get('marketCount', 0)),
        ('30일 내 청약', next30d.get('subscription', 0)),
        ('30일 내 상장', next30d.get('listing', 0)),
        ('30일 내 보호예수', next30d.get('unlock', 0)),
    ]
    list_items = ''.join(f'<li><strong>{html.escape(str(k))}</strong> <span>{html.escape(str(v))}</span></li>' for k, v in rows)
    base = public_base_url.rstrip('/') if public_base_url else ''
    json_url = f"{base}/mobile-feed.json" if base else 'mobile-feed.json'
    health_url = f"{base}/health.json" if base else 'health.json'
    fallback_json_url = f"{fallback_base_url.rstrip('/')}/mobile-feed.json" if fallback_base_url else ''
    issues_csv = html.escape(sources.get('issuesCsv') or '-')
    market_csv = html.escape(sources.get('marketCsv') or '-')
    fallback_block = ''
    if fallback_json_url:
        fallback_block = f'<p>대체 주소: <a href="{html.escape(fallback_json_url)}">{html.escape(fallback_json_url)}</a></p>'
    return f"""<!doctype html>
<html lang="ko">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>공모주 알리미 모바일 피드</title>
    <style>
      body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; margin: 0; padding: 24px; background: #f7f8fb; color: #111827; }}
      .wrap {{ max-width: 860px; margin: 0 auto; }}
      .card {{ background: white; border-radius: 16px; padding: 20px; box-shadow: 0 8px 24px rgba(15, 23, 42, 0.08); margin-bottom: 16px; }}
      h1 {{ margin: 0 0 8px; font-size: 28px; }}
      p {{ line-height: 1.6; }}
      ul {{ list-style: none; padding: 0; display: grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr)); gap: 12px; }}
      li {{ background: #f3f4f6; padding: 12px 14px; border-radius: 12px; }}
      a {{ color: #1d4ed8; text-decoration: none; }}
      code {{ background: #f3f4f6; padding: 2px 6px; border-radius: 6px; }}
    </style>
  </head>
  <body>
    <div class="wrap">
      <div class="card">
        <h1>공모주 알리미 모바일 피드</h1>
        <p>모바일 앱이 읽는 JSON 피드입니다. 기본 주소는 <code>{html.escape(json_url)}</code> 입니다.</p>
        {fallback_block}
        <p>생성 시각: <strong>{generated_at}</strong></p>
        <ul>{list_items}</ul>
      </div>
      <div class="card">
        <h2>바로가기</h2>
        <p><a href="{html.escape(json_url)}">mobile-feed.json 열기</a></p>
        <p><a href="{html.escape(health_url)}">health.json 열기</a></p>
      </div>
      <div class="card">
        <h2>입력 원본</h2>
        <p>공모주 일정: <code>{issues_csv}</code></p>
        <p>시장 지표: <code>{market_csv}</code></p>
      </div>
    </div>
  </body>
</html>
"""


def render_readme_md(public_base_url: str, fallback_base_url: str = '') -> str:
    base = public_base_url.rstrip('/') if public_base_url else '.'
    lines = [
        '# 모바일 피드 출력물',
        '',
        f'- 기본 주소: `{base}/mobile-feed.json`',
        f'- 상태 주소: `{base}/health.json`',
    ]
    if fallback_base_url:
        fb = fallback_base_url.rstrip('/')
        lines.append(f'- 대체 주소: `{fb}/mobile-feed.json`')
    lines += [
        '',
        '이 디렉터리는 GitHub Actions가 자동 갱신합니다.',
        '직접 수정하지 않는 것을 권장합니다.',
    ]
    return "\n".join(lines) + "\n"


def write_bundle(output_dir: Path, feed: dict[str, Any], public_base_url: str = '', fallback_base_url: str = '') -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    write_json(output_dir / 'mobile-feed.json', feed)
    write_json(
        output_dir / 'health.json',
        {
            'status': 'ok',
            'generatedAt': feed.get('generatedAt'),
            'summary': feed.get('summary'),
            'source': feed.get('source'),
            'publicBaseUrl': public_base_url or None,
            'fallbackBaseUrl': fallback_base_url or None,
        },
    )
    (output_dir / 'index.html').write_text(render_index_html(feed, public_base_url, fallback_base_url), encoding='utf-8')
    (output_dir / 'README.md').write_text(render_readme_md(public_base_url, fallback_base_url), encoding='utf-8')


def main() -> None:
    parser = argparse.ArgumentParser(description='기존 공모주 저장소를 모바일 앱용 JSON 피드로 변환합니다.')
    parser.add_argument('--repo', default='.', help='CSV 캐시가 있는 저장소 경로')
    parser.add_argument('--output', default='', help='생성할 JSON 파일 경로')
    parser.add_argument('--output-dir', default='', help='mobile-feed.json/health.json/index.html 을 쓸 출력 디렉터리')
    parser.add_argument('--site-dir', default='', help='이전 옵션 호환용 별칭(output-dir와 동일)')
    parser.add_argument('--public-base-url', default='', help='앱이 우선 읽을 공개 기본 주소(jsDelivr 등)')
    parser.add_argument('--fallback-base-url', default='', help='대체 공개 주소(raw.githubusercontent.com 등)')
    args = parser.parse_args()

    repo = Path(args.repo).resolve()
    feed = build_feed(repo)
    output_dir = args.output_dir or args.site_dir

    if args.output:
        write_json(Path(args.output).resolve(), feed)
        print(f'Wrote {Path(args.output).resolve()}')
    if output_dir:
        write_bundle(Path(output_dir).resolve(), feed, args.public_base_url, args.fallback_base_url)
        print(f'Wrote bundle {Path(output_dir).resolve()}')
    if not args.output and not output_dir:
        print(json.dumps(feed, ensure_ascii=False, indent=2))


if __name__ == '__main__':
    main()
