#!/usr/bin/env python3
from __future__ import annotations

import argparse
import html
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

try:
    from pykrx import stock as pykrx_stock
except Exception:  # pragma: no cover - optional runtime dependency
    pykrx_stock = None

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.services.calculations import latest_signal_from_history
from src.services.ipo_pipeline import IPODataHub
from src.services.public_quote_service import PublicQuoteService
from src.utils import normalize_name_key, normalize_symbol_text, parse_date_columns, standardize_issue_frame, today_kst


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

UNLOCK_TERM_META = {
    '15D': {'date': 'unlock_date_15d', 'shares': 'unlock_shares_15d', 'ratio': 'unlock_ratio_15d', 'remaining': 'remaining_locked_shares_15d'},
    '1M': {'date': 'unlock_date_1m', 'shares': 'unlock_shares_1m', 'ratio': 'unlock_ratio_1m', 'remaining': 'remaining_locked_shares_1m'},
    '3M': {'date': 'unlock_date_3m', 'shares': 'unlock_shares_3m', 'ratio': 'unlock_ratio_3m', 'remaining': 'remaining_locked_shares_3m'},
    '6M': {'date': 'unlock_date_6m', 'shares': 'unlock_shares_6m', 'ratio': 'unlock_ratio_6m', 'remaining': 'remaining_locked_shares_6m'},
    '1Y': {'date': 'unlock_date_1y', 'shares': 'unlock_shares_1y', 'ratio': 'unlock_ratio_1y', 'remaining': 'remaining_locked_shares_1y'},
}
UNLOCK_TERM_TO_COL = {term: meta['date'] for term, meta in UNLOCK_TERM_META.items()}
OFFICIAL_CACHE_NAMES = [
    'official_ksd_name_lookup_live',
    'official_ksd_market_codes_live',
    'official_ksd_listing_info_live',
    'official_ksd_corp_basic_live',
    'official_ksd_shareholder_summary_live',
]
AUX_CACHE_NAMES = [
    'public_quotes_latest',
    'public_quotes_pykrx_latest',
    'public_technical_latest',
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

SCHEMA_VERSION = 3
GENERATOR_NAME = 'scripts/export_mobile_feed.py'



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


def text_value(value: Any) -> str:
    cleaned = clean(value)
    return '' if cleaned is None else str(cleaned).strip()


def pick_value(*values: Any) -> Any:
    for value in values:
        cleaned = clean(value)
        if cleaned is None:
            continue
        if isinstance(cleaned, str) and not cleaned.strip():
            continue
        return cleaned
    return None


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
    if getattr(parsed, 'tzinfo', None) is None:
        return pd.Timestamp(parsed).isoformat()
    return parsed.isoformat()


def parse_underwriters(value: Any) -> list[str]:
    text = text_value(value)
    if not text:
        return []
    normalized = text.replace('·', ',').replace('/', ',')
    return [part.strip() for part in normalized.split(',') if part.strip()]


def _valid_symbol(value: Any) -> str | None:
    text = text_value(value)
    return text if text.isdigit() and len(text) == 6 else None


def _latest_timestamp(values: list[Any]) -> str | None:
    parsed = pd.to_datetime(pd.Series(values, dtype='object'), errors='coerce')
    parsed = parsed.dropna()
    if parsed.empty:
        return None
    latest = parsed.max()
    return latest.isoformat()


def _read_meta_json(path: Path) -> dict[str, Any]:
    if not path.exists() or path.stat().st_size == 0:
        return {}
    try:
        return json.loads(path.read_text(encoding='utf-8'))
    except Exception:
        return {}




def _read_cache_frame(repo: Path, name: str) -> pd.DataFrame:
    path = repo / 'data' / 'cache' / f'{name}.csv'
    df = read_csv_safe(path)
    if df.empty:
        return df
    if 'name_key' not in df.columns and 'name' in df.columns:
        df['name_key'] = df.get('name').map(normalize_name_key)
    if 'query_name_key' not in df.columns and 'query_name' in df.columns:
        df['query_name_key'] = df.get('query_name').map(normalize_name_key)
    if 'symbol' in df.columns:
        df['symbol'] = df.get('symbol').map(_valid_symbol)
    return parse_date_columns(df)


def augment_cache_inventory(repo: Path, cache_inventory: pd.DataFrame) -> pd.DataFrame:
    base = cache_inventory.copy() if isinstance(cache_inventory, pd.DataFrame) else pd.DataFrame()
    if base.empty:
        base = pd.DataFrame(columns=['name', 'rows', 'saved_at', 'source', 'notes'])
    if 'name' not in base.columns:
        base['name'] = pd.NA
    existing_names = set(base['name'].dropna().astype(str).tolist())
    extra_rows: list[dict[str, Any]] = []
    for cache_name in OFFICIAL_CACHE_NAMES + AUX_CACHE_NAMES + ['kind_listing_live', 'kind_public_offering_live', 'kind_pubprice_live']:
        if cache_name in existing_names:
            continue
        meta = _read_meta_json(repo / 'data' / 'cache' / f'{cache_name}.meta.json')
        csv_path = repo / 'data' / 'cache' / f'{cache_name}.csv'
        if not meta and not csv_path.exists():
            continue
        rows = meta.get('row_count')
        if rows is None and csv_path.exists():
            rows = len(read_csv_safe(csv_path))
        extra_rows.append(
            {
                'name': cache_name,
                'rows': rows,
                'saved_at': meta.get('saved_at'),
                'source': meta.get('source'),
                'notes': meta.get('notes'),
            }
        )
    if extra_rows:
        base = pd.concat([base, pd.DataFrame(extra_rows)], ignore_index=True)
    return base


def _group_first_map(df: pd.DataFrame, key_cols: list[str], value_cols: list[str], sort_cols: list[str] | None = None) -> dict[str, dict[str, Any]]:
    if df is None or df.empty:
        return {}
    work = df.copy()
    if sort_cols:
        sort_existing = [c for c in sort_cols if c in work.columns]
        if sort_existing:
            ascending = [False] * len(sort_existing)
            work = work.sort_values(sort_existing, ascending=ascending, na_position='last')
    out: dict[str, dict[str, Any]] = {}
    for _, row in work.iterrows():
        key = None
        for col in key_cols:
            value = text_value(row.get(col))
            if value:
                key = value
                break
        if not key or key in out:
            continue
        out[key] = {col: clean(row.get(col)) for col in value_cols}
    return out


def apply_official_cache_overlays(repo: Path, issues: pd.DataFrame) -> pd.DataFrame:
    work = standardize_issue_frame(issues.copy()) if issues is not None and not issues.empty else pd.DataFrame()
    if work.empty:
        return work

    for extra_col in ['listing_status', 'delisting_date', 'homep_url']:
        if extra_col not in work.columns:
            work[extra_col] = pd.NA

    name_map = _read_cache_frame(repo, 'official_ksd_name_lookup_live')
    market_codes = _read_cache_frame(repo, 'official_ksd_market_codes_live')
    listing_info = _read_cache_frame(repo, 'official_ksd_listing_info_live')
    corp_basic = _read_cache_frame(repo, 'official_ksd_corp_basic_live')
    shareholder = _read_cache_frame(repo, 'official_ksd_shareholder_summary_live')

    name_symbol_map = _group_first_map(
        name_map,
        ['query_name_key', 'name_key'],
        ['symbol', 'isin', 'issuco_custno'],
        sort_cols=['last_refresh_ts', 'query_name_key', 'name_key'],
    )
    market_symbol_map = _group_first_map(
        market_codes,
        ['name_key'],
        ['symbol', 'market', 'listing_status', 'delisting_date'],
        sort_cols=['last_refresh_ts', 'name_key'],
    )
    listing_by_symbol = _group_first_map(
        listing_info,
        ['symbol'],
        ['listing_date', 'delisting_date', 'listing_status', 'market', 'isin'],
        sort_cols=['last_refresh_ts', 'listing_date'],
    )
    listing_by_name = _group_first_map(
        listing_info,
        ['name_key'],
        ['listing_date', 'delisting_date', 'listing_status', 'market', 'isin'],
        sort_cols=['last_refresh_ts', 'listing_date'],
    )
    corp_by_symbol = _group_first_map(
        corp_basic,
        ['symbol', 'name_key'],
        ['post_listing_total_shares', 'homep_url', 'listing_date'],
        sort_cols=['last_refresh_ts', 'listing_date'],
    )
    shareholder_by_symbol = _group_first_map(
        shareholder,
        ['symbol', 'name_key'],
        ['major_shareholder_ratio', 'institution_shareholder_ratio', 'corporate_shareholder_ratio', 'employee_shareholder_ratio', 'shareholder_distribution_note'],
        sort_cols=['last_refresh_ts', 'distribution_date'],
    )

    enriched_rows: list[dict[str, Any]] = []
    for _, row in work.iterrows():
        item = row.to_dict()
        name_key = normalize_name_key(pick_value(item.get('name_key'), item.get('name')))
        symbol = _valid_symbol(item.get('symbol'))

        symbol_source = name_symbol_map.get(name_key) or market_symbol_map.get(name_key) or {}
        if symbol is None and symbol_source.get('symbol'):
            symbol = _valid_symbol(symbol_source.get('symbol'))
            if symbol:
                item['symbol'] = symbol

        listing_source = (listing_by_symbol.get(symbol) if symbol else None) or listing_by_name.get(name_key) or {}
        corp_source = (corp_by_symbol.get(symbol) if symbol else None) or corp_by_symbol.get(name_key) or {}
        shareholder_source = (shareholder_by_symbol.get(symbol) if symbol else None) or shareholder_by_symbol.get(name_key) or {}
        market_source = market_symbol_map.get(name_key) or {}

        if clean(item.get('market')) is None:
            item['market'] = pick_value(listing_source.get('market'), market_source.get('market'))
        if pd.isna(pd.to_datetime(item.get('listing_date'), errors='coerce')):
            item['listing_date'] = pick_value(listing_source.get('listing_date'), corp_source.get('listing_date'))
        if clean(item.get('listing_status')) is None:
            item['listing_status'] = pick_value(listing_source.get('listing_status'), market_source.get('listing_status'))
        if pd.isna(pd.to_datetime(item.get('delisting_date'), errors='coerce')) and pick_value(listing_source.get('delisting_date'), market_source.get('delisting_date')) is not None:
            item['delisting_date'] = pick_value(listing_source.get('delisting_date'), market_source.get('delisting_date'))
        if clean(item.get('post_listing_total_shares')) is None and corp_source.get('post_listing_total_shares') is not None:
            item['post_listing_total_shares'] = corp_source.get('post_listing_total_shares')
        if clean(item.get('homep_url')) is None and corp_source.get('homep_url') is not None:
            item['homep_url'] = corp_source.get('homep_url')
        if clean(item.get('notes')) is None and shareholder_source.get('shareholder_distribution_note') is not None:
            item['notes'] = shareholder_source.get('shareholder_distribution_note')
        if clean(item.get('existing_shareholder_ratio')) is None:
            major = pd.to_numeric(pd.Series([shareholder_source.get('major_shareholder_ratio')]), errors='coerce').iloc[0]
            if pd.notna(major):
                item['existing_shareholder_ratio'] = float(major)
        enriched_rows.append(item)
    return parse_date_columns(pd.DataFrame(enriched_rows))
def enrich_issues_with_unlocks(issues: pd.DataFrame, unlocks: pd.DataFrame) -> pd.DataFrame:
    work = standardize_issue_frame(issues.copy()) if issues is not None and not issues.empty else pd.DataFrame()
    if work.empty:
        return work

    for meta in UNLOCK_TERM_META.values():
        for col in meta.values():
            if col not in work.columns:
                work[col] = pd.NA

    if unlocks is None or unlocks.empty:
        return parse_date_columns(work)

    unlocks_work = parse_date_columns(unlocks.copy())
    if 'name_key' not in unlocks_work.columns:
        unlocks_work['name_key'] = unlocks_work.get('name', pd.Series(dtype='object')).map(normalize_name_key)
    for col in ['unlock_shares', 'unlock_ratio', 'remaining_locked_shares', 'offer_price', 'current_price']:
        if col in unlocks_work.columns:
            unlocks_work[col] = pd.to_numeric(unlocks_work.get(col), errors='coerce')

    aggregated: dict[str, dict[str, Any]] = {}

    def ensure_slot(key: str) -> dict[str, Any]:
        slot = aggregated.get(key)
        if slot is None:
            slot = {'listing_date': None, 'market': None, 'offer_price': None, 'current_price': None}
            for term_meta in UNLOCK_TERM_META.values():
                for col in term_meta.values():
                    slot[col] = None
            aggregated[key] = slot
        return slot

    for _, row in unlocks_work.iterrows():
        keys: list[str] = []
        symbol = _valid_symbol(row.get('symbol'))
        name_key = normalize_name_key(pick_value(row.get('name_key'), row.get('name')))
        if symbol:
            keys.append(f'symbol::{symbol}')
        if name_key:
            keys.append(f'name::{name_key}')
        if not keys:
            continue
        unlock_date = pd.to_datetime(row.get('unlock_date'), errors='coerce')
        listing_date = pd.to_datetime(row.get('listing_date'), errors='coerce')
        term = text_value(row.get('term')).upper()
        term_meta = UNLOCK_TERM_META.get(term)
        market = clean(row.get('market'))
        offer_price = clean(row.get('offer_price') if 'offer_price' in row.index else row.get('ipo_price'))
        current_price = clean(row.get('current_price'))
        unlock_shares = clean(row.get('unlock_shares'))
        unlock_ratio = clean(row.get('unlock_ratio'))
        remaining_locked = clean(row.get('remaining_locked_shares'))
        for key in keys:
            slot = ensure_slot(key)
            if term_meta and not pd.isna(unlock_date):
                date_col = term_meta['date']
                existing = pd.to_datetime(slot.get(date_col), errors='coerce')
                if pd.isna(existing) or unlock_date < existing:
                    slot[date_col] = unlock_date
                if unlock_shares is not None:
                    slot[term_meta['shares']] = (slot.get(term_meta['shares']) or 0) + float(unlock_shares)
                if unlock_ratio is not None:
                    slot[term_meta['ratio']] = (slot.get(term_meta['ratio']) or 0) + float(unlock_ratio)
                if remaining_locked is not None:
                    slot[term_meta['remaining']] = (slot.get(term_meta['remaining']) or 0) + float(remaining_locked)
            if not pd.isna(listing_date) and slot.get('listing_date') is None:
                slot['listing_date'] = listing_date
            if slot.get('market') is None and market is not None:
                slot['market'] = market
            if slot.get('offer_price') is None and offer_price is not None:
                slot['offer_price'] = offer_price
            if slot.get('current_price') is None and current_price is not None:
                slot['current_price'] = current_price

    updated_rows: list[dict[str, Any]] = []
    for _, row in work.iterrows():
        item = row.to_dict()
        keys: list[str] = []
        symbol = _valid_symbol(item.get('symbol'))
        name_key = normalize_name_key(pick_value(item.get('name_key'), item.get('name')))
        if symbol:
            keys.append(f'symbol::{symbol}')
        if name_key:
            keys.append(f'name::{name_key}')
        for key in keys:
            slot = aggregated.get(key)
            if not slot:
                continue
            if pd.isna(pd.to_datetime(item.get('listing_date'), errors='coerce')) and slot.get('listing_date') is not None:
                item['listing_date'] = slot['listing_date']
            if clean(item.get('market')) is None and slot.get('market') is not None:
                item['market'] = slot['market']
            if clean(item.get('offer_price')) is None and slot.get('offer_price') is not None:
                item['offer_price'] = slot['offer_price']
            if clean(item.get('current_price')) is None and slot.get('current_price') is not None:
                item['current_price'] = slot['current_price']
            for term_meta in UNLOCK_TERM_META.values():
                for col in term_meta.values():
                    current = item.get(col)
                    has_current = False
                    if term_meta['date'] == col:
                        has_current = not pd.isna(pd.to_datetime(current, errors='coerce'))
                    else:
                        has_current = clean(current) is not None
                    if not has_current and slot.get(col) is not None:
                        item[col] = slot[col]
        updated_rows.append(item)
    return parse_date_columns(pd.DataFrame(updated_rows))


def build_item(row: pd.Series) -> dict[str, Any]:
    symbol = clean(row.get('symbol'))
    unlock_schedule = {
        '15d': as_date_str(row.get('unlock_date_15d')),
        '1m': as_date_str(row.get('unlock_date_1m')),
        '3m': as_date_str(row.get('unlock_date_3m')),
        '6m': as_date_str(row.get('unlock_date_6m')),
        '1y': as_date_str(row.get('unlock_date_1y')),
    }
    unlock_details = {
        '15d': {
            'date': unlock_schedule.get('15d'),
            'shares': clean(row.get('unlock_shares_15d')),
            'ratio': clean(row.get('unlock_ratio_15d')),
            'remainingLockedShares': clean(row.get('remaining_locked_shares_15d')),
        },
        '1m': {
            'date': unlock_schedule.get('1m'),
            'shares': clean(row.get('unlock_shares_1m')),
            'ratio': clean(row.get('unlock_ratio_1m')),
            'remainingLockedShares': clean(row.get('remaining_locked_shares_1m')),
        },
        '3m': {
            'date': unlock_schedule.get('3m'),
            'shares': clean(row.get('unlock_shares_3m')),
            'ratio': clean(row.get('unlock_ratio_3m')),
            'remainingLockedShares': clean(row.get('remaining_locked_shares_3m')),
        },
        '6m': {
            'date': unlock_schedule.get('6m'),
            'shares': clean(row.get('unlock_shares_6m')),
            'ratio': clean(row.get('unlock_ratio_6m')),
            'remainingLockedShares': clean(row.get('remaining_locked_shares_6m')),
        },
        '1y': {
            'date': unlock_schedule.get('1y'),
            'shares': clean(row.get('unlock_shares_1y')),
            'ratio': clean(row.get('unlock_ratio_1y')),
            'remainingLockedShares': clean(row.get('remaining_locked_shares_1y')),
        },
    }
    return {
        'id': str(pick_value(row.get('ipo_id'), row.get('id'), row.get('name'), row.get('name_key'), 'unknown')),
        'name': pick_value(row.get('name'), '이름없음'),
        'nameKey': clean(row.get('name_key')),
        'market': clean(row.get('market')),
        'symbol': str(symbol) if symbol is not None else None,
        'sector': clean(row.get('sector')),
        'stage': clean(row.get('stage')),
        'underwriters': parse_underwriters(row.get('underwriters')),
        'subscriptionStart': as_date_str(pick_value(row.get('subscription_start'), row.get('subscription_start_date'))),
        'subscriptionEnd': as_date_str(pick_value(row.get('subscription_end'), row.get('subscription_end_date'))),
        'listingDate': as_date_str(row.get('listing_date')),
        'forecastDate': as_date_str(row.get('forecast_date')),
        'priceBandLow': clean(row.get('price_band_low')),
        'priceBandHigh': clean(row.get('price_band_high')),
        'offerPrice': clean(row.get('offer_price')),
        'retailCompetitionRatio': clean(row.get('retail_competition_ratio_live')),
        'institutionalCompetitionRatio': clean(row.get('institutional_competition_ratio')),
        'allocationRatioRetail': clean(row.get('allocation_ratio_retail')),
        'allocationRatioProportional': clean(row.get('allocation_ratio_proportional')),
        'lockupCommitmentRatio': clean(row.get('lockup_commitment_ratio')),
        'employeeSubscriptionRatio': clean(row.get('employee_subscription_ratio')),
        'employeeForfeitRatio': clean(row.get('employee_forfeit_ratio')),
        'circulatingSharesOnListing': clean(row.get('circulating_shares_on_listing')),
        'circulatingSharesRatioOnListing': clean(row.get('circulating_shares_ratio_on_listing')),
        'existingShareholderRatio': clean(row.get('existing_shareholder_ratio')),
        'totalOfferShares': clean(row.get('total_offer_shares')),
        'newShares': clean(row.get('new_shares')),
        'sellingShares': clean(row.get('selling_shares')),
        'secondarySaleRatio': clean(row.get('secondary_sale_ratio')),
        'postListingTotalShares': clean(row.get('post_listing_total_shares')),
        'currentPrice': clean(row.get('current_price')),
        'dayChangePct': clean(row.get('day_change_pct')),
        'ma20': clean(row.get('ma20')),
        'ma60': clean(row.get('ma60')),
        'rsi14': clean(row.get('rsi14')),
        'irTitle': clean(row.get('ir_title')),
        'irDate': as_date_str(row.get('ir_date')),
        'irUrl': clean(row.get('ir_url')),
        'irPdfUrl': clean(row.get('ir_pdf_url')),
        'irSourcePage': clean(row.get('ir_source_page')),
        'dartReceiptNo': clean(row.get('dart_receipt_no')),
        'dartViewerUrl': clean(row.get('dart_viewer_url')),
        'dartReportNm': clean(row.get('dart_report_nm')),
        'dartFilingDate': as_date_str(row.get('dart_filing_date')),
        'listingStatus': clean(row.get('listing_status')),
        'delistingDate': as_date_str(row.get('delisting_date')),
        'homepUrl': clean(row.get('homep_url')),
        'source': str(clean(row.get('source'))) if clean(row.get('source')) is not None else None,
        'sourceDetail': clean(row.get('source_detail')),
        'notes': clean(row.get('notes')),
        'lastRefreshTs': as_datetime_str(row.get('last_refresh_ts')),
        'unlockSchedule': unlock_schedule,
        'unlockDetails': unlock_details,
    }


def build_events(item: dict[str, Any]) -> list[dict[str, Any]]:
    defs = [
        ('subscription_start', item.get('subscriptionStart'), None),
        ('subscription_end', item.get('subscriptionEnd'), None),
        ('listing', item.get('listingDate'), None),
        ('unlock_15d', (item.get('unlockDetails') or {}).get('15d', {}).get('date'), (item.get('unlockDetails') or {}).get('15d')),
        ('unlock_1m', (item.get('unlockDetails') or {}).get('1m', {}).get('date'), (item.get('unlockDetails') or {}).get('1m')),
        ('unlock_3m', (item.get('unlockDetails') or {}).get('3m', {}).get('date'), (item.get('unlockDetails') or {}).get('3m')),
        ('unlock_6m', (item.get('unlockDetails') or {}).get('6m', {}).get('date'), (item.get('unlockDetails') or {}).get('6m')),
        ('unlock_1y', (item.get('unlockDetails') or {}).get('1y', {}).get('date'), (item.get('unlockDetails') or {}).get('1y')),
    ]
    events: list[dict[str, Any]] = []
    for event_type, date_value, detail in defs:
        if not date_value:
            continue
        event = {
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
        if isinstance(detail, dict):
            event['shares'] = clean(detail.get('shares'))
            event['ratio'] = clean(detail.get('ratio'))
            event['remainingLockedShares'] = clean(detail.get('remainingLockedShares'))
        events.append(event)
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
        key = pick_value(item.get('id'), item.get('name'))
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



def build_warnings(cache_inventory: pd.DataFrame, source_status: pd.DataFrame, events: list[dict[str, Any]]) -> list[str]:
    warnings: list[str] = []

    event_types = {str(event.get('type') or '') for event in events}
    if 'listing' not in event_types:
        warnings.append('listing 이벤트가 없습니다.')
    if not any(event_type.startswith('unlock_') for event_type in event_types):
        warnings.append('보호예수 해제 이벤트가 없습니다.')

    if cache_inventory is not None and not cache_inventory.empty and {'name', 'rows'}.issubset(cache_inventory.columns):
        keyed = cache_inventory.set_index('name', drop=False)
        official_total = 0
        for cache_name in OFFICIAL_CACHE_NAMES:
            if cache_name in keyed.index:
                rows = pd.to_numeric(keyed.loc[cache_name, 'rows'], errors='coerce')
                if pd.notna(rows) and int(rows) > 0:
                    official_total += int(rows)
        if official_total <= 0:
            kind_missing = 0
            for cache_name in ['kind_listing_live', 'kind_public_offering_live', 'kind_pubprice_live']:
                if cache_name in keyed.index:
                    rows = pd.to_numeric(keyed.loc[cache_name, 'rows'], errors='coerce')
                    if pd.isna(rows) or int(rows) <= 0:
                        kind_missing += 1
            if kind_missing == 3 and ('listing' not in event_types or not any(event_type.startswith('unlock_') for event_type in event_types)):
                warnings.append('KIND 보조 캐시가 비어 있고 일부 일정 데이터가 부족할 수 있습니다.')

    if source_status is not None and not source_status.empty and 'ok' in source_status.columns:
        failures = source_status[~source_status['ok'].fillna(False)]
        for _, row in failures.head(5).iterrows():
            label = clean(row.get('source')) or 'unknown'
            detail = text_value(row.get('detail'))
            warnings.append(f'{label} source failed' + (f': {detail}' if detail else ''))

    seen: set[str] = set()
    deduped: list[str] = []
    for item in warnings:
        if item in seen:
            continue
        seen.add(item)
        deduped.append(item)
    return deduped


def load_issues_inputs(repo: Path, *, prefer_live: bool = False, use_cache: bool = True) -> dict[str, Any]:
    market_path = first_existing(repo, DEFAULT_MARKET_PATHS)
    market = read_csv_safe(market_path) if market_path else pd.DataFrame()
    market_meta = _read_meta_json(repo / 'data' / 'cache' / 'market_snapshot_last_success.meta.json')

    result: dict[str, Any] = {
        'issues': pd.DataFrame(),
        'unlocks': pd.DataFrame(),
        'market': market,
        'market_path': market_path,
        'market_meta': market_meta,
        'issues_source': None,
        'issues_source_mode': None,
        'cache_inventory': pd.DataFrame(),
        'source_status': pd.DataFrame(),
    }

    data_dir = repo / 'data'
    if data_dir.exists():
        try:
            hub = IPODataHub(data_dir)
            bundle = hub.load_bundle(
                prefer_live=prefer_live,
                use_cache=use_cache,
                allow_sample_fallback=True,
                allow_packaged_sample_paths=True,
            )
            issues = enrich_issues_with_unlocks(bundle.issues, bundle.all_unlocks)
            if not issues.empty:
                result.update(
                    {
                        'issues': issues,
                        'unlocks': parse_date_columns(bundle.all_unlocks.copy()) if not bundle.all_unlocks.empty else pd.DataFrame(),
                        'issues_source': 'IPODataHub.load_bundle()',
                        'issues_source_mode': 'bundle',
                        'cache_inventory': bundle.cache_inventory.copy(),
                        'source_status': bundle.source_status.copy(),
                    }
                )
                return result
        except Exception:
            pass

    issues_path = first_existing(repo, DEFAULT_ISSUE_PATHS)
    if not issues_path:
        raise FileNotFoundError('공모주 일정 입력 원본을 찾지 못했습니다.')
    issues = standardize_issue_frame(read_csv_safe(issues_path))
    result.update(
        {
            'issues': issues,
            'issues_source': str(issues_path.relative_to(repo)),
            'issues_source_mode': 'csv-fallback',
        }
    )
    return result



def _missing(value: Any) -> bool:
    if value is None:
        return True
    try:
        if pd.isna(value):
            return True
    except Exception:
        pass
    if isinstance(value, str):
        return value.strip() == '' or value.strip() in {'-', '--', '미상', 'N/A'}
    return False


def _quote_cache_map(df: pd.DataFrame) -> tuple[dict[str, dict[str, Any]], dict[str, dict[str, Any]]]:
    if df is None or df.empty:
        return {}, {}
    work = df.copy()
    if 'name_key' not in work.columns and 'name' in work.columns:
        work['name_key'] = work.get('name').map(normalize_name_key)
    if 'symbol' in work.columns:
        work['symbol'] = work.get('symbol').map(normalize_symbol_text)
    symbol_map: dict[str, dict[str, Any]] = {}
    name_map: dict[str, dict[str, Any]] = {}
    sort_cols = [c for c in ['quote_asof', 'asof', 'symbol', 'name_key'] if c in work.columns]
    if sort_cols:
        work = work.sort_values(sort_cols, ascending=[False] * len(sort_cols), na_position='last')
    for _, row in work.iterrows():
        payload = row.to_dict()
        symbol = text_value(row.get('symbol'))
        name_key = text_value(row.get('name_key'))
        if symbol and symbol not in symbol_map:
            symbol_map[symbol] = payload
        if name_key and name_key not in name_map:
            name_map[name_key] = payload
    return symbol_map, name_map


def apply_public_quote_overlay(repo: Path, issues: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, Any]]:
    work = standardize_issue_frame(issues.copy()) if issues is not None and not issues.empty else pd.DataFrame()
    if work.empty:
        return work, {'source': 'public-quote-overlay', 'ok': False, 'rows': 0, 'detail': 'issues empty'}
    listed = pd.to_datetime(work.get('listing_date'), errors='coerce')
    today = today_kst()
    mask = work.get('symbol', pd.Series(dtype='object')).map(normalize_symbol_text).notna() & listed.notna() & (listed.dt.normalize() <= today)
    request_df = work.loc[mask, [c for c in ['name', 'symbol', 'market', 'listing_date'] if c in work.columns]].copy()
    if request_df.empty:
        return work, {'source': 'public-quote-overlay', 'ok': False, 'rows': 0, 'detail': 'listed symbols unavailable'}
    service = PublicQuoteService(repo / 'data')
    try:
        quotes = service.get_quotes(request_df, max_items=max(len(request_df), 120))
    except Exception as exc:
        try:
            quotes = service._read_cache()  # type: ignore[attr-defined]
        except Exception:
            quotes = pd.DataFrame()
        if quotes is None or quotes.empty:
            return work, {'source': 'public-quote-overlay', 'ok': False, 'rows': 0, 'detail': str(exc)}
    if quotes is None or quotes.empty:
        return work, {'source': 'public-quote-overlay', 'ok': False, 'rows': 0, 'detail': 'quote lookup returned empty'}
    symbol_map, name_map = _quote_cache_map(quotes)
    rows: list[dict[str, Any]] = []
    applied = 0
    for _, row in work.iterrows():
        item = row.to_dict()
        symbol = text_value(normalize_symbol_text(item.get('symbol')))
        name_key = text_value(item.get('name_key') or normalize_name_key(item.get('name')))
        source = symbol_map.get(symbol) if symbol else None
        if source is None and name_key:
            source = name_map.get(name_key)
        if source is not None:
            touched = False
            for col in ['current_price', 'day_change_pct', 'market']:
                if _missing(item.get(col)) and not _missing(source.get(col)):
                    item[col] = source.get(col)
                    touched = True
            if touched:
                applied += 1
        rows.append(item)
    return parse_date_columns(pd.DataFrame(rows)), {'source': 'public-quote-overlay', 'ok': applied > 0, 'rows': int(len(quotes)), 'detail': f'applied={applied}'}


def _read_technical_cache(path: Path) -> pd.DataFrame:
    df = read_csv_safe(path)
    if df.empty:
        return pd.DataFrame(columns=['symbol', 'current_price', 'ma20', 'ma60', 'rsi14', 'signal', 'asof'])
    if 'symbol' in df.columns:
        df['symbol'] = df.get('symbol').map(normalize_symbol_text)
    if 'asof' in df.columns:
        df['asof'] = pd.to_datetime(df.get('asof'), errors='coerce')
    return df


def _write_technical_cache(path: Path, df: pd.DataFrame) -> None:
    if df is None or df.empty:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    out = df.copy()
    if 'asof' in out.columns:
        out['asof'] = pd.to_datetime(out['asof'], errors='coerce')
    out.to_csv(path, index=False)


def _fetch_pykrx_history(symbol: str, *, lookback_days: int = 180) -> pd.DataFrame:
    if pykrx_stock is None:
        return pd.DataFrame(columns=['date', 'close'])
    end = today_kst().normalize()
    start = (end - pd.Timedelta(days=lookback_days)).normalize()
    try:
        raw = pykrx_stock.get_market_ohlcv_by_date(start.strftime('%Y%m%d'), end.strftime('%Y%m%d'), symbol)
    except Exception:
        return pd.DataFrame(columns=['date', 'close'])
    if raw is None or raw.empty:
        return pd.DataFrame(columns=['date', 'close'])
    frame = raw.reset_index()
    date_col = next((col for col in frame.columns if str(col) in {'날짜', 'date', 'Date'}), frame.columns[0])
    close_col = next((col for col in frame.columns if str(col) in {'종가', 'close', 'Close'}), None)
    if close_col is None:
        return pd.DataFrame(columns=['date', 'close'])
    out = pd.DataFrame({
        'date': pd.to_datetime(frame[date_col], errors='coerce'),
        'close': pd.to_numeric(frame[close_col].astype(str).str.replace(',', '', regex=False), errors='coerce'),
    }).dropna(subset=['date', 'close'])
    return out.sort_values('date').reset_index(drop=True)


def apply_public_technical_overlay(repo: Path, issues: pd.DataFrame, *, max_symbols: int = 180) -> tuple[pd.DataFrame, dict[str, Any]]:
    work = standardize_issue_frame(issues.copy()) if issues is not None and not issues.empty else pd.DataFrame()
    if work.empty:
        return work, {'source': 'public-technical-overlay', 'ok': False, 'rows': 0, 'detail': 'issues empty'}
    cache_path = repo / 'data' / 'cache' / 'public_technical_latest.csv'
    cached = _read_technical_cache(cache_path)
    today = today_kst().normalize()
    listed = pd.to_datetime(work.get('listing_date'), errors='coerce')
    symbols = work.get('symbol', pd.Series(dtype='object')).map(normalize_symbol_text)
    mask = symbols.notna() & listed.notna() & (listed.dt.normalize() <= today)
    target = work.loc[mask].copy()
    if target.empty:
        return work, {'source': 'public-technical-overlay', 'ok': False, 'rows': 0, 'detail': 'listed symbols unavailable'}
    target['symbol'] = target.get('symbol').map(normalize_symbol_text)
    target['listing_date'] = pd.to_datetime(target.get('listing_date'), errors='coerce')
    target = target.sort_values(['listing_date', 'symbol'], ascending=[False, True], na_position='last')
    target_symbols = target['symbol'].dropna().drop_duplicates().tolist()[:max_symbols]
    cache_fresh_cutoff = today - pd.Timedelta(days=7)
    fresh_cached_symbols: set[str] = set()
    if not cached.empty and 'asof' in cached.columns:
        fresh_cached_symbols = set(cached.loc[cached['asof'].dt.normalize() >= cache_fresh_cutoff, 'symbol'].dropna().astype(str).tolist())
    new_rows: list[dict[str, Any]] = []
    for symbol in target_symbols:
        if symbol in fresh_cached_symbols:
            continue
        history = _fetch_pykrx_history(symbol, lookback_days=220)
        if history.empty or len(history) < 30:
            continue
        signal = latest_signal_from_history(history)
        if all(_missing(signal.get(key)) for key in ['current_price', 'ma20', 'ma60', 'rsi14']):
            continue
        new_rows.append({
            'symbol': symbol,
            'current_price': signal.get('current_price'),
            'ma20': signal.get('ma20'),
            'ma60': signal.get('ma60'),
            'rsi14': signal.get('rsi14'),
            'signal': signal.get('signal'),
            'asof': today,
        })
    merged = cached.copy() if not cached.empty else pd.DataFrame(columns=['symbol', 'current_price', 'ma20', 'ma60', 'rsi14', 'signal', 'asof'])
    if new_rows:
        merged = pd.concat([pd.DataFrame(new_rows), merged], ignore_index=True)
        if 'asof' in merged.columns:
            merged['asof'] = pd.to_datetime(merged.get('asof'), errors='coerce')
            merged = merged.sort_values(['asof', 'symbol'], ascending=[False, True], na_position='last')
        merged = merged.drop_duplicates(subset=['symbol'], keep='first')
        _write_technical_cache(cache_path, merged)
    if merged.empty:
        return work, {'source': 'public-technical-overlay', 'ok': False, 'rows': 0, 'detail': 'technical cache empty'}
    tech_symbol_map, _ = _quote_cache_map(merged)
    rows: list[dict[str, Any]] = []
    applied = 0
    for _, row in work.iterrows():
        item = row.to_dict()
        symbol = text_value(normalize_symbol_text(item.get('symbol')))
        source = tech_symbol_map.get(symbol) if symbol else None
        if source is not None:
            touched = False
            for col in ['current_price', 'ma20', 'ma60', 'rsi14']:
                if _missing(item.get(col)) and not _missing(source.get(col)):
                    item[col] = source.get(col)
                    touched = True
            if touched:
                applied += 1
        rows.append(item)
    return parse_date_columns(pd.DataFrame(rows)), {'source': 'public-technical-overlay', 'ok': applied > 0, 'rows': int(len(merged)), 'detail': f'targets={len(target_symbols)}, applied={applied}'}


def build_feed(repo: Path, *, prefer_live: bool = False, use_cache: bool = True) -> dict[str, Any]:
    inputs = load_issues_inputs(repo, prefer_live=prefer_live, use_cache=use_cache)
    issues = apply_official_cache_overlays(repo, inputs['issues'])
    market = inputs['market']
    market_path = inputs['market_path']
    market_meta = inputs['market_meta'] or {}
    cache_inventory = inputs['cache_inventory'] if isinstance(inputs['cache_inventory'], pd.DataFrame) else pd.DataFrame()
    cache_inventory = augment_cache_inventory(repo, cache_inventory)
    source_status = inputs['source_status'] if isinstance(inputs['source_status'], pd.DataFrame) else pd.DataFrame()

    extra_status_rows: list[dict[str, Any]] = []
    issues, quote_status = apply_public_quote_overlay(repo, issues)
    extra_status_rows.append(quote_status)
    issues, technical_status = apply_public_technical_overlay(repo, issues)
    extra_status_rows.append(technical_status)
    if extra_status_rows:
        source_status = pd.concat([source_status, pd.DataFrame(extra_status_rows)], ignore_index=True)

    items = dedupe_items([build_item(row) for _, row in issues.iterrows()])
    events: list[dict[str, Any]] = []
    for item in items:
        events.extend(build_events(item))
    quotes = build_quotes(market)

    events.sort(key=lambda record: (record.get('date') or '', record.get('name') or ''))
    generated_at = datetime.now(timezone.utc).astimezone().isoformat()
    today = pd.Timestamp.now(tz='Asia/Seoul').tz_localize(None).normalize()

    freshness_values: list[Any] = []
    if not cache_inventory.empty and 'saved_at' in cache_inventory.columns:
        freshness_values.extend(cache_inventory['saved_at'].dropna().tolist())
    if market_meta.get('saved_at'):
        freshness_values.append(market_meta.get('saved_at'))
    upstream_updated_at = _latest_timestamp(freshness_values)

    official_cache_rows = {}
    if not cache_inventory.empty and {'name', 'rows'}.issubset(cache_inventory.columns):
        official_subset = cache_inventory[cache_inventory['name'].isin(OFFICIAL_CACHE_NAMES)].copy()
        if not official_subset.empty:
            official_cache_rows = {
                text_value(row.get('name')): clean(row.get('rows'))
                for _, row in official_subset.iterrows()
                if text_value(row.get('name'))
            }
    sources = {
        'issuesMode': inputs.get('issues_source_mode'),
        'issuesSource': inputs.get('issues_source'),
        'marketCsv': str(market_path.relative_to(repo)) if isinstance(market_path, Path) else None,
        'officialCaches': official_cache_rows,
    }

    cache_summaries: list[dict[str, Any]] = []
    if not cache_inventory.empty:
        for _, row in cache_inventory.sort_values(['name']).iterrows():
            cache_summaries.append(
                {
                    'name': clean(row.get('name')),
                    'rows': clean(row.get('rows')),
                    'savedAt': clean(row.get('saved_at')),
                    'source': clean(row.get('source')),
                    'notes': clean(row.get('notes')),
                }
            )

    source_status_rows: list[dict[str, Any]] = []
    if not source_status.empty:
        for _, row in source_status.iterrows():
            source_status_rows.append(
                {
                    'source': clean(row.get('source')),
                    'ok': bool(row.get('ok')) if pd.notna(row.get('ok')) else None,
                    'rows': clean(row.get('rows')),
                    'detail': clean(row.get('detail')),
                }
            )

    warnings = build_warnings(cache_inventory, source_status, events)

    return {
        'schemaVersion': SCHEMA_VERSION,
        'generator': GENERATOR_NAME,
        'appName': '공모주 알리미',
        'generatedAt': generated_at,
        'upstreamUpdatedAt': upstream_updated_at,
        'source': str(repo),
        'sources': sources,
        'summary': {
            'itemCount': len(items),
            'eventCount': len(events),
            'marketCount': len(quotes),
            'next30d': compute_counts(events, today, days=30),
        },
        'warnings': warnings,
        'marketQuotes': quotes,
        'items': items,
        'events': events,
        'cacheInventory': cache_summaries,
        'sourceStatus': source_status_rows,
    }


def write_json(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open('w', encoding='utf-8') as fp:
        json.dump(data, fp, ensure_ascii=False, indent=2)


def render_index_html(feed: dict[str, Any], site_base_url: str) -> str:
    generated_at = html.escape(feed.get('generatedAt') or '-')
    upstream_updated_at = html.escape(feed.get('upstreamUpdatedAt') or '-')
    schema_version = html.escape(str(feed.get('schemaVersion') or '-'))
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
    json_url = f"{site_base_url.rstrip('/')}/mobile-feed.json"
    health_url = f"{site_base_url.rstrip('/')}/health.json"
    issues_source = html.escape(sources.get('issuesSource') or '-')
    market_csv = html.escape(sources.get('marketCsv') or '-')
    issues_mode = html.escape(sources.get('issuesMode') or '-')
    warning_items = ''.join(f'<li>{html.escape(str(item))}</li>' for item in (feed.get('warnings') or [])) or '<li>없음</li>'
    return f"""<!doctype html>
<html lang=\"ko\">
  <head>
    <meta charset=\"utf-8\" />
    <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\" />
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
    <div class=\"wrap\">
      <div class=\"card\">
        <h1>공모주 알리미 모바일 피드</h1>
        <p>모바일 앱이 읽는 JSON 피드입니다. 앱 설정의 피드 URL에는 <code>{html.escape(json_url)}</code> 를 넣으면 됩니다.</p>
        <p>피드 스키마 버전: <strong>{schema_version}</strong></p>
        <p>피드 생성 시각: <strong>{generated_at}</strong></p>
        <p>원본 데이터 기준 시각: <strong>{upstream_updated_at}</strong></p>
        <ul>{list_items}</ul>
      </div>
      <div class=\"card\">
        <h2>바로가기</h2>
        <p><a href=\"{html.escape(json_url)}\">mobile-feed.json 열기</a></p>
        <p><a href=\"{html.escape(health_url)}\">health.json 열기</a></p>
      </div>
      <div class=\"card\">
        <h2>입력 원본</h2>
        <p>공모주 원본: <code>{issues_source}</code></p>
        <p>공모주 로딩 방식: <code>{issues_mode}</code></p>
        <p>시장 지표: <code>{market_csv}</code></p>
      </div>
    </div>
  </body>
</html>
"""


def write_site(site_dir: Path, feed: dict[str, Any], site_base_url: str) -> None:
    site_dir.mkdir(parents=True, exist_ok=True)
    write_json(site_dir / 'mobile-feed.json', feed)
    write_json(
        site_dir / 'health.json',
        {
            'status': 'ok',
            'generatedAt': feed.get('generatedAt'),
            'upstreamUpdatedAt': feed.get('upstreamUpdatedAt'),
            'summary': feed.get('summary'),
            'source': feed.get('source'),
        },
    )
    (site_dir / 'index.html').write_text(render_index_html(feed, site_base_url), encoding='utf-8')
    (site_dir / '.nojekyll').write_text('', encoding='utf-8')


def main() -> None:
    parser = argparse.ArgumentParser(description='기존 공모주 저장소를 모바일 앱용 JSON 피드로 변환합니다.')
    parser.add_argument('--repo', default='.', help='저장소 루트 경로')
    parser.add_argument('--output', default='', help='생성할 JSON 파일 경로')
    parser.add_argument('--output-dir', default='', help='mobile-feed.json/health.json/index.html 을 쓸 출력 디렉터리')
    parser.add_argument('--site-dir', default='', help='이전 옵션 호환용 별칭(output-dir와 동일)')
    parser.add_argument('--public-base-url', default='', help='앱이 우선 읽을 공개 기본 주소(jsDelivr 등)')
    parser.add_argument('--fallback-base-url', default='', help='대체 공개 주소(raw.githubusercontent.com 등)')
    parser.add_argument('--site-base-url', default='', help='이전 옵션 호환용 별칭(public-base-url와 동일)')
    parser.add_argument('--prefer-live', action='store_true', help='가능하면 번들 구성 시 live source도 즉시 시도합니다.')
    parser.add_argument('--no-cache', action='store_true', help='번들 구성 시 캐시를 사용하지 않습니다.')
    args = parser.parse_args()

    repo = Path(args.repo).resolve()
    feed = build_feed(repo, prefer_live=args.prefer_live, use_cache=not args.no_cache)

    output_dir = args.output_dir or args.site_dir
    public_base_url = args.public_base_url or args.site_base_url or 'https://example.github.io/repo'

    if args.output:
        write_json(Path(args.output).resolve(), feed)
        print(f'Wrote {Path(args.output).resolve()}')
    if output_dir:
        write_site(Path(output_dir).resolve(), feed, public_base_url)
        print(f'Wrote site {Path(output_dir).resolve()}')
    if not args.output and not output_dir:
        print(json.dumps(feed, ensure_ascii=False, indent=2))


if __name__ == '__main__':
    main()
