#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

OFFICIAL_CACHE_NAMES = [
    'official_ksd_name_lookup_live',
    'official_ksd_market_codes_live',
    'official_ksd_listing_info_live',
    'official_ksd_corp_basic_live',
    'official_ksd_shareholder_summary_live',
    'official_issue_overlay_live',
    'official_krx_listed_info_live',
]


def read_json(path: Path, default: Any = None) -> Any:
    if not path.exists() or path.stat().st_size == 0:
        return default
    try:
        with path.open('r', encoding='utf-8') as fp:
            return json.load(fp)
    except Exception:
        return default


def read_text(path: Path | None) -> str:
    if path is None or not path.exists() or path.stat().st_size == 0:
        return ''
    try:
        return path.read_text(encoding='utf-8', errors='replace')
    except Exception:
        return ''


def read_csv_rows(path: Path) -> int | None:
    if not path.exists() or path.stat().st_size == 0:
        return None
    try:
        return int(len(pd.read_csv(path)))
    except Exception:
        return None


def collect_cache_rows(data_dir: Path) -> dict[str, int | None]:
    cache_dir = data_dir / 'cache'
    rows: dict[str, int | None] = {}
    for name in OFFICIAL_CACHE_NAMES:
        meta = read_json(cache_dir / f'{name}.meta.json', {}) or {}
        row_count = meta.get('row_count')
        if row_count is None:
            row_count = read_csv_rows(cache_dir / f'{name}.csv')
        try:
            rows[name] = int(row_count) if row_count is not None else None
        except Exception:
            rows[name] = None
    return rows


def main() -> None:
    parser = argparse.ArgumentParser(description='공공데이터 API 갱신 상태를 표준 JSON으로 저장합니다.')
    parser.add_argument('--data-dir', required=True)
    parser.add_argument('--raw', required=True)
    parser.add_argument('--output', required=True)
    parser.add_argument('--exit-code', type=int, required=True)
    parser.add_argument('--stderr', default='')
    parser.add_argument('--key-configured', action='store_true')
    args = parser.parse_args()

    data_dir = Path(args.data_dir).resolve()
    raw_path = Path(args.raw).resolve()
    output_path = Path(args.output).resolve()
    stderr_path = Path(args.stderr).resolve() if args.stderr else None

    raw_text = read_text(raw_path).strip()
    stderr_text = read_text(stderr_path).strip()
    raw_json = None
    warnings: list[str] = []

    if raw_text:
        try:
            raw_json = json.loads(raw_text)
        except Exception:
            warnings.append('official refresh stdout is not valid JSON')
    else:
        warnings.append('official refresh stdout is empty')

    if stderr_text:
        warnings.append('official refresh stderr not empty')

    cache_rows = collect_cache_rows(data_dir)
    populated_cache_count = sum(1 for value in cache_rows.values() if isinstance(value, int) and value > 0)
    ok = bool(args.key_configured) and args.exit_code == 0 and populated_cache_count > 0

    if isinstance(raw_json, dict) and raw_json.get('ok') is False and populated_cache_count == 0:
        ok = False
    if not args.key_configured:
        warnings.append('PUBLIC_DATA_SERVICE_KEY missing')
    if populated_cache_count == 0:
        warnings.append('official cache rows are all zero or missing')

    raw_reason = raw_json.get('reason') if isinstance(raw_json, dict) else None
    if isinstance(raw_json, dict):
        for field in ('warnings', 'errors'):
            entries = raw_json.get(field)
            if isinstance(entries, list):
                for entry in entries[:20]:
                    text = str(entry).strip()
                    if text:
                        warnings.append(text)

    deduped: list[str] = []
    seen: set[str] = set()
    for entry in warnings:
        text = str(entry).strip()
        if not text or text in seen:
            continue
        seen.add(text)
        deduped.append(text)

    report = {
        'ok': ok,
        'generatedAt': datetime.now(timezone.utc).isoformat(),
        'serviceKeyConfigured': bool(args.key_configured),
        'refreshReturnCode': int(args.exit_code),
        'refreshSucceeded': int(args.exit_code) == 0,
        'cachePopulated': populated_cache_count > 0,
        'cacheRows': cache_rows,
        'rawReport': raw_json if raw_json is not None else None,
        'rawReason': raw_reason,
        'stdoutPreview': raw_text[:4000] if raw_json is None else '',
        'stderrPreview': stderr_text[:4000],
        'warnings': deduped,
    }
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open('w', encoding='utf-8') as fp:
        json.dump(report, fp, ensure_ascii=False, indent=2)
    print(json.dumps(report, ensure_ascii=False))


if __name__ == '__main__':
    main()
