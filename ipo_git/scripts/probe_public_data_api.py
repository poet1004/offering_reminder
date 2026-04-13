from __future__ import annotations

import argparse
import json
import os
import sys
import urllib.parse
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.services.public_data_client import KSDPublicDataClient, KSD_CORP_BASE_URL, KSD_STOCK_BASE_URL
from src.utils import load_project_env


def _mask_url(url: str, service_key: str) -> str:
    try:
        parsed = urllib.parse.urlsplit(url)
        query = urllib.parse.parse_qsl(parsed.query, keep_blank_values=True)
        masked = []
        for key, value in query:
            if key.lower() == 'servicekey':
                masked.append((key, '***'))
            else:
                masked.append((key, value))
        return urllib.parse.urlunsplit((parsed.scheme, parsed.netloc, parsed.path, urllib.parse.urlencode(masked), parsed.fragment))
    except Exception:
        return url.replace(service_key, '***') if service_key else url


def _probe_once(client: KSDPublicDataClient, base_url: str, endpoint: str, params: dict[str, Any], key_name: str) -> dict[str, Any]:
    query = {k: v for k, v in params.items() if v not in {None, ''}}
    query.setdefault('numOfRows', 1)
    query.setdefault('pageNo', 1)
    query[key_name] = client.service_key
    url = f"{base_url.rstrip('/')}/{endpoint.lstrip('/')}"
    try:
        response = client.session.get(url, params=query, timeout=15)
        status = response.status_code
        content_type = response.headers.get('content-type', '')
        body_preview = response.text[:500]
    except Exception as exc:
        return {
            'ok': False,
            'transportOk': False,
            'baseUrl': base_url,
            'endpoint': endpoint,
            'keyParam': key_name,
            'error': str(exc),
        }
    result: dict[str, Any] = {
        'ok': False,
        'transportOk': True,
        'httpStatus': status,
        'contentType': content_type,
        'baseUrl': base_url,
        'endpoint': endpoint,
        'keyParam': key_name,
        'url': _mask_url(response.url, client.service_key),
    }
    try:
        parsed = client._parse_xml(response.content, url=response.url, params=query)
        result.update({
            'ok': bool(parsed.ok and len(parsed.items) > 0),
            'parsedOk': bool(parsed.ok),
            'resultCode': parsed.code,
            'resultMsg': parsed.message,
            'itemCount': len(parsed.items),
            'firstItemKeys': list(parsed.items[0].keys())[:20] if parsed.items else [],
        })
    except Exception as exc:
        result.update({
            'parsedOk': False,
            'parseError': str(exc),
            'bodyPreview': body_preview,
        })
    return result


def run_probe() -> dict[str, Any]:
    load_project_env()
    client = KSDPublicDataClient.from_env(cache_dir=ROOT / 'data' / 'cache')
    if client is None:
        return {
            'ok': False,
            'serviceKeyConfigured': False,
            'reason': 'PUBLIC_DATA_SERVICE_KEY missing',
            'probes': [],
        }
    probes: list[dict[str, Any]] = []
    cases = [
        {
            'name': 'market_codes_kospi',
            'base_url': KSD_STOCK_BASE_URL,
            'endpoint': 'getShotnByMartN1',
            'params': {'martTpcd': '11', 'numOfRows': 1, 'pageNo': 1},
        },
        {
            'name': 'stock_name_samsung',
            'base_url': KSD_STOCK_BASE_URL,
            'endpoint': 'getStkIsinByNmN1',
            'params': {'secnNm': '삼성전자', 'numOfRows': 1, 'pageNo': 1},
        },
        {
            'name': 'corp_name_samsung',
            'base_url': KSD_CORP_BASE_URL,
            'endpoint': 'getIssucoCustnoByNm',
            'params': {'issucoNm': '삼성전자', 'numOfRows': 1, 'pageNo': 1},
        },
    ]
    for case in cases:
        base_urls = [case['base_url']]
        if str(case['base_url']).startswith('http://'):
            base_urls.append('https://' + str(case['base_url'])[len('http://'):])
        attempts: list[dict[str, Any]] = []
        for base_url in base_urls:
            for key_name in ('serviceKey', 'ServiceKey'):
                probe = _probe_once(client, base_url, case['endpoint'], case['params'], key_name)
                probe['name'] = case['name']
                attempts.append(probe)
                if probe.get('ok'):
                    break
            if any(item.get('ok') for item in attempts):
                break
        probes.append({
            'name': case['name'],
            'success': any(item.get('ok') for item in attempts),
            'attempts': attempts,
        })
    any_success = any(item.get('success') for item in probes)
    return {
        'ok': any_success,
        'serviceKeyConfigured': True,
        'probes': probes,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description='공공데이터 KSD API 연결 상태 프로브')
    parser.add_argument('--output', default='')
    args = parser.parse_args()
    report = run_probe()
    if args.output:
        output_path = Path(args.output).resolve()
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding='utf-8')
    print(json.dumps(report, ensure_ascii=False, indent=2))


if __name__ == '__main__':
    main()
