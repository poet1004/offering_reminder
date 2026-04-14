from __future__ import annotations

import argparse
import json
import os
import urllib.parse
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Any

import requests


ROOT = Path(__file__).resolve().parents[1]

try:
    from src.utils import load_project_env  # type: ignore
except Exception:  # pragma: no cover
    def load_project_env() -> None:
        return None


def _service_key() -> str:
    for key in (
        'PUBLIC_DATA_SERVICE_KEY',
        'KSD_PUBLIC_DATA_SERVICE_KEY',
        'KRX_LISTED_INFO_SERVICE_KEY',
        'DATA_GO_SERVICE_KEY',
    ):
        value = os.getenv(key, '').strip()
        if value:
            return value
    return ''


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


def _probe_krx(service_key: str) -> dict[str, Any]:
    url = 'https://apis.data.go.kr/1160100/service/GetKrxListedInfoService/getItemInfo'
    params = {
        'serviceKey': service_key,
        'numOfRows': 1,
        'pageNo': 1,
        'resultType': 'json',
    }
    try:
        response = requests.get(url, params=params, timeout=20)
        response.raise_for_status()
        payload = response.json()
        header = (payload.get('response') or {}).get('header') or {}
        body = (payload.get('response') or {}).get('body') or {}
        items = ((body.get('items') or {}).get('item') or [])
        if isinstance(items, dict):
            items = [items]
        return {
            'ok': str(header.get('resultCode') or '') in {'', '00'} and int(body.get('totalCount') or 0) > 0,
            'scope': 'KRX_LISTED_INFO',
            'baseUrl': 'https://apis.data.go.kr/1160100/service/GetKrxListedInfoService',
            'endpoint': 'getItemInfo',
            'httpStatus': response.status_code,
            'resultCode': header.get('resultCode'),
            'resultMsg': header.get('resultMsg'),
            'itemCount': int(body.get('totalCount') or 0),
            'firstItemKeys': list((items[0] if items else {}).keys())[:20],
            'url': _mask_url(response.url, service_key),
        }
    except Exception as exc:
        return {
            'ok': False,
            'scope': 'KRX_LISTED_INFO',
            'error': str(exc),
            'baseUrl': 'https://apis.data.go.kr/1160100/service/GetKrxListedInfoService',
            'endpoint': 'getItemInfo',
        }


def _parse_xml_result(text: str) -> tuple[str, str, int, list[dict[str, Any]]]:
    root = ET.fromstring(text)
    code = (root.findtext('.//resultCode') or '').strip()
    msg = (root.findtext('.//resultMsg') or '').strip()
    items: list[dict[str, Any]] = []
    for item in root.findall('.//items/item'):
        row: dict[str, Any] = {}
        for child in list(item):
            row[child.tag] = (child.text or '').strip()
        items.append(row)
    return code, msg, len(items), items


def _probe_ksd_stock(service_key: str) -> dict[str, Any]:
    # data.go.kr 문서에 실제 서비스URL로 노출되는 대표 엔드포인트
    url = 'http://api.seibro.or.kr/openapi/service/StockSvc/getKDRSecnInfo'
    attempts: list[dict[str, Any]] = []
    for resolved_url in (url, url.replace('http://', 'https://')):
        for key_name in ('ServiceKey', 'serviceKey'):
            params = {key_name: service_key, 'caltotMartTpcd': '12'}
            try:
                response = requests.get(resolved_url, params=params, timeout=20)
                status = response.status_code
                code, msg, item_count, items = _parse_xml_result(response.text)
                attempts.append({
                    'url': _mask_url(response.url, service_key),
                    'httpStatus': status,
                    'keyParam': key_name,
                    'resultCode': code,
                    'resultMsg': msg,
                    'itemCount': item_count,
                    'firstItemKeys': list((items[0] if items else {}).keys())[:20],
                    'ok': code in {'', '00'} and item_count > 0,
                })
                if code in {'', '00'} and item_count > 0:
                    return {
                        'ok': True,
                        'scope': 'KSD_STOCK',
                        'baseUrl': 'http://api.seibro.or.kr/openapi/service/StockSvc',
                        'endpoint': 'getKDRSecnInfo',
                        'attempts': attempts,
                    }
            except Exception as exc:
                attempts.append({
                    'url': resolved_url,
                    'keyParam': key_name,
                    'ok': False,
                    'error': str(exc),
                })
    return {
        'ok': False,
        'scope': 'KSD_STOCK',
        'baseUrl': 'http://api.seibro.or.kr/openapi/service/StockSvc',
        'endpoint': 'getKDRSecnInfo',
        'attempts': attempts,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description='공공데이터 API 연결 상태 프로브(KRX + KSD 주식정보서비스)')
    parser.add_argument('--output', default='')
    args = parser.parse_args()

    load_project_env()
    key = _service_key()
    if not key:
        report = {
            'ok': False,
            'serviceKeyConfigured': False,
            'reason': 'PUBLIC_DATA_SERVICE_KEY missing',
            'manualTests': {},
            'scopes': {},
        }
    else:
        krx = _probe_krx(key)
        ksd_stock = _probe_ksd_stock(key)
        diagnosis = ''
        ksd_messages = ' | '.join(str(a.get('resultMsg') or a.get('error') or '') for a in (ksd_stock.get('attempts') or []))
        if krx.get('ok') and not ksd_stock.get('ok') and 'SERVICE KEY IS NOT REGISTERED ERROR' in ksd_messages:
            diagnosis = 'KRX key works, but KSD StockSvc backend still says SERVICE KEY IS NOT REGISTERED ERROR. This usually means per-service backend registration/sync lag on the provider side.'
        report = {
            'ok': bool(krx.get('ok') or ksd_stock.get('ok')),
            'serviceKeyConfigured': True,
            'manualTests': {
                'krxListedInfo': 'https://apis.data.go.kr/1160100/service/GetKrxListedInfoService/getItemInfo?serviceKey=***&numOfRows=1&pageNo=1&resultType=xml',
                'ksdStockSvc': 'http://api.seibro.or.kr/openapi/service/StockSvc/getKDRSecnInfo?ServiceKey=***&caltotMartTpcd=12',
                'ksdWadl': 'https://api.seibro.or.kr/openapi/service/StockSvc?_wadl&type=xml',
            },
            'diagnosis': diagnosis,
            'scopes': {
                'krx': krx,
                'ksdStock': ksd_stock,
            },
        }

    if args.output:
        out = Path(args.output).resolve()
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding='utf-8')
    print(json.dumps(report, ensure_ascii=False, indent=2))


if __name__ == '__main__':
    main()
