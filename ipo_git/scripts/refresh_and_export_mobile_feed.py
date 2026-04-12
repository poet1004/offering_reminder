#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path
from typing import Any

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.services.dart_client import DartClient
from src.services.ipo_pipeline import IPODataHub
from src.services.kis_client import KISClient
from src.services.market_service import MarketService
from src.utils import load_project_env


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='라이브 캐시 갱신 + 모바일 피드 내보내기 + 검증을 한 번에 실행합니다.')
    parser.add_argument('--data-dir', default=str(ROOT / 'data'))
    parser.add_argument('--skip-refresh', action='store_true', help='KIND/38/시장 캐시 갱신은 건너뜁니다.')
    parser.add_argument('--skip-market', action='store_true', help='시장 캐시 갱신은 건너뜁니다.')
    parser.add_argument('--skip-kind', action='store_true', help='KIND 캐시 갱신은 건너뜁니다.')
    parser.add_argument('--skip-38', action='store_true', help='38/Seibro 캐시 갱신은 건너뜁니다.')
    parser.add_argument('--skip-official', action='store_true', help='공식 API(KSD) 캐시 갱신은 건너뜁니다.')
    parser.add_argument('--output', default='data/mobile/mobile-feed.json')
    parser.add_argument('--site-dir', default='mobile-feed')
    parser.add_argument('--site-base-url', default='https://cdn.jsdelivr.net/gh/poet1004/offering_reminder@main/mobile-feed')
    parser.add_argument('--verify-json-out', default='data/runtime/mobile_feed_verify.json')
    parser.add_argument('--report-json-out', default='data/runtime/mobile_feed_pipeline_report.json')
    return parser.parse_args()


def run_subprocess(cmd: list[str], cwd: Path) -> dict[str, Any]:
    proc = subprocess.run(cmd, cwd=str(cwd), capture_output=True, text=True)
    return {
        'returncode': proc.returncode,
        'stdout': (proc.stdout or '').strip(),
        'stderr': (proc.stderr or '').strip(),
        'cmd': cmd,
    }


def refresh_sources(data_dir: Path, *, skip_market: bool, skip_kind: bool, skip_38: bool, skip_official: bool) -> dict[str, Any]:
    load_project_env()
    hub = IPODataHub(data_dir, dart_client=DartClient.from_env())
    report: dict[str, Any] = {
        'ipo': hub.refresh_live_cache(fetch_kind=not skip_kind, fetch_38=not skip_38),
    }
    if not skip_official:
        official_cmd = [sys.executable, str(ROOT / 'scripts' / 'refresh_official_api_cache.py'), '--data-dir', str(data_dir)]
        report['official'] = run_subprocess(official_cmd, ROOT)
    if not skip_market:
        market = MarketService(data_dir, kis_client=KISClient.from_env())
        report['market'] = market.refresh_market_cache(periods=['1mo', '3mo', '6mo', '1y'])
    return report


def main() -> int:
    args = parse_args()
    repo = ROOT
    data_dir = Path(args.data_dir).expanduser().resolve()
    report: dict[str, Any] = {
        'generatedAt': pd.Timestamp.now(tz='Asia/Seoul').tz_localize(None).isoformat(),
        'repo': str(repo),
    }

    if not args.skip_refresh:
        try:
            report['refresh'] = refresh_sources(
                data_dir,
                skip_market=args.skip_market,
                skip_kind=args.skip_kind,
                skip_38=args.skip_38,
                skip_official=args.skip_official,
            )
        except Exception as exc:
            report['refreshError'] = str(exc)
    else:
        report['refreshSkipped'] = True

    export_cmd = [
        sys.executable,
        str(repo / 'scripts' / 'export_mobile_feed.py'),
        '--repo',
        str(repo),
        '--output',
        str((repo / args.output).resolve()),
        '--site-dir',
        str((repo / args.site_dir).resolve()),
        '--site-base-url',
        args.site_base_url,
    ]
    report['export'] = run_subprocess(export_cmd, repo)

    verify_cmd = [
        sys.executable,
        str(repo / 'scripts' / 'verify_mobile_feed.py'),
        '--path',
        str((repo / args.output).resolve()),
        '--json-out',
        str((repo / args.verify_json_out).resolve()),
    ]
    report['verify'] = run_subprocess(verify_cmd, repo)

    try:
        payload = json.loads((repo / args.output).read_text(encoding='utf-8'))
        report['feedSummary'] = payload.get('summary')
        report['feedWarnings'] = payload.get('warnings') or []
        report['schemaVersion'] = payload.get('schemaVersion')
    except Exception as exc:
        report['feedReadError'] = str(exc)

    report_path = (repo / args.report_json_out).resolve()
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding='utf-8')
    print(json.dumps(report, ensure_ascii=False, indent=2))

    export_ok = report.get('export', {}).get('returncode') == 0
    verify_ok = report.get('verify', {}).get('returncode') == 0
    return 0 if export_ok and verify_ok else 1


if __name__ == '__main__':
    raise SystemExit(main())
