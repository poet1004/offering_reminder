#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def run(cmd: list[str]) -> dict[str, object]:
    proc = subprocess.run(cmd, cwd=str(ROOT), capture_output=True, text=True)
    return {
        'cmd': cmd,
        'returncode': proc.returncode,
        'stdout': (proc.stdout or '').strip(),
        'stderr': (proc.stderr or '').strip(),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description='라이브 캐시 갱신 + 모바일 피드 생성 + GitHub Pages 정적 사이트 생성')
    parser.add_argument('--output', default='_site', help='정적 사이트 출력 폴더')
    parser.add_argument('--skip-refresh', action='store_true')
    args = parser.parse_args()

    report: dict[str, object] = {}
    if args.skip_refresh:
        refresh_cmd = [sys.executable, str(ROOT / 'scripts' / 'refresh_and_export_mobile_feed.py'), '--skip-refresh']
    else:
        refresh_cmd = [sys.executable, str(ROOT / 'scripts' / 'refresh_and_export_mobile_feed.py')]
    report['refresh_and_export'] = run(refresh_cmd)
    if report['refresh_and_export']['returncode'] != 0:
        print(json.dumps(report, ensure_ascii=False, indent=2))
        return 1

    build_cmd = [sys.executable, str(ROOT / 'scripts' / 'build_pages_site.py'), '--repo', str(ROOT), '--output', str((ROOT / args.output).resolve())]
    report['build_pages'] = run(build_cmd)
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0 if report['build_pages']['returncode'] == 0 else 1


if __name__ == '__main__':
    raise SystemExit(main())
