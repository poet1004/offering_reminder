from __future__ import annotations

import argparse
import csv
import json
import shutil
from pathlib import Path
from typing import Any


def read_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    with path.open('r', encoding='utf-8') as fp:
        return json.load(fp)


def write_json(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open('w', encoding='utf-8') as fp:
        json.dump(data, fp, ensure_ascii=False, indent=2)


def copy_tree(src: Path, dst: Path) -> None:
    if dst.exists():
        shutil.rmtree(dst)
    shutil.copytree(src, dst)


def load_backtest_summary(csv_path: Path) -> list[dict[str, Any]]:
    if not csv_path.exists():
        return []
    rows: list[dict[str, Any]] = []
    with csv_path.open('r', encoding='utf-8-sig', newline='') as fp:
        reader = csv.DictReader(fp)
        for row in reader:
            cleaned = {k: _coerce_number(v) for k, v in row.items()}
            rows.append(cleaned)
    rows.sort(key=lambda row: float(row.get('compound_ret') or -1e18), reverse=True)
    return rows


def _coerce_number(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return value
    text = str(value).strip()
    if text == '':
        return None
    try:
        if '.' in text or '-' in text:
            return float(text)
        return int(text)
    except ValueError:
        return text


def build_site(repo: Path, output_dir: Path, feed_path: Path | None = None, cname: str = '') -> dict[str, Any]:
    static_src = repo / 'static_pages'
    if not static_src.exists():
        raise FileNotFoundError(f'static_pages 폴더가 없습니다: {static_src}')

    output_dir.mkdir(parents=True, exist_ok=True)
    copy_tree(static_src, output_dir)

    actual_feed_path = feed_path or (repo / 'mobile-feed' / 'mobile-feed.json')
    if not actual_feed_path.exists():
        fallback = repo / 'data' / 'mobile' / 'mobile-feed.json'
        if fallback.exists():
            actual_feed_path = fallback
        else:
            raise FileNotFoundError('mobile-feed.json을 찾지 못했습니다. mobile-feed/mobile-feed.json 또는 data/mobile/mobile-feed.json 필요')

    feed = read_json(actual_feed_path, {})
    verify = read_json(repo / 'data' / 'runtime' / 'mobile_feed_verify.json', {})
    preflight = read_json(repo / 'data' / 'runtime' / 'preflight_report.json', {})
    backtest_summary = load_backtest_summary(repo / 'data' / 'backtest' / 'versions_summary_pretty.csv')

    data_dir = output_dir / 'data'
    write_json(data_dir / 'mobile-feed.json', feed)
    write_json(data_dir / 'mobile-feed-verify.json', verify)
    write_json(data_dir / 'preflight-report.json', preflight)
    write_json(data_dir / 'backtest-summary.json', backtest_summary)
    write_json(
        data_dir / 'site-meta.json',
        {
            'generatedAt': feed.get('generatedAt'),
            'upstreamUpdatedAt': feed.get('upstreamUpdatedAt'),
            'itemCount': (feed.get('summary') or {}).get('itemCount'),
            'eventCount': (feed.get('summary') or {}).get('eventCount'),
            'schemaVersion': feed.get('schemaVersion'),
            'feedSourcePath': str(actual_feed_path.relative_to(repo)),
        },
    )

    (output_dir / '.nojekyll').write_text('', encoding='utf-8')
    (output_dir / '404.html').write_text(
        '<!doctype html><meta charset="utf-8"><meta http-equiv="refresh" content="0; url=./index.html"><title>Redirect</title>',
        encoding='utf-8',
    )
    if cname:
        (output_dir / 'CNAME').write_text(cname.strip() + '\n', encoding='utf-8')

    return {
        'ok': True,
        'output_dir': str(output_dir),
        'feed_source': str(actual_feed_path),
        'item_count': (feed.get('summary') or {}).get('itemCount'),
        'event_count': (feed.get('summary') or {}).get('eventCount'),
        'schema_version': feed.get('schemaVersion'),
        'backtest_rows': len(backtest_summary),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description='GitHub Pages용 정적 공모주 사이트를 생성합니다.')
    parser.add_argument('--repo', default='.', help='저장소 루트 경로')
    parser.add_argument('--output', default='_site', help='정적 사이트 출력 폴더')
    parser.add_argument('--feed', default='', help='입력 mobile-feed.json 경로')
    parser.add_argument('--cname', default='', help='선택: 사용자 지정 도메인')
    args = parser.parse_args()

    repo = Path(args.repo).resolve()
    output_dir = Path(args.output).resolve()
    feed_path = Path(args.feed).resolve() if args.feed else None
    result = build_site(repo, output_dir, feed_path=feed_path, cname=args.cname)
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == '__main__':
    main()
