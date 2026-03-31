from __future__ import annotations

import argparse
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_ENV_FILES = [ROOT / '.env.local', ROOT / '.env.real', ROOT / '.env.practice', ROOT / '.env.example']
DEFAULT_LAB_ROOT = ROOT / 'integrated_lab' / 'ipo_lockup_unified_lab'


def parse_env_file(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    if not path.exists():
        return values
    for raw in path.read_text(encoding='utf-8').splitlines():
        line = raw.strip()
        if not line or line.startswith('#') or '=' not in line:
            continue
        key, value = line.split('=', 1)
        values[key.strip()] = value.strip().strip('"').strip("'")
    return values


def build_account_no(values: dict[str, str], prefix: str = '') -> str:
    full = values.get(f'{prefix}ACCOUNT_NO', '').strip()
    if full:
        return full
    cano = values.get(f'{prefix}CANO', '').strip()
    acnt = values.get(f'{prefix}ACNT_PRDT_CD', '').strip()
    return f'{cano}-{acnt}' if cano and acnt else ''


def write_text_if_value(path: Path, value: str) -> bool:
    if not value.strip():
        return False
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(value, encoding='utf-8')
    return True


def main() -> None:
    parser = argparse.ArgumentParser(description='프로젝트 .env의 KIS/DART 키를 통합 lab용 key 파일로 동기화합니다.')
    parser.add_argument('--env-file', default='', help='기본은 .env.local 자동 탐색')
    parser.add_argument('--lab-root', default=str(DEFAULT_LAB_ROOT), help='통합 lab 루트 경로')
    args = parser.parse_args()

    env_file = Path(args.env_file).expanduser().resolve() if args.env_file else next((p for p in DEFAULT_ENV_FILES if p.exists()), DEFAULT_ENV_FILES[0])
    values = parse_env_file(env_file)
    lab_root = Path(args.lab_root).expanduser().resolve()
    lab_root.mkdir(parents=True, exist_ok=True)

    real_account = build_account_no(values, 'KIS_')
    demo_account = build_account_no(values, 'KIS_DEMO_')

    real_value = ''
    if values.get('KIS_APP_KEY') and values.get('KIS_APP_SECRET') and real_account:
        real_value = f"{values['KIS_APP_KEY'].strip()}\n{values['KIS_APP_SECRET'].strip()}\n{real_account}\n"
    demo_value = ''
    if values.get('KIS_DEMO_APP_KEY') and values.get('KIS_DEMO_APP_SECRET') and demo_account:
        demo_value = f"{values['KIS_DEMO_APP_KEY'].strip()}\n{values['KIS_DEMO_APP_SECRET'].strip()}\n{demo_account}\n"
    dart_value = f"{values['DART_API_KEY'].strip()}\n" if values.get('DART_API_KEY') else ''

    real_written = write_text_if_value(lab_root / 'real_key.txt', real_value)
    demo_written = write_text_if_value(lab_root / 'practice_key.txt', demo_value)
    dart_written = write_text_if_value(lab_root / 'dart_key.txt', dart_value)

    report = {
        'env_file': str(env_file),
        'lab_root': str(lab_root),
        'real_key_written': real_written,
        'practice_key_written': demo_written,
        'dart_key_written': dart_written,
        'files': {
            'real_key': str(lab_root / 'real_key.txt'),
            'practice_key': str(lab_root / 'practice_key.txt'),
            'dart_key': str(lab_root / 'dart_key.txt'),
        },
    }
    print(json.dumps(report, ensure_ascii=False, indent=2))


if __name__ == '__main__':
    main()
