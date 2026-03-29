from __future__ import annotations

import argparse
import json
import shutil
import sys
import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.services.unified_lab_bridge import UnifiedLabBridgeService
from src.utils import ensure_dir, runtime_dir


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Unified Lab zip 또는 workspace zip을 풀어서 연결 가능한 폴더를 준비합니다.")
    parser.add_argument("--zip-path", required=True, help="압축파일 경로")
    parser.add_argument("--out-dir", default=None, help="압축해제 대상 폴더. 기본은 data/imports/<zip_stem>")
    parser.add_argument("--clean", action="store_true", help="대상 폴더가 이미 있으면 먼저 삭제")
    parser.add_argument("--report-json", default=None, help="추출 결과 리포트 JSON 경로")
    return parser.parse_args()


def strip_single_root(extract_dir: Path) -> Path:
    entries = [p for p in extract_dir.iterdir() if p.name != "__MACOSX"]
    if len(entries) == 1 and entries[0].is_dir():
        return entries[0]
    return extract_dir


def main() -> None:
    args = parse_args()
    zip_path = Path(args.zip_path).expanduser().resolve()
    if not zip_path.exists():
        raise SystemExit(f"zip 파일을 찾지 못했습니다: {zip_path}")

    out_dir = Path(args.out_dir).expanduser().resolve() if args.out_dir else ROOT / "data" / "imports" / zip_path.stem
    if out_dir.exists() and args.clean:
        shutil.rmtree(out_dir)
    ensure_dir(out_dir)

    with zipfile.ZipFile(zip_path) as zf:
        zf.extractall(out_dir)

    extracted_root = strip_single_root(out_dir)
    bridge = UnifiedLabBridgeService(ROOT / "data")
    workspace = bridge.auto_detect_workspace(extracted_root)

    report = {
        "zip_path": str(zip_path),
        "out_dir": str(out_dir),
        "extracted_root": str(extracted_root),
        "detected_workspace": str(workspace) if workspace is not None else "",
        "workspace_found": workspace is not None,
    }
    report_path = Path(args.report_json).expanduser().resolve() if args.report_json else runtime_dir() / f"import_unified_lab_{zip_path.stem}.json"
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")

    print(json.dumps(report, ensure_ascii=False, indent=2))
    print(f"report saved: {report_path}")


if __name__ == "__main__":
    main()
