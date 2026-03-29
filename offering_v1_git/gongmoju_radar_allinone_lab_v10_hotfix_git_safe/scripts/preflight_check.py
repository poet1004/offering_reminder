from __future__ import annotations

import argparse
import importlib.util
import json
import os
import platform
import subprocess
import sys
from pathlib import Path
from typing import Any

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.services.backtest_repository import BacktestRepository
from src.services.dart_client import DartClient
from src.services.ipo_pipeline import IPODataHub
from src.services.kis_client import KISClient
from src.services.market_service import MarketService
from src.services.unified_lab_bridge import UnifiedLabBridgeService
from src.utils import detect_project_env_file, load_project_env, mask_secret, runtime_dir


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="앱 실행 전 환경/데이터/브리지 상태를 점검합니다.")
    parser.add_argument("--workspace", default=None, help="Unified Lab workspace 경로")
    parser.add_argument("--external-unlock", default=None, help="external unlock CSV 경로")
    parser.add_argument("--local-kind", default=None, help="로컬 KIND export 경로")
    parser.add_argument("--check-api", action="store_true", help="KIS/DART API 연결도 시도")
    parser.add_argument("--skip-smoke", action="store_true", help="smoke_test.py 실행 생략")
    parser.add_argument("--json-out", default=None, help="JSON 결과 저장 경로")
    return parser.parse_args()


def latest_files_table(root: Path, patterns: list[str]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for pattern in patterns:
        for path in sorted(root.glob(pattern), key=lambda p: p.stat().st_mtime, reverse=True)[:5]:
            stat = path.stat()
            rows.append(
                {
                    "name": path.name,
                    "path": str(path),
                    "size_bytes": stat.st_size,
                    "modified_at": pd.Timestamp(stat.st_mtime, unit="s").strftime("%Y-%m-%d %H:%M:%S"),
                }
            )
    return rows


def main() -> int:
    args = parse_args()
    load_project_env()
    data_dir = ROOT / "data"
    report_path = Path(args.json_out).expanduser().resolve() if args.json_out else runtime_dir() / "preflight_report.json"

    checks: list[dict[str, Any]] = []

    def add_check(name: str, ok: bool, detail: str = "", severity: str = "warning") -> None:
        checks.append({"name": name, "ok": bool(ok), "severity": severity, "detail": detail})

    add_check("app.py 존재", (ROOT / "app.py").exists(), str(ROOT / "app.py"), severity="critical")
    add_check("requirements.txt 존재", (ROOT / "requirements.txt").exists(), str(ROOT / "requirements.txt"), severity="critical")
    add_check("Python 버전", sys.version_info >= (3, 10), sys.version.replace("\n", " "), severity="critical")
    add_check(
        "Python 아키텍처",
        sys.maxsize > 2**32,
        f"machine={platform.machine()}, arch={platform.architecture()[0]}",
        severity="warning",
    )

    env_file = detect_project_env_file(ROOT)
    add_check(
        ".env 파일",
        env_file is not None or bool(os.getenv("KIS_APP_KEY") or os.getenv("DART_API_KEY")),
        str(env_file or "환경변수 직접 주입"),
        severity="info",
    )

    for module_name in ["pandas", "streamlit", "requests", "openpyxl", "lxml"]:
        add_check(
            f"패키지: {module_name}",
            importlib.util.find_spec(module_name) is not None,
            "import 가능" if importlib.util.find_spec(module_name) is not None else "미설치",
            severity="warning",
        )

    kis_client = KISClient.from_env()
    dart_client = DartClient.from_env()
    add_check("KIS 환경변수", kis_client is not None, mask_secret(os.getenv("KIS_APP_KEY", "")), severity="info")
    add_check("DART 환경변수", dart_client is not None, mask_secret(os.getenv("DART_API_KEY", "")), severity="info")

    hub = IPODataHub(data_dir)
    try:
        bundle = hub.load_bundle(
            prefer_live=False,
            use_cache=True,
            external_unlock_path=args.external_unlock,
            local_kind_export_path=args.local_kind,
            allow_sample_fallback=False,
            allow_packaged_sample_paths=False,
        )
        detail = f"issues={len(bundle.issues)}, unlocks={len(bundle.all_unlocks)}"
        if bundle.issues.empty:
            detail += " (실데이터 소스 미연결 또는 캐시 없음)"
        add_check("IPO bundle 적재", not bundle.issues.empty, detail, severity="info")
    except Exception as exc:
        bundle = None
        add_check("IPO bundle 적재", False, str(exc), severity="critical")

    try:
        versions = BacktestRepository(data_dir).available_versions()
        add_check("백테스트 버전", len(versions) > 0, ", ".join(versions), severity="critical")
    except Exception as exc:
        add_check("백테스트 버전", False, str(exc), severity="critical")

    bridge = UnifiedLabBridgeService(data_dir)
    try:
        unified = bridge.load_bundle(args.workspace)
        detail = (
            f"workspace={unified.paths.workspace}, unlocks={len(unified.unlocks)}, "
            f"signals={len(unified.signals)}, minute_jobs={len(unified.minute_jobs)}"
        )
        add_check("Unified Lab workspace", unified.paths.workspace is not None, detail, severity="info")
    except Exception as exc:
        unified = None
        add_check("Unified Lab workspace", False, str(exc), severity="warning")

    market_service = MarketService(data_dir, kis_client=kis_client)
    try:
        market_bundle = market_service.get_market_snapshot_bundle(prefer_live=True, allow_sample_fallback=False)
        diag_df = market_bundle.get("diagnostics", pd.DataFrame())
        failures = (
            diag_df[~diag_df["ok"].fillna(False)]
            if isinstance(diag_df, pd.DataFrame) and not diag_df.empty and "ok" in diag_df.columns
            else pd.DataFrame()
        )
        detail = f"source={market_bundle['source']}, rows={len(market_bundle['frame'])}, failures={len(failures)}"
        if not failures.empty:
            preview = failures[[c for c in ["name", "provider", "detail"] if c in failures.columns]].head(3).to_dict("records")
            detail += f" | preview={preview}"
        add_check("시장 스냅샷 live", not market_bundle["frame"].empty, detail, severity="warning")
    except Exception as exc:
        add_check("시장 스냅샷 live", False, str(exc), severity="warning")

    if args.check_api and kis_client is not None:
        try:
            res = kis_client.get_index_price("0001")
            add_check(
                "KIS API 호출",
                True,
                json.dumps({"price": res.get("price"), "change_pct": res.get("change_pct")}, ensure_ascii=False),
                severity="info",
            )
        except Exception as exc:
            add_check("KIS API 호출", False, str(exc), severity="warning")

    if args.check_api and dart_client is not None:
        try:
            corp_codes = dart_client.load_corp_codes(base_dir=data_dir / "cache")
            add_check("DART API / corp codes", not corp_codes.empty, f"rows={len(corp_codes)}", severity="info")
        except Exception as exc:
            add_check("DART API / corp codes", False, str(exc), severity="warning")

    if not args.skip_smoke:
        cmd = [sys.executable, str(ROOT / "scripts" / "smoke_test.py")]
        proc = subprocess.run(cmd, capture_output=True, text=True, cwd=str(ROOT))
        smoke_ok = proc.returncode == 0
        smoke_output = (proc.stdout or proc.stderr).strip()
        add_check("smoke_test.py", smoke_ok, smoke_output or f"returncode={proc.returncode}", severity="critical")

    latest_exports = latest_files_table(data_dir / "exports", ["*.csv", "*.json"])
    latest_runtime = latest_files_table(data_dir / "runtime", ["*.csv", "*.json"])

    critical_failures = [row for row in checks if row["severity"] == "critical" and not row["ok"]]
    warnings = [row for row in checks if row["severity"] != "critical" and not row["ok"]]

    report = {
        "project_root": str(ROOT),
        "generated_at": pd.Timestamp.now(tz="Asia/Seoul").tz_localize(None).strftime("%Y-%m-%d %H:%M:%S"),
        "env_file": str(env_file) if env_file is not None else "",
        "checks": checks,
        "critical_failures": len(critical_failures),
        "warnings": len(warnings),
        "latest_exports": latest_exports,
        "latest_runtime": latest_runtime,
    }
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")

    print("[preflight]", f"critical_failures={len(critical_failures)}", f"warnings={len(warnings)}")
    for row in checks:
        status = "OK" if row["ok"] else "FAIL"
        print(f"- {status:4} | {row['severity']:8} | {row['name']} | {row['detail']}")
    print(f"report saved: {report_path}")

    return 1 if critical_failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
