from __future__ import annotations

import os
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Sequence

ROOT = Path(__file__).resolve().parent
ROOT_REPO = ROOT.parents[1]
PY = sys.executable
ROOT_DATA = ROOT / "workspace"
CACHE_DIR = ROOT_DATA / "cache_kis"
DATASET_DIR = ROOT_DATA / "dataset_out"
UNLOCK_DIR = ROOT_DATA / "unlock_out"
MINUTE_DB = ROOT_DATA / "data" / "curated" / "lockup_minute.db"
SIGNAL_DIR = ROOT_DATA / "signal_out"
BACKTEST_DIR = ROOT_DATA / "backtest_out"
TURNOVER_BT_DIR = ROOT_DATA / "turnover_backtest_out"
ANALYSIS_DIR = ROOT_DATA / "analysis_out"
LOG_DIR = ROOT_DATA / "logs"


try:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    if hasattr(sys.stderr, "reconfigure"):
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass

PROGRAM_DAILY = ROOT / "ipo_lockup_program.py"
PROGRAM_DART = ROOT / "dart_unlock_events_builder.py"
PROGRAM_UNLOCK_CONVERT = ROOT / "unlock_events_to_backtest_input.py"
PROGRAM_MINUTE = ROOT / "kiwoom_minute_pipeline.py"
PROGRAM_SIGNAL = ROOT / "turnover_signal_engine.py"
PROGRAM_TURNOVER_BT = ROOT / "turnover_daily_backtest.py"
PROGRAM_BETA = ROOT / "trade_window_beta.py"

CONFIG_DAILY = ROOT / "ipo_lockup_config_example.json"
CONFIG_TURNOVER = ROOT / "turnover_backtest_config_example.json"
REQUIREMENTS = ROOT / "requirements.txt"
SYNC_ENV_SCRIPT = ROOT_REPO / "scripts" / "sync_env_to_lab_keys.py"
EXPORT_SEED_SCRIPT = ROOT_REPO / "scripts" / "export_ipo_seed_to_lab.py"


def _quote(parts: Sequence[object]) -> str:
    return " ".join([f'"{x}"' if " " in str(x) else str(x) for x in parts])


def run_cmd(cmd: list[object], *, label: str | None = None) -> tuple[int, Path]:
    _ensure_workspace_dirs()
    safe_cmd = [str(x) for x in cmd]
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_path = LOG_DIR / f"{stamp}_{label or 'command'}.log"
    print("\n실행 명령:")
    print(_quote(safe_cmd))
    print(f"로그 파일: {log_path}\n")

    env = os.environ.copy()
    env.setdefault("PYTHONUNBUFFERED", "1")
    if safe_cmd and safe_cmd[0].endswith("python") and "-u" not in safe_cmd[1:3]:
        safe_cmd = [safe_cmd[0], "-u", *safe_cmd[1:]]

    with log_path.open("w", encoding="utf-8", errors="replace") as fh:
        fh.write(f"$ {_quote(safe_cmd)}\n\n")
        proc = subprocess.Popen(
            safe_cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding="utf-8",
            errors="replace",
            cwd=str(ROOT),
            env=env,
        )
        assert proc.stdout is not None
        for line in proc.stdout:
            sys.stdout.write(line)
            fh.write(line)
        rc = proc.wait()
        fh.write(f"\n[exit_code] {rc}\n")

    print(f"\n[종료 코드] {rc}")
    print(f"[로그] {log_path}\n")
    return rc, log_path


def _ensure_workspace_dirs() -> None:
    for path in [
        ROOT_DATA,
        CACHE_DIR,
        DATASET_DIR,
        UNLOCK_DIR,
        ROOT_DATA / "data" / "curated",
        SIGNAL_DIR,
        BACKTEST_DIR,
        TURNOVER_BT_DIR,
        ANALYSIS_DIR,
        ROOT_DATA / "cache_dart",
        LOG_DIR,
    ]:
        path.mkdir(parents=True, exist_ok=True)


def _find_first(paths: list[Path]) -> Path | None:
    for p in paths:
        if p.exists():
            return p
    return None


def _pick_existing(prompt: str, cands: list[Path], allow_empty: bool = False) -> Path | None:
    found = _find_first(cands)
    if found:
        print(f"자동으로 찾은 파일: {found}")
        ans = input("이 경로를 사용할까요? [Y/n] ").strip().lower()
        if ans in ("", "y", "yes"):
            return found
    while True:
        s = input(prompt).strip().strip('"')
        if allow_empty and s == "":
            return None
        p = Path(s)
        if p.exists():
            return p
        print("경로를 찾지 못했습니다.")


def _pick_key_file() -> Path:
    cands = [
        ROOT / "real_key.txt",
        ROOT / "practice_key.txt",
        Path.home() / "Desktop" / "한국투자증권" / "real_key.txt",
        Path.home() / "Desktop" / "한국투자증권" / "practice_key.txt",
    ]
    return _pick_existing("KIS 키 파일 경로를 입력하세요: ", cands)  # type: ignore[return-value]


def _pick_dart_key_file() -> Path:
    cands = [
        ROOT / "dart_key.txt",
        Path.home() / "Desktop" / "한국투자증권" / "dart_key.txt",
        Path.home() / "Desktop" / "dart_key.txt",
    ]
    return _pick_existing("DART 키 파일 경로를 입력하세요: ", cands)  # type: ignore[return-value]


def open_path(p: Path) -> None:
    p.parent.mkdir(parents=True, exist_ok=True)
    if os.name == "nt":
        os.startfile(p if p.exists() else p.parent)  # type: ignore[attr-defined]
    else:
        print(p)


def maybe_sync_lab_keys() -> None:
    missing = [name for name in ["real_key.txt", "practice_key.txt", "dart_key.txt"] if not (ROOT / name).exists()]
    if not missing or not SYNC_ENV_SCRIPT.exists():
        return
    print(f"[정보] 누락된 lab key 파일이 있어 자동 동기화를 시도합니다: {', '.join(missing)}")
    run_cmd([PY, str(SYNC_ENV_SCRIPT), "--lab-root", str(ROOT)], label="sync_lab_keys")


def maybe_export_seed() -> None:
    if not EXPORT_SEED_SCRIPT.exists():
        print(f"[경고] seed export script를 찾지 못했습니다: {EXPORT_SEED_SCRIPT}")
        return
    print("[정보] 앱 live/cache IPO 데이터를 integrated lab seed master로 내보냅니다.")
    rc, _ = run_cmd([PY, str(EXPORT_SEED_SCRIPT), "--lab-root", str(ROOT)], label="export_ipo_seed")
    if rc != 0:
        print("[경고] seed export가 완전히 성공하지 않았습니다. 기존 로컬 파일/38 보조 소스로 계속 진행합니다.")


def menu() -> None:
    _ensure_workspace_dirs()
    maybe_sync_lab_keys()
    while True:
        print("\n메뉴")
        print("1. 패키지 설치")
        print("2. 합성 버킷 dataset 만들기 (dataset_out만 생성)")
        print("3. 합성 버킷 daily backtest")
        print("4. DART 실제 unlock 이벤트 생성")
        print("5. unlock 이벤트를 backtest 입력형식으로 변환")
        print("6. minute DB 초기화 + unlock 이벤트 기반 job 생성")
        print("7. 외부 minute CSV를 DB로 가져오기")
        print("8. turnover 신호 생성 (1x/2x 등)")
        print("9. turnover daily backtest")
        print("10. benchmark 대비 trade-window beta proxy 계산")
        print("11. 결과 폴더 열기")
        print("12. 설정 파일 열기")
        print("0. 종료")
        choice = input("번호를 입력하세요 > ").strip()

        if choice == "1":
            run_cmd([PY, "-m", "pip", "install", "-r", str(REQUIREMENTS)], label="install_requirements")
        elif choice == "2":
            maybe_export_seed()
            cmd = [
                PY,
                str(PROGRAM_DAILY),
                "build-dataset",
                "--config",
                str(CONFIG_DAILY),
                "--cache-dir",
                str(CACHE_DIR),
                "--out-dir",
                str(DATASET_DIR),
            ]
            rc, _ = run_cmd(cmd, label="build_dataset")
            if rc == 0:
                print(f"[안내] dataset 생성 단계가 끝났습니다: {DATASET_DIR}")
                print("[안내] backtest_out이 비어 있는 것은 정상입니다. daily 백테스트 결과를 만들려면 다음으로 메뉴 3을 실행하세요.")
        elif choice == "3":
            key_file = _pick_key_file()
            dataset_csv = DATASET_DIR / "synthetic_ipo_events.csv"
            cmd = [
                PY,
                str(PROGRAM_DAILY),
                "backtest",
                "--key-file",
                str(key_file),
                "--config",
                str(CONFIG_DAILY),
                "--cache-dir",
                str(CACHE_DIR),
                "--out-dir",
                str(BACKTEST_DIR),
                "--dataset-csv",
                str(dataset_csv),
            ]
            run_cmd(cmd, label="daily_backtest")
        elif choice == "4":
            dart_key = _pick_dart_key_file()
            master_csv = _pick_existing(
                "filtered_master.csv 경로를 입력하세요: ",
                [DATASET_DIR / "filtered_master.csv"],
            )
            out_csv = UNLOCK_DIR / "unlock_events_dart.csv"
            cache_dir = ROOT_DATA / "cache_dart"
            cmd = [
                PY,
                str(PROGRAM_DART),
                "--master-csv",
                str(master_csv),
                "--dart-key-file",
                str(dart_key),
                "--out-csv",
                str(out_csv),
                "--cache-dir",
                str(cache_dir),
            ]
            run_cmd(cmd, label="build_unlock_events_dart")
        elif choice == "5":
            unlock_csv = _pick_existing(
                "unlock_events_dart.csv 경로를 입력하세요: ",
                [UNLOCK_DIR / "unlock_events_dart.csv"],
            )
            master_csv = _pick_existing(
                "filtered_master.csv 경로를 입력하세요: ",
                [DATASET_DIR / "filtered_master.csv"],
            )
            out_csv = UNLOCK_DIR / "unlock_events_backtest_input.csv"
            cmd = [
                PY,
                str(PROGRAM_UNLOCK_CONVERT),
                "--unlock-csv",
                str(unlock_csv),
                "--master-csv",
                str(master_csv),
                "--out-csv",
                str(out_csv),
            ]
            run_cmd(cmd, label="unlock_to_backtest_input")
        elif choice == "6":
            unlock_csv = _pick_existing(
                "unlock_events_backtest_input.csv 경로를 입력하세요: ",
                [UNLOCK_DIR / "unlock_events_backtest_input.csv"],
            )
            interval_min = input("봉간격 분 (기본 5) > ").strip() or "5"
            pre_days = input("이벤트 전 캘린더일 (기본 2) > ").strip() or "2"
            post_days = input("이벤트 후 캘린더일 (기본 5) > ").strip() or "5"
            run_cmd([PY, str(PROGRAM_MINUTE), "--db-path", str(MINUTE_DB), "init-db"], label="minute_init_db")
            cmd = [
                PY,
                str(PROGRAM_MINUTE),
                "--db-path",
                str(MINUTE_DB),
                "enqueue-from-unlock",
                "--unlock-csv",
                str(unlock_csv),
                "--interval-min",
                str(interval_min),
                "--pre-days",
                str(pre_days),
                "--post-days",
                str(post_days),
            ]
            run_cmd(cmd, label="minute_enqueue_from_unlock")
            run_cmd([PY, str(PROGRAM_MINUTE), "--db-path", str(MINUTE_DB), "show-queue"], label="minute_show_queue")
        elif choice == "7":
            pattern = input("minute CSV 파일 경로 또는 glob 패턴을 입력하세요 > ").strip().strip('"')
            interval_min = input("봉간격 분 (기본 5) > ").strip() or "5"
            if any(ch in pattern for ch in ["*", "?", "["]):
                cmd = [
                    PY,
                    str(PROGRAM_MINUTE),
                    "--db-path",
                    str(MINUTE_DB),
                    "import-minute-glob",
                    "--glob",
                    pattern,
                    "--interval-min",
                    str(interval_min),
                ]
            else:
                symbol = input("종목코드 override (없으면 엔터) > ").strip()
                cmd = [
                    PY,
                    str(PROGRAM_MINUTE),
                    "--db-path",
                    str(MINUTE_DB),
                    "import-minute-csv",
                    "--csv-path",
                    pattern,
                    "--interval-min",
                    str(interval_min),
                ]
                if symbol:
                    cmd += ["--symbol", symbol]
            run_cmd(cmd, label="minute_import")
            run_cmd([PY, str(PROGRAM_MINUTE), "--db-path", str(MINUTE_DB), "show-queue"], label="minute_show_queue")
        elif choice == "8":
            unlock_csv = _pick_existing(
                "unlock_events_backtest_input.csv 경로를 입력하세요: ",
                [UNLOCK_DIR / "unlock_events_backtest_input.csv"],
            )
            interval_min = input("봉간격 분 (기본 5) > ").strip() or "5"
            multiples = input("turnover multiple들 (기본 1,2) > ").strip() or "1,2"
            price_filter = input(
                "price filter (none / reclaim_open / reclaim_vwap / reclaim_open_or_vwap / range_top40 / open_and_vwap) > "
            ).strip() or "reclaim_open_or_vwap"
            max_days_after = input("unlock 이후 최대 캘린더일 (기본 5) > ").strip() or "5"
            aggregate_by = input("aggregate by (type / term / day / none, 기본 type) > ").strip() or "type"
            cum_scope = input("cum scope (through_window / same_day, 기본 through_window) > ").strip() or "through_window"
            out_csv = SIGNAL_DIR / "turnover_signals.csv"
            miss_csv = SIGNAL_DIR / "turnover_signals_misses.csv"
            cmd = [
                PY,
                str(PROGRAM_SIGNAL),
                "--unlock-csv",
                str(unlock_csv),
                "--db-path",
                str(MINUTE_DB),
                "--out-csv",
                str(out_csv),
                "--miss-csv",
                str(miss_csv),
                "--interval-min",
                str(interval_min),
                "--multiples",
                multiples,
                "--price-filter",
                price_filter,
                "--max-days-after",
                str(max_days_after),
                "--aggregate-by",
                aggregate_by,
                "--cum-scope",
                cum_scope,
            ]
            run_cmd(cmd, label="turnover_signal")
        elif choice == "9":
            key_file = _pick_key_file()
            signals_csv = _pick_existing(
                "turnover_signals.csv 경로를 입력하세요: ",
                [SIGNAL_DIR / "turnover_signals.csv"],
            )
            cmd = [
                PY,
                str(PROGRAM_TURNOVER_BT),
                "--signals-csv",
                str(signals_csv),
                "--config",
                str(CONFIG_TURNOVER),
                "--key-file",
                str(key_file),
                "--cache-dir",
                str(CACHE_DIR),
                "--out-dir",
                str(TURNOVER_BT_DIR),
            ]
            run_cmd(cmd, label="turnover_daily_backtest")
        elif choice == "10":
            trades_csv = _pick_existing(
                "all_trades.csv 경로를 입력하세요: ",
                [TURNOVER_BT_DIR / "all_trades.csv", BACKTEST_DIR / "all_trades.csv"],
            )
            bench_csv = _pick_existing(
                "benchmark csv 경로를 입력하세요: ",
                [ROOT / "benchmark_daily.csv"],
                allow_empty=True,
            )
            out_csv = ANALYSIS_DIR / "trade_window_beta_summary.csv"
            cmd = [PY, str(PROGRAM_BETA), "--trades-csv", str(trades_csv), "--out-csv", str(out_csv)]
            if bench_csv is not None:
                cmd += ["--benchmark-csv", str(bench_csv)]
            run_cmd(cmd, label="trade_window_beta")
        elif choice == "11":
            open_path(ROOT_DATA)
        elif choice == "12":
            open_path(CONFIG_DAILY)
            open_path(CONFIG_TURNOVER)
        elif choice == "0":
            break
        else:
            print("지원하지 않는 메뉴입니다.")


if __name__ == "__main__":
    menu()
