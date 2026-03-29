from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.services.ipo_pipeline import IPODataHub
from src.services.ipo_scrapers import fetch_38_schedule, standardize_38_schedule_table
from src.utils import load_project_env, normalize_name_key

DEFAULT_LAB_ROOT = ROOT / "integrated_lab" / "ipo_lockup_unified_lab"


def _first_notna_series(frame: pd.DataFrame, columns: list[str]) -> pd.Series:
    out = pd.Series([pd.NA] * len(frame), index=frame.index, dtype="object")
    for col in columns:
        if col not in frame.columns:
            continue
        series = frame[col]
        if not isinstance(series, pd.Series):
            series = pd.Series(series, index=frame.index)
        mask = out.isna() & series.notna()
        out.loc[mask] = series.loc[mask]
    return out


def build_seed_from_issues(issues: pd.DataFrame) -> pd.DataFrame:
    if issues is None or issues.empty:
        return pd.DataFrame(columns=["name", "name_key", "symbol", "listing_date", "ipo_price", "market", "lead_manager", "listed_shares"])
    work = issues.copy()
    work["ipo_price"] = pd.to_numeric(
        _first_notna_series(work, ["offer_price", "price_band_high", "price_band_low"])
        .astype(str)
        .str.replace(",", "", regex=False)
        .str.extract(r"([0-9]+(?:\.[0-9]+)?)", expand=False),
        errors="coerce",
    )
    work["lead_manager"] = _first_notna_series(work, ["underwriters"])
    work["listed_shares"] = pd.to_numeric(
        _first_notna_series(
            work,
            [
                "post_listing_total_shares",
                "circulating_shares_on_listing",
                "total_offer_shares",
            ],
        )
        .astype(str)
        .str.replace(",", "", regex=False)
        .str.extract(r"([0-9]+(?:\.[0-9]+)?)", expand=False),
        errors="coerce",
    )
    work["symbol"] = (
        work.get("symbol", pd.Series(dtype="object"))
        .astype(str)
        .str.extract(r"(\d{6})", expand=False)
    )
    work["listing_date"] = pd.to_datetime(work.get("listing_date"), errors="coerce")
    work["name"] = work.get("name", pd.Series(dtype="object")).astype(str).str.strip()
    work["name_key"] = work.get("name_key")
    work["name_key"] = work["name_key"].fillna(work["name"]).map(normalize_name_key)
    work["market"] = work.get("market", pd.Series(dtype="object")).astype("object")

    out = work[["name", "name_key", "symbol", "listing_date", "ipo_price", "market", "lead_manager", "listed_shares"]].copy()
    out = out[out["name"].notna() & (out["name"].astype(str) != "")].copy()
    out = out[out["listing_date"].notna()].copy()
    out = out.sort_values(["listing_date", "name_key"], na_position="last").drop_duplicates(subset=["name_key"], keep="last")
    out = out.reset_index(drop=True)
    return out


def _direct_38_fallback_issues() -> pd.DataFrame:
    try:
        raw = fetch_38_schedule(include_detail_links=True)
        return standardize_38_schedule_table(raw, fetch_details=True)
    except Exception:
        return pd.DataFrame()


def main() -> int:
    parser = argparse.ArgumentParser(description="앱의 live/cache IPO bundle을 기반으로 integrated lab용 seed master(kind_master.csv)를 생성합니다.")
    parser.add_argument("--data-dir", default=str(ROOT / "data"))
    parser.add_argument("--lab-root", default=str(DEFAULT_LAB_ROOT))
    parser.add_argument("--allow-sample-fallback", action="store_true")
    parser.add_argument("--skip-refresh", action="store_true")
    args = parser.parse_args()

    load_project_env()
    data_dir = Path(args.data_dir).expanduser().resolve()
    lab_root = Path(args.lab_root).expanduser().resolve()
    lab_root.mkdir(parents=True, exist_ok=True)
    workspace = lab_root / "workspace"
    workspace.mkdir(parents=True, exist_ok=True)
    (workspace / "dataset_out").mkdir(parents=True, exist_ok=True)

    report: dict[str, Any] = {
        "ok": False,
        "data_dir": str(data_dir),
        "lab_root": str(lab_root),
        "seed_csv": str(lab_root / "kind_master.csv"),
        "issues_rows": 0,
        "seed_rows": 0,
        "used_sample": False,
        "refresh_report": None,
        "notes": [],
    }

    hub = IPODataHub(data_dir)
    if not args.skip_refresh:
        try:
            report["refresh_report"] = hub.refresh_live_cache(fetch_kind=True, fetch_38=True)
        except Exception as exc:
            report["notes"].append(f"refresh_live_cache failed: {exc}")

    bundle = hub.load_bundle(
        prefer_live=False,
        use_cache=True,
        allow_sample_fallback=False,
        allow_packaged_sample_paths=False,
    )
    issues = bundle.issues.copy()
    if issues.empty:
        report["notes"].append("cached IPO bundle was empty; trying direct live bundle")
        try:
            bundle_live = hub.load_bundle(
                prefer_live=True,
                use_cache=True,
                allow_sample_fallback=False,
                allow_packaged_sample_paths=False,
            )
            issues = bundle_live.issues.copy()
        except Exception as exc:
            report["notes"].append(f"direct live bundle failed: {exc}")

    if issues.empty:
        report["notes"].append("hub bundle still empty; trying direct 38 fallback")
        issues = _direct_38_fallback_issues().copy()

    if issues.empty and args.allow_sample_fallback:
        bundle = hub.load_bundle(
            prefer_live=False,
            use_cache=True,
            allow_sample_fallback=True,
            allow_packaged_sample_paths=False,
        )
        issues = bundle.issues.copy()
        report["used_sample"] = not issues.empty

    report["issues_rows"] = int(len(issues))
    seed = build_seed_from_issues(issues)
    report["seed_rows"] = int(len(seed))

    if not seed.empty:
        seed_path = lab_root / "kind_master.csv"
        seed.to_csv(seed_path, index=False, encoding="utf-8-sig")
        seed.to_csv(workspace / "dataset_out" / "live_issue_seed.csv", index=False, encoding="utf-8-sig")
        report["ok"] = True
    else:
        report["notes"].append("seed rows == 0; live/cache IPO issue bundle was empty or lacked listing_date")

    report_path = workspace / "seed_export_report.json"
    report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
