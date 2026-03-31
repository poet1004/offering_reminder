from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pandas as pd

from src.services.ipo_pipeline import IPODataHub
from src.services.ipo_repository import IPORepository
from src.services.unified_lab_bridge import UnifiedLabBridgeService


def summarize_missing(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    rows = []
    for col in cols:
        if col not in df.columns:
            rows.append({"column": col, "missing": len(df), "filled": 0})
            continue
        series = df[col]
        missing = int(pd.isna(series).sum())
        rows.append({"column": col, "missing": missing, "filled": int(len(df) - missing)})
    return pd.DataFrame(rows)


def main() -> None:
    data_dir = ROOT / "data"
    repo = IPORepository(data_dir)
    unified = UnifiedLabBridgeService(data_dir)
    workspace = unified.auto_detect_workspace(allow_packaged_sample=False)
    external = repo.auto_detect_external_unlock_dataset(allow_packaged_sample=False)

    print("== Auto-detected paths ==")
    print(f"workspace: {workspace}")
    print(f"external unlock csv: {external}")
    print()

    hub = IPODataHub(data_dir)
    bundle = hub.load_bundle(
        prefer_live=True,
        use_cache=True,
        external_unlock_path=external,
        allow_sample_fallback=False,
        allow_packaged_sample_paths=False,
    )
    issues = bundle.issues.copy()
    unlocks = bundle.all_unlocks.copy()

    print("== Source status ==")
    if bundle.source_status.empty:
        print("(empty)")
    else:
        print(bundle.source_status.to_string(index=False))
    print()

    print("== Issue summary ==")
    print(f"rows: {len(issues)}")
    if not issues.empty:
        print(summarize_missing(issues, [
            "market",
            "symbol",
            "sector",
            "subscription_start",
            "subscription_end",
            "listing_date",
            "institutional_competition_ratio",
            "current_price",
        ]).to_string(index=False))
        print()
        preview_cols = [c for c in [
            "name",
            "market",
            "symbol",
            "sector",
            "subscription_start",
            "subscription_end",
            "listing_date",
            "underwriters",
            "institutional_competition_ratio",
            "current_price",
            "source",
            "source_detail",
        ] if c in issues.columns]
        print(issues[preview_cols].head(20).to_string(index=False))
    print()

    print("== Unlock summary ==")
    print(f"rows: {len(unlocks)}")
    if not unlocks.empty:
        preview_cols = [c for c in ["name", "symbol", "unlock_date", "term", "source"] if c in unlocks.columns]
        print(unlocks[preview_cols].head(20).to_string(index=False))


if __name__ == "__main__":
    main()
