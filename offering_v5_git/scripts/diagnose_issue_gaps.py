from __future__ import annotations

import json
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.services.dart_client import DartClient
from src.services.ipo_pipeline import IPODataHub
from src.services.kis_client import KISClient
from src.utils import runtime_dir


def main() -> None:
    data_dir = ROOT / "data"
    hub = IPODataHub(data_dir, kis_client=KISClient.from_env(), dart_client=DartClient.from_env())
    bundle = hub.load_bundle(prefer_live=False, use_cache=True, allow_sample_fallback=False)
    issues = bundle.issues.copy()
    if issues.empty:
        print(json.dumps({"ok": False, "detail": "issues empty"}, ensure_ascii=False, indent=2))
        return

    focus_cols = [
        "name", "market", "stage", "listing_date", "subscription_start", "subscription_end",
        "symbol", "underwriters", "institutional_competition_ratio", "offer_price",
        "total_offer_shares", "secondary_sale_ratio", "post_listing_total_shares",
        "dart_receipt_no", "source", "source_detail",
    ]
    display = issues[[c for c in focus_cols if c in issues.columns]].copy()
    display["missing_symbol"] = display.get("symbol").isna() if "symbol" in display.columns else True
    display["missing_offer_structure"] = display[[c for c in ["total_offer_shares", "secondary_sale_ratio", "post_listing_total_shares"] if c in display.columns]].isna().all(axis=1)
    display["has_dart_overlay"] = display.get("dart_receipt_no").notna() if "dart_receipt_no" in display.columns else False
    display["likely_pre_listing"] = pd.to_datetime(display.get("listing_date"), errors="coerce").ge(pd.Timestamp.today().normalize()) if "listing_date" in display.columns else False

    out_dir = runtime_dir()
    csv_path = out_dir / "issue_gap_diagnostic.csv"
    json_path = out_dir / "issue_gap_diagnostic_summary.json"
    display.to_csv(csv_path, index=False, encoding="utf-8-sig")
    summary = {
        "ok": True,
        "rows": int(len(display)),
        "missing_symbol_rows": int(display["missing_symbol"].sum()),
        "missing_offer_structure_rows": int(display["missing_offer_structure"].sum()),
        "has_dart_overlay_rows": int(display["has_dart_overlay"].sum()),
        "csv_path": str(csv_path),
    }
    json_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
