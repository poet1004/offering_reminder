from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd

from src.utils import cache_dir, ensure_dir, parse_date_columns, today_kst


class LiveCacheStore:
    def __init__(self, base_dir: Path | None = None) -> None:
        self.base_dir = ensure_dir(base_dir or cache_dir())

    def frame_path(self, name: str) -> Path:
        return self.base_dir / f"{name}.csv"

    def meta_path(self, name: str) -> Path:
        return self.base_dir / f"{name}.meta.json"

    def write_frame(self, name: str, df: pd.DataFrame, meta: dict[str, Any] | None = None) -> Path:
        path = self.frame_path(name)
        df.to_csv(path, index=False, encoding="utf-8-sig")
        full_meta = {
            "name": name,
            "row_count": int(len(df)),
            "saved_at": today_kst().isoformat(),
        }
        if meta:
            full_meta.update(meta)
        self.meta_path(name).write_text(json.dumps(full_meta, ensure_ascii=False, indent=2), encoding="utf-8")
        return path

    def read_frame(self, name: str, parse_dates: bool = True) -> pd.DataFrame:
        path = self.frame_path(name)
        if not path.exists():
            return pd.DataFrame()
        df = pd.read_csv(path)
        if parse_dates:
            df = parse_date_columns(df)
        return df

    def read_meta(self, name: str) -> dict[str, Any]:
        path = self.meta_path(name)
        if not path.exists():
            return {}
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            return {}

    def list_inventory(self) -> pd.DataFrame:
        rows: list[dict[str, Any]] = []
        for path in sorted(self.base_dir.glob("*.csv")):
            name = path.stem
            meta = self.read_meta(name)
            rows.append(
                {
                    "name": name,
                    "file": str(path),
                    "rows": meta.get("row_count"),
                    "saved_at": meta.get("saved_at"),
                    "source": meta.get("source"),
                    "notes": meta.get("notes"),
                }
            )
        return pd.DataFrame(rows)
