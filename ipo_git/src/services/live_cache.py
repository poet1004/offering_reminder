from __future__ import annotations

import shutil
import json
from pathlib import Path
from typing import Any

import pandas as pd

from src.utils import cache_dir, ensure_dir, parse_date_columns, today_kst


class LiveCacheStore:
    def __init__(self, base_dir: Path | None = None) -> None:
        self.base_dir = ensure_dir(base_dir or cache_dir())
        self.bootstrap_dir = self.base_dir.parent / "bootstrap_cache"

    def frame_path(self, name: str) -> Path:
        return self.base_dir / f"{name}.csv"

    def meta_path(self, name: str) -> Path:
        return self.base_dir / f"{name}.meta.json"

    def bootstrap_frame_path(self, name: str) -> Path:
        return self.bootstrap_dir / f"{name}.csv"

    def bootstrap_meta_path(self, name: str) -> Path:
        return self.bootstrap_dir / f"{name}.meta.json"

    def _seed_from_bootstrap(self, name: str) -> None:
        frame_path = self.frame_path(name)
        bootstrap_frame = self.bootstrap_frame_path(name)
        if frame_path.exists() or not bootstrap_frame.exists():
            return
        ensure_dir(frame_path.parent)
        shutil.copy2(bootstrap_frame, frame_path)
        bootstrap_meta = self.bootstrap_meta_path(name)
        if bootstrap_meta.exists() and not self.meta_path(name).exists():
            shutil.copy2(bootstrap_meta, self.meta_path(name))

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
        self._seed_from_bootstrap(name)
        path = self.frame_path(name)
        if not path.exists():
            bootstrap = self.bootstrap_frame_path(name)
            path = bootstrap if bootstrap.exists() else path
        if not path.exists():
            return pd.DataFrame()
        df = pd.read_csv(path)
        if parse_dates:
            df = parse_date_columns(df)
        return df

    def read_meta(self, name: str) -> dict[str, Any]:
        self._seed_from_bootstrap(name)
        path = self.meta_path(name)
        if not path.exists():
            bootstrap = self.bootstrap_meta_path(name)
            path = bootstrap if bootstrap.exists() else path
        if not path.exists():
            return {}
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            return {}

    def list_inventory(self) -> pd.DataFrame:
        rows: list[dict[str, Any]] = []
        csv_paths = {path.name: path for path in self.base_dir.glob("*.csv")}
        if self.bootstrap_dir.exists():
            for path in self.bootstrap_dir.glob("*.csv"):
                csv_paths.setdefault(path.name, path)
        for path in sorted(csv_paths.values()):
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
