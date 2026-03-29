from __future__ import annotations

import importlib
import json
import platform
import sys
from pathlib import Path


def module_version(name: str) -> str:
    try:
        module = importlib.import_module(name)
    except Exception as exc:  # noqa: BLE001
        return f"import-failed: {type(exc).__name__}: {exc}"
    return getattr(module, "__version__", "unknown")


def main() -> int:
    info = {
        "python": sys.version.replace("\n", " "),
        "executable": sys.executable,
        "machine": platform.machine(),
        "architecture": platform.architecture()[0],
        "maxsize": sys.maxsize,
        "packages": {
            name: module_version(name)
            for name in ["streamlit", "pandas", "numpy", "requests", "lxml", "openpyxl", "mojito"]
        },
    }
    out = Path(__file__).resolve().parents[1] / "data" / "runtime" / "python_env_diagnostic.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(info, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(info, ensure_ascii=False, indent=2))
    print(f"saved: {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
