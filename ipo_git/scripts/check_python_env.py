#!/usr/bin/env python3
from __future__ import annotations
import platform
import struct
import sys


def main() -> int:
    bits = struct.calcsize("P") * 8
    print(f"python_executable={sys.executable}")
    print(f"python_version={sys.version.split()[0]}")
    print(f"python_implementation={platform.python_implementation()}")
    print(f"platform={platform.platform()}")
    print(f"machine={platform.machine()}")
    print(f"bits={bits}")
    try:
        import pip  # type: ignore
        print(f"pip_version={pip.__version__}")
    except Exception:
        print("pip_version=<unavailable>")

    if sys.version_info[:2] != (3, 11):
        print("status=BAD_VERSION")
        print("hint=Install or select Python 3.11.x, then create .venv with py -3.11 -m venv .venv.")
        return 1
    if bits != 64:
        print("status=BAD_ARCH")
        print("hint=Use Python 3.11 64-bit on Windows and recreate .venv.")
        return 2
    print("status=OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
