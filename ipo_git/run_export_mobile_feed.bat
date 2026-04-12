@echo off
setlocal
chcp 65001 >nul
cd /d "%~dp0"
if not exist ".venv\Scripts\python.exe" (
  echo [ERROR] Missing virtual environment: .venv\Scripts\python.exe
  echo Run setup_py311.bat first.
  pause
  exit /b 1
)
set "PY=.venv\Scripts\python.exe"
%PY% scripts\export_mobile_feed.py --repo . --output data/mobile/mobile-feed.json || goto :fail
%PY% scripts\export_mobile_feed.py --repo . --site-dir data/mobile/site --site-base-url https://example.invalid/mobile || goto :fail
%PY% scripts\export_mobile_feed.py --repo . --site-dir mobile-feed --site-base-url https://cdn.jsdelivr.net/gh/poet1004/offering_reminder@main/mobile-feed || goto :fail
echo.
echo [OK] mobile feed export completed.
pause
exit /b 0
:fail
echo.
echo [ERROR] export_mobile_feed failed.
pause
exit /b 1
