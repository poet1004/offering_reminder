@echo off
setlocal
cd /d "%~dp0"
python scripts\refresh_live_cache.py
if errorlevel 1 (
  echo.
  echo [ERROR] refresh_live_cache failed.
  pause
  exit /b 1
)
echo.
echo [OK] live cache refresh completed.
pause
