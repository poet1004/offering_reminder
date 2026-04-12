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
%PY% scripts\generate_daily_shorts.py --source-mode "캐시 우선" --allow-packaged-sample %*
if errorlevel 1 (
  echo.
  echo [ERROR] generate_daily_shorts failed.
  pause
  exit /b 1
)
endlocal
