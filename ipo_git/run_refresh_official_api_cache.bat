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
%PY% scripts\refresh_official_api_cache.py 
if errorlevel 1 (
  echo.
  echo [ERROR] Command failed.
  pause
  exit /b 1
)
pause
