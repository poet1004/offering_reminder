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
%PY% -c "import streamlit" >nul 2>nul
if errorlevel 1 (
  echo [ERROR] streamlit is not installed in .venv.
  echo Run setup_py311.bat first.
  pause
  exit /b 1
)
%PY% -m streamlit run app.py
pause
