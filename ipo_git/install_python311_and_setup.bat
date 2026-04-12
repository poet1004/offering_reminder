@echo off
setlocal
chcp 65001 >nul
cd /d "%~dp0"
where py >nul 2>nul
if errorlevel 1 (
  echo [ERROR] Could not find the Windows Python launcher: py
  echo Install Python from python.org first, then run setup_py311.bat.
  pause
  exit /b 1
)

py -3.11 -c "import sys; print(sys.version)" >nul 2>nul
if errorlevel 1 (
  echo [INFO] Python 3.11 not found via py launcher. Trying: py install 3.11
  py install 3.11
)

py -3.11 -c "import sys; print(sys.version)" >nul 2>nul
if errorlevel 1 (
  where winget >nul 2>nul
  if not errorlevel 1 (
    echo [INFO] Trying Windows package manager: winget install Python 3.11
    winget install -e --id Python.Python.3.11 --accept-source-agreements --accept-package-agreements
  )
)

call setup_py311.bat
