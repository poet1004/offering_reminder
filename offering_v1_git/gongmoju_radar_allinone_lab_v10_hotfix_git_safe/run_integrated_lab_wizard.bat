@echo off
chcp 65001 >nul
set PYTHONUTF8=1
set PYTHONIOENCODING=utf-8
setlocal
set "ROOT=%~dp0"
cd /d "%ROOT%"
set "PY=python"
if exist "%ROOT%\.venv\Scripts\python.exe" set "PY=%ROOT%\.venv\Scripts\python.exe"
set "LOG=%ROOT%data\runtime\integrated_lab_wizard_latest.log"
if not exist "%ROOT%data\runtime" mkdir "%ROOT%data\runtime" >nul 2>nul

echo [1/4] Sync .env to lab key files...
"%PY%" "%ROOT%scripts\sync_env_to_lab_keys.py" >> "%LOG%" 2>&1 || goto :error

echo [2/4] Prepare integrated lab workspace...
"%PY%" "%ROOT%scripts\prepare_integrated_lab_workspace.py" >> "%LOG%" 2>&1 || goto :error

echo [3/4] Export IPO seed master for lab (non-fatal)...
"%PY%" "%ROOT%scripts\export_ipo_seed_to_lab.py" --lab-root "%ROOT%integrated_lab\ipo_lockup_unified_lab" >> "%LOG%" 2>&1

echo [4/4] Launch integrated lab wizard...
echo Pre-step log: "%LOG%"
pushd "%ROOT%integrated_lab\ipo_lockup_unified_lab"
"%PY%" run_lockup_lab_wizard.py
set "RC=%ERRORLEVEL%"
popd
if not "%RC%"=="0" goto :wizard_error

echo.
echo Integrated lab wizard exited normally.
echo Pre-step log: "%LOG%"
pause
exit /b 0

:error
echo.
echo Failed before launching integrated lab wizard.
echo Log file: "%LOG%"
type "%LOG%"
pause
exit /b 1

:wizard_error
echo.
echo Integrated lab wizard exited with code %RC%.
echo Check workspace\logs and this pre-step log if needed: "%LOG%"
pause
exit /b %RC%
