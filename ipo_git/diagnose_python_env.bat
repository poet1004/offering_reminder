@echo off
setlocal
cd /d "%~dp0"

echo ===== where python =====
where python

echo.
echo ===== where py =====
where py

echo.
echo ===== py list =====
py list

echo.
echo ===== default python info =====
python -c "import sys, platform, struct; print('Version:', sys.version); print('Executable:', sys.executable); print('Bits:', struct.calcsize('P')*8); print('Machine:', platform.machine()); print('Platform:', platform.platform())"

echo.
echo ===== python 3.11 check =====
py -3.11 -c "import sys, platform, struct; print('Version:', sys.version); print('Executable:', sys.executable); print('Bits:', struct.calcsize('P')*8); print('Machine:', platform.machine()); print('Platform:', platform.platform())"

echo.
echo ===== pip info for python 3.11 =====
py -3.11 -m pip --version

echo.
if exist ".venv\Scripts\python.exe" (
  echo ===== venv python info =====
  .venv\Scripts\python.exe -c "import sys, platform, struct; print('Version:', sys.version); print('Executable:', sys.executable); print('Bits:', struct.calcsize('P')*8); print('Machine:', platform.machine()); print('Platform:', platform.platform())"
  echo.
  echo ===== venv pip info =====
  .venv\Scripts\python.exe -m pip --version
)

pause
