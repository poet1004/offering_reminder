@echo off
setlocal
chcp 65001 >nul
cd /d "%~dp0"

where py >nul 2>nul
if errorlevel 1 (
  echo [ERROR] Could not find the Windows Python launcher: py
  echo Install the official Python Install Manager or Python 3.11 x64 from python.org.
  pause
  exit /b 1
)

py -3.11 -c "import sys; sys.exit(0 if sys.version_info[:2]==(3,11) else 1)" >nul 2>nul
if errorlevel 1 (
  echo [INFO] Python 3.11 was not found. Trying: py install 3.11
  py install 3.11
  py -3.11 -c "import sys; sys.exit(0 if sys.version_info[:2]==(3,11) else 1)" >nul 2>nul
  if errorlevel 1 (
    echo [ERROR] Python 3.11 is still not available.
    echo Install Python 3.11 x64, then run setup_py311.bat again.
    echo After installation, this command must work:
    echo   py -3.11 -c "import sys; print(sys.version)"
    pause
    exit /b 1
  )
)

if exist ".venv\Scripts\python.exe" (
  .venv\Scripts\python.exe scripts\check_python_env.py >nul 2>nul
  if errorlevel 1 (
    echo [INFO] Existing .venv is not Python 3.11 x64. Recreating .venv...
    rmdir /s /q .venv
  )
)

if not exist ".venv\Scripts\python.exe" (
  py -3.11 -m venv .venv
  if errorlevel 1 (
    echo [ERROR] Failed to create the virtual environment.
    echo Try this first:
    echo   py -0p
    echo   py -3.11 -c "import sys; print(sys.version)"
    pause
    exit /b 1
  )
)

set "PY=.venv\Scripts\python.exe"
%PY% scripts\check_python_env.py
if errorlevel 1 (
  echo.
  echo [ERROR] .venv is not using Python 3.11 x64.
  echo Remove .venv and rerun setup_py311.bat after Python 3.11 is installed.
  echo Helpful checks:
  echo   py -0p
  echo   .venv\Scripts\python.exe scripts\check_python_env.py
  pause
  exit /b 1
)

%PY% -m pip install --upgrade pip setuptools wheel
if errorlevel 1 (
  echo [ERROR] Failed to upgrade pip/setuptools/wheel.
  pause
  exit /b 1
)

%PY% -m pip install --only-binary=:all: numpy==1.26.4 pandas==2.2.3
if errorlevel 1 (
  echo.
  echo [ERROR] Failed to install prebuilt numpy/pandas wheels.
  echo This usually means pip is not using Python 3.11 x64.
  echo Run: .venv\Scripts\python.exe scripts\check_python_env.py
  pause
  exit /b 1
)

%PY% -m pip install --prefer-binary -r requirements.txt
if errorlevel 1 (
  echo [ERROR] requirements.txt installation failed.
  echo Run: .venv\Scripts\python.exe scripts\check_python_env.py
  pause
  exit /b 1
)

echo.
echo [INFO] Base web app dependencies are installed in .venv.
echo [INFO] Optional KIS helpers: .venv\Scripts\python.exe -m pip install -r requirements-optional.txt
echo [INFO] Run the app with: run_app.bat
pause
