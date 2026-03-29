@echo off
setlocal
cd /d "%~dp0"
if not exist .venv (
  python -m venv .venv || goto :error
)
call .venv\Scripts\activate.bat || goto :error
python -m pip install --upgrade pip setuptools wheel || goto :error
python -m pip install -r requirements.txt || goto :error
python scripts\preflight_check.py --skip-smoke

echo.
echo Clean venv ready.
echo Run: .venv\Scripts\activate.bat ^&^& streamlit run app.py
exit /b 0

:error
echo Failed to build clean venv.
exit /b 1
