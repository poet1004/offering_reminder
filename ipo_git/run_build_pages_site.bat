@echo off
setlocal
cd /d "%~dp0"
python scripts\build_pages_site.py --repo . --output _site
if errorlevel 1 goto :eof
echo.
echo Built _site
