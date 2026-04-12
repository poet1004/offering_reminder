@echo off
setlocal
cd /d "%~dp0"
python scripts\refresh_export_and_build_pages.py
