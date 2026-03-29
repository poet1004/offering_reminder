@echo off
cd /d %~dp0
python scripts\diagnose_live_sources.py
pause
