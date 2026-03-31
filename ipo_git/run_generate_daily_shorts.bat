@echo off
setlocal
cd /d "%~dp0"
python scripts\generate_daily_shorts.py --source-mode "캐시 우선" --allow-packaged-sample %*
endlocal
