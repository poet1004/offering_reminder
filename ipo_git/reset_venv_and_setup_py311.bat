@echo off
setlocal
chcp 65001 >nul
cd /d "%~dp0"
if exist ".venv" (
  echo [INFO] Removing existing .venv ...
  rmdir /s /q .venv
)
call setup_py311.bat
