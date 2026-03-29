@echo off
setlocal
cd /d %~dp0
set "PY=python"
if exist ..\..\.venv\Scripts\python.exe set "PY=..\..\.venv\Scripts\python.exe"
%PY% run_lockup_lab_wizard.py
pause
