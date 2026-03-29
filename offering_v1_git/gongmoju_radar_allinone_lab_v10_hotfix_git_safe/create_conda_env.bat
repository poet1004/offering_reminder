@echo off
conda env create -f environment.yml
call conda activate gongmoju-radar
python scripts\preflight_check.py
