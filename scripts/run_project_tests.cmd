@echo off
setlocal
cd /d "%~dp0.."
python scripts\run_project_tests.py %*
