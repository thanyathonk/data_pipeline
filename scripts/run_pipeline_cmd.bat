@echo off
setlocal enableextensions enabledelayedexpansion

rem Change working dir to repo root (this script lives under scripts\)
cd /d "%~dp0.."

echo [INFO] Repository: %cd%

rem ---- Locate conda ----
set "CONDA_BAT="
for /f "delims=" %%i in ('where conda 2^>nul') do (
  set "CONDA_BAT=%%i"
  goto :found
)
echo [ERROR] conda not found in PATH. Please open Anaconda Prompt and run again.
exit /b 1

:found
echo [INFO] Using conda: %CONDA_BAT%

rem ---- Activate env ----
call "%CONDA_BAT%" activate pipeline || (
  echo [ERROR] Failed to activate env 'pipeline'
  exit /b 1
)

rem ---- Pre-run cleanup to avoid disk full ----
echo [CLEAN] Removing large temporary indexes under data\split\ ...
if exist data\split\adr_sid_index.sqlite del /f /q data\split\adr_sid_index.sqlite
if exist data\split\drug_sid_index.sqlite del /f /q data\split\drug_sid_index.sqlite

echo [CLEAN] Tidying logs/steps (safe to remove) ...
if exist data\logs\steps rmdir /s /q data\logs\steps
if not exist data\logs\steps mkdir data\logs\steps

echo [CLEAN] Removing cache folders if present ...
if exist data\.cache rmdir /s /q data\.cache

rem Optional: clear old logs older than 7 days (ignore errors if forfiles not available)
forfiles /p data\logs /s /m *.* /d -7 /c "cmd /c del /q @path" 2>nul

rem ---- Ensure requirements (best-effort) ----
python -m pip install -r requirements.txt || echo [WARN] pip install skipped/failed; relying on preinstalled packages

rem ---- Run pipeline (all stages) ----
echo [RUN] Starting pipeline ...
python run_pipeline.py --no-confirm -y --qps 4 --max-workers 8 --force
set "EXITCODE=%ERRORLEVEL%"

echo [DONE] Pipeline finished with exit code %EXITCODE%
exit /b %EXITCODE%
