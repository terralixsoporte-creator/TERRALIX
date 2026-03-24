@echo off
setlocal
set "SCRIPT_DIR=%~dp0"
pushd "%SCRIPT_DIR%\.."
if not exist "terr\Scripts\python.exe" (
  echo [ERROR] Virtualenv not found: terr\Scripts\python.exe
  echo Create it with: python -m venv terr
  pause
  exit /b 1
)
set "PY=%CD%\terr\Scripts\python.exe"
echo Using Python: %PY%
"%PY%" TERRALIX.py
set "EXITCODE=%ERRORLEVEL%"
echo Exit code: %EXITCODE%
popd
exit /b %EXITCODE%

