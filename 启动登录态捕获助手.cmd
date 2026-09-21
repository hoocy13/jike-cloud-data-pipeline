@echo off
chcp 65001 >nul
setlocal
cd /d "%~dp0"
py -3 scripts\auth_capture_assistant.py %*
set "CAPTURE_EXIT=%ERRORLEVEL%"

if not "%~1"=="" exit /b %CAPTURE_EXIT%

echo.
if not "%CAPTURE_EXIT%"=="0" echo [ERROR] Capture assistant exited with code %CAPTURE_EXIT%.
echo Press any key to close this window.
pause >nul
exit /b %CAPTURE_EXIT%
