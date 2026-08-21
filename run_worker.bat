@echo off
REM ============================================================
REM  Arb Scanner - Home Feed Worker (Windows)
REM  Double-click this file. First run asks two questions
REM  (your app URL and scan key), then it runs forever,
REM  pushing fresh sportsbook odds to your Render app.
REM  Keep this window open while you want live DK/MGM feeds.
REM ============================================================
cd /d "%~dp0"

where py >nul 2>nul
if errorlevel 1 (
    where python >nul 2>nul
    if errorlevel 1 (
        echo.
        echo Python is not installed on this computer.
        echo.
        echo Opening the download page now. In the installer, CHECK THE BOX
        echo "Add python.exe to PATH", finish the install, then double-click
        echo this file again.
        echo.
        start https://www.python.org/downloads/
        pause
        exit /b 1
    )
    set PYCMD=python
) else (
    set PYCMD=py
)

echo Installing/checking the one required package (requests)...
%PYCMD% -m pip install --quiet --disable-pip-version-warning requests

echo.
echo Starting the feed worker. Leave this window open.
echo Press Ctrl+C or close the window to stop.
echo.
%PYCMD% fetch_worker.py
pause
