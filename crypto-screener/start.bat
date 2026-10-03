@echo off
setlocal EnableExtensions
REM =====================================================================
REM  Crypto Screener launcher (ASCII-only on purpose).
REM
REM  This file MUST stay pure ASCII: cmd.exe reads .bat files using the
REM  active codepage, not UTF-8, so any Cyrillic here turns into mojibake
REM  and can break parsing on machines with a non-UTF-8 codepage.
REM  The Russian menu lives in start.ps1 (saved as UTF-8 with BOM).
REM  tests/test_encodings.py enforces both rules.
REM =====================================================================
cd /d "%~dp0"
title Crypto Screener

REM ---------- find Python ----------
set "PY="
python --version >nul 2>&1 && set "PY=python"
if not defined PY ( py -3 --version >nul 2>&1 && set "PY=py -3" )
if not defined PY ( python3 --version >nul 2>&1 && set "PY=python3" )
if not defined PY (
    echo [ERROR] Python 3.10+ not found in PATH.
    echo         Install from https://python.org and tick "Add python.exe to PATH".
    echo.
    pause
    exit /b 1
)
for /f "tokens=*" %%v in ('%PY% --version 2^>^&1') do set "PYVER=%%v"
echo Python: %PYVER%  (command: %PY%)

REM ---------- dependencies ----------
echo.
REM Check via app.preflight, not a bare "import ccxt": importing succeeds even
REM on an old ccxt that has no aster/hyperliquid classes, so a plain import
REM check would report OK and the exchanges would silently fail to connect.
%PY% -c "import sys; sys.path.insert(0,'.'); from app.preflight import check; sys.exit(0 if check().ok else 1)" >nul 2>&1
if errorlevel 1 (
    echo Installing dependencies from requirements.txt ...
    %PY% -m pip install --upgrade pip --quiet
    %PY% -m pip install -r requirements.txt
    if errorlevel 1 (
        echo [ERROR] pip install failed.
        pause
        exit /b 1
    )
    %PY% -c "import sys; sys.path.insert(0,'.'); from app.preflight import check, format_report; r=check(); print(format_report(r)); sys.exit(0 if r.ok else 1)"
    if errorlevel 1 (
        echo.
        echo [ERROR] Dependencies are still incomplete after install.
        echo         Run "python doctor.py" for details.
        pause
        exit /b 1
    )
) else (
    echo Dependencies: OK
)

REM ---------- menu ----------
:menu
echo.
echo ================= Crypto Screener =================
echo   1. DEMO      - offline, no exchanges needed   (~150 MB RAM)
echo                  uses the recorded market snapshot in data\
echo   2. DOCTOR    - diagnose why exchanges fail     (DNS/TLS/HTTP/ccxt)
echo   3. LIVE lite - 4 exchanges                     (~400 MB RAM)
echo                  Binance Futures, Bybit, OKX, Gate.io
echo   4. LIVE full - 8 exchanges incl. MEXC/Aster/Hyperliquid (2-3 GB RAM)
echo   5. LIVE via proxy - if exchanges block your region
echo   6. Try the PowerShell menu in Russian (start.ps1)
echo   0. Exit
echo ===================================================
set "CH="
set /p "CH=Choice: "

if "%CH%"=="0" exit /b 0
if "%CH%"=="6" goto ps1
if "%CH%"=="2" goto doctor

set "PORT="
set /p "PORT=Port [8000]: "
if not defined PORT set "PORT=8000"

if "%CH%"=="1" (
    set "MODE=replay"
    goto run
)
if "%CH%"=="3" (
    set "MODE=live"
    set "EXCHANGES=binanceusdm,bybit,okx,gate"
    set "TOP_N=60"
    set "BOOKS=25"
    goto run
)
if "%CH%"=="4" (
    set "MODE=live"
    set "TOP_N=150"
    set "BOOKS=40"
    goto run
)
if "%CH%"=="5" goto proxy
echo Unknown choice.
goto menu

REM ---------- diagnostics ----------
REM No port prompt here: the doctor does not start a server, and asking
REM for a port made it look like the script had hung.
:doctor
echo.
echo Running diagnostics. Every step prints as it goes.
echo On a blocked network this can take a couple of minutes - it IS working,
echo it is not frozen. Use --quick to skip the slow ccxt layer.
echo.
%PY% doctor.py
echo.
pause
goto menu

REM ---------- proxy ----------
REM Separate label instead of a parenthesised block: inside (...) every %VAR%
REM is expanded at PARSE time, i.e. BEFORE set /p runs. So the value typed by
REM the user is not visible there unless EnableDelayedExpansion is on.
:proxy
set "PROXY_URL="
set /p "PROXY_URL=Proxy URL (e.g. http://127.0.0.1:1080): "
if not defined PROXY_URL (
    echo No proxy given.
    goto menu
)
set "MODE=live"
set "EXCHANGES=binanceusdm,bybit,okx,gate"
set "TOP_N=60"
set "BOOKS=25"
goto run

REM ---------- start server ----------
:run
set "URL=http://localhost:%PORT%"
echo.
echo Starting on %URL%
echo The browser opens in 3 seconds. Press Ctrl+C to stop the server.
echo Note: for the first 3-5 minutes some metrics are empty - candles and
echo       minute windows still need to accumulate. That is normal.
start "" /min cmd /c "timeout /t 3 >nul & start %URL%"
%PY% run.py
echo.
echo Server stopped.
pause
goto menu

REM ---------- PowerShell menu ----------
:ps1
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0start.ps1"
if errorlevel 1 (
    echo.
    echo [WARN] start.ps1 failed to run. Most likely cause: the file is not
    echo        saved as UTF-8 with BOM, so PowerShell 5.1 misreads Cyrillic.
    echo        Use this ASCII menu instead - it does the same thing.
    echo.
    pause
)
goto menu
