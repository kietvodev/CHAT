@echo off
title TransChat Server
cd /d "%~dp0"
echo.
echo  ╔══════════════════════════════════╗
echo  ║       TransChat - DeepL          ║
echo  ╚══════════════════════════════════╝
echo.

where node >nul 2>&1
if errorlevel 1 (
    echo [LOI] Chua cai Node.js. Tai tai: https://nodejs.org
    pause
    exit /b 1
)

if not exist node_modules (
    echo [INFO] Dang cai dependencies...
    npm install
    npm rebuild better-sqlite3
    echo.
)

echo [INFO] Dang kiem tra Playwright Chromium...
if not exist "%LOCALAPPDATA%\ms-playwright" (
    npx playwright install chromium
    echo.
)

echo [INFO] Khoi dong server tai http://localhost:3000
echo [INFO] Nhan Ctrl+C de dung
echo.

:restart
node server.js
echo.
echo [WARN] Server bi tat. Khoi dong lai sau 3 giay... (Ctrl+C de dung)
timeout /t 3 /nobreak >nul
goto restart
