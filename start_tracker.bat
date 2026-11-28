@echo off
chcp 65001 >nul
title bStocks Tracker (:8898)
cd /d "%~dp0"
set PYTHONIOENCODING=utf-8
powershell -NoProfile -Command "Get-NetTCPConnection -LocalPort 8898 -ErrorAction SilentlyContinue | Select-Object -ExpandProperty OwningProcess -Unique | ForEach-Object { taskkill /F /PID $_ 2>$null }" >nul 2>&1
echo ============================================================
echo   bStocks Tracker Dashboard (http://localhost:8898)
echo ============================================================
python tracker_server.py
pause
