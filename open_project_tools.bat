@echo off
REM ============================================================
REM  Smart Agriculture Analytics - Project Launcher
REM  Only OPENS the required tools/apps. Does NOT run any
REM  docker compose / spark-submit / kafka / python commands.
REM ============================================================

set PROJECT_DIR=D:\SEM-5\BIG_DATA

echo Opening Docker Desktop...
start "" "C:\Program Files\Docker\Docker\Docker Desktop.exe"

echo Opening VS Code at project folder...
code "%PROJECT_DIR%"

REM If you use IntelliJ IDEA instead of VS Code, comment the line above
REM and uncomment the line below (adjust path to your IntelliJ installation):
REM start "" "C:\Program Files\JetBrains\IntelliJ IDEA\bin\idea64.exe" "%PROJECT_DIR%\scala-ingestion"

echo Opening project folder in File Explorer...
start "" explorer "%PROJECT_DIR%"

echo Opening a Command Prompt at project folder...
start "" cmd /k "cd /d %PROJECT_DIR%"

echo Opening Hadoop NameNode Web UI in browser...
start "" http://localhost:9870

echo.
echo All required tools have been opened.
echo (Docker Desktop, VS Code, File Explorer, CMD, NameNode UI)
pause
