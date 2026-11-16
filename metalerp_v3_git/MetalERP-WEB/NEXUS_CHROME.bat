@echo off
rem Abre Nexus como ventana propia usando Chrome (para PCs sin WebView2, ej. Windows Server 2012 R2)
cd /d "%~dp0"
netstat -ano | find ":5100 " | find "LISTENING" >nul
if errorlevel 1 (
  start "" pythonw monitor.py
  timeout /t 4 /nobreak >nul
)
set CHROME=%ProgramFiles%\Google\Chrome\Application\chrome.exe
if not exist "%CHROME%" set CHROME=%ProgramFiles(x86)%\Google\Chrome\Application\chrome.exe
if not exist "%CHROME%" set CHROME=%LocalAppData%\Google\Chrome\Application\chrome.exe
start "" "%CHROME%" --app=http://127.0.0.1:5100 --window-size=1320,860
