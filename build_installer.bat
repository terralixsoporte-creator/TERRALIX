@echo off
REM ============================================================
REM  Build script for Terralix ERP installer
REM  Prerequisites:
REM    - Python venv activated (terr\Scripts\activate)
REM    - PyInstaller installed:  pip install pyinstaller
REM    - Inno Setup 6 installed (default path or in PATH)
REM ============================================================

setlocal
cd /d "%~dp0"
set "NO_PAUSE=0"
if /I "%~1"=="--no-pause" set "NO_PAUSE=1"

if not exist "terr\Scripts\python.exe" (
    echo ERROR: No se encontro terr\Scripts\python.exe
    echo Crea/activa el entorno virtual en la carpeta "terr" antes de compilar.
    call :maybe_pause
    exit /b 1
)

echo.
echo ========================================
echo  [1/4] Limpiando build anterior...
echo ========================================
if exist "build\TERRALIX" rmdir /s /q "build\TERRALIX"
if exist "dist\TERRALIX"  rmdir /s /q "dist\TERRALIX"
if exist "dist\installer" rmdir /s /q "dist\installer"

echo.
echo ========================================
echo  [2/4] Instalando Chromium de Playwright...
echo ========================================
set "PLAYWRIGHT_BROWSERS_PATH=%LOCALAPPDATA%\ms-playwright"
terr\Scripts\python.exe -m playwright install chromium
if errorlevel 1 (
    echo ERROR: No se pudo instalar Chromium de Playwright.
    call :maybe_pause
    exit /b 1
)

echo.
echo ========================================
echo  [3/4] Construyendo con PyInstaller...
echo ========================================
terr\Scripts\python.exe -m PyInstaller TERRALIX.spec --noconfirm
if errorlevel 1 (
    echo ERROR: PyInstaller fallo. Revisa los errores arriba.
    call :maybe_pause
    exit /b 1
)

echo.
echo ========================================
echo  [4/4] Creando instalador con Inno Setup...
echo ========================================
REM Try common Inno Setup install locations
set "ISCC="
if exist "C:\Program Files (x86)\Inno Setup 6\ISCC.exe" (
    set "ISCC=C:\Program Files (x86)\Inno Setup 6\ISCC.exe"
) else if exist "C:\Program Files\Inno Setup 6\ISCC.exe" (
    set "ISCC=C:\Program Files\Inno Setup 6\ISCC.exe"
) else if exist "%LOCALAPPDATA%\Programs\Inno Setup 6\ISCC.exe" (
    set "ISCC=%LOCALAPPDATA%\Programs\Inno Setup 6\ISCC.exe"
) else (
    where iscc >nul 2>&1
    if not errorlevel 1 (
        set "ISCC=iscc"
    )
)

if "%ISCC%"=="" (
    echo.
    echo AVISO: Inno Setup no encontrado.
    echo   Instala Inno Setup 6 desde: https://jrsoftware.org/isdl.php
    echo   O abre installer.iss manualmente en Inno Setup Compiler.
    echo.
    echo PyInstaller completo. El ejecutable esta en: dist\TERRALIX\
    call :maybe_pause
    exit /b 0
)

"%ISCC%" installer.iss
if errorlevel 1 (
    echo ERROR: Inno Setup fallo. Revisa los errores arriba.
    call :maybe_pause
    exit /b 1
)

echo.
echo ========================================
echo  BUILD COMPLETO!
echo  Instalador: dist\installer\TerralixERP_Setup.exe
echo ========================================
call :maybe_pause
exit /b 0

:maybe_pause
if "%NO_PAUSE%"=="1" exit /b 0
pause
exit /b 0
