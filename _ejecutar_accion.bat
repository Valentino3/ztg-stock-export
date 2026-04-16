@echo off
setlocal

cd /d "%~dp0"

if "%~1"=="" (
    echo Error: falta indicar la accion a ejecutar.
    pause
    exit /b 1
)

if not exist ".env" (
    echo Error: no se encontro el archivo .env
    echo Completa tus credenciales antes de usar la aplicacion.
    pause
    exit /b 1
)

if not exist "config.toml" (
    echo Error: no se encontro el archivo config.toml
    pause
    exit /b 1
)

call preparar_entorno.bat
if errorlevel 1 (
    echo.
    echo No se pudo preparar la aplicacion.
    pause
    exit /b 1
)

echo %~2
echo.

".venv\Scripts\python.exe" -m gn_stock_export %~1
if errorlevel 1 (
    echo.
    echo La accion termino con error.
    pause
    exit /b 1
)

echo.
echo Proceso finalizado correctamente.

if exist "exports" (
    start "" "exports"
)

pause
