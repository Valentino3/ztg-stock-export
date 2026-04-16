@echo off
setlocal

cd /d "%~dp0"

if exist ".venv\Scripts\python.exe" goto check_dependencies

call :find_python
if errorlevel 1 (
    echo Error: no se encontro Python instalado en Windows.
    echo Instala Python 3.12 o superior y volve a intentar.
    exit /b 1
)

echo Preparando entorno por primera vez...
%PYTHON_CMD% -m venv .venv
if errorlevel 1 (
    echo Error: no se pudo crear la venv.
    exit /b 1
)

:check_dependencies
".venv\Scripts\python.exe" -c "import httpx, openpyxl, pandas, typer, dotenv" >nul 2>&1
if errorlevel 1 (
    echo Instalando dependencias...
    ".venv\Scripts\python.exe" -m pip install -e .
    if errorlevel 1 (
        echo Error: no se pudieron instalar las dependencias.
        exit /b 1
    )
)

endlocal & exit /b 0

:find_python
where py >nul 2>&1
if not errorlevel 1 (
    py -3.12 -V >nul 2>&1
    if not errorlevel 1 (
        set "PYTHON_CMD=py -3.12"
        exit /b 0
    )
    py -3 -V >nul 2>&1
    if not errorlevel 1 (
        set "PYTHON_CMD=py -3"
        exit /b 0
    )
    py -V >nul 2>&1
    if not errorlevel 1 (
        set "PYTHON_CMD=py"
        exit /b 0
    )
)

where python >nul 2>&1
if not errorlevel 1 (
    python -V >nul 2>&1
    if not errorlevel 1 (
        set "PYTHON_CMD=python"
        exit /b 0
    )
)

exit /b 1
