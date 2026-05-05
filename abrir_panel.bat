@echo off
setlocal

cd /d "%~dp0"

:menu
cls
echo ================================
echo      GN STOCK EXPORT
echo ================================
echo.
echo 1. Exportar productos
echo 2. Exportar y comparar
echo 3. Comparar ultimos snapshots
echo 4. Ver productos GN en crudo
echo 5. Exportar categorias GN
echo 6. Probar flujo completo
echo 7. Probar sync Tienda Nube
echo 8. Sincronizar Tienda Nube
echo 9. Sincronizar imagenes Tienda Nube
echo 10. Editar config.toml
echo 11. Abrir carpeta exports
echo 12. Salir
echo.
set /p opcion=Elegi una opcion y presiona Enter: 

if "%opcion%"=="1" (
    call exportar_productos.bat
    goto menu
)
if "%opcion%"=="2" (
    call sincronizar_productos.bat
    goto menu
)
if "%opcion%"=="3" (
    call comparar_snapshots.bat
    goto menu
)
if "%opcion%"=="4" (
    call ver_productos_gn_crudo.bat
    goto menu
)
if "%opcion%"=="5" (
    call exportar_categorias.bat
    goto menu
)
if "%opcion%"=="6" (
    call probar_flujo_completo.bat
    goto menu
)
if "%opcion%"=="7" (
    call probar_sync_tiendanube.bat
    goto menu
)
if "%opcion%"=="8" (
    call sincronizar_tiendanube.bat
    goto menu
)
if "%opcion%"=="9" (
    call sincronizar_imagenes_tiendanube.bat
    goto menu
)
if "%opcion%"=="10" (
    notepad "config.toml"
    goto menu
)
if "%opcion%"=="11" (
    if not exist "exports" mkdir "exports"
    start "" "exports"
    goto menu
)
if "%opcion%"=="12" (
    exit /b 0
)

echo.
echo Opcion no valida.
pause
goto menu
