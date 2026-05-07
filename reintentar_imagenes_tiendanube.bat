@echo off
cd /d "%~dp0"
call "_ejecutar_accion.bat" sync-tiendanube-images-failed "Reintentando imagenes fallidas en Tienda Nube..."
