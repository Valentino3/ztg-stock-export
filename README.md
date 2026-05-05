# GN Stock Export

Aplicacion local para consultar la API de Grupo Nucleo, exportar productos en formato compatible con Tienda Nube y comparar el ultimo snapshot contra el anterior.

## Requisitos

- Python 3.12+
- Credenciales validas en `.env`
- Dependencias instaladas en la venv

## Instalacion

La forma mas simple para el cliente en Windows es usar los `.bat`.

En el primer uso, la app puede tardar unos minutos porque prepara la venv e instala dependencias si hace falta.

Si querés instalar manualmente el proyecto en la venv:

```bash
./.venv/Scripts/pip.exe install -e .
```

## Archivos Principales

- `.env`: credenciales para Grupo Nucleo y Tienda Nube
- `config.toml`: reglas de precios, publicacion y salida
- `brand_map.csv`: reemplazo opcional de marcas
- `category_map.csv`: reemplazo opcional de categorias, subcategorias e ID de categoria de Tienda Nube

## Configuracion

1. Completar `.env`:

```env
NUCLEO_ID=123
NUCLEO_USERNAME=usuario
NUCLEO_PASSWORD=secreto
TIENDANUBE_STORE_ID=123456
TIENDANUBE_ACCESS_TOKEN=token
TIENDANUBE_USER_AGENT=Mi App (mail@dominio.com)
```

2. Ajustar `config.toml` segun la estrategia comercial:

- `[pricing]`: cotizacion USD, margen, markup, redondeo y modo del campo `Costo`
- `[publication]`: reglas de publicacion, stock minimo, envio, producto fisico y filtro de categorias
- `[content]`: longitudes SEO, prefijos/sufijos de descripcion y marca por defecto
- `[mappings]`: rutas a `brand_map.csv` y `category_map.csv`
- `[diff]`: tolerancia para detectar cambios de precio entre snapshots
- `[output]`: carpeta de salida y formatos a generar
- `[tiendanube_sync]`: flags del sync completo por API, handle estable y politica de imagenes

3. Si hace falta, completar los mappings opcionales:

- `brand_map.csv`: `source_brand,target_brand`
- `category_map.csv`: `source_category,source_subcategory,target_category,target_subcategory,target_category_id`

Para el sync por API, `target_category_id` debe ser el ID real de la categoria en Tienda Nube.

El filtro `allowed_categories` / `excluded_categories` se aplica sobre la categoria final, despues de leer `category_map.csv`.
Por eso, si GN devuelve una categoria con otro nombre, primero la remapeamos en `category_map.csv` y despues la app decide si se sube o no.

## Como Se Calcula El Precio

La app toma el `precioNeto_USD` desde la API y arma el precio final asi:

1. Convierte el costo a ARS usando el dolar de la API o el override manual.
2. Aplica los impuestos informados por Grupo Nucleo sobre ese costo base.
3. Aplica el margen configurado.
4. Suma el recargo fijo si existe.
5. Redondea el resultado segun `rounding_step` y `rounding_mode`.
6. Exporta ese valor en la columna `Precio`.

La columna `Costo` puede salir como:

- `ars_neto`: costo en pesos antes del margen
- `usd_origen`: costo original en USD
- `ars_final`: mismo valor que el precio final en ARS

## Uso

### Exportar productos

Genera un snapshot interno y exporta el CSV/Excel listo para Tienda Nube:

```bash
./.venv/Scripts/python.exe -m gn_stock_export export
```

En Windows tambien podés usar:

```bat
exportar_productos.bat
```

Ese archivo se puede abrir con doble click y genera el export automaticamente.

Tambien hay accesos directos listos para usar:

- `abrir_panel.bat`: abre un menu con las acciones principales
- `exportar_productos.bat`: genera el export
- `sincronizar_productos.bat`: exporta y compara
- `comparar_snapshots.bat`: compara los dos ultimos snapshots
- `ver_productos_gn_crudo.bat`: exporta el catalogo original de GN sin transformaciones
- `exportar_categorias.bat`: exporta categorias/subcategorias GN para revisar filtros y mappings
- `probar_flujo_completo.bat`: genera una prueba completa usando pocos productos
- `probar_sync_tiendanube.bat`: simula el sync completo por API sin escribir en la tienda
- `sincronizar_tiendanube.bat`: sincroniza productos GN en Tienda Nube
- `sincronizar_imagenes_tiendanube.bat`: sincroniza solo imagenes GN sobre productos ya gestionados

### Comparar snapshots

Compara los dos snapshots mas recientes y genera un Excel con diferencias:

```bash
./.venv/Scripts/python.exe -m gn_stock_export compare
```

### Ver catalogo crudo de GN

Exporta el catalogo tal como llega desde Grupo Nucleo, sin conversiones para Tienda Nube:

```bash
./.venv/Scripts/python.exe -m gn_stock_export raw-export
```

### Exportar categorias GN

Genera un listado agrupado de categorias y subcategorias que llegan desde Grupo Nucleo:

```bash
./.venv/Scripts/python.exe -m gn_stock_export categories-export
```

El archivo sale en `exports/` como `gn_categorias_*.csv` y `gn_categorias_*.xlsx`.

Columnas principales:

- `source_category` y `source_subcategory`: categorias originales de GN
- `target_category`, `target_subcategory` y `target_category_id`: columnas editables para mapping con Tienda Nube
- `product_count`, `products_with_stock` y `stock_total`: ayuda para decidir que categorias filtrar o subir

### Probar flujo completo

Genera una prueba con pocos productos para ver como quedaria el proceso completo:

```bash
./.venv/Scripts/python.exe -m gn_stock_export test-flow
```

La cantidad de productos de prueba se define en:

```toml
[output]
test_product_limit = 20
```

### Exportar y comparar en un solo paso

Hace la exportacion y, si ya habia un snapshot anterior, genera tambien la comparacion:

```bash
./.venv/Scripts/python.exe -m gn_stock_export sync
```

### Sync completo con Tienda Nube

Hace un dry-run del sync completo sin escribir cambios reales:

```bash
./.venv/Scripts/python.exe -m gn_stock_export sync-tiendanube-test
```

Ejecuta el sync completo contra Tienda Nube:

```bash
./.venv/Scripts/python.exe -m gn_stock_export sync-tiendanube
```

Sincroniza solo imagenes GN sobre productos ya administrados por la app:

```bash
./.venv/Scripts/python.exe -m gn_stock_export sync-tiendanube-images
```

## Servidor Linux Con Cron

Para ejecutar el sync automaticamente sin depender de una PC, se puede alojar el proyecto en un servidor Linux y programarlo con `cron`.

Ruta recomendada:

```bash
/opt/gn-stock-export
```

Preparacion inicial en el servidor:

```bash
cd /opt/gn-stock-export
python3 -m venv .venv
.venv/bin/pip install -e .
```

Completar en el servidor:

- `.env` con credenciales reales de Grupo Nucleo y Tienda Nube
- `config.toml` con reglas comerciales finales
- `brand_map.csv` y `category_map.csv` si se usan mappings

Antes de activar el cron, ejecutar una prueba sin escribir cambios:

```bash
cd /opt/gn-stock-export
.venv/bin/python -m gn_stock_export sync-tiendanube-test --config config.toml --env-file .env
```

Luego probar el script productivo una vez de forma manual:

```bash
cd /opt/gn-stock-export
scripts/run_tiendanube_sync.sh
```

El script usa `flock` para evitar dos sincronizaciones al mismo tiempo y guarda logs diarios en:

```bash
logs/tiendanube_sync_YYYYMMDD.log
```

Para instalar la ejecucion automatica tres veces por dia:

```bash
crontab -e
```

Agregar:

```cron
0 8,14,20 * * * /opt/gn-stock-export/scripts/run_tiendanube_sync.sh >> /opt/gn-stock-export/logs/cron.log 2>&1
```

Tambien queda un ejemplo listo en:

```bash
deploy/cron.example
```

Archivos importantes en servidor:

- `logs/`: logs del script y de cron
- `exports/tiendanube_sync/`: reportes CSV, Excel y JSON de cada corrida
- `snapshots/tiendanube_sync/`: snapshots del catalogo normalizado
- `snapshots/tiendanube_sync_state.json`: estado local para no duplicar imagenes ya sincronizadas

## Salidas

- `exports/productos_*.xlsx`
- `exports/productos_*.csv`
- `exports/gn_productos_crudo_*.json`
- `exports/gn_productos_crudo_*.csv`
- `exports/gn_productos_crudo_*.xlsx`
- `exports/gn_categorias_*.csv`
- `exports/gn_categorias_*.xlsx`
- `exports/test/productos_*.csv`
- `exports/test/productos_*.xlsx`
- `exports/test/gn_productos_crudo_*.json`
- `exports/stock_diff_*.xlsx`
- `exports/tiendanube_sync/*.csv`
- `exports/tiendanube_sync/*.xlsx`
- `exports/tiendanube_sync/*.json`
- `snapshots/stock_snapshot_*.json`
- `snapshots/tiendanube_sync/stock_snapshot_*.json`
- `snapshots/tiendanube_sync_state.json`

Los archivos dentro de `exports/` salen listos con la estructura de la plantilla `productos.csv` para importacion en Tienda Nube.

## Como Actualizar Productos En Tienda Nube

El CSV exportado sirve tanto para crear productos como para actualizar productos existentes, siempre que mantengas el mismo `Identificador de URL`.

Desde esta etapa, el `Identificador de URL` se genera de forma estable como `gn-<item_id>`. Esto permite:

- importar una primera vez por CSV sin depender del nombre del producto
- actualizar productos existentes sin duplicarlos
- usar el mismo handle estable para el sync completo por API con Tienda Nube

En la practica:

- exportas el archivo
- revisas precios, stock o textos si hace falta
- lo importas en Tienda Nube
- Tienda Nube actualiza los productos que coincidan con ese identificador

## Ejemplos Rapidos

### Usar dolar manual

```toml
[pricing]
use_api_usd_exchange = true
use_usd_override = true
usd_exchange_override = 1450.0
margin_pct = 60.0
```

### Publicar solo si hay stock

```toml
[publication]
publish_with_stock_only = true
min_stock_to_publish = 1
```

### Subir solo categorias permitidas

```toml
[publication]
allowed_categories = ["Tecnología", "Computación", "Celulares", "Gaming", "Audio y TV", "Seguridad", "Movilidad"]
excluded_categories = ["Hogar", "Electro", "Herramientas", "Automotor", "Varios"]
```

Si `allowed_categories` tiene valores, solo se suben esas categorias finales. Si `excluded_categories` tiene valores, esas categorias nunca se suben.

### Agregar un prefijo a todas las descripciones

```toml
[content]
description_prefix = "Producto importado desde proveedor oficial."
description_suffix = ""
```

### Habilitar el sync completo por API

```toml
[tiendanube_sync]
enabled = true
dry_run = true
managed_tag = "GN_SYNC"
handle_prefix = "gn"
unpublish_missing = true
image_mode = "append_only"
test_product_limit = 20
```
