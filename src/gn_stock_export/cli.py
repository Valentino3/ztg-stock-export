from __future__ import annotations

from pathlib import Path

import typer

from gn_stock_export.config import (
    ConfigError,
    CredentialsError,
    load_app_config,
    load_credentials,
    load_tiendanube_credentials,
)
from gn_stock_export.service import (
    StockExportService,
    TiendaNubeCleanupResult,
    TiendaNubeImageRetryResult,
    TiendaNubeSyncResult,
)
from gn_stock_export.storage import SnapshotError
from gn_stock_export.tiendanube_cleanup import DELETE_ALL_CONFIRMATION


app = typer.Typer(
    add_completion=False,
    help="CLI local para exportar y comparar stock desde la API de Grupo Nucleo.",
    no_args_is_help=True,
)


def _build_service(
    config_path: Path,
    env_path: Path | None,
    *,
    require_gn: bool = True,
    require_tiendanube: bool = False,
) -> StockExportService:
    config = load_app_config(config_path)
    credentials = None
    if env_path:
        try:
            credentials = load_credentials(env_path)
        except CredentialsError:
            if require_gn:
                raise
    tiendanube_credentials = None
    if env_path:
        try:
            tiendanube_credentials = load_tiendanube_credentials(env_path)
        except CredentialsError:
            if require_tiendanube:
                raise
    return StockExportService(
        workspace_dir=config_path.resolve().parent,
        config=config,
        credentials=credentials,
        tiendanube_credentials=tiendanube_credentials,
    )


@app.command("export")
def export_command(
    config_path: Path = typer.Option(Path("config.toml"), "--config", help="Ruta al config.toml."),
    env_path: Path = typer.Option(Path(".env"), "--env-file", help="Ruta al archivo .env."),
) -> None:
    """Genera un snapshot nuevo y exporta stock a Excel/CSV."""
    try:
        service = _build_service(config_path, env_path)
        result = service.export()
    except (ConfigError, CredentialsError, SnapshotError, RuntimeError, ValueError) as exc:
        _abort_with_error(str(exc))

    typer.echo(f"Stock exportado: {result.row_count} articulos")
    typer.echo(f"Cotizacion USD usada: {result.usd_exchange}")
    typer.echo(f"Snapshot: {result.snapshot_path}")
    for label, path in result.outputs.items():
        typer.echo(f"{label.upper()}: {path}")


@app.command("raw-export")
def raw_export_command(
    config_path: Path = typer.Option(Path("config.toml"), "--config", help="Ruta al config.toml."),
    env_path: Path = typer.Option(Path(".env"), "--env-file", help="Ruta al archivo .env."),
) -> None:
    """Exporta el catalogo crudo de GN sin transformaciones comerciales."""
    try:
        service = _build_service(config_path, env_path)
        result = service.export_gn_raw()
    except (ConfigError, CredentialsError, SnapshotError, RuntimeError, ValueError) as exc:
        _abort_with_error(str(exc))

    typer.echo(f"Catalogo GN crudo exportado: {result.row_count} articulos")
    typer.echo(f"Cotizacion USD GN: {result.usd_exchange}")
    for label, path in result.outputs.items():
        typer.echo(f"{label.upper()}: {path}")


@app.command("categories-export")
def categories_export_command(
    config_path: Path = typer.Option(Path("config.toml"), "--config", help="Ruta al config.toml."),
    env_path: Path = typer.Option(Path(".env"), "--env-file", help="Ruta al archivo .env."),
) -> None:
    """Exporta categorias y subcategorias GN agrupadas desde el catalogo."""
    try:
        service = _build_service(config_path, env_path)
        result = service.export_categories()
    except (ConfigError, CredentialsError, SnapshotError, RuntimeError, ValueError) as exc:
        _abort_with_error(str(exc))

    typer.echo(f"Categorias GN exportadas: {result.row_count}")
    typer.echo(f"Productos analizados: {result.product_count}")
    typer.echo(f"Cotizacion USD usada: {result.usd_exchange}")
    for label, path in result.outputs.items():
        typer.echo(f"{label.upper()}: {path}")


@app.command("test-flow")
def test_flow_command(
    config_path: Path = typer.Option(Path("config.toml"), "--config", help="Ruta al config.toml."),
    env_path: Path = typer.Option(Path(".env"), "--env-file", help="Ruta al archivo .env."),
) -> None:
    """Ejecuta un flujo de prueba con una cantidad limitada de productos."""
    try:
        service = _build_service(config_path, env_path)
        result = service.test_flow()
    except (ConfigError, CredentialsError, SnapshotError, RuntimeError, ValueError) as exc:
        _abort_with_error(str(exc))

    typer.echo(f"Flujo de prueba generado: {result.row_count} articulos")
    typer.echo(f"Cotizacion USD usada: {result.usd_exchange}")
    typer.echo(f"Snapshot test: {result.snapshot_path}")
    for label, path in result.outputs.items():
        typer.echo(f"{label.upper()}: {path}")

    if result.compare_result is None:
        typer.echo("No habia un snapshot de prueba anterior. Se omitio la comparacion.")
        return

    typer.echo(f"Comparacion test generada: {result.compare_result.workbook_path}")
    typer.echo(f"Cambios detectados: {len(result.compare_result.comparison.changes)}")
    for status, count in result.compare_result.comparison.counts.items():
        typer.echo(f"{status}: {count}")


@app.command("sync-tiendanube-test")
def sync_tiendanube_test_command(
    config_path: Path = typer.Option(Path("config.toml"), "--config", help="Ruta al config.toml."),
    env_path: Path = typer.Option(Path(".env"), "--env-file", help="Ruta al archivo .env."),
) -> None:
    """Analiza el sync completo con Tienda Nube sin escribir cambios reales."""
    try:
        service = _build_service(config_path, env_path, require_tiendanube=True)
        result = service.sync_tiendanube_test()
    except (ConfigError, CredentialsError, SnapshotError, RuntimeError, ValueError) as exc:
        _abort_with_error(str(exc))

    _render_tiendanube_sync_result(result)


@app.command("sync-tiendanube")
def sync_tiendanube_command(
    config_path: Path = typer.Option(Path("config.toml"), "--config", help="Ruta al config.toml."),
    env_path: Path = typer.Option(Path(".env"), "--env-file", help="Ruta al archivo .env."),
) -> None:
    """Sincroniza productos GN contra Tienda Nube en modo productivo."""
    try:
        service = _build_service(config_path, env_path, require_tiendanube=True)
        result = service.sync_tiendanube()
    except (ConfigError, CredentialsError, SnapshotError, RuntimeError, ValueError) as exc:
        _abort_with_error(str(exc))

    _render_tiendanube_sync_result(result)


@app.command("sync-tiendanube-images")
def sync_tiendanube_images_command(
    config_path: Path = typer.Option(Path("config.toml"), "--config", help="Ruta al config.toml."),
    env_path: Path = typer.Option(Path(".env"), "--env-file", help="Ruta al archivo .env."),
) -> None:
    """Sincroniza solo las imagenes GN sobre productos ya gestionados por la app."""
    try:
        service = _build_service(config_path, env_path, require_tiendanube=True)
        result = service.sync_tiendanube_images()
    except (ConfigError, CredentialsError, SnapshotError, RuntimeError, ValueError) as exc:
        _abort_with_error(str(exc))

    _render_tiendanube_sync_result(result)


@app.command("sync-tiendanube-images-failed")
def sync_tiendanube_images_failed_command(
    config_path: Path = typer.Option(Path("config.toml"), "--config", help="Ruta al config.toml."),
    env_path: Path = typer.Option(Path(".env"), "--env-file", help="Ruta al archivo .env."),
    failures_path: Path | None = typer.Option(
        None,
        "--failures-file",
        help="Reporte CSV de imagenes fallidas. Si se omite, usa el ultimo disponible.",
    ),
) -> None:
    """Reintenta solo las imagenes que fallaron en el ultimo reporte."""
    try:
        service = _build_service(config_path, env_path, require_gn=False, require_tiendanube=True)
        result = service.sync_tiendanube_failed_images(failures_path=failures_path)
    except (ConfigError, CredentialsError, SnapshotError, RuntimeError, ValueError) as exc:
        _abort_with_error(str(exc))

    _render_tiendanube_image_retry_result(result)


@app.command("clear-tiendanube-test")
def clear_tiendanube_test_command(
    config_path: Path = typer.Option(Path("config.toml"), "--config", help="Ruta al config.toml."),
    env_path: Path = typer.Option(Path(".env"), "--env-file", help="Ruta al archivo .env."),
) -> None:
    """Lista todos los productos que se borrarian de Tienda Nube sin borrar nada."""
    try:
        service = _build_service(config_path, env_path, require_gn=False, require_tiendanube=True)
        result = service.clear_tiendanube_products(dry_run=True)
    except (ConfigError, CredentialsError, SnapshotError, RuntimeError, ValueError) as exc:
        _abort_with_error(str(exc))

    _render_tiendanube_cleanup_result(result)


@app.command("clear-tiendanube")
def clear_tiendanube_command(
    config_path: Path = typer.Option(Path("config.toml"), "--config", help="Ruta al config.toml."),
    env_path: Path = typer.Option(Path(".env"), "--env-file", help="Ruta al archivo .env."),
    confirm: str = typer.Option(
        "",
        "--confirm",
        help=f"Confirmacion obligatoria: {DELETE_ALL_CONFIRMATION}",
    ),
) -> None:
    """Borra TODOS los productos de Tienda Nube. Usar solo para reiniciar una tienda de prueba."""
    try:
        service = _build_service(config_path, env_path, require_gn=False, require_tiendanube=True)
        result = service.clear_tiendanube_products(dry_run=False, confirm=confirm)
    except (ConfigError, CredentialsError, SnapshotError, RuntimeError, ValueError) as exc:
        _abort_with_error(str(exc))

    _render_tiendanube_cleanup_result(result)


@app.command("compare")
def compare_command(
    config_path: Path = typer.Option(Path("config.toml"), "--config", help="Ruta al config.toml."),
) -> None:
    """Compara automaticamente los dos ultimos snapshots."""
    try:
        service = _build_service(config_path, env_path=None)
        result = service.compare_latest()
    except (ConfigError, SnapshotError, RuntimeError, ValueError) as exc:
        _abort_with_error(str(exc))

    typer.echo(f"Comparacion generada: {result.workbook_path}")
    typer.echo(f"Cambios detectados: {len(result.comparison.changes)}")
    for status, count in result.comparison.counts.items():
        typer.echo(f"{status}: {count}")


@app.command("sync")
def sync_command(
    config_path: Path = typer.Option(Path("config.toml"), "--config", help="Ruta al config.toml."),
    env_path: Path = typer.Option(Path(".env"), "--env-file", help="Ruta al archivo .env."),
) -> None:
    """Exporta stock y compara contra el ultimo snapshot disponible."""
    try:
        service = _build_service(config_path, env_path)
        export_result, compare_result = service.export_then_compare()
    except (ConfigError, CredentialsError, SnapshotError, RuntimeError, ValueError) as exc:
        _abort_with_error(str(exc))

    typer.echo(f"Stock exportado: {export_result.row_count} articulos")
    typer.echo(f"Snapshot: {export_result.snapshot_path}")
    for label, path in export_result.outputs.items():
        typer.echo(f"{label.upper()}: {path}")

    if compare_result is None:
        typer.echo("No habia un snapshot anterior. Se omitio la comparacion.")
        return

    typer.echo(f"Comparacion generada: {compare_result.workbook_path}")
    typer.echo(f"Cambios detectados: {len(compare_result.comparison.changes)}")
    for status, count in compare_result.comparison.counts.items():
        typer.echo(f"{status}: {count}")


def _abort_with_error(message: str) -> None:
    typer.secho(f"Error: {message}", fg=typer.colors.RED, err=True)
    raise typer.Exit(code=1)


def _render_tiendanube_sync_result(result: TiendaNubeSyncResult) -> None:
    typer.echo(f"Sync Tienda Nube ejecutado sobre {result.row_count} articulos")
    typer.echo(f"Modo: {'dry-run' if result.dry_run else 'productivo'}")
    typer.echo(f"Cotizacion USD usada: {result.usd_exchange}")
    typer.echo(f"Snapshot sync: {result.snapshot_path}")
    typer.echo(f"Estado local: {result.state_path}")
    for label, path in result.report_paths.items():
        typer.echo(f"{label.upper()}: {path}")
    for status, count in result.counts.items():
        typer.echo(f"{status}: {count}")


def _render_tiendanube_cleanup_result(result: TiendaNubeCleanupResult) -> None:
    typer.echo(f"Borrado Tienda Nube evaluado sobre {result.row_count} productos")
    typer.echo(f"Modo: {'dry-run' if result.dry_run else 'productivo'}")
    for label, path in result.report_paths.items():
        typer.echo(f"{label.upper()}: {path}")
    for status, count in result.counts.items():
        typer.echo(f"{status}: {count}")


def _render_tiendanube_image_retry_result(result: TiendaNubeImageRetryResult) -> None:
    typer.echo(f"Reintento de imagenes Tienda Nube sobre {result.row_count} fallas")
    typer.echo(f"Reporte origen: {result.failures_path}")
    typer.echo(f"Estado local: {result.state_path}")
    for label, path in result.report_paths.items():
        typer.echo(f"{label.upper()}: {path}")
    for status, count in result.counts.items():
        typer.echo(f"{status}: {count}")
