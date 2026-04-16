from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime, timezone
from pathlib import Path

from gn_stock_export.api import GNApiClient
from gn_stock_export.config import AppConfig, Credentials, TiendaNubeCredentials
from gn_stock_export.diffing import ComparisonResult, compare_frames
from gn_stock_export.exporter import write_comparison_workbook, write_gn_raw_exports, write_stock_exports
from gn_stock_export.normalizer import build_export_frame
from gn_stock_export.storage import (
    SnapshotData,
    SnapshotError,
    list_snapshot_paths,
    load_snapshot,
    save_snapshot,
    timestamp_slug,
)
from gn_stock_export.tiendanube_api import TiendaNubeApiClient
from gn_stock_export.tiendanube_sync import TiendaNubeSyncRun, run_tiendanube_sync


@dataclass
class ExportResult:
    generated_at: datetime
    snapshot_path: Path
    outputs: dict[str, Path]
    row_count: int
    usd_exchange: float


@dataclass
class CompareResult:
    workbook_path: Path
    comparison: ComparisonResult


@dataclass
class RawExportResult:
    generated_at: datetime
    outputs: dict[str, Path]
    row_count: int
    usd_exchange: float


@dataclass
class TestFlowResult:
    generated_at: datetime
    snapshot_path: Path
    outputs: dict[str, Path]
    row_count: int
    usd_exchange: float
    compare_result: CompareResult | None


@dataclass
class TiendaNubeSyncResult:
    generated_at: datetime
    snapshot_path: Path
    report_paths: dict[str, Path]
    state_path: Path
    row_count: int
    usd_exchange: float
    dry_run: bool
    counts: dict[str, int]


class StockExportService:
    def __init__(
        self,
        workspace_dir: Path,
        config: AppConfig,
        credentials: Credentials | None = None,
        tiendanube_credentials: TiendaNubeCredentials | None = None,
        api_client_class: type[GNApiClient] = GNApiClient,
        tiendanube_api_client_class: type[TiendaNubeApiClient] = TiendaNubeApiClient,
    ) -> None:
        self.workspace_dir = workspace_dir.resolve()
        self.config = config
        self.credentials = credentials
        self.tiendanube_credentials = tiendanube_credentials
        self.api_client_class = api_client_class
        self.tiendanube_api_client_class = tiendanube_api_client_class
        self.snapshot_dir = self.workspace_dir / "snapshots"

    def export(self) -> ExportResult:
        if self.credentials is None:
            raise ValueError("Las credenciales son obligatorias para exportar.")

        generated_at = datetime.now(timezone.utc)
        with self.api_client_class(self.credentials) as api:
            catalog = api.get_catalog()
            usd_exchange = self._resolve_usd_exchange(api)

        frame = build_export_frame(catalog, usd_exchange, self.config, exported_at=generated_at)
        snapshot_path = save_snapshot(frame, self.snapshot_dir, generated_at, usd_exchange, self.config)
        outputs = write_stock_exports(frame, generated_at, self.config)

        return ExportResult(
            generated_at=generated_at,
            snapshot_path=snapshot_path,
            outputs=outputs,
            row_count=len(frame),
            usd_exchange=usd_exchange,
        )

    def compare_latest(self) -> CompareResult:
        snapshot_paths = list_snapshot_paths(self.snapshot_dir)
        if len(snapshot_paths) < 2:
            raise SnapshotError("Se necesitan al menos dos snapshots para comparar.")

        base_snapshot = load_snapshot(snapshot_paths[-2])
        current_snapshot = load_snapshot(snapshot_paths[-1])
        return self.compare_specific(base_snapshot, current_snapshot)

    def compare_specific(self, base_snapshot: SnapshotData, current_snapshot: SnapshotData) -> CompareResult:
        return self._compare_specific(base_snapshot, current_snapshot, self.config.output.output_dir)

    def _compare_specific(
        self,
        base_snapshot: SnapshotData,
        current_snapshot: SnapshotData,
        output_dir: Path,
    ) -> CompareResult:
        comparison = compare_frames(
            base_snapshot.frame,
            current_snapshot.frame,
            self.config.diff.price_tolerance_ars,
            base_snapshot.path,
            current_snapshot.path,
        )
        filename = (
            f"stock_diff_{timestamp_slug(base_snapshot.generated_at)}"
            f"_vs_{timestamp_slug(current_snapshot.generated_at)}.xlsx"
        )
        workbook_path = output_dir / filename
        write_comparison_workbook(comparison.summary, comparison.changes, workbook_path)
        return CompareResult(workbook_path=workbook_path, comparison=comparison)

    def export_then_compare(self) -> tuple[ExportResult, CompareResult | None]:
        previous_paths = list_snapshot_paths(self.snapshot_dir)
        export_result = self.export()

        current_snapshot = load_snapshot(export_result.snapshot_path)
        if not previous_paths:
            return export_result, None

        previous_snapshot = load_snapshot(previous_paths[-1])
        compare_result = self.compare_specific(previous_snapshot, current_snapshot)
        return export_result, compare_result

    def export_gn_raw(self) -> RawExportResult:
        if self.credentials is None:
            raise ValueError("Las credenciales son obligatorias para exportar.")

        generated_at = datetime.now(timezone.utc)
        with self.api_client_class(self.credentials) as api:
            catalog = api.get_catalog()
            usd_exchange = api.get_usd_exchange()

        outputs = write_gn_raw_exports(catalog, generated_at, self.config, usd_exchange)
        return RawExportResult(
            generated_at=generated_at,
            outputs=outputs,
            row_count=len(catalog),
            usd_exchange=usd_exchange,
        )

    def test_flow(self) -> TestFlowResult:
        if self.credentials is None:
            raise ValueError("Las credenciales son obligatorias para exportar.")

        generated_at = datetime.now(timezone.utc)
        with self.api_client_class(self.credentials) as api:
            catalog = api.get_catalog()
            usd_exchange = self._resolve_usd_exchange(api)

        limit = self.config.output.test_product_limit
        if limit > 0:
            catalog = catalog[:limit]

        test_output_dir = self.config.output.output_dir / "test"
        test_config = replace(self.config, output=replace(self.config.output, output_dir=test_output_dir))
        test_snapshot_dir = self.snapshot_dir / "test"
        previous_paths = list_snapshot_paths(test_snapshot_dir)

        frame = build_export_frame(catalog, usd_exchange, test_config, exported_at=generated_at)
        snapshot_path = save_snapshot(frame, test_snapshot_dir, generated_at, usd_exchange, test_config)
        outputs = write_stock_exports(frame, generated_at, test_config)
        raw_outputs = write_gn_raw_exports(catalog, generated_at, test_config, usd_exchange)
        outputs.update({f"raw_{label}": path for label, path in raw_outputs.items()})

        compare_result: CompareResult | None = None
        if previous_paths:
            previous_snapshot = load_snapshot(previous_paths[-1])
            current_snapshot = load_snapshot(snapshot_path)
            compare_result = self._compare_specific(previous_snapshot, current_snapshot, test_output_dir)

        return TestFlowResult(
            generated_at=generated_at,
            snapshot_path=snapshot_path,
            outputs=outputs,
            row_count=len(frame),
            usd_exchange=usd_exchange,
            compare_result=compare_result,
        )

    def _resolve_usd_exchange(self, api: GNApiClient) -> float:
        if self.config.pricing.use_usd_override:
            return self.config.pricing.usd_exchange_override
        if self.config.pricing.use_api_usd_exchange:
            return api.get_usd_exchange()
        raise ValueError("No hay una fuente de cotizacion USD configurada.")

    def sync_tiendanube_test(self) -> TiendaNubeSyncResult:
        return self._sync_tiendanube(dry_run=True, limit=self.config.tiendanube_sync.test_product_limit, images_only=False)

    def sync_tiendanube(self) -> TiendaNubeSyncResult:
        return self._sync_tiendanube(dry_run=False, limit=None, images_only=False)

    def sync_tiendanube_images(self) -> TiendaNubeSyncResult:
        return self._sync_tiendanube(dry_run=False, limit=None, images_only=True)

    def _sync_tiendanube(self, *, dry_run: bool, limit: int | None, images_only: bool) -> TiendaNubeSyncResult:
        if self.credentials is None:
            raise ValueError("Las credenciales de Grupo Nucleo son obligatorias para sincronizar con Tienda Nube.")
        if self.tiendanube_credentials is None:
            raise ValueError("Las credenciales de Tienda Nube son obligatorias para sincronizar.")

        generated_at = datetime.now(timezone.utc)
        with self.api_client_class(self.credentials) as api:
            catalog = api.get_catalog()
            usd_exchange = self._resolve_usd_exchange(api)

        frame = build_export_frame(catalog, usd_exchange, self.config, exported_at=generated_at)
        snapshot_dir = self.snapshot_dir / "tiendanube_sync"
        snapshot_path = save_snapshot(frame, snapshot_dir, generated_at, usd_exchange, self.config)
        sync_run: TiendaNubeSyncRun = run_tiendanube_sync(
            stock_frame=frame,
            config=self.config,
            credentials=self.tiendanube_credentials,
            workspace_dir=self.workspace_dir,
            dry_run=dry_run,
            limit=limit,
            images_only=images_only,
            api_client_class=self.tiendanube_api_client_class,
        )
        return TiendaNubeSyncResult(
            generated_at=generated_at,
            snapshot_path=snapshot_path,
            report_paths=sync_run.report_paths,
            state_path=sync_run.state_path,
            row_count=sync_run.row_count,
            usd_exchange=usd_exchange,
            dry_run=sync_run.dry_run,
            counts=sync_run.counts,
        )
