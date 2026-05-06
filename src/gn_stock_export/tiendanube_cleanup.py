from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from openpyxl.utils import get_column_letter
import pandas as pd

from gn_stock_export.config import AppConfig, TiendaNubeCredentials
from gn_stock_export.storage import ensure_directory, timestamp_slug
from gn_stock_export.tiendanube_api import TiendaNubeApiClient, TiendaNubeApiError
from gn_stock_export.tiendanube_sync import _extract_handle


DELETE_ALL_CONFIRMATION = "BORRAR_TODOS_LOS_PRODUCTOS"


@dataclass
class TiendaNubeCleanupRun:
    generated_at: datetime
    dry_run: bool
    row_count: int
    report_paths: dict[str, Path]
    counts: dict[str, int]


def run_tiendanube_cleanup(
    *,
    config: AppConfig,
    credentials: TiendaNubeCredentials,
    dry_run: bool,
    confirm: str = "",
    api_client_class: type[TiendaNubeApiClient] = TiendaNubeApiClient,
) -> TiendaNubeCleanupRun:
    if not dry_run and confirm != DELETE_ALL_CONFIRMATION:
        raise ValueError(
            "Para borrar productos reales tenes que pasar "
            f"`--confirm {DELETE_ALL_CONFIRMATION}`."
        )

    generated_at = datetime.now(timezone.utc)
    report_dir = ensure_directory(config.output.output_dir / "tiendanube_cleanup")
    rows: list[dict[str, object]] = []

    with api_client_class(credentials=credentials) as client:
        products = client.list_all_products()
        for product in products:
            product_id = _as_int(product.get("id"))
            row = _base_report_row(product)

            if dry_run:
                rows.append({**row, "action": "DELETE", "status": "DRY_RUN_DELETE", "details": "Se borraria el producto."})
                continue

            try:
                client.delete_product(product_id)
            except TiendaNubeApiError as exc:
                rows.append({**row, "action": "DELETE", "status": "ERROR", "details": str(exc)})
                continue

            rows.append({**row, "action": "DELETE", "status": "DELETED", "details": "Producto borrado."})

    report_paths, counts = _write_cleanup_report(rows, generated_at, report_dir, dry_run=dry_run)
    return TiendaNubeCleanupRun(
        generated_at=generated_at,
        dry_run=dry_run,
        row_count=len(rows),
        report_paths=report_paths,
        counts=counts,
    )


def _base_report_row(product: dict[str, Any]) -> dict[str, object]:
    return {
        "product_id": _as_int(product.get("id")),
        "handle": _extract_handle(product),
        "name": _localized_text(product.get("name")),
        "tags": _tags_text(product.get("tags")),
    }


def _write_cleanup_report(
    rows: list[dict[str, object]],
    generated_at: datetime,
    report_dir: Path,
    *,
    dry_run: bool,
) -> tuple[dict[str, Path], dict[str, int]]:
    slug = timestamp_slug(generated_at)
    mode = "dry_run" if dry_run else "productivo"
    frame = pd.DataFrame(
        rows,
        columns=["product_id", "handle", "name", "tags", "action", "status", "details"],
    )
    csv_path = report_dir / f"tiendanube_cleanup_{mode}_{slug}.csv"
    xlsx_path = report_dir / f"tiendanube_cleanup_{mode}_{slug}.xlsx"
    frame.to_csv(csv_path, index=False, sep=";", encoding="utf-8-sig")
    _write_xlsx(frame, xlsx_path)
    counts = frame["status"].value_counts().to_dict() if not frame.empty else {}
    return {"csv": csv_path, "xlsx": xlsx_path}, {str(key): int(value) for key, value in counts.items()}


def _write_xlsx(frame: pd.DataFrame, path: Path) -> None:
    ensure_directory(path.parent)
    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        frame.to_excel(writer, sheet_name="borrado", index=False)
        worksheet = writer.sheets["borrado"]
        for index, column in enumerate(frame.columns, start=1):
            values = frame[column].astype(str).tolist()
            max_length = max([len(str(column)), *(len(value) for value in values)] or [len(str(column))])
            worksheet.column_dimensions[get_column_letter(index)].width = min(max_length + 2, 60)


def _localized_text(value: object) -> str:
    if isinstance(value, dict):
        for key in ("es", "pt", "en"):
            candidate = value.get(key)
            if candidate:
                return str(candidate)
        for candidate in value.values():
            if candidate:
                return str(candidate)
        return ""
    if value is None:
        return ""
    return str(value)


def _tags_text(value: object) -> str:
    if isinstance(value, list):
        return ", ".join(str(item) for item in value if item)
    if value is None:
        return ""
    return str(value)


def _as_int(value: object) -> int:
    if value in (None, ""):
        return 0
    return int(float(str(value)))
