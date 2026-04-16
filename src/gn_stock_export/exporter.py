from __future__ import annotations

from datetime import datetime
import json
from pathlib import Path

from openpyxl.utils import get_column_letter
import pandas as pd

from gn_stock_export.config import AppConfig
from gn_stock_export.product_import import build_product_import_frame
from gn_stock_export.storage import ensure_directory, timestamp_slug


def write_stock_exports(frame: pd.DataFrame, generated_at: datetime, config: AppConfig) -> dict[str, Path]:
    output_dir = ensure_directory(config.output.output_dir)
    slug = timestamp_slug(generated_at)
    written: dict[str, Path] = {}
    import_frame = build_product_import_frame(frame, config)

    if config.output.include_xlsx:
        xlsx_path = output_dir / f"productos_{slug}.xlsx"
        with pd.ExcelWriter(xlsx_path, engine="openpyxl") as writer:
            import_frame.to_excel(writer, sheet_name="productos", index=False)
            _format_worksheet(writer.book["productos"])
        written["xlsx"] = xlsx_path

    if config.output.include_csv:
        csv_path = output_dir / f"productos_{slug}.csv"
        import_frame.to_csv(csv_path, index=False, sep=";", encoding="utf-8-sig")
        written["csv"] = csv_path

    return written


def write_gn_raw_exports(
    catalog: list[dict[str, object]],
    generated_at: datetime,
    config: AppConfig,
    usd_exchange: float,
) -> dict[str, Path]:
    output_dir = ensure_directory(config.output.output_dir)
    slug = timestamp_slug(generated_at)
    written: dict[str, Path] = {}

    raw_json_path = output_dir / f"gn_productos_crudo_{slug}.json"
    raw_json_path.write_text(
        json.dumps(
            {
                "generated_at": generated_at.isoformat(),
                "usd_exchange_api": usd_exchange,
                "records": catalog,
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    written["json"] = raw_json_path

    raw_frame = _build_raw_frame(catalog)

    if config.output.include_xlsx:
        xlsx_path = output_dir / f"gn_productos_crudo_{slug}.xlsx"
        with pd.ExcelWriter(xlsx_path, engine="openpyxl") as writer:
            raw_frame.to_excel(writer, sheet_name="gn_crudo", index=False)
            _format_worksheet(writer.book["gn_crudo"])
        written["xlsx"] = xlsx_path

    if config.output.include_csv:
        csv_path = output_dir / f"gn_productos_crudo_{slug}.csv"
        raw_frame.to_csv(csv_path, index=False, sep=";", encoding="utf-8-sig")
        written["csv"] = csv_path

    return written


def write_comparison_workbook(
    summary: pd.DataFrame,
    changes: pd.DataFrame,
    output_path: Path,
) -> Path:
    ensure_directory(output_path.parent)
    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        summary.to_excel(writer, sheet_name="resumen", index=False)
        changes.to_excel(writer, sheet_name="cambios", index=False)
        _format_worksheet(writer.book["resumen"])
        _format_worksheet(writer.book["cambios"])
    return output_path


def _format_worksheet(worksheet: object) -> None:
    worksheet.freeze_panes = "A2"
    worksheet.auto_filter.ref = worksheet.dimensions

    for idx, column_cells in enumerate(worksheet.columns, start=1):
        max_length = 0
        for cell in column_cells:
            value = "" if cell.value is None else str(cell.value)
            max_length = max(max_length, len(value))
        worksheet.column_dimensions[get_column_letter(idx)].width = min(max(max_length + 2, 12), 60)


def _build_raw_frame(catalog: list[dict[str, object]]) -> pd.DataFrame:
    if not catalog:
        return pd.DataFrame()

    normalized_rows: list[dict[str, object]] = []
    for item in catalog:
        row: dict[str, object] = {}
        for key, value in item.items():
            if isinstance(value, (list, dict)):
                row[key] = json.dumps(value, ensure_ascii=False)
            else:
                row[key] = value
        normalized_rows.append(row)

    return pd.DataFrame(normalized_rows)
