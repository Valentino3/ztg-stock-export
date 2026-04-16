from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd


CHANGE_ORDER = ["ALTA", "BAJA", "CAMBIO_STOCK", "CAMBIO_PRECIO", "CAMBIO_STOCK_Y_PRECIO"]


@dataclass
class ComparisonResult:
    base_snapshot: Path
    current_snapshot: Path
    changes: pd.DataFrame
    summary: pd.DataFrame
    counts: dict[str, int]


def compare_frames(
    previous_df: pd.DataFrame,
    current_df: pd.DataFrame,
    price_tolerance_ars: float,
    base_snapshot: Path,
    current_snapshot: Path,
) -> ComparisonResult:
    prev_index = _index_by_item_id(previous_df)
    curr_index = _index_by_item_id(current_df)
    all_item_ids = sorted(set(prev_index) | set(curr_index))

    changes: list[dict[str, Any]] = []
    counts = {status: 0 for status in CHANGE_ORDER}

    for item_id in all_item_ids:
        previous = prev_index.get(item_id)
        current = curr_index.get(item_id)

        if previous is None and current is not None:
            status = "ALTA"
        elif previous is not None and current is None:
            status = "BAJA"
        else:
            assert previous is not None and current is not None
            stock_changed = _as_int(previous.get("stock_total")) != _as_int(current.get("stock_total"))
            price_changed = (
                abs(_as_float(previous.get("precio_final_ars")) - _as_float(current.get("precio_final_ars")))
                > price_tolerance_ars
            )
            if stock_changed and price_changed:
                status = "CAMBIO_STOCK_Y_PRECIO"
            elif stock_changed:
                status = "CAMBIO_STOCK"
            elif price_changed:
                status = "CAMBIO_PRECIO"
            else:
                continue

        counts[status] += 1
        reference = current or previous or {}
        stock_prev = _as_int(previous.get("stock_total")) if previous is not None else 0
        stock_curr = _as_int(current.get("stock_total")) if current is not None else 0
        price_prev = _as_float(previous.get("precio_final_ars")) if previous is not None else 0.0
        price_curr = _as_float(current.get("precio_final_ars")) if current is not None else 0.0

        changes.append(
            {
                "estado_cambio": status,
                "item_id": item_id,
                "codigo": _as_text(reference.get("codigo")),
                "ean": _as_text(reference.get("ean")),
                "partNumber": _as_text(reference.get("partNumber")),
                "marca": _as_text(reference.get("marca")),
                "categoria": _as_text(reference.get("categoria")),
                "subcategoria": _as_text(reference.get("subcategoria")),
                "descripcion_corta": _as_text(reference.get("descripcion_corta")),
                "descripcion_larga": _as_text(reference.get("descripcion_larga")),
                "stock_total_anterior": stock_prev,
                "stock_total_actual": stock_curr,
                "stock_delta": stock_curr - stock_prev,
                "precio_final_ars_anterior": price_prev,
                "precio_final_ars_actual": price_curr,
                "precio_delta_ars": round(price_curr - price_prev, 4),
                "precioNeto_USD_anterior": _as_float(previous.get("precioNeto_USD")) if previous is not None else 0.0,
                "precioNeto_USD_actual": _as_float(current.get("precioNeto_USD")) if current is not None else 0.0,
                "disponible_anterior": bool(previous.get("disponible")) if previous is not None else False,
                "disponible_actual": bool(current.get("disponible")) if current is not None else False,
            }
        )

    changes_df = pd.DataFrame(changes)
    if not changes_df.empty:
        changes_df["estado_cambio"] = pd.Categorical(changes_df["estado_cambio"], categories=CHANGE_ORDER, ordered=True)
        changes_df = changes_df.sort_values(by=["estado_cambio", "item_id"], kind="stable").reset_index(drop=True)
        changes_df["estado_cambio"] = changes_df["estado_cambio"].astype(str)

    summary_rows = [
        {"metrica": "snapshot_anterior", "valor": str(base_snapshot.name)},
        {"metrica": "snapshot_actual", "valor": str(current_snapshot.name)},
        {"metrica": "items_snapshot_anterior", "valor": len(prev_index)},
        {"metrica": "items_snapshot_actual", "valor": len(curr_index)},
        {"metrica": "cambios_totales", "valor": len(changes_df)},
    ]
    summary_rows.extend({"metrica": status, "valor": counts[status]} for status in CHANGE_ORDER)
    summary_df = pd.DataFrame(summary_rows)

    return ComparisonResult(
        base_snapshot=base_snapshot,
        current_snapshot=current_snapshot,
        changes=changes_df,
        summary=summary_df,
        counts=counts,
    )


def _index_by_item_id(frame: pd.DataFrame) -> dict[int, dict[str, Any]]:
    if frame.empty:
        return {}
    if frame["item_id"].duplicated().any():
        raise ValueError("Hay item_id duplicados dentro del snapshot.")
    indexed = frame.set_index("item_id", drop=False)
    return {int(item_id): row.to_dict() for item_id, row in indexed.iterrows()}


def _as_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _as_float(value: Any) -> float:
    if value in (None, ""):
        return 0.0
    return float(value)


def _as_int(value: Any) -> int:
    if value in (None, ""):
        return 0
    return int(float(value))

