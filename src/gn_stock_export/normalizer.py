from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR, ROUND_HALF_UP
import json
from typing import Any

import pandas as pd

from gn_stock_export.config import AppConfig


EXPORT_COLUMNS = [
    "item_id",
    "codigo",
    "ean",
    "partNumber",
    "marca",
    "categoria",
    "subcategoria",
    "item_desc_0",
    "item_desc_1",
    "item_desc_2",
    "descripcion_corta",
    "descripcion_larga",
    "peso_gr",
    "alto_cm",
    "ancho_cm",
    "largo_cm",
    "volumen_cm3",
    "stock_mdp",
    "stock_caba",
    "stock_total",
    "disponible",
    "precioNeto_USD",
    "cotizacion_usd",
    "precio_neto_ars",
    "impuestos_pct_total",
    "impuestos_detalle",
    "precio_final_ars",
    "impuestos",
    "url_imagenes",
    "imagen_principal",
    "imagenes",
    "fecha_exportacion",
]


def build_export_frame(
    catalog: list[dict[str, Any]],
    usd_exchange: float,
    config: AppConfig,
    exported_at: datetime | None = None,
) -> pd.DataFrame:
    generated_at = exported_at or datetime.now(timezone.utc)
    timestamp = generated_at.astimezone(timezone.utc).replace(microsecond=0).isoformat()
    rows: list[dict[str, Any]] = []

    for item in catalog:
        taxes = item.get("impuestos") or []
        image_urls = _normalize_url_list(item.get("url_imagenes"))
        stock_mdp = _as_int(item.get("stock_mdp"))
        stock_caba = _as_int(item.get("stock_caba"))
        stock_total = stock_mdp + stock_caba
        precio_neto_usd = _as_float(item.get("precioNeto_USD"))
        impuestos_pct_total = sum(_as_float(tax.get("imp_porcentaje")) for tax in taxes if isinstance(tax, dict))
        precio_neto_ars = precio_neto_usd * usd_exchange
        precio_base_gn_ars = precio_neto_ars * (1 + impuestos_pct_total / 100.0)
        precio_con_margen = (
            precio_base_gn_ars * (1 + config.pricing.margin_pct / 100.0) + config.pricing.fixed_markup_ars
        )
        precio_final_ars = apply_rounding(precio_con_margen, config.pricing.rounding_step, config.pricing.rounding_mode)

        desc0 = _as_text(item.get("item_desc_0"))
        desc1 = _as_text(item.get("item_desc_1"))
        desc2 = _as_text(item.get("item_desc_2"))
        descripcion_larga = " ".join(part for part in [desc0, desc1, desc2] if part)
        descripcion_corta = desc0 or desc1 or desc2

        rows.append(
            {
                "item_id": _as_int(item.get("item_id")),
                "codigo": _as_text(item.get("codigo")),
                "ean": _as_text(item.get("ean")),
                "partNumber": _as_text(item.get("partNumber")),
                "marca": _as_text(item.get("marca")),
                "categoria": _as_text(item.get("categoria")),
                "subcategoria": _as_text(item.get("subcategoria")),
                "item_desc_0": desc0,
                "item_desc_1": desc1,
                "item_desc_2": desc2,
                "descripcion_corta": descripcion_corta,
                "descripcion_larga": descripcion_larga,
                "peso_gr": _as_float(item.get("peso_gr")),
                "alto_cm": _as_float(item.get("alto_cm")),
                "ancho_cm": _as_float(item.get("ancho_cm")),
                "largo_cm": _as_float(item.get("largo_cm")),
                "volumen_cm3": _as_float(item.get("volumen_cm3")),
                "stock_mdp": stock_mdp,
                "stock_caba": stock_caba,
                "stock_total": stock_total,
                "disponible": stock_total > 0,
                "precioNeto_USD": precio_neto_usd,
                "cotizacion_usd": usd_exchange,
                "precio_neto_ars": round(precio_neto_ars, 4),
                "impuestos_pct_total": round(impuestos_pct_total, 4),
                "impuestos_detalle": _format_tax_details(taxes),
                "precio_final_ars": precio_final_ars,
                "impuestos": json.dumps(taxes, ensure_ascii=False),
                "url_imagenes": json.dumps(image_urls, ensure_ascii=False),
                "imagen_principal": image_urls[0] if image_urls else "",
                "imagenes": " | ".join(image_urls),
                "fecha_exportacion": timestamp,
            }
        )

    frame = pd.DataFrame(rows, columns=EXPORT_COLUMNS)
    if not frame.empty:
        frame = frame.sort_values(by=["marca", "descripcion_corta", "item_id"], kind="stable").reset_index(drop=True)
    return frame


def apply_rounding(value: float, step: float, mode: str) -> float:
    if step <= 0:
        raise ValueError("El paso de redondeo debe ser mayor a 0.")

    rounding_map = {
        "nearest": ROUND_HALF_UP,
        "up": ROUND_CEILING,
        "down": ROUND_FLOOR,
    }
    if mode not in rounding_map:
        raise ValueError(f"Modo de redondeo invalido: {mode}")

    decimal_value = Decimal(str(value))
    decimal_step = Decimal(str(step))
    units = (decimal_value / decimal_step).quantize(Decimal("1"), rounding=rounding_map[mode])
    return float((units * decimal_step).normalize())


def _format_tax_details(taxes: list[dict[str, Any]]) -> str:
    formatted: list[str] = []
    for tax in taxes:
        if not isinstance(tax, dict):
            continue
        desc = _as_text(tax.get("imp_desc"))
        pct = _as_float(tax.get("imp_porcentaje"))
        if desc:
            formatted.append(f"{desc}: {pct:g}%")
        else:
            formatted.append(f"{pct:g}%")
    return " | ".join(formatted)


def _normalize_url_list(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    return [str(item).strip() for item in value if str(item).strip()]


def _as_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _as_float(value: Any) -> float:
    if value in (None, ""):
        return 0.0
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _as_int(value: Any) -> int:
    if value in (None, ""):
        return 0
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return 0
