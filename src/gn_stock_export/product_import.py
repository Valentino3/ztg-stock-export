from __future__ import annotations

import ast
from dataclasses import dataclass
from pathlib import Path
import json
import re
import unicodedata

import pandas as pd

from gn_stock_export.config import AppConfig
from gn_stock_export.template_contract import PRODUCT_TEMPLATE_COLUMNS

PLACEHOLDER_BRANDS = {
    "0 cargar una marca",
    "sin marca",
    "n/a",
    "na",
}


@dataclass(frozen=True)
class CategoryMapping:
    target_category: str
    target_subcategory: str
    target_category_id: int | None


@dataclass(frozen=True)
class PreparedProduct:
    item_id: str
    handle: str
    name: str
    category_path: str
    category_name: str
    subcategory_name: str
    category_id: int | None
    price: float
    cost: float
    weight_kg: float
    height_cm: float
    width_cm: float
    depth_cm: float
    stock: int
    sku: str
    barcode: str
    published: bool
    free_shipping: bool
    description: str
    tags: str
    seo_title: str
    seo_description: str
    brand: str
    physical: bool
    mpn: str
    gender: str
    age_range: str
    image_urls: list[str]


def prepare_products(stock_frame: pd.DataFrame, config: AppConfig) -> list[PreparedProduct]:
    brand_map = _load_brand_map(config.mappings.brand_map_csv)
    category_map = _load_category_map(config.mappings.category_map_csv)
    rows: list[PreparedProduct] = []

    for _, row in stock_frame.fillna("").iterrows():
        item_id = _as_text(row.get("item_id"))
        category_mapping = _map_category(
            _as_text(row.get("categoria")),
            _as_text(row.get("subcategoria")),
            category_map,
        )
        brand = _build_brand(
            _as_text(row.get("marca")),
            brand_map,
            config.content.default_brand_when_empty,
        )
        name = _normalize_text(
            _first_non_empty(
                _as_text(row.get("descripcion_corta")),
                _as_text(row.get("descripcion_larga")),
                _as_text(row.get("item_desc_0")),
                _as_text(row.get("codigo")),
                f"producto-{item_id}" if item_id else "producto",
            )
        )
        stock_total = _as_int(row.get("stock_total"))
        part_number = _as_text(row.get("partNumber"))
        code = _as_text(row.get("codigo"))
        sku = _first_non_empty(part_number, code, item_id)
        description_base = _normalize_text(_first_non_empty(_as_text(row.get("descripcion_larga")), name))
        description = _build_description(
            description_base,
            config.content.description_prefix,
            config.content.description_suffix,
        )
        seo_title = _truncate_text(name, config.content.seo_title_max_length)
        seo_description = _truncate_text(
            _strip_html_like(description),
            config.content.seo_description_max_length,
        )

        rows.append(
            PreparedProduct(
                item_id=item_id,
                handle=_build_managed_handle(item_id, config.tiendanube_sync.handle_prefix),
                name=name,
                category_path=_build_category_path(category_mapping.target_category, category_mapping.target_subcategory),
                category_name=category_mapping.target_category,
                subcategory_name=category_mapping.target_subcategory,
                category_id=category_mapping.target_category_id,
                price=round(_as_float(row.get("precio_final_ars")), 2),
                cost=round(_resolve_cost(row, config.pricing.cost_field_mode), 2),
                weight_kg=round(_as_float(row.get("peso_gr")) / 1000, 3),
                height_cm=round(_as_float(row.get("alto_cm")), 2),
                width_cm=round(_as_float(row.get("ancho_cm")), 2),
                depth_cm=round(_as_float(row.get("largo_cm")), 2),
                stock=stock_total,
                sku=sku,
                barcode=_as_text(row.get("ean")),
                published=_should_publish(stock_total, config),
                free_shipping=config.publication.free_shipping,
                description=description,
                tags=_build_tags(category_mapping.target_category, category_mapping.target_subcategory, brand),
                seo_title=seo_title,
                seo_description=seo_description,
                brand=brand,
                physical=config.publication.product_physical,
                mpn=part_number,
                gender="",
                age_range="",
                image_urls=_extract_image_urls(row),
            )
        )

    return rows


def build_product_import_frame(stock_frame: pd.DataFrame, config: AppConfig) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for product in prepare_products(stock_frame, config):
        rows.append(
            {
                "Identificador de URL": product.handle,
                "Nombre": product.name,
                "Categorías": product.category_path,
                "Nombre de propiedad 1": "",
                "Valor de propiedad 1": "",
                "Nombre de propiedad 2": "",
                "Valor de propiedad 2": "",
                "Nombre de propiedad 3": "",
                "Valor de propiedad 3": "",
                "Precio": product.price,
                "Precio promocional": "",
                "Peso (kg)": product.weight_kg,
                "Alto (cm)": product.height_cm,
                "Ancho (cm)": product.width_cm,
                "Profundidad (cm)": product.depth_cm,
                "Stock": product.stock,
                "SKU": product.sku,
                "Código de barras": product.barcode,
                "Mostrar en tienda": "SI" if product.published else "NO",
                "Envío sin cargo": "SI" if product.free_shipping else "NO",
                "Descripción": product.description,
                "Tags": product.tags,
                "Título para SEO": product.seo_title,
                "Descripción para SEO": product.seo_description,
                "Marca": product.brand,
                "Producto Físico": "SI" if product.physical else "NO",
                "MPN (Número de pieza del fabricante)": product.mpn,
                "Sexo": product.gender,
                "Rango de edad": product.age_range,
                "Costo": product.cost,
            }
        )

    return pd.DataFrame(rows, columns=PRODUCT_TEMPLATE_COLUMNS)


def _load_brand_map(path: Path) -> dict[str, str]:
    resolved = Path(path)
    if not resolved.exists():
        return {}

    frame = pd.read_csv(resolved, keep_default_na=False)
    expected = {"source_brand", "target_brand"}
    if not expected.issubset(frame.columns):
        raise ValueError(f"{resolved.name} debe tener columnas source_brand,target_brand.")

    mapping: dict[str, str] = {}
    for _, row in frame.iterrows():
        source_brand = _normalize_key(_as_text(row.get("source_brand")))
        target_brand = _normalize_text(_as_text(row.get("target_brand")))
        if source_brand:
            mapping[source_brand] = target_brand
    return mapping


def _load_category_map(path: Path) -> dict[tuple[str, str], CategoryMapping]:
    resolved = Path(path)
    if not resolved.exists():
        return {}

    frame = pd.read_csv(resolved, keep_default_na=False)
    expected = {"source_category", "source_subcategory", "target_category", "target_subcategory"}
    if not expected.issubset(frame.columns):
        raise ValueError(
            f"{resolved.name} debe tener columnas source_category,source_subcategory,target_category,target_subcategory."
        )

    mapping: dict[tuple[str, str], CategoryMapping] = {}
    for _, row in frame.iterrows():
        source_category = _normalize_key(_as_text(row.get("source_category")))
        source_subcategory = _normalize_key(_as_text(row.get("source_subcategory")))
        if not source_category:
            continue
        mapping[(source_category, source_subcategory)] = CategoryMapping(
            target_category=_normalize_text(_as_text(row.get("target_category"))),
            target_subcategory=_normalize_text(_as_text(row.get("target_subcategory"))),
            target_category_id=_optional_int(row.get("target_category_id")),
        )
    return mapping


def _map_category(
    category: str,
    subcategory: str,
    category_map: dict[tuple[str, str], CategoryMapping],
) -> CategoryMapping:
    exact_key = (_normalize_key(category), _normalize_key(subcategory))
    category_only_key = (_normalize_key(category), "")
    if exact_key in category_map:
        return category_map[exact_key]
    if category_only_key in category_map:
        return category_map[category_only_key]
    return CategoryMapping(
        target_category=_normalize_text(category),
        target_subcategory=_normalize_text(subcategory),
        target_category_id=None,
    )


def _build_brand(raw_brand: str, brand_map: dict[str, str], default_brand: str) -> str:
    brand = _clean_brand(raw_brand)
    mapped = brand_map.get(_normalize_key(brand), brand)
    if mapped:
        return _normalize_text(mapped)
    return _normalize_text(default_brand)


def _build_managed_handle(item_id: str, handle_prefix: str) -> str:
    normalized_prefix = unicodedata.normalize("NFKD", handle_prefix)
    ascii_prefix = normalized_prefix.encode("ascii", "ignore").decode("ascii").lower()
    slug_prefix = re.sub(r"[^a-z0-9]+", "-", ascii_prefix).strip("-") or "gn"
    if item_id:
        return f"{slug_prefix}-{item_id}"
    return slug_prefix


def _build_category_path(category: str, subcategory: str) -> str:
    if category and subcategory and category != subcategory:
        return f"{category} > {subcategory}"
    return category or subcategory


def _build_tags(*values: str) -> str:
    clean_values: list[str] = []
    for value in values:
        normalized = _normalize_text(value)
        if normalized and normalized not in clean_values:
            clean_values.append(normalized)
    return ", ".join(clean_values)


def _build_description(base_description: str, prefix: str, suffix: str) -> str:
    parts = [_normalize_text(prefix), _normalize_text(base_description), _normalize_text(suffix)]
    return " ".join(part for part in parts if part)


def _clean_brand(value: str) -> str:
    normalized = _normalize_text(value)
    if normalized.lower() in PLACEHOLDER_BRANDS:
        return ""
    return normalized


def _should_publish(stock_total: int, config: AppConfig) -> bool:
    if not config.publication.publish_with_stock_only:
        return True
    return stock_total >= config.publication.min_stock_to_publish


def _resolve_cost(row: pd.Series, cost_field_mode: str) -> float:
    if cost_field_mode == "ars_neto":
        return _as_float(row.get("precio_neto_ars"))
    if cost_field_mode == "ars_final":
        return _as_float(row.get("precio_final_ars"))
    return _as_float(row.get("precioNeto_USD"))


def _extract_image_urls(row: pd.Series) -> list[str]:
    raw_urls = row.get("url_imagenes")
    if isinstance(raw_urls, str) and raw_urls.strip():
        try:
            payload = json.loads(raw_urls)
            if isinstance(payload, list):
                normalized: list[str] = []
                for item in payload:
                    candidate = _normalize_image_candidate(item)
                    if candidate and candidate not in normalized:
                        normalized.append(candidate)
                if normalized:
                    return normalized
        except ValueError:
            pass

    joined = _as_text(row.get("imagenes"))
    if joined:
        values = []
        for part in joined.split("|"):
            candidate = _normalize_image_candidate(part)
            if candidate and candidate not in values:
                values.append(candidate)
        if values:
            return values

    primary = _normalize_image_candidate(row.get("imagen_principal"))
    if primary:
        return [primary]

    return []


def _normalize_image_candidate(value: object) -> str:
    if isinstance(value, dict):
        return _normalize_text(_as_text(value.get("url")))

    if isinstance(value, str):
        candidate = _normalize_text(value)
    else:
        candidate = _normalize_text(_as_text(value))

    if not candidate:
        return ""

    if candidate.startswith("{") and candidate.endswith("}"):
        try:
            parsed = ast.literal_eval(candidate)
        except (SyntaxError, ValueError):
            return candidate
        if isinstance(parsed, dict):
            return _normalize_text(_as_text(parsed.get("url")))

    return candidate


def _strip_html_like(value: str) -> str:
    without_tags = re.sub(r"<[^>]+>", " ", value)
    return _normalize_text(without_tags)


def _truncate_text(value: str, max_length: int) -> str:
    normalized = _normalize_text(value)
    if len(normalized) <= max_length:
        return normalized
    truncated = normalized[:max_length].rstrip()
    if " " not in truncated:
        return truncated
    return truncated.rsplit(" ", 1)[0].rstrip()


def _normalize_text(value: str) -> str:
    return re.sub(r"\s+", " ", value).strip()


def _normalize_key(value: str) -> str:
    return _normalize_text(value).lower()


def _first_non_empty(*values: str) -> str:
    for value in values:
        if value:
            return value
    return ""


def _as_text(value: object) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _as_float(value: object) -> float:
    if value in (None, ""):
        return 0.0
    return float(value)


def _as_int(value: object) -> int:
    if value in (None, ""):
        return 0
    return int(float(value))


def _optional_int(value: object) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(float(str(value)))
    except (TypeError, ValueError):
        return None
