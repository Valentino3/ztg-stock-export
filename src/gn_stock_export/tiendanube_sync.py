from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from openpyxl.utils import get_column_letter
import pandas as pd

from gn_stock_export.config import AppConfig, TiendaNubeCredentials
from gn_stock_export.product_import import PreparedProduct, prepare_products
from gn_stock_export.storage import ensure_directory, timestamp_slug
from gn_stock_export.tiendanube_api import TiendaNubeApiClient, TiendaNubeApiError


@dataclass
class TiendaNubeSyncRun:
    generated_at: datetime
    dry_run: bool
    row_count: int
    report_paths: dict[str, Path]
    state_path: Path
    counts: dict[str, int]


@dataclass
class TiendaNubeImageRetryRun:
    generated_at: datetime
    failures_path: Path
    row_count: int
    report_paths: dict[str, Path]
    state_path: Path
    counts: dict[str, int]


@dataclass
class _StateEntry:
    item_id: str
    handle: str
    product_id: int | None
    variant_id: int | None
    uploaded_gn_images: list[str]
    last_synced_at: str
    last_result: str


def run_tiendanube_sync(
    *,
    stock_frame: pd.DataFrame,
    config: AppConfig,
    credentials: TiendaNubeCredentials,
    workspace_dir: Path,
    dry_run: bool,
    limit: int | None,
    images_only: bool = False,
    api_client_class: type[TiendaNubeApiClient] = TiendaNubeApiClient,
) -> TiendaNubeSyncRun:
    if not config.tiendanube_sync.enabled:
        raise ValueError("La sincronizacion con Tienda Nube esta deshabilitada en `config.toml`.")

    generated_at = datetime.now(timezone.utc)
    prepared_products = prepare_products(stock_frame, config)
    if limit is not None and limit > 0:
        prepared_products = prepared_products[:limit]

    report_dir = ensure_directory(config.output.output_dir / "tiendanube_sync")
    state_path = workspace_dir / "snapshots" / "tiendanube_sync_state.json"
    state = _load_sync_state(state_path)
    report_rows: list[dict[str, object]] = []

    with api_client_class(credentials=credentials) as client:
        all_products = client.list_all_products()
        all_by_handle = {handle: product for product in all_products if (handle := _extract_handle(product))}
        managed_by_handle = {
            handle: product
            for handle, product in all_by_handle.items()
            if _has_managed_tag(product, config.tiendanube_sync.managed_tag)
        }

        current_handles = {product.handle for product in prepared_products}
        for product in prepared_products:
            row = _sync_single_product(
                client=client,
                product=product,
                config=config,
                dry_run=dry_run,
                all_by_handle=all_by_handle,
                state=state,
                images_only=images_only,
            )
            report_rows.append(row)

        if not images_only and config.tiendanube_sync.unpublish_missing:
            missing_handles = sorted(set(managed_by_handle) - current_handles)
            for handle in missing_handles:
                row = _unpublish_missing_product(
                    client=client,
                    product=managed_by_handle[handle],
                    state=state,
                    dry_run=dry_run,
                )
                report_rows.append(row)

    if not dry_run:
        _save_sync_state(state_path, state)

    report_paths, counts = _write_sync_report(report_rows, generated_at, report_dir, dry_run=dry_run, images_only=images_only)
    return TiendaNubeSyncRun(
        generated_at=generated_at,
        dry_run=dry_run,
        row_count=len(prepared_products),
        report_paths=report_paths,
        state_path=state_path,
        counts=counts,
    )


def _sync_single_product(
    *,
    client: TiendaNubeApiClient,
    product: PreparedProduct,
    config: AppConfig,
    dry_run: bool,
    all_by_handle: dict[str, dict[str, Any]],
    state: dict[str, _StateEntry],
    images_only: bool,
) -> dict[str, object]:
    existing = all_by_handle.get(product.handle)
    if existing is None:
        if images_only:
            return _report_row(
                product=product,
                action="SKIP",
                status="NOT_FOUND_IN_TN",
                details="No existe el producto en Tienda Nube para sincronizar solo imagenes.",
            )
        if dry_run:
            return _report_row(
                product=product,
                action="CREATE",
                status="DRY_RUN_CREATE",
                details="Se crearia el producto en Tienda Nube.",
            )

        created = client.create_product(_build_product_payload(product, config))
        created_variant = _extract_primary_variant(created)
        image_result = _sync_images(
            client=client,
            product=product,
            product_id=_as_int(created.get("id")),
            state=state,
            dry_run=False,
            image_mode=config.tiendanube_sync.image_mode,
        )
        state[product.handle] = _StateEntry(
            item_id=product.item_id,
            handle=product.handle,
            product_id=_as_int(created.get("id")),
            variant_id=_as_int(created_variant.get("id")),
            uploaded_gn_images=image_result["uploaded_gn_images"],
            last_synced_at=datetime.now(timezone.utc).isoformat(),
            last_result="CREATED",
        )
        return _report_row(
            product=product,
            action="CREATE",
            status="CREATED",
            product_id=_as_int(created.get("id")),
            variant_id=_as_int(created_variant.get("id")),
            image_count=len(image_result["new_images"]),
            details=image_result["details"] or "Producto creado correctamente.",
            image_failures=image_result["image_failures"],
        )

    if not _has_managed_tag(existing, config.tiendanube_sync.managed_tag):
        return _report_row(
            product=product,
            action="SKIP",
            status="NOT_MANAGED",
            product_id=_as_int(existing.get("id")),
            details="Existe un producto con el mismo handle pero no esta marcado como gestionado por la app.",
        )

    existing_variant = _extract_primary_variant(existing)
    base_changes = _detect_base_changes(existing, product, config.tiendanube_sync.managed_tag)
    variant_changes = _detect_variant_changes(existing_variant, product)
    image_result = _sync_images(
        client=client,
        product=product,
        product_id=_as_int(existing.get("id")),
        state=state,
        dry_run=dry_run,
        image_mode=config.tiendanube_sync.image_mode,
    )

    if images_only:
        if dry_run:
            return _report_row(
                product=product,
                action="SYNC_IMAGES",
                status="DRY_RUN_IMAGE_SYNC" if image_result["new_images"] else "NO_CHANGES",
                product_id=_as_int(existing.get("id")),
                variant_id=_as_int(existing_variant.get("id")),
                image_count=len(image_result["new_images"]),
                details=image_result["details"],
                image_failures=image_result["image_failures"],
            )

        state[product.handle] = _StateEntry(
            item_id=product.item_id,
            handle=product.handle,
            product_id=_as_int(existing.get("id")),
            variant_id=_as_int(existing_variant.get("id")),
            uploaded_gn_images=image_result["uploaded_gn_images"],
            last_synced_at=datetime.now(timezone.utc).isoformat(),
            last_result="SYNCED_IMAGES",
        )
        return _report_row(
            product=product,
            action="SYNC_IMAGES",
            status="SYNCED_IMAGES" if image_result["new_images"] else "NO_CHANGES",
            product_id=_as_int(existing.get("id")),
            variant_id=_as_int(existing_variant.get("id")),
            image_count=len(image_result["new_images"]),
            details=image_result["details"],
            image_failures=image_result["image_failures"],
        )

    if dry_run:
        changes = base_changes + variant_changes
        if image_result["new_images"]:
            changes.append("images")
        return _report_row(
            product=product,
            action="UPDATE" if changes else "NOOP",
            status="DRY_RUN_UPDATE" if changes else "NO_CHANGES",
            product_id=_as_int(existing.get("id")),
            variant_id=_as_int(existing_variant.get("id")),
            image_count=len(image_result["new_images"]),
            details=", ".join(changes) if changes else "Sin cambios.",
            image_failures=image_result["image_failures"],
        )

    if base_changes:
        client.update_product(_as_int(existing.get("id")), _build_product_payload(product, config, include_variant=False))

    if variant_changes:
        variant_payload = dict(existing_variant)
        variant_payload.update(_build_variant_payload(product))
        client.update_variant(_as_int(existing.get("id")), _as_int(existing_variant.get("id")), variant_payload)

    state[product.handle] = _StateEntry(
        item_id=product.item_id,
        handle=product.handle,
        product_id=_as_int(existing.get("id")),
        variant_id=_as_int(existing_variant.get("id")),
        uploaded_gn_images=image_result["uploaded_gn_images"],
        last_synced_at=datetime.now(timezone.utc).isoformat(),
        last_result="UPDATED" if (base_changes or variant_changes or image_result["new_images"]) else "NO_CHANGES",
    )
    changes = base_changes + variant_changes
    if image_result["new_images"]:
        changes.append("images")
    return _report_row(
        product=product,
        action="UPDATE" if changes else "NOOP",
        status="UPDATED" if changes else "NO_CHANGES",
        product_id=_as_int(existing.get("id")),
        variant_id=_as_int(existing_variant.get("id")),
        image_count=len(image_result["new_images"]),
        details=", ".join(changes) if changes else "Sin cambios.",
        image_failures=image_result["image_failures"],
    )


def run_tiendanube_failed_image_retry(
    *,
    config: AppConfig,
    credentials: TiendaNubeCredentials,
    workspace_dir: Path,
    failures_path: Path | None = None,
    api_client_class: type[TiendaNubeApiClient] = TiendaNubeApiClient,
) -> TiendaNubeImageRetryRun:
    generated_at = datetime.now(timezone.utc)
    report_dir = ensure_directory(config.output.output_dir / "tiendanube_sync")
    failures_path = failures_path or _latest_image_failures_path(report_dir)
    failures = _read_image_failure_report(failures_path)
    state_path = workspace_dir / "snapshots" / "tiendanube_sync_state.json"
    state = _load_sync_state(state_path)
    report_rows: list[dict[str, object]] = []

    with api_client_class(credentials=credentials) as client:
        all_products = client.list_all_products()
        all_by_handle = {handle: product for product in all_products if (handle := _extract_handle(product))}

        for failure in failures:
            row = _retry_failed_image(
                client=client,
                failure=failure,
                all_by_handle=all_by_handle,
                state=state,
            )
            report_rows.append(row)

    _save_sync_state(state_path, state)
    report_paths, counts = _write_image_retry_report(report_rows, generated_at, report_dir)
    return TiendaNubeImageRetryRun(
        generated_at=generated_at,
        failures_path=failures_path,
        row_count=len(failures),
        report_paths=report_paths,
        state_path=state_path,
        counts=counts,
    )


def _unpublish_missing_product(
    *,
    client: TiendaNubeApiClient,
    product: dict[str, Any],
    state: dict[str, _StateEntry],
    dry_run: bool,
) -> dict[str, object]:
    handle = _extract_handle(product)
    name = _localized_text(product.get("name"))
    variant = _extract_primary_variant(product)
    item_id = handle.split("-")[-1] if handle else ""

    if dry_run:
        return {
            "item_id": item_id,
            "handle": handle,
            "name": name,
            "action": "UNPUBLISH",
            "status": "DRY_RUN_UNPUBLISH",
            "product_id": _as_int(product.get("id")),
            "variant_id": _as_int(variant.get("id")),
            "image_count": 0,
            "details": "Se despublicaria el producto y se dejaria stock 0.",
        }

    client.update_product(_as_int(product.get("id")), {"published": False})
    variant_payload = dict(variant)
    variant_payload["stock"] = 0
    client.update_variant(_as_int(product.get("id")), _as_int(variant.get("id")), variant_payload)

    if handle in state:
        entry = state[handle]
        state[handle] = _StateEntry(
            item_id=entry.item_id,
            handle=entry.handle,
            product_id=entry.product_id,
            variant_id=entry.variant_id,
            uploaded_gn_images=entry.uploaded_gn_images,
            last_synced_at=datetime.now(timezone.utc).isoformat(),
            last_result="UNPUBLISHED",
        )

    return {
        "item_id": item_id,
        "handle": handle,
        "name": name,
        "action": "UNPUBLISH",
        "status": "UNPUBLISHED",
        "product_id": _as_int(product.get("id")),
        "variant_id": _as_int(variant.get("id")),
        "image_count": 0,
        "details": "Producto despublicado y stock llevado a 0.",
    }


def _sync_images(
    *,
    client: TiendaNubeApiClient,
    product: PreparedProduct,
    product_id: int,
    state: dict[str, _StateEntry],
    dry_run: bool,
    image_mode: str,
) -> dict[str, object]:
    if image_mode != "append_only":
        raise ValueError(f"Modo de imagen no soportado: {image_mode}")

    entry = state.get(product.handle)
    uploaded = list(entry.uploaded_gn_images) if entry else []
    if not product.image_urls:
        return {
            "new_images": [],
            "uploaded_gn_images": uploaded,
            "image_failures": [],
            "details": "Producto sin imagenes GN.",
        }

    invalid_images = [url for url in product.image_urls if not _is_valid_image_url(url)]
    valid_images = [url for url in product.image_urls if _is_valid_image_url(url)]
    new_images = [url for url in valid_images if url not in uploaded]
    if not new_images:
        return {
            "new_images": [],
            "uploaded_gn_images": uploaded,
            "image_failures": _image_failure_entries(invalid_images, "INVALID_URL", "URL invalida."),
            "details": _join_detail_parts(
                [
                    "No hay imagenes nuevas para sincronizar.",
                    f"Se omitieron {len(invalid_images)} URL(s) invalidas." if invalid_images else "",
                ]
            ),
        }
    if dry_run:
        return {
            "new_images": new_images,
            "uploaded_gn_images": uploaded,
            "image_failures": _image_failure_entries(invalid_images, "INVALID_URL", "URL invalida."),
            "details": _join_detail_parts(
                [
                    f"Se subirian {len(new_images)} imagen(es).",
                    f"Se omitirian {len(invalid_images)} URL(s) invalidas." if invalid_images else "",
                ]
            ),
        }

    failed_images: list[dict[str, str]] = []
    for index, image_url in enumerate(new_images, start=len(uploaded) + 1):
        try:
            client.create_product_image(product_id, image_url, position=index)
        except TiendaNubeApiError as exc:
            failed_images.append(
                {
                    "image_url": image_url,
                    "failure_type": "UPLOAD_ERROR",
                    "error": str(exc),
                }
            )
            continue
        uploaded.append(image_url)

    return {
        "new_images": new_images,
        "uploaded_gn_images": uploaded,
        "image_failures": _image_failure_entries(invalid_images, "INVALID_URL", "URL invalida.") + failed_images,
        "details": _join_detail_parts(
            [
                f"Se agregaron {len(uploaded) - (len(entry.uploaded_gn_images) if entry else 0)} imagen(es).",
                f"Se omitieron {len(invalid_images)} URL(s) invalidas." if invalid_images else "",
                f"Fallaron {len(failed_images)} imagen(es) al subir." if failed_images else "",
            ]
        ),
    }


def _is_valid_image_url(value: str) -> bool:
    parsed = urlparse(_as_text(value))
    return parsed.scheme in {"http", "https"} and bool(parsed.netloc)


def _image_failure_entries(urls: list[str], failure_type: str, error: str) -> list[dict[str, str]]:
    return [
        {
            "image_url": url,
            "failure_type": failure_type,
            "error": error,
        }
        for url in urls
    ]


def _join_detail_parts(parts: list[str]) -> str:
    return " ".join(part for part in parts if part).strip()


def _build_product_payload(product: PreparedProduct, config: AppConfig, *, include_variant: bool = True) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "name": {"es": product.name},
        "description": {"es": product.description},
        "handle": {"es": product.handle},
        "brand": product.brand or None,
        "published": product.published,
        "free_shipping": product.free_shipping,
        "requires_shipping": product.physical,
        "seo_title": product.seo_title,
        "seo_description": product.seo_description,
        "tags": _merge_tags(product.tags, config.tiendanube_sync.managed_tag),
    }
    if product.category_id is not None:
        payload["categories"] = [product.category_id]
    if include_variant:
        payload["variants"] = [_build_variant_payload(product)]
    return payload


def _build_variant_payload(product: PreparedProduct) -> dict[str, Any]:
    return {
        "price": product.price,
        "stock": product.stock,
        "weight": product.weight_kg,
        "width": product.width_cm,
        "height": product.height_cm,
        "depth": product.depth_cm,
        "sku": product.sku or None,
        "barcode": product.barcode or None,
        "mpn": product.mpn or None,
        "age_group": product.age_range or None,
        "gender": product.gender or None,
        "cost": product.cost if product.cost > 0 else None,
    }


def _detect_base_changes(existing: dict[str, Any], product: PreparedProduct, managed_tag: str) -> list[str]:
    changes: list[str] = []
    if _localized_text(existing.get("name")) != product.name:
        changes.append("name")
    if _localized_text(existing.get("description")) != product.description:
        changes.append("description")
    if _localized_text(existing.get("handle")) != product.handle:
        changes.append("handle")
    if _as_text(existing.get("brand")) != product.brand:
        changes.append("brand")
    if bool(existing.get("published")) != product.published:
        changes.append("published")
    if bool(existing.get("free_shipping")) != product.free_shipping:
        changes.append("free_shipping")
    if bool(existing.get("requires_shipping", True)) != product.physical:
        changes.append("requires_shipping")
    if _as_text(existing.get("seo_title")) != product.seo_title:
        changes.append("seo_title")
    if _as_text(existing.get("seo_description")) != product.seo_description:
        changes.append("seo_description")
    if _normalize_tags(existing.get("tags")) != _normalize_tags(_merge_tags(product.tags, managed_tag)):
        changes.append("tags")
    existing_categories = sorted(_extract_category_ids(existing))
    target_categories = [product.category_id] if product.category_id is not None else []
    if existing_categories != sorted(target_categories):
        changes.append("categories")
    return changes


def _detect_variant_changes(existing_variant: dict[str, Any], product: PreparedProduct) -> list[str]:
    changes: list[str] = []
    for field, current, target in (
        ("price", _as_float(existing_variant.get("price")), product.price),
        ("stock", _as_int(existing_variant.get("stock")), product.stock),
        ("weight", _as_float(existing_variant.get("weight")), product.weight_kg),
        ("width", _as_float(existing_variant.get("width")), product.width_cm),
        ("height", _as_float(existing_variant.get("height")), product.height_cm),
        ("depth", _as_float(existing_variant.get("depth")), product.depth_cm),
        ("cost", _as_float(existing_variant.get("cost")), product.cost if product.cost > 0 else 0.0),
    ):
        if round(current, 3) != round(target, 3):
            changes.append(field)

    for field, current, target in (
        ("sku", _as_text(existing_variant.get("sku")), product.sku),
        ("barcode", _as_text(existing_variant.get("barcode")), product.barcode),
        ("mpn", _as_text(existing_variant.get("mpn")), product.mpn),
        ("age_group", _as_text(existing_variant.get("age_group")), product.age_range),
        ("gender", _as_text(existing_variant.get("gender")), product.gender),
    ):
        if current != target:
            changes.append(field)

    return changes


def _extract_primary_variant(product: dict[str, Any]) -> dict[str, Any]:
    variants = product.get("variants")
    if isinstance(variants, list) and variants:
        return variants[0]
    raise TiendaNubeApiError("El producto de Tienda Nube no tiene variantes para sincronizar.")


def _extract_handle(product: dict[str, Any]) -> str:
    handle = product.get("handle")
    return _localized_text(handle)


def _extract_category_ids(product: dict[str, Any]) -> list[int]:
    categories = product.get("categories")
    if not isinstance(categories, list):
        return []

    ids: list[int] = []
    for category in categories:
        if isinstance(category, dict):
            category_id = category.get("id")
        else:
            category_id = category
        try:
            ids.append(int(category_id))
        except (TypeError, ValueError):
            continue
    return ids


def _has_managed_tag(product: dict[str, Any], managed_tag: str) -> bool:
    tags = _normalize_tags(product.get("tags"))
    return managed_tag.lower() in tags


def _merge_tags(existing_tags: str, managed_tag: str) -> str:
    values = {tag.strip() for tag in existing_tags.split(",") if tag.strip()}
    values.add(managed_tag)
    return ", ".join(sorted(values))


def _normalize_tags(value: object) -> set[str]:
    if value is None:
        return set()
    return {part.strip().lower() for part in str(value).split(",") if part.strip()}


def _localized_text(value: object) -> str:
    if isinstance(value, dict):
        for key in ("es", "es_AR", "en", "pt"):
            if key in value and value[key]:
                return str(value[key]).strip()
        for localized_value in value.values():
            if localized_value:
                return str(localized_value).strip()
        return ""
    return _as_text(value)


def _load_sync_state(path: Path) -> dict[str, _StateEntry]:
    if not path.exists():
        return {}

    payload = json.loads(path.read_text(encoding="utf-8"))
    products = payload.get("products", {})
    state: dict[str, _StateEntry] = {}
    if not isinstance(products, dict):
        return state

    for handle, entry in products.items():
        if not isinstance(entry, dict):
            continue
        state[handle] = _StateEntry(
            item_id=_as_text(entry.get("item_id")),
            handle=_as_text(entry.get("handle")) or handle,
            product_id=_optional_int(entry.get("product_id")),
            variant_id=_optional_int(entry.get("variant_id")),
            uploaded_gn_images=[_as_text(url) for url in entry.get("uploaded_gn_images", []) if _as_text(url)],
            last_synced_at=_as_text(entry.get("last_synced_at")),
            last_result=_as_text(entry.get("last_result")),
        )
    return state


def _save_sync_state(path: Path, state: dict[str, _StateEntry]) -> Path:
    ensure_directory(path.parent)
    payload = {
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "products": {handle: asdict(entry) for handle, entry in state.items()},
    }
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def _latest_image_failures_path(report_dir: Path) -> Path:
    candidates = sorted(report_dir.glob("tiendanube_image_failures_*.csv"))
    if not candidates:
        raise ValueError(f"No encontre reportes de imagenes fallidas en {report_dir}.")
    return candidates[-1]


def _read_image_failure_report(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        raise ValueError(f"No existe el reporte de imagenes fallidas: {path}")
    frame = pd.read_csv(path, sep=";", keep_default_na=False)
    required = {"item_id", "handle", "name", "product_id", "image_url", "failure_type", "error"}
    if not required.issubset(frame.columns):
        raise ValueError(f"El reporte {path} no tiene el formato esperado de imagenes fallidas.")
    return [
        {column: _as_text(row.get(column)) for column in frame.columns}
        for _, row in frame.iterrows()
    ]


def _retry_failed_image(
    *,
    client: TiendaNubeApiClient,
    failure: dict[str, str],
    all_by_handle: dict[str, dict[str, Any]],
    state: dict[str, _StateEntry],
) -> dict[str, object]:
    item_id = _as_text(failure.get("item_id"))
    handle = _as_text(failure.get("handle"))
    name = _as_text(failure.get("name"))
    image_url = _as_text(failure.get("image_url"))
    failure_type = _as_text(failure.get("failure_type"))

    base_row = {
        "item_id": item_id,
        "handle": handle,
        "name": name,
        "product_id": _as_int(failure.get("product_id")),
        "image_url": image_url,
        "original_failure_type": failure_type,
        "original_error": _as_text(failure.get("error")),
    }

    if failure_type != "UPLOAD_ERROR":
        return {
            **base_row,
            "action": "SKIP",
            "status": "SKIP_NOT_RETRYABLE",
            "details": "La falla no es reintentable automaticamente.",
        }
    if not _is_valid_image_url(image_url):
        return {
            **base_row,
            "action": "SKIP",
            "status": "SKIP_INVALID_URL",
            "details": "La URL de imagen sigue siendo invalida.",
        }

    product_id = _as_int(failure.get("product_id"))
    existing = all_by_handle.get(handle)
    if product_id <= 0 and existing is not None:
        product_id = _as_int(existing.get("id"))
    if product_id <= 0:
        return {
            **base_row,
            "action": "SKIP",
            "status": "NOT_FOUND_IN_TN",
            "details": "No se encontro el producto en Tienda Nube para reintentar la imagen.",
        }

    entry = state.get(handle)
    if entry and image_url in entry.uploaded_gn_images:
        return {
            **base_row,
            "product_id": product_id,
            "action": "SKIP",
            "status": "ALREADY_UPLOADED",
            "details": "La imagen ya figura como subida en el estado local.",
        }

    try:
        client.create_product_image(product_id, image_url)
    except TiendaNubeApiError as exc:
        return {
            **base_row,
            "product_id": product_id,
            "action": "RETRY_IMAGE",
            "status": "FAILED_AGAIN",
            "details": str(exc),
        }

    uploaded = list(entry.uploaded_gn_images) if entry else []
    uploaded.append(image_url)
    state[handle] = _StateEntry(
        item_id=item_id,
        handle=handle,
        product_id=product_id,
        variant_id=entry.variant_id if entry else None,
        uploaded_gn_images=uploaded,
        last_synced_at=datetime.now(timezone.utc).isoformat(),
        last_result="RETRIED_IMAGE",
    )
    return {
        **base_row,
        "product_id": product_id,
        "action": "RETRY_IMAGE",
        "status": "RETRIED",
        "details": "Imagen subida correctamente al reintentar.",
    }


def _write_sync_report(
    report_rows: list[dict[str, object]],
    generated_at: datetime,
    output_dir: Path,
    *,
    dry_run: bool,
    images_only: bool,
) -> tuple[dict[str, Path], dict[str, int]]:
    ensure_directory(output_dir)
    slug = timestamp_slug(generated_at)
    mode = "dry_run" if dry_run else "productivo"
    prefix = "tiendanube_sync_imagenes" if images_only else "tiendanube_sync"

    image_failure_rows = _collect_image_failure_rows(report_rows)
    details = pd.DataFrame([{key: value for key, value in row.items() if key != "image_failures"} for row in report_rows])
    if details.empty:
        details = pd.DataFrame(columns=["item_id", "handle", "name", "action", "status", "product_id", "variant_id", "image_count", "details"])
    summary = (
        details.groupby("status", dropna=False)
        .size()
        .reset_index(name="count")
        .sort_values(by=["count", "status"], ascending=[False, True], kind="stable")
    )
    counts = {str(row["status"]): int(row["count"]) for _, row in summary.iterrows()}

    csv_path = output_dir / f"{prefix}_{mode}_{slug}.csv"
    xlsx_path = output_dir / f"{prefix}_{mode}_{slug}.xlsx"
    json_path = output_dir / f"{prefix}_{mode}_{slug}.json"

    details.to_csv(csv_path, index=False, sep=";", encoding="utf-8-sig")
    with pd.ExcelWriter(xlsx_path, engine="openpyxl") as writer:
        summary.to_excel(writer, sheet_name="resumen", index=False)
        details.to_excel(writer, sheet_name="detalle", index=False)
        _format_worksheet(writer.book["resumen"])
        _format_worksheet(writer.book["detalle"])
    json_path.write_text(
        json.dumps(
            {
                "generated_at": generated_at.isoformat(),
                "dry_run": dry_run,
                "images_only": images_only,
                "counts": counts,
                "rows": report_rows,
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    report_paths = {"csv": csv_path, "xlsx": xlsx_path, "json": json_path}
    if image_failure_rows:
        report_paths.update(_write_image_failure_report(image_failure_rows, generated_at, output_dir, dry_run=dry_run))
    return report_paths, counts


def _collect_image_failure_rows(report_rows: list[dict[str, object]]) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for row in report_rows:
        failures = row.get("image_failures", [])
        if not isinstance(failures, list):
            continue
        for failure in failures:
            if not isinstance(failure, dict):
                continue
            rows.append(
                {
                    "item_id": row.get("item_id", ""),
                    "handle": row.get("handle", ""),
                    "name": row.get("name", ""),
                    "product_id": row.get("product_id", ""),
                    "image_url": failure.get("image_url", ""),
                    "failure_type": failure.get("failure_type", ""),
                    "error": failure.get("error", ""),
                }
            )
    return rows


def _write_image_failure_report(
    rows: list[dict[str, object]],
    generated_at: datetime,
    output_dir: Path,
    *,
    dry_run: bool,
) -> dict[str, Path]:
    slug = timestamp_slug(generated_at)
    mode = "dry_run" if dry_run else "productivo"
    frame = pd.DataFrame(
        rows,
        columns=["item_id", "handle", "name", "product_id", "image_url", "failure_type", "error"],
    )
    csv_path = output_dir / f"tiendanube_image_failures_{mode}_{slug}.csv"
    xlsx_path = output_dir / f"tiendanube_image_failures_{mode}_{slug}.xlsx"
    json_path = output_dir / f"tiendanube_image_failures_{mode}_{slug}.json"
    frame.to_csv(csv_path, index=False, sep=";", encoding="utf-8-sig")
    with pd.ExcelWriter(xlsx_path, engine="openpyxl") as writer:
        frame.to_excel(writer, sheet_name="imagenes_fallidas", index=False)
        _format_worksheet(writer.book["imagenes_fallidas"])
    json_path.write_text(
        json.dumps(
            {
                "generated_at": generated_at.isoformat(),
                "dry_run": dry_run,
                "rows": rows,
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    return {
        "image_failures_csv": csv_path,
        "image_failures_xlsx": xlsx_path,
        "image_failures_json": json_path,
    }


def _write_image_retry_report(
    rows: list[dict[str, object]],
    generated_at: datetime,
    output_dir: Path,
) -> tuple[dict[str, Path], dict[str, int]]:
    slug = timestamp_slug(generated_at)
    frame = pd.DataFrame(
        rows,
        columns=[
            "item_id",
            "handle",
            "name",
            "product_id",
            "image_url",
            "original_failure_type",
            "original_error",
            "action",
            "status",
            "details",
        ],
    )
    if frame.empty:
        frame = pd.DataFrame(columns=[
            "item_id",
            "handle",
            "name",
            "product_id",
            "image_url",
            "original_failure_type",
            "original_error",
            "action",
            "status",
            "details",
        ])
    counts = frame["status"].value_counts().to_dict() if not frame.empty else {}
    csv_path = output_dir / f"tiendanube_image_retry_{slug}.csv"
    xlsx_path = output_dir / f"tiendanube_image_retry_{slug}.xlsx"
    json_path = output_dir / f"tiendanube_image_retry_{slug}.json"
    frame.to_csv(csv_path, index=False, sep=";", encoding="utf-8-sig")
    with pd.ExcelWriter(xlsx_path, engine="openpyxl") as writer:
        summary = (
            frame.groupby("status", dropna=False)
            .size()
            .reset_index(name="count")
            .sort_values(by=["count", "status"], ascending=[False, True], kind="stable")
            if not frame.empty
            else pd.DataFrame(columns=["status", "count"])
        )
        summary.to_excel(writer, sheet_name="resumen", index=False)
        frame.to_excel(writer, sheet_name="detalle", index=False)
        _format_worksheet(writer.book["resumen"])
        _format_worksheet(writer.book["detalle"])
    json_path.write_text(
        json.dumps(
            {
                "generated_at": generated_at.isoformat(),
                "counts": {str(key): int(value) for key, value in counts.items()},
                "rows": rows,
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    return {"csv": csv_path, "xlsx": xlsx_path, "json": json_path}, {str(key): int(value) for key, value in counts.items()}


def _format_worksheet(worksheet: object) -> None:
    worksheet.freeze_panes = "A2"
    worksheet.auto_filter.ref = worksheet.dimensions

    for idx, column_cells in enumerate(worksheet.columns, start=1):
        max_length = 0
        for cell in column_cells:
            value = "" if cell.value is None else str(cell.value)
            max_length = max(max_length, len(value))
        worksheet.column_dimensions[get_column_letter(idx)].width = min(max(max_length + 2, 12), 80)


def _report_row(
    *,
    product: PreparedProduct,
    action: str,
    status: str,
    details: str,
    product_id: int | None = None,
    variant_id: int | None = None,
    image_count: int = 0,
    image_failures: list[dict[str, str]] | None = None,
) -> dict[str, object]:
    return {
        "item_id": product.item_id,
        "handle": product.handle,
        "name": product.name,
        "action": action,
        "status": status,
        "product_id": product_id,
        "variant_id": variant_id,
        "image_count": image_count,
        "details": details,
        "image_failures": image_failures or [],
    }


def _as_text(value: object) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _as_float(value: object) -> float:
    if value in (None, ""):
        return 0.0
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _as_int(value: object) -> int:
    if value in (None, ""):
        return 0
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return 0


def _optional_int(value: object) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return None
