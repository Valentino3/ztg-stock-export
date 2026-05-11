import json
from pathlib import Path

from gn_stock_export.config import (
    AppConfig,
    ContentConfig,
    Credentials,
    DiffConfig,
    MappingConfig,
    OutputConfig,
    PricingConfig,
    PublicationConfig,
    TiendaNubeCredentials,
    TiendaNubeSyncConfig,
)
from gn_stock_export.service import StockExportService
from gn_stock_export.tiendanube_api import TiendaNubeApiError


def test_sync_tiendanube_test_generates_dry_run_report(tmp_path: Path) -> None:
    class FakeGNApiClient:
        def __init__(self, credentials: Credentials) -> None:
            self.credentials = credentials

        def __enter__(self) -> "FakeGNApiClient":
            return self

        def __exit__(self, *_args: object) -> None:
            return None

        def get_catalog(self) -> list[dict[str, object]]:
            return [_catalog_item()]

        def get_usd_exchange(self) -> float:
            return 1000.0

    class FakeTiendaNubeApiClient:
        def __init__(self, credentials: TiendaNubeCredentials) -> None:
            self.credentials = credentials

        def __enter__(self) -> "FakeTiendaNubeApiClient":
            return self

        def __exit__(self, *_args: object) -> None:
            return None

        def list_all_products(self) -> list[dict[str, object]]:
            return []

    config = _make_sync_config(tmp_path, test_limit=1)
    service = StockExportService(
        workspace_dir=tmp_path,
        config=config,
        credentials=Credentials(client_id=1, username="demo", password="secret"),
        tiendanube_credentials=TiendaNubeCredentials(store_id=10, access_token="token", user_agent="tests"),
        api_client_class=FakeGNApiClient,
        tiendanube_api_client_class=FakeTiendaNubeApiClient,
    )

    result = service.sync_tiendanube_test()

    assert result.dry_run is True
    assert result.row_count == 1
    assert result.snapshot_path.exists()
    assert result.report_paths["csv"].exists()
    assert result.report_paths["xlsx"].exists()
    assert result.counts["DRY_RUN_CREATE"] == 1
    assert not result.state_path.exists()


def test_sync_tiendanube_creates_product_and_persists_state(tmp_path: Path) -> None:
    class FakeGNApiClient:
        def __init__(self, credentials: Credentials) -> None:
            self.credentials = credentials

        def __enter__(self) -> "FakeGNApiClient":
            return self

        def __exit__(self, *_args: object) -> None:
            return None

        def get_catalog(self) -> list[dict[str, object]]:
            return [_catalog_item()]

        def get_usd_exchange(self) -> float:
            return 1000.0

    class FakeTiendaNubeApiClient:
        create_calls = 0
        image_calls = 0

        def __init__(self, credentials: TiendaNubeCredentials) -> None:
            self.credentials = credentials

        def __enter__(self) -> "FakeTiendaNubeApiClient":
            return self

        def __exit__(self, *_args: object) -> None:
            return None

        def list_all_products(self) -> list[dict[str, object]]:
            return []

        def create_product(self, payload: dict[str, object]) -> dict[str, object]:
            type(self).create_calls += 1
            return {
                "id": 101,
                "handle": {"es": payload["handle"]["es"]},
                "tags": payload["tags"],
                "variants": [{"id": 501}],
            }

        def create_product_image(self, product_id: int, src: str, *, position: int | None = None) -> dict[str, object]:
            type(self).image_calls += 1
            return {"id": 900, "product_id": product_id, "src": src, "position": position}

    config = _make_sync_config(tmp_path, test_limit=20)
    service = StockExportService(
        workspace_dir=tmp_path,
        config=config,
        credentials=Credentials(client_id=1, username="demo", password="secret"),
        tiendanube_credentials=TiendaNubeCredentials(store_id=10, access_token="token", user_agent="tests"),
        api_client_class=FakeGNApiClient,
        tiendanube_api_client_class=FakeTiendaNubeApiClient,
    )

    result = service.sync_tiendanube()

    assert result.dry_run is False
    assert result.report_paths["json"].exists()
    assert result.counts["CREATED"] == 1
    assert FakeTiendaNubeApiClient.create_calls == 1
    assert FakeTiendaNubeApiClient.image_calls == 1
    assert result.state_path.exists()

    payload = json.loads(result.state_path.read_text(encoding="utf-8"))
    assert payload["products"]["gn-1"]["product_id"] == 101
    assert payload["products"]["gn-1"]["variant_id"] == 501
    assert payload["products"]["gn-1"]["uploaded_gn_images"] == ["https://example.com/image-1.jpg"]


def test_sync_tiendanube_auto_creates_missing_categories_and_assigns_product(tmp_path: Path) -> None:
    class FakeGNApiClient:
        def __init__(self, credentials: Credentials) -> None:
            self.credentials = credentials

        def __enter__(self) -> "FakeGNApiClient":
            return self

        def __exit__(self, *_args: object) -> None:
            return None

        def get_catalog(self) -> list[dict[str, object]]:
            return [_catalog_item()]

        def get_usd_exchange(self) -> float:
            return 1000.0

    class FakeTiendaNubeApiClient:
        last_payload: dict[str, object] | None = None
        created_categories: list[dict[str, object]] = []

        def __init__(self, credentials: TiendaNubeCredentials) -> None:
            self.credentials = credentials

        def __enter__(self) -> "FakeTiendaNubeApiClient":
            return self

        def __exit__(self, *_args: object) -> None:
            return None

        def list_all_products(self) -> list[dict[str, object]]:
            return []

        def list_all_categories(self) -> list[dict[str, object]]:
            return []

        def create_category(self, name: str, *, parent_id: int | None = None) -> dict[str, object]:
            category_id = 201 + len(type(self).created_categories)
            category = {"id": category_id, "name": {"es": name}, "parent": parent_id}
            type(self).created_categories.append(category)
            return category

        def create_product(self, payload: dict[str, object]) -> dict[str, object]:
            type(self).last_payload = payload
            return {
                "id": 101,
                "handle": {"es": payload["handle"]["es"]},
                "tags": payload["tags"],
                "variants": [{"id": 501}],
            }

        def create_product_image(self, product_id: int, src: str, *, position: int | None = None) -> dict[str, object]:
            return {"id": 900, "product_id": product_id, "src": src, "position": position}

    config = _make_sync_config(tmp_path, test_limit=20, with_category_id=False)
    service = StockExportService(
        workspace_dir=tmp_path,
        config=config,
        credentials=Credentials(client_id=1, username="demo", password="secret"),
        tiendanube_credentials=TiendaNubeCredentials(store_id=10, access_token="token", user_agent="tests"),
        api_client_class=FakeGNApiClient,
        tiendanube_api_client_class=FakeTiendaNubeApiClient,
    )

    result = service.sync_tiendanube()

    assert result.counts["CREATED"] == 1
    assert result.counts["CREATED_CATEGORY"] == 2
    assert FakeTiendaNubeApiClient.created_categories == [
        {"id": 201, "name": {"es": "TN Categoria"}, "parent": None},
        {"id": 202, "name": {"es": "TN Subcategoria"}, "parent": 201},
    ]
    assert FakeTiendaNubeApiClient.last_payload is not None
    assert FakeTiendaNubeApiClient.last_payload["categories"] == [202]


def test_sync_tiendanube_unpublishes_managed_products_missing_in_gn(tmp_path: Path) -> None:
    class FakeGNApiClient:
        def __init__(self, credentials: Credentials) -> None:
            self.credentials = credentials

        def __enter__(self) -> "FakeGNApiClient":
            return self

        def __exit__(self, *_args: object) -> None:
            return None

        def get_catalog(self) -> list[dict[str, object]]:
            return []

        def get_usd_exchange(self) -> float:
            return 1000.0

    class FakeTiendaNubeApiClient:
        update_product_calls = 0
        update_variant_calls = 0

        def __init__(self, credentials: TiendaNubeCredentials) -> None:
            self.credentials = credentials

        def __enter__(self) -> "FakeTiendaNubeApiClient":
            return self

        def __exit__(self, *_args: object) -> None:
            return None

        def list_all_products(self) -> list[dict[str, object]]:
            return [
                {
                    "id": 77,
                    "name": {"es": "Producto viejo"},
                    "handle": {"es": "gn-77"},
                    "tags": "GN_SYNC, Audio",
                    "variants": [{"id": 88, "stock": 10}],
                }
            ]

        def update_product(self, product_id: int, payload: dict[str, object]) -> dict[str, object]:
            type(self).update_product_calls += 1
            return {"id": product_id, **payload}

        def update_variant(self, product_id: int, variant_id: int, payload: dict[str, object]) -> dict[str, object]:
            type(self).update_variant_calls += 1
            return {"id": variant_id, "product_id": product_id, **payload}

    config = _make_sync_config(tmp_path, test_limit=20)
    service = StockExportService(
        workspace_dir=tmp_path,
        config=config,
        credentials=Credentials(client_id=1, username="demo", password="secret"),
        tiendanube_credentials=TiendaNubeCredentials(store_id=10, access_token="token", user_agent="tests"),
        api_client_class=FakeGNApiClient,
        tiendanube_api_client_class=FakeTiendaNubeApiClient,
    )

    result = service.sync_tiendanube()

    assert result.counts["UNPUBLISHED"] == 1
    assert FakeTiendaNubeApiClient.update_product_calls == 1
    assert FakeTiendaNubeApiClient.update_variant_calls == 1


def test_limited_sync_does_not_unpublish_missing_products(tmp_path: Path) -> None:
    class FakeGNApiClient:
        def __init__(self, credentials: Credentials) -> None:
            self.credentials = credentials

        def __enter__(self) -> "FakeGNApiClient":
            return self

        def __exit__(self, *_args: object) -> None:
            return None

        def get_catalog(self) -> list[dict[str, object]]:
            return []

        def get_usd_exchange(self) -> float:
            return 1000.0

    class FakeTiendaNubeApiClient:
        update_product_calls = 0
        update_variant_calls = 0

        def __init__(self, credentials: TiendaNubeCredentials) -> None:
            self.credentials = credentials

        def __enter__(self) -> "FakeTiendaNubeApiClient":
            return self

        def __exit__(self, *_args: object) -> None:
            return None

        def list_all_products(self) -> list[dict[str, object]]:
            return [
                {
                    "id": 77,
                    "name": {"es": "Producto viejo"},
                    "handle": {"es": "gn-77"},
                    "tags": "GN_SYNC, Audio",
                    "variants": [{"id": 88, "stock": 10}],
                }
            ]

        def update_product(self, product_id: int, payload: dict[str, object]) -> dict[str, object]:
            type(self).update_product_calls += 1
            return {"id": product_id, **payload}

        def update_variant(self, product_id: int, variant_id: int, payload: dict[str, object]) -> dict[str, object]:
            type(self).update_variant_calls += 1
            return {"id": variant_id, "product_id": product_id, **payload}

    service = StockExportService(
        workspace_dir=tmp_path,
        config=_make_sync_config(tmp_path, test_limit=20),
        credentials=Credentials(client_id=1, username="demo", password="secret"),
        tiendanube_credentials=TiendaNubeCredentials(store_id=10, access_token="token", user_agent="tests"),
        api_client_class=FakeGNApiClient,
        tiendanube_api_client_class=FakeTiendaNubeApiClient,
    )

    result = service._sync_tiendanube(dry_run=False, limit=20, images_only=False)

    assert result.counts == {}
    assert FakeTiendaNubeApiClient.update_product_calls == 0
    assert FakeTiendaNubeApiClient.update_variant_calls == 0


def test_sync_tiendanube_writes_image_failure_report(tmp_path: Path) -> None:
    class FakeGNApiClient:
        def __init__(self, credentials: Credentials) -> None:
            self.credentials = credentials

        def __enter__(self) -> "FakeGNApiClient":
            return self

        def __exit__(self, *_args: object) -> None:
            return None

        def get_catalog(self) -> list[dict[str, object]]:
            return [_catalog_item()]

        def get_usd_exchange(self) -> float:
            return 1000.0

    class FakeTiendaNubeApiClient:
        def __init__(self, credentials: TiendaNubeCredentials) -> None:
            self.credentials = credentials

        def __enter__(self) -> "FakeTiendaNubeApiClient":
            return self

        def __exit__(self, *_args: object) -> None:
            return None

        def list_all_products(self) -> list[dict[str, object]]:
            return []

        def create_product(self, payload: dict[str, object]) -> dict[str, object]:
            return {
                "id": 101,
                "handle": {"es": payload["handle"]["es"]},
                "tags": payload["tags"],
                "variants": [{"id": 501}],
            }

        def create_product_image(self, product_id: int, src: str, *, position: int | None = None) -> dict[str, object]:
            raise TiendaNubeApiError("imagen rechazada")

    service = StockExportService(
        workspace_dir=tmp_path,
        config=_make_sync_config(tmp_path, test_limit=20),
        credentials=Credentials(client_id=1, username="demo", password="secret"),
        tiendanube_credentials=TiendaNubeCredentials(store_id=10, access_token="token", user_agent="tests"),
        api_client_class=FakeGNApiClient,
        tiendanube_api_client_class=FakeTiendaNubeApiClient,
    )

    result = service.sync_tiendanube()

    assert result.counts["CREATED"] == 1
    assert result.report_paths["image_failures_csv"].exists()
    failure_report = result.report_paths["image_failures_csv"].read_text(encoding="utf-8-sig")
    assert "https://example.com/image-1.jpg" in failure_report
    assert "UPLOAD_ERROR" in failure_report


def test_sync_tiendanube_failed_images_retries_only_reported_failures(tmp_path: Path) -> None:
    class FakeTiendaNubeApiClient:
        uploaded_urls: list[str] = []

        def __init__(self, credentials: TiendaNubeCredentials) -> None:
            self.credentials = credentials

        def __enter__(self) -> "FakeTiendaNubeApiClient":
            return self

        def __exit__(self, *_args: object) -> None:
            return None

        def list_all_products(self) -> list[dict[str, object]]:
            return [
                {
                    "id": 101,
                    "name": {"es": "Producto demo"},
                    "handle": {"es": "gn-1"},
                    "tags": "GN_SYNC",
                    "variants": [{"id": 501}],
                }
            ]

        def create_product_image(self, product_id: int, src: str, *, position: int | None = None) -> dict[str, object]:
            type(self).uploaded_urls.append(src)
            return {"id": 900, "product_id": product_id, "src": src}

    failures_path = tmp_path / "failures.csv"
    failures_path.write_text(
        "\n".join(
            [
                "item_id;handle;name;product_id;image_url;failure_type;error",
                "1;gn-1;Producto demo;101;https://example.com/image-1.jpg;UPLOAD_ERROR;imagen rechazada",
                "1;gn-1;Producto demo;101;not-a-url;INVALID_URL;URL invalida.",
            ]
        ),
        encoding="utf-8",
    )
    service = StockExportService(
        workspace_dir=tmp_path,
        config=_make_sync_config(tmp_path, test_limit=20),
        tiendanube_credentials=TiendaNubeCredentials(store_id=10, access_token="token", user_agent="tests"),
        tiendanube_api_client_class=FakeTiendaNubeApiClient,
    )

    result = service.sync_tiendanube_failed_images(failures_path=failures_path)

    assert result.row_count == 2
    assert result.counts["RETRIED"] == 1
    assert result.counts["SKIP_NOT_RETRYABLE"] == 1
    assert FakeTiendaNubeApiClient.uploaded_urls == ["https://example.com/image-1.jpg"]
    assert result.report_paths["csv"].exists()


def _make_sync_config(tmp_path: Path, *, test_limit: int, with_category_id: bool = True) -> AppConfig:
    category_map = tmp_path / "category_map.csv"
    category_id = "123" if with_category_id else ""
    category_map.write_text(
        "source_category,source_subcategory,target_category,target_subcategory,target_category_id\n"
        f"Categoria,Subcategoria,TN Categoria,TN Subcategoria,{category_id}\n",
        encoding="utf-8",
    )

    return AppConfig(
        pricing=PricingConfig(
            use_api_usd_exchange=True,
            use_usd_override=False,
            usd_exchange_override=0.0,
            margin_pct=60.0,
            fixed_markup_ars=0.0,
            rounding_step=1.0,
            rounding_mode="nearest",
            cost_field_mode="ars_neto",
        ),
        publication=PublicationConfig(
            publish_with_stock_only=True,
            min_stock_to_publish=1,
            free_shipping=False,
            product_physical=True,
        ),
        content=ContentConfig(
            default_brand_when_empty="",
            seo_title_max_length=70,
            seo_description_max_length=160,
            description_prefix="",
            description_suffix="",
        ),
        mappings=MappingConfig(
            brand_map_csv=tmp_path / "brand_map.csv",
            category_map_csv=category_map,
        ),
        diff=DiffConfig(price_tolerance_ars=0.5),
        output=OutputConfig(
            output_dir=tmp_path / "exports",
            include_csv=True,
            include_xlsx=True,
            test_product_limit=20,
        ),
        tiendanube_sync=TiendaNubeSyncConfig(
            enabled=True,
            dry_run=True,
            managed_tag="GN_SYNC",
            handle_prefix="gn",
            unpublish_missing=True,
            image_mode="append_only",
            test_product_limit=test_limit,
        ),
    )


def _catalog_item() -> dict[str, object]:
    return {
        "item_id": 1,
        "codigo": "SKU1",
        "ean": "779000000001",
        "partNumber": "PN-1",
        "marca": "Marca Demo",
        "categoria": "Categoria",
        "subcategoria": "Subcategoria",
        "item_desc_0": "Producto demo",
        "item_desc_1": "",
        "item_desc_2": "",
        "peso_gr": 250.0,
        "alto_cm": 10.0,
        "ancho_cm": 20.0,
        "largo_cm": 30.0,
        "volumen_cm3": 150.0,
        "precioNeto_USD": 2.5,
        "stock_mdp": 4,
        "stock_caba": 0,
        "impuestos": [],
        "url_imagenes": ["https://example.com/image-1.jpg"],
    }
