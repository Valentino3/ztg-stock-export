from pathlib import Path

import pytest

from gn_stock_export.config import (
    AppConfig,
    ContentConfig,
    DiffConfig,
    MappingConfig,
    OutputConfig,
    PricingConfig,
    PublicationConfig,
    TiendaNubeCredentials,
    TiendaNubeSyncConfig,
)
from gn_stock_export.tiendanube_cleanup import DELETE_ALL_CONFIRMATION, run_tiendanube_cleanup


def test_tiendanube_cleanup_dry_run_does_not_delete_products(tmp_path: Path) -> None:
    class FakeTiendaNubeApiClient:
        delete_calls = 0

        def __init__(self, credentials: TiendaNubeCredentials) -> None:
            self.credentials = credentials

        def __enter__(self) -> "FakeTiendaNubeApiClient":
            return self

        def __exit__(self, *_args: object) -> None:
            return None

        def list_all_products(self) -> list[dict[str, object]]:
            return [_tn_product(10), _tn_product(20)]

        def delete_product(self, product_id: int) -> None:
            type(self).delete_calls += 1

    result = run_tiendanube_cleanup(
        config=_make_config(tmp_path),
        credentials=TiendaNubeCredentials(store_id=10, access_token="token", user_agent="tests"),
        dry_run=True,
        api_client_class=FakeTiendaNubeApiClient,
    )

    assert result.dry_run is True
    assert result.row_count == 2
    assert result.counts["DRY_RUN_DELETE"] == 2
    assert result.report_paths["csv"].exists()
    assert FakeTiendaNubeApiClient.delete_calls == 0


def test_tiendanube_cleanup_requires_explicit_confirmation_for_real_delete(tmp_path: Path) -> None:
    class FakeTiendaNubeApiClient:
        def __init__(self, credentials: TiendaNubeCredentials) -> None:
            self.credentials = credentials

    with pytest.raises(ValueError, match=DELETE_ALL_CONFIRMATION):
        run_tiendanube_cleanup(
            config=_make_config(tmp_path),
            credentials=TiendaNubeCredentials(store_id=10, access_token="token", user_agent="tests"),
            dry_run=False,
            confirm="",
            api_client_class=FakeTiendaNubeApiClient,
        )


def test_tiendanube_cleanup_deletes_all_products_when_confirmed(tmp_path: Path) -> None:
    class FakeTiendaNubeApiClient:
        deleted_ids: list[int] = []

        def __init__(self, credentials: TiendaNubeCredentials) -> None:
            self.credentials = credentials

        def __enter__(self) -> "FakeTiendaNubeApiClient":
            return self

        def __exit__(self, *_args: object) -> None:
            return None

        def list_all_products(self) -> list[dict[str, object]]:
            return [_tn_product(10), _tn_product(20)]

        def delete_product(self, product_id: int) -> None:
            type(self).deleted_ids.append(product_id)

    result = run_tiendanube_cleanup(
        config=_make_config(tmp_path),
        credentials=TiendaNubeCredentials(store_id=10, access_token="token", user_agent="tests"),
        dry_run=False,
        confirm=DELETE_ALL_CONFIRMATION,
        api_client_class=FakeTiendaNubeApiClient,
    )

    assert result.dry_run is False
    assert result.counts["DELETED"] == 2
    assert FakeTiendaNubeApiClient.deleted_ids == [10, 20]


def _make_config(tmp_path: Path) -> AppConfig:
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
            category_map_csv=tmp_path / "category_map.csv",
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
            test_product_limit=20,
        ),
    )


def _tn_product(product_id: int) -> dict[str, object]:
    return {
        "id": product_id,
        "name": {"es": f"Producto {product_id}"},
        "handle": {"es": f"producto-{product_id}"},
        "tags": "Demo",
    }
