from pathlib import Path

import pandas as pd

from gn_stock_export.config import (
    AppConfig,
    ContentConfig,
    Credentials,
    DiffConfig,
    MappingConfig,
    OutputConfig,
    PricingConfig,
    PublicationConfig,
)
from gn_stock_export.service import StockExportService


def test_export_uses_usd_override_without_calling_exchange_endpoint(tmp_path: Path) -> None:
    class FakeApiClient:
        usd_exchange_calls = 0

        def __init__(self, credentials: Credentials) -> None:
            self.credentials = credentials

        def __enter__(self) -> "FakeApiClient":
            return self

        def __exit__(self, *_args: object) -> None:
            return None

        def get_catalog(self) -> list[dict[str, object]]:
            return [
                {
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
                    "impuestos": [{"imp_desc": "IVA 21%", "imp_porcentaje": 21.0}],
                    "url_imagenes": [],
                }
            ]

        def get_usd_exchange(self) -> float:
            type(self).usd_exchange_calls += 1
            return 999.0

    config = AppConfig(
        pricing=PricingConfig(
            use_api_usd_exchange=True,
            use_usd_override=True,
            usd_exchange_override=1500.0,
            margin_pct=10.0,
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
            include_xlsx=False,
            test_product_limit=20,
        ),
    )
    credentials = Credentials(client_id=1, username="demo", password="secret")
    service = StockExportService(
        workspace_dir=tmp_path,
        config=config,
        credentials=credentials,
        api_client_class=FakeApiClient,
    )

    result = service.export()

    assert result.usd_exchange == 1500.0
    assert FakeApiClient.usd_exchange_calls == 0
    assert Path(result.outputs["csv"]).exists()


def test_raw_export_writes_gn_catalog_outputs(tmp_path: Path) -> None:
    class FakeApiClient:
        def __init__(self, credentials: Credentials) -> None:
            self.credentials = credentials

        def __enter__(self) -> "FakeApiClient":
            return self

        def __exit__(self, *_args: object) -> None:
            return None

        def get_catalog(self) -> list[dict[str, object]]:
            return [
                {
                    "item_id": 1,
                    "codigo": "SKU1",
                    "precioNeto_USD": 2.5,
                    "impuestos": [{"imp_desc": "IVA 21%", "imp_porcentaje": 21.0}],
                    "url_imagenes": ["https://example.com/1.jpg"],
                }
            ]

        def get_usd_exchange(self) -> float:
            return 1450.0

    config = AppConfig(
        pricing=PricingConfig(
            use_api_usd_exchange=True,
            use_usd_override=False,
            usd_exchange_override=0.0,
            margin_pct=10.0,
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
    )
    credentials = Credentials(client_id=1, username="demo", password="secret")
    service = StockExportService(
        workspace_dir=tmp_path,
        config=config,
        credentials=credentials,
        api_client_class=FakeApiClient,
    )

    result = service.export_gn_raw()

    assert result.row_count == 1
    assert result.usd_exchange == 1450.0
    assert Path(result.outputs["json"]).exists()
    assert Path(result.outputs["csv"]).exists()
    assert Path(result.outputs["xlsx"]).exists()


def test_categories_export_writes_unique_category_summary(tmp_path: Path) -> None:
    class FakeApiClient:
        def __init__(self, credentials: Credentials) -> None:
            self.credentials = credentials

        def __enter__(self) -> "FakeApiClient":
            return self

        def __exit__(self, *_args: object) -> None:
            return None

        def get_catalog(self) -> list[dict[str, object]]:
            return [
                {
                    "item_id": 1,
                    "codigo": "SKU1",
                    "ean": "111",
                    "partNumber": "",
                    "marca": "Marca",
                    "categoria": "Componentes",
                    "subcategoria": "Fuentes",
                    "item_desc_0": "Fuente 1",
                    "item_desc_1": "",
                    "item_desc_2": "",
                    "peso_gr": 100.0,
                    "alto_cm": 1.0,
                    "ancho_cm": 2.0,
                    "largo_cm": 3.0,
                    "volumen_cm3": 6.0,
                    "precioNeto_USD": 10.0,
                    "stock_mdp": 2,
                    "stock_caba": 3,
                    "impuestos": [],
                    "url_imagenes": [],
                },
                {
                    "item_id": 2,
                    "codigo": "SKU2",
                    "ean": "222",
                    "partNumber": "",
                    "marca": "Marca",
                    "categoria": "Componentes",
                    "subcategoria": "Fuentes",
                    "item_desc_0": "Fuente 2",
                    "item_desc_1": "",
                    "item_desc_2": "",
                    "peso_gr": 100.0,
                    "alto_cm": 1.0,
                    "ancho_cm": 2.0,
                    "largo_cm": 3.0,
                    "volumen_cm3": 6.0,
                    "precioNeto_USD": 20.0,
                    "stock_mdp": 0,
                    "stock_caba": 0,
                    "impuestos": [],
                    "url_imagenes": [],
                },
                {
                    "item_id": 3,
                    "codigo": "SKU3",
                    "ean": "333",
                    "partNumber": "",
                    "marca": "Marca",
                    "categoria": "Monitores",
                    "subcategoria": "Monitores Led",
                    "item_desc_0": "Monitor",
                    "item_desc_1": "",
                    "item_desc_2": "",
                    "peso_gr": 100.0,
                    "alto_cm": 1.0,
                    "ancho_cm": 2.0,
                    "largo_cm": 3.0,
                    "volumen_cm3": 6.0,
                    "precioNeto_USD": 30.0,
                    "stock_mdp": 1,
                    "stock_caba": 0,
                    "impuestos": [],
                    "url_imagenes": [],
                },
            ]

        def get_usd_exchange(self) -> float:
            return 1000.0

    config = AppConfig(
        pricing=PricingConfig(
            use_api_usd_exchange=True,
            use_usd_override=False,
            usd_exchange_override=0.0,
            margin_pct=10.0,
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
    )
    credentials = Credentials(client_id=1, username="demo", password="secret")
    service = StockExportService(
        workspace_dir=tmp_path,
        config=config,
        credentials=credentials,
        api_client_class=FakeApiClient,
    )

    result = service.export_categories()

    assert result.row_count == 2
    assert result.product_count == 3
    assert Path(result.outputs["csv"]).exists()
    assert Path(result.outputs["xlsx"]).exists()

    frame = pd.read_csv(result.outputs["csv"], sep=";")
    component_row = frame[frame["source_subcategory"] == "Fuentes"].iloc[0]
    assert component_row["source_category"] == "Componentes"
    assert component_row["target_category"] == "Componentes"
    assert component_row["target_subcategory"] == "Fuentes"
    assert component_row["product_count"] == 2
    assert component_row["products_with_stock"] == 1
    assert component_row["stock_total"] == 5


def test_test_flow_limits_products_and_writes_test_outputs(tmp_path: Path) -> None:
    class FakeApiClient:
        def __init__(self, credentials: Credentials) -> None:
            self.credentials = credentials

        def __enter__(self) -> "FakeApiClient":
            return self

        def __exit__(self, *_args: object) -> None:
            return None

        def get_catalog(self) -> list[dict[str, object]]:
            return [
                {
                    "item_id": 1,
                    "codigo": "SKU1",
                    "ean": "111",
                    "partNumber": "",
                    "item_desc_0": "Prod 1",
                    "item_desc_1": "",
                    "item_desc_2": "",
                    "marca": "Marca 1",
                    "categoria": "Cat",
                    "subcategoria": "Sub",
                    "peso_gr": 100.0,
                    "alto_cm": 1.0,
                    "ancho_cm": 2.0,
                    "largo_cm": 3.0,
                    "volumen_cm3": 6.0,
                    "precioNeto_USD": 1.0,
                    "impuestos": [],
                    "stock_mdp": 1,
                    "stock_caba": 0,
                    "url_imagenes": [],
                },
                {
                    "item_id": 2,
                    "codigo": "SKU2",
                    "ean": "222",
                    "partNumber": "",
                    "item_desc_0": "Prod 2",
                    "item_desc_1": "",
                    "item_desc_2": "",
                    "marca": "Marca 2",
                    "categoria": "Cat",
                    "subcategoria": "Sub",
                    "peso_gr": 100.0,
                    "alto_cm": 1.0,
                    "ancho_cm": 2.0,
                    "largo_cm": 3.0,
                    "volumen_cm3": 6.0,
                    "precioNeto_USD": 2.0,
                    "impuestos": [],
                    "stock_mdp": 1,
                    "stock_caba": 0,
                    "url_imagenes": [],
                },
            ]

        def get_usd_exchange(self) -> float:
            return 1000.0

    config = AppConfig(
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
            include_xlsx=False,
            test_product_limit=1,
        ),
    )
    credentials = Credentials(client_id=1, username="demo", password="secret")
    service = StockExportService(
        workspace_dir=tmp_path,
        config=config,
        credentials=credentials,
        api_client_class=FakeApiClient,
    )

    result = service.test_flow()

    assert result.row_count == 1
    assert Path(result.outputs["csv"]).exists()
    assert Path(result.outputs["raw_json"]).exists()
    assert "test" in str(result.outputs["csv"])
