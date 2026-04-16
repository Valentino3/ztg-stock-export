from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from gn_stock_export.config import (
    AppConfig,
    ContentConfig,
    DiffConfig,
    MappingConfig,
    OutputConfig,
    PricingConfig,
    PublicationConfig,
)
from gn_stock_export.exporter import write_stock_exports
from gn_stock_export.product_import import build_product_import_frame, prepare_products
from gn_stock_export.template_contract import PRODUCT_TEMPLATE_COLUMNS, read_product_template_csv


def test_build_product_import_frame_matches_template_columns_and_mapping(tmp_path: Path) -> None:
    config = _make_config(tmp_path)
    stock_frame = pd.DataFrame(
        [
            {
                "item_id": 3389,
                "codigo": "195",
                "ean": "0000195",
                "partNumber": "",
                "marca": "GENERICO",
                "categoria": "Informatica Accesorios",
                "subcategoria": "Cables y Adaptadores",
                "item_desc_0": "Cable Alimentacion 220V 0180",
                "item_desc_1": "",
                "item_desc_2": "",
                "descripcion_corta": "Cable Alimentacion 220V 0180",
                "descripcion_larga": "Cable Alimentacion 220V 0180",
                "peso_gr": 100.0,
                "alto_cm": 2.0,
                "ancho_cm": 1.0,
                "largo_cm": 1.9,
                "stock_total": 253,
                "disponible": True,
                "precioNeto_USD": 1.44,
                "precio_neto_ars": 2000.0,
                "precio_final_ars": 2500.0,
            }
        ]
    )

    frame = build_product_import_frame(stock_frame, config)
    row = frame.iloc[0]

    assert list(frame.columns) == PRODUCT_TEMPLATE_COLUMNS
    assert row["Identificador de URL"] == "gn-3389"
    assert row["Nombre"] == "Cable Alimentacion 220V 0180"
    assert row["Categorías"] == "Informatica Accesorios > Cables y Adaptadores"
    assert row["Precio"] == 2500.0
    assert row["Peso (kg)"] == 0.1
    assert row["Stock"] == 253
    assert row["SKU"] == "195"
    assert row["Código de barras"] == "0000195"
    assert row["Mostrar en tienda"] == "SI"
    assert row["Descripción"] == "Cable Alimentacion 220V 0180"
    assert row["Tags"] == "Informatica Accesorios, Cables y Adaptadores, GENERICO"
    assert row["Producto Físico"] == "SI"
    assert row["Costo"] == 2000.0


def test_build_product_import_frame_applies_content_publication_and_mapping_rules(tmp_path: Path) -> None:
    brand_map = tmp_path / "brand_map.csv"
    brand_map.write_text("source_brand,target_brand\nMarca Vieja,Marca Nueva\n", encoding="utf-8")
    category_map = tmp_path / "category_map.csv"
    category_map.write_text(
        "source_category,source_subcategory,target_category,target_subcategory\nAudio,Auriculares,Audio y Sonido,Headsets\n",
        encoding="utf-8",
    )
    config = _make_config(
        tmp_path,
        pricing=PricingConfig(
            use_api_usd_exchange=True,
            use_usd_override=False,
            usd_exchange_override=0.0,
            margin_pct=15.0,
            fixed_markup_ars=0.0,
            rounding_step=1.0,
            rounding_mode="nearest",
            cost_field_mode="ars_neto",
        ),
        publication=PublicationConfig(
            publish_with_stock_only=True,
            min_stock_to_publish=5,
            free_shipping=True,
            product_physical=False,
        ),
        content=ContentConfig(
            default_brand_when_empty="Marca Fallback",
            seo_title_max_length=50,
            seo_description_max_length=80,
            description_prefix="Oferta",
            description_suffix="Garantia oficial",
        ),
        mappings=MappingConfig(
            brand_map_csv=brand_map,
            category_map_csv=category_map,
        ),
    )
    stock_frame = pd.DataFrame(
        [
            {
                "item_id": 99,
                "codigo": "SKU99",
                "ean": "779000000099",
                "partNumber": "",
                "marca": "Marca Vieja",
                "categoria": "Audio",
                "subcategoria": "Auriculares",
                "descripcion_corta": "Auricular Bluetooth Premium con cancelacion activa de ruido y bateria extendida",
                "descripcion_larga": "Auricular <strong>Bluetooth</strong> Premium con cancelacion activa de ruido, bateria extendida y estuche de carga magnetico para uso diario.",
                "peso_gr": 320.0,
                "alto_cm": 12.0,
                "ancho_cm": 8.0,
                "largo_cm": 5.0,
                "stock_total": 3,
                "disponible": True,
                "precioNeto_USD": 10.0,
                "precio_neto_ars": 12345.67,
                "precio_final_ars": 35000.0,
            }
        ]
    )

    frame = build_product_import_frame(stock_frame, config)
    row = frame.iloc[0]

    assert row["Marca"] == "Marca Nueva"
    assert row["Categorías"] == "Audio y Sonido > Headsets"
    assert row["Tags"] == "Audio y Sonido, Headsets, Marca Nueva"
    assert row["Mostrar en tienda"] == "NO"
    assert row["Envío sin cargo"] == "SI"
    assert row["Producto Físico"] == "NO"
    assert row["Descripción"].startswith("Oferta ")
    assert row["Descripción"].endswith(" Garantia oficial")
    assert len(row["Título para SEO"]) <= 50
    assert len(row["Descripción para SEO"]) <= 80
    assert row["Costo"] == 12345.67


def test_build_product_import_frame_uses_default_brand_when_original_is_placeholder(tmp_path: Path) -> None:
    config = _make_config(
        tmp_path,
        content=ContentConfig(
            default_brand_when_empty="Marca Default",
            seo_title_max_length=70,
            seo_description_max_length=160,
            description_prefix="",
            description_suffix="",
        ),
    )
    stock_frame = pd.DataFrame(
        [
            {
                "item_id": 10,
                "codigo": "SKU10",
                "ean": "779000000010",
                "partNumber": "",
                "marca": "0 Cargar una Marca",
                "categoria": "Audio",
                "subcategoria": "Auriculares",
                "descripcion_corta": "Auricular",
                "descripcion_larga": "Auricular",
                "peso_gr": 100.0,
                "alto_cm": 1.0,
                "ancho_cm": 2.0,
                "largo_cm": 3.0,
                "stock_total": 10,
                "disponible": True,
                "precioNeto_USD": 10.0,
                "precio_neto_ars": 10000.0,
                "precio_final_ars": 15000.0,
            }
        ]
    )

    frame = build_product_import_frame(stock_frame, config)
    row = frame.iloc[0]

    assert row["Marca"] == "Marca Default"
    assert row["Tags"] == "Audio, Auriculares, Marca Default"


def test_prepare_products_normalizes_gn_image_dict_strings(tmp_path: Path) -> None:
    config = _make_config(tmp_path)
    stock_frame = pd.DataFrame(
        [
            {
                "item_id": 11,
                "codigo": "SKU11",
                "ean": "779000000011",
                "partNumber": "",
                "marca": "Marca Demo",
                "categoria": "Audio",
                "subcategoria": "Auriculares",
                "descripcion_corta": "Auricular",
                "descripcion_larga": "Auricular",
                "peso_gr": 100.0,
                "alto_cm": 1.0,
                "ancho_cm": 2.0,
                "largo_cm": 3.0,
                "stock_total": 10,
                "disponible": True,
                "precioNeto_USD": 10.0,
                "precio_neto_ars": 10000.0,
                "precio_final_ars": 15000.0,
                "imagenes": (
                    "{'url': 'https://example.com/imagen-1.jpg'} | "
                    "{'url': 'https://example.com/imagen-2.jpg'}"
                ),
            }
        ]
    )

    products = prepare_products(stock_frame, config)

    assert products[0].image_urls == [
        "https://example.com/imagen-1.jpg",
        "https://example.com/imagen-2.jpg",
    ]


def test_write_stock_exports_generates_template_import_csv(tmp_path: Path) -> None:
    config = _make_config(
        tmp_path,
        output=OutputConfig(
            output_dir=tmp_path / "exports",
            include_csv=True,
            include_xlsx=True,
        ),
    )
    stock_frame = pd.DataFrame(
        [
            {
                "item_id": 1,
                "codigo": "SKU1",
                "ean": "779000000001",
                "partNumber": "PN-1",
                "marca": "Marca Demo",
                "categoria": "Categoria",
                "subcategoria": "Subcategoria",
                "descripcion_corta": "Producto demo",
                "descripcion_larga": "Producto demo largo",
                "peso_gr": 250.0,
                "alto_cm": 10.0,
                "ancho_cm": 20.0,
                "largo_cm": 30.0,
                "stock_total": 4,
                "disponible": True,
                "precioNeto_USD": 2.5,
                "precio_neto_ars": 3450.0,
                "precio_final_ars": 999.99,
            }
        ]
    )

    outputs = write_stock_exports(stock_frame, datetime(2026, 4, 16, 18, 0, 0, tzinfo=timezone.utc), config)

    assert outputs["csv"].name == "productos_20260416_180000.csv"
    assert outputs["xlsx"].name == "productos_20260416_180000.xlsx"

    import_frame = read_product_template_csv(outputs["csv"])
    assert list(import_frame.columns) == PRODUCT_TEMPLATE_COLUMNS
    assert len(import_frame) == 1
    assert import_frame.loc[0, "SKU"] == "PN-1"
    assert import_frame.loc[0, "Precio"] == 999.99
    assert import_frame.loc[0, "Costo"] == 3450.0

    workbook_frame = pd.read_excel(outputs["xlsx"])
    assert list(workbook_frame.columns) == PRODUCT_TEMPLATE_COLUMNS
    assert workbook_frame.loc[0, "SKU"] == "PN-1"


def _make_config(
    tmp_path: Path,
    *,
    pricing: PricingConfig | None = None,
    publication: PublicationConfig | None = None,
    content: ContentConfig | None = None,
    mappings: MappingConfig | None = None,
    diff: DiffConfig | None = None,
    output: OutputConfig | None = None,
) -> AppConfig:
    return AppConfig(
        pricing=pricing
        or PricingConfig(
            use_api_usd_exchange=True,
            use_usd_override=False,
            usd_exchange_override=0.0,
            margin_pct=15.0,
            fixed_markup_ars=0.0,
            rounding_step=1.0,
            rounding_mode="nearest",
            cost_field_mode="ars_neto",
        ),
        publication=publication
        or PublicationConfig(
            publish_with_stock_only=True,
            min_stock_to_publish=1,
            free_shipping=False,
            product_physical=True,
        ),
        content=content
        or ContentConfig(
            default_brand_when_empty="",
            seo_title_max_length=70,
            seo_description_max_length=160,
            description_prefix="",
            description_suffix="",
        ),
        mappings=mappings
        or MappingConfig(
            brand_map_csv=tmp_path / "brand_map.csv",
            category_map_csv=tmp_path / "category_map.csv",
        ),
        diff=diff or DiffConfig(price_tolerance_ars=0.5),
        output=output
        or OutputConfig(
            output_dir=tmp_path / "exports",
            include_csv=True,
            include_xlsx=True,
            test_product_limit=20,
        ),
    )
