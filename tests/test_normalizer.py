from datetime import datetime, timezone
from pathlib import Path

from gn_stock_export.config import (
    AppConfig,
    ContentConfig,
    DiffConfig,
    MappingConfig,
    OutputConfig,
    PricingConfig,
    PublicationConfig,
)
from gn_stock_export.normalizer import apply_rounding, build_export_frame


def test_apply_rounding_supports_expected_modes() -> None:
    assert apply_rounding(12.4, 1.0, "nearest") == 12.0
    assert apply_rounding(12.6, 1.0, "nearest") == 13.0
    assert apply_rounding(12.1, 5.0, "up") == 15.0
    assert apply_rounding(12.9, 5.0, "down") == 10.0


def test_apply_rounding_supports_nearest_thousand_for_prices() -> None:
    assert apply_rounding(797662, 1000.0, "nearest") == 798000.0
    assert apply_rounding(124560, 1000.0, "nearest") == 125000.0
    assert apply_rounding(805040, 1000.0, "nearest") == 805000.0
    assert apply_rounding(56608, 1000.0, "nearest") == 57000.0


def test_build_export_frame_calculates_prices_and_user_friendly_columns() -> None:
    config = AppConfig(
        pricing=PricingConfig(
            use_api_usd_exchange=True,
            use_usd_override=False,
            usd_exchange_override=0.0,
            margin_pct=20.0,
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
            brand_map_csv=Path("brand_map.csv"),
            category_map_csv=Path("category_map.csv"),
        ),
        diff=DiffConfig(price_tolerance_ars=0.5),
        output=OutputConfig(
            output_dir=Path("exports"),
            include_csv=True,
            include_xlsx=True,
            test_product_limit=20,
        ),
    )
    catalog = [
        {
            "item_id": 10,
            "codigo": "ABC",
            "ean": "000123",
            "partNumber": "",
            "marca": "Marca X",
            "categoria": "Cat",
            "subcategoria": "Sub",
            "item_desc_0": "Producto",
            "item_desc_1": "Modelo",
            "item_desc_2": "2026",
            "peso_gr": 10,
            "alto_cm": 1.5,
            "ancho_cm": 2.5,
            "largo_cm": 3.5,
            "volumen_cm3": 13.125,
            "precioNeto_USD": 10.0,
            "stock_mdp": 7,
            "stock_caba": 5,
            "impuestos": [
                {"imp_desc": "IVA 21%", "imp_porcentaje": 21.0},
                {"imp_desc": "Interno", "imp_porcentaje": 5.0},
            ],
            "url_imagenes": ["https://img/1.jpg", "https://img/2.jpg"],
        }
    ]

    frame = build_export_frame(
        catalog,
        usd_exchange=100.0,
        config=config,
        exported_at=datetime(2026, 4, 16, 12, 0, tzinfo=timezone.utc),
    )
    row = frame.iloc[0]

    assert row["stock_total"] == 12
    assert bool(row["disponible"]) is True
    assert row["descripcion_corta"] == "Producto"
    assert row["descripcion_larga"] == "Producto Modelo 2026"
    assert row["impuestos_pct_total"] == 26.0
    assert row["precio_neto_ars"] == 1000.0
    assert row["precio_final_ars"] == 1512.0
    assert row["imagen_principal"] == "https://img/1.jpg"
    assert row["imagenes"] == "https://img/1.jpg | https://img/2.jpg"
