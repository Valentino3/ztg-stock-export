from pathlib import Path

import pandas as pd
import pytest

from gn_stock_export.template_contract import (
    PRODUCT_TEMPLATE_COLUMNS,
    read_product_template_csv,
    validate_product_template_columns,
)


def test_product_template_sample_matches_expected_columns_in_order() -> None:
    frame = read_product_template_csv("productos.csv")

    validate_product_template_columns(frame)

    assert list(frame.columns) == PRODUCT_TEMPLATE_COLUMNS
    assert len(frame.columns) == 30
    assert frame.columns.is_unique


def test_product_template_sample_keeps_parent_and_variant_row_shape() -> None:
    frame = read_product_template_csv("productos.csv")
    grouped = frame.groupby("Identificador de URL", sort=False)

    single_product = grouped.get_group("pantalon-corto").reset_index(drop=True)
    assert len(single_product) == 1
    assert single_product.loc[0, "Nombre"] == "Pantalón corto"
    assert single_product.loc[0, "Categorías"] == "Pantalones"
    assert single_product.loc[0, "Mostrar en tienda"] == "SI"

    variant_product = grouped.get_group("camisa-manga-corta").reset_index(drop=True)
    assert len(variant_product) == 3

    parent = variant_product.iloc[0]
    children = variant_product.iloc[1:]

    assert parent["Nombre"] == "Camisa manga corta"
    assert parent["Categorías"] == "Camisas"
    assert parent["Mostrar en tienda"] == "SI"
    assert parent["Nombre de propiedad 1"] == "Color"
    assert parent["Nombre de propiedad 2"] == "Talle"

    assert (children["Nombre"] == "").all()
    assert (children["Categorías"] == "").all()
    assert (children["Mostrar en tienda"] == "").all()
    assert (children["SKU"] != "").all()
    assert (children["Nombre de propiedad 1"] == "Color").all()
    assert (children["Nombre de propiedad 2"] == "Talle").all()


def test_validate_product_template_columns_fails_when_order_changes() -> None:
    frame = pd.DataFrame(columns=list(reversed(PRODUCT_TEMPLATE_COLUMNS)))

    with pytest.raises(ValueError):
        validate_product_template_columns(frame)


def test_product_template_sample_has_required_business_fields_populated() -> None:
    frame = read_product_template_csv("productos.csv")

    assert (frame["Identificador de URL"] != "").all()
    assert (frame["Precio"] != "").all()
    assert (frame["Peso (kg)"] != "").all()
    assert (frame["Alto (cm)"] != "").all()
    assert (frame["Ancho (cm)"] != "").all()
    assert (frame["Profundidad (cm)"] != "").all()
