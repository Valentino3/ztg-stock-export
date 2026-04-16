from __future__ import annotations

from pathlib import Path

import pandas as pd


PRODUCT_TEMPLATE_COLUMNS = [
    "Identificador de URL",
    "Nombre",
    "Categorías",
    "Nombre de propiedad 1",
    "Valor de propiedad 1",
    "Nombre de propiedad 2",
    "Valor de propiedad 2",
    "Nombre de propiedad 3",
    "Valor de propiedad 3",
    "Precio",
    "Precio promocional",
    "Peso (kg)",
    "Alto (cm)",
    "Ancho (cm)",
    "Profundidad (cm)",
    "Stock",
    "SKU",
    "Código de barras",
    "Mostrar en tienda",
    "Envío sin cargo",
    "Descripción",
    "Tags",
    "Título para SEO",
    "Descripción para SEO",
    "Marca",
    "Producto Físico",
    "MPN (Número de pieza del fabricante)",
    "Sexo",
    "Rango de edad",
    "Costo",
]


def read_product_template_csv(path: str | Path) -> pd.DataFrame:
    return pd.read_csv(
        Path(path),
        sep=";",
        encoding="utf-8-sig",
        keep_default_na=False,
    )


def validate_product_template_columns(frame: pd.DataFrame) -> None:
    current_columns = list(frame.columns)
    if current_columns != PRODUCT_TEMPLATE_COLUMNS:
        raise ValueError(
            "La plantilla no coincide con las columnas esperadas. "
            f"Esperadas: {PRODUCT_TEMPLATE_COLUMNS}. "
            f"Actuales: {current_columns}."
        )

