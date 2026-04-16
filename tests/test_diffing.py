from pathlib import Path

import pandas as pd

from gn_stock_export.diffing import compare_frames


def test_compare_frames_detects_all_expected_statuses() -> None:
    previous = pd.DataFrame(
        [
            {
                "item_id": 1,
                "codigo": "A",
                "descripcion_corta": "Baja",
                "descripcion_larga": "Baja",
                "stock_total": 5,
                "precio_final_ars": 100.0,
                "precioNeto_USD": 1.0,
                "disponible": True,
            },
            {
                "item_id": 2,
                "codigo": "B",
                "descripcion_corta": "Stock",
                "descripcion_larga": "Stock",
                "stock_total": 3,
                "precio_final_ars": 200.0,
                "precioNeto_USD": 2.0,
                "disponible": True,
            },
            {
                "item_id": 3,
                "codigo": "C",
                "descripcion_corta": "Precio",
                "descripcion_larga": "Precio",
                "stock_total": 8,
                "precio_final_ars": 300.0,
                "precioNeto_USD": 3.0,
                "disponible": True,
            },
            {
                "item_id": 4,
                "codigo": "D",
                "descripcion_corta": "Ambos",
                "descripcion_larga": "Ambos",
                "stock_total": 10,
                "precio_final_ars": 400.0,
                "precioNeto_USD": 4.0,
                "disponible": True,
            },
        ]
    )
    current = pd.DataFrame(
        [
            {
                "item_id": 2,
                "codigo": "B",
                "descripcion_corta": "Stock",
                "descripcion_larga": "Stock",
                "stock_total": 9,
                "precio_final_ars": 200.0,
                "precioNeto_USD": 2.0,
                "disponible": True,
            },
            {
                "item_id": 3,
                "codigo": "C",
                "descripcion_corta": "Precio",
                "descripcion_larga": "Precio",
                "stock_total": 8,
                "precio_final_ars": 350.0,
                "precioNeto_USD": 3.5,
                "disponible": True,
            },
            {
                "item_id": 4,
                "codigo": "D",
                "descripcion_corta": "Ambos",
                "descripcion_larga": "Ambos",
                "stock_total": 6,
                "precio_final_ars": 420.0,
                "precioNeto_USD": 4.2,
                "disponible": True,
            },
            {
                "item_id": 5,
                "codigo": "E",
                "descripcion_corta": "Alta",
                "descripcion_larga": "Alta",
                "stock_total": 1,
                "precio_final_ars": 50.0,
                "precioNeto_USD": 0.5,
                "disponible": True,
            },
        ]
    )

    result = compare_frames(
        previous,
        current,
        price_tolerance_ars=0.5,
        base_snapshot=Path("prev.json"),
        current_snapshot=Path("curr.json"),
    )

    statuses = set(result.changes["estado_cambio"])
    assert statuses == {"ALTA", "BAJA", "CAMBIO_STOCK", "CAMBIO_PRECIO", "CAMBIO_STOCK_Y_PRECIO"}
    assert result.counts["ALTA"] == 1
    assert result.counts["BAJA"] == 1
    assert result.counts["CAMBIO_STOCK"] == 1
    assert result.counts["CAMBIO_PRECIO"] == 1
    assert result.counts["CAMBIO_STOCK_Y_PRECIO"] == 1

