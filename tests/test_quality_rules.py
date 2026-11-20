import pandas as pd


def test_regla_cuarentena_precios_negativos():
    df = pd.DataFrame({
        "order_id": ["ord_1", "ord_2"],
        "price": [120.50, -50.00],
    })
    validos = df[df["price"] > 0]
    invalidos = df[df["price"] <= 0]
    assert len(validos) == 1
    assert len(invalidos) == 1
    assert invalidos.iloc[0]["order_id"] == "ord_2"
