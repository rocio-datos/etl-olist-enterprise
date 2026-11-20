import glob
import io
import os
from typing import Optional
import boto3
import pandas as pd


def obtener_cliente_s3():
    return boto3.client(
        "s3",
        endpoint_url=os.getenv("AWS_ENDPOINT_URL", "http://olist-minio:9000"),
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID", "admin"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", "password123"),
        region_name="us-east-1",
    )


def encontrar_csv(patron: str) -> Optional[str]:
    rutas = glob.glob(f"/opt/airflow/data/raw/*{patron}*.csv") + glob.glob(f"data/raw/*{patron}*.csv")
    return rutas[0] if rutas else None


def subir_lotes_diarios_s3(dias_a_simular: int = 5) -> None:
    s3 = obtener_cliente_s3()
    bucket = "olist-landing"

    ruta_orders = encontrar_csv("orders")
    ruta_items = encontrar_csv("items")

    if not ruta_orders:
        print("Error: No se encontró ningún CSV de 'orders' en data/raw.")
        return

    df_orders = pd.read_csv(ruta_orders)
    df_items = pd.read_csv(ruta_items) if ruta_items else pd.DataFrame()

    df_orders["fecha_compra"] = pd.to_datetime(
        df_orders["order_purchase_timestamp"]
    ).dt.strftime("%Y-%m-%d")

    fechas_lote = sorted(df_orders["fecha_compra"].dropna().unique())[250 : 250 + dias_a_simular]

    for fecha in fechas_lote:
        anio, mes, dia = fecha.split("-")
        prefijo_s3 = f"orders/year={anio}/month={mes}/day={dia}"

        orders_dia = df_orders[df_orders["fecha_compra"] == fecha].drop(columns=["fecha_compra"])
        buffer = io.StringIO()
        orders_dia.to_csv(buffer, index=False)
        s3.put_object(Bucket=bucket, Key=f"{prefijo_s3}/orders.csv", Body=buffer.getvalue())
        print(f"[S3 OK] Subido s3://{bucket}/{prefijo_s3}/orders.csv ({len(orders_dia)} filas)")

        if not df_items.empty:
            items_dia = df_items[df_items["order_id"].isin(set(orders_dia["order_id"]))].copy()
            if not items_dia.empty:
                items_dia.iloc[0, items_dia.columns.get_loc("price")] = -50.00
                buf_i = io.StringIO()
                items_dia.to_csv(buf_i, index=False)
                s3.put_object(Bucket=bucket, Key=f"{prefijo_s3}/order_items.csv", Body=buf_i.getvalue())
                print(f"[S3 OK] Subido s3://{bucket}/{prefijo_s3}/order_items.csv ({len(items_dia)} filas)")


if __name__ == "__main__":
    subir_lotes_diarios_s3()
