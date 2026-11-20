import io
import os
from datetime import datetime, timezone
import boto3
import pandas as pd
import pyarrow as pa
from pyiceberg.catalog import load_catalog
from pyiceberg.exceptions import NamespaceAlreadyExistsError, TableAlreadyExistsError


def obtener_catalogo_iceberg():
    return load_catalog(
        "rest",
        **{
            "uri": os.getenv("ICEBERG_REST_URI", "http://olist-iceberg-rest:8181"),
            "s3.endpoint": os.getenv("AWS_ENDPOINT_URL", "http://olist-minio:9000"),
            "s3.access-key-id": os.getenv("AWS_ACCESS_KEY_ID", "admin"),
            "s3.secret-access-key": os.getenv("AWS_SECRET_ACCESS_KEY", "password123"),
            "s3.region": "us-east-1",
        },
    )


def obtener_s3():
    return boto3.client(
        "s3",
        endpoint_url=os.getenv("AWS_ENDPOINT_URL", "http://olist-minio:9000"),
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID", "admin"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", "password123"),
        region_name="us-east-1",
    )


def procesar_landing_a_bronze() -> None:
    s3 = obtener_s3()
    catalog = obtener_catalogo_iceberg()

    try:
        catalog.create_namespace("bronze")
    except NamespaceAlreadyExistsError:
        pass

    objetos = s3.list_objects_v2(Bucket="olist-landing", Prefix="orders/")
    if "Contents" not in objetos:
        print("No hay archivos nuevos en olist-landing.")
        return

    lotes_orders, lotes_items, lotes_cuarentena = [], [], []
    timestamp_ingesta = datetime.now(timezone.utc).isoformat()

    for obj in objetos["Contents"]:
        key = obj["Key"]
        contenido = s3.get_object(Bucket="olist-landing", Key=key)["Body"].read()
        df = pd.read_csv(io.BytesIO(contenido))
        df["_ingested_at"] = timestamp_ingesta
        df["_source_file"] = f"s3://olist-landing/{key}"

        if key.endswith("orders.csv"):
            lotes_orders.append(df)
        elif key.endswith("order_items.csv"):
            invalidos = df[df["price"] <= 0].copy()
            validos = df[df["price"] > 0].copy()
            if not invalidos.empty:
                invalidos["_rejection_reason"] = "price <= 0 (Data Contract violation)"
                lotes_cuarentena.append(invalidos)
            lotes_items.append(validos)

    if lotes_cuarentena:
        df_bad = pd.concat(lotes_cuarentena, ignore_index=True)
        buf_bad = io.StringIO()
        df_bad.to_csv(buf_bad, index=False)
        ruta_q = f"rejected_items/run={datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.csv"
        s3.put_object(Bucket="olist-quarantine", Key=ruta_q, Body=buf_bad.getvalue())
        print(f"[QUARANTINE] {len(df_bad)} filas corruptas aisladas en s3://olist-quarantine/{ruta_q}")

    for nombre_tabla, lista_dfs in [("bronze.orders", lotes_orders), ("bronze.order_items", lotes_items)]:
        if not lista_dfs:
            continue
        df_total = pd.concat(lista_dfs, ignore_index=True)
        arrow_table = pa.Table.from_pandas(df_total, preserve_index=False)

        try:
            tabla = catalog.create_table(nombre_tabla, schema=arrow_table.schema)
        except TableAlreadyExistsError:
            tabla = catalog.load_table(nombre_tabla)

        tabla.overwrite(arrow_table)
        print(f"[ICEBERG BRONZE] Tabla '{nombre_tabla}' actualizada con {len(df_total)} registros ACID.")


if __name__ == "__main__":
    procesar_landing_a_bronze()
