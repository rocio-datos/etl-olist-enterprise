import os
from datetime import datetime, timezone
import pandas as pd
import pyarrow as pa
from pyiceberg.catalog import load_catalog
from pyiceberg.exceptions import NamespaceAlreadyExistsError, TableAlreadyExistsError


def obtener_catalogo():
    return load_catalog(
        "rest",
        **{
            "uri": os.getenv("ICEBERG_REST_URI", "http://olist-iceberg-rest:8181"),
            "s3.endpoint": os.getenv("AWS_ENDPOINT_URL", "http://olist-minio:9000"),
            "s3.access-key-id": os.getenv("AWS_ACCESS_KEY_ID", "admin"),
            "s3.secret-access-key": os.getenv("AWS_SECRET_ACCESS_KEY", "password123"),
            "s3.region": "us-east-1",
            "downcast-ns-timestamp-to-us-on-write": "true",
        },
    )


def guardar_tabla_iceberg(catalog, namespace: str, tabla_nombre: str, df: pd.DataFrame) -> None:
    try:
        catalog.create_namespace(namespace)
    except NamespaceAlreadyExistsError:
        pass

    full_name = f"{namespace}.{tabla_nombre}"
    arrow_table = pa.Table.from_pandas(df, preserve_index=False)

    try:
        tabla = catalog.create_table(full_name, schema=arrow_table.schema)
    except TableAlreadyExistsError:
        tabla = catalog.load_table(full_name)

    tabla.overwrite(arrow_table)
    print(f"[ICEBERG {namespace.upper()}] Tabla '{full_name}' actualizada ({len(df)} registros ACID).")


def construir_silver_y_gold() -> None:
    catalog = obtener_catalogo()

    df_orders = catalog.load_table("bronze.orders").scan().to_pandas()
    df_items = catalog.load_table("bronze.order_items").scan().to_pandas()

    # Capa SILVER: Deduplicación y precisión de microsegundos compatible con Iceberg
    df_orders_clean = df_orders.drop_duplicates(subset=["order_id"]).copy()
    for col in ["order_purchase_timestamp", "order_delivered_customer_date", "order_estimated_delivery_date"]:
        df_orders_clean[col] = pd.to_datetime(df_orders_clean[col]).astype("datetime64[us]")

    df_orders_clean["es_entrega_tardia"] = (
        df_orders_clean["order_delivered_customer_date"] > df_orders_clean["order_estimated_delivery_date"]
    ).fillna(False)
    df_orders_clean["fecha_orden"] = df_orders_clean["order_purchase_timestamp"].dt.strftime("%Y-%m-%d")

    df_items_clean = df_items.drop_duplicates(subset=["order_id", "order_item_id"]).copy()
    df_items_clean["total_item_value"] = df_items_clean["price"] + df_items_clean["freight_value"]

    guardar_tabla_iceberg(catalog, "silver", "orders_clean", df_orders_clean)
    guardar_tabla_iceberg(catalog, "silver", "order_items_clean", df_items_clean)

    # Capa GOLD: KPIs Ejecutivos Diarios
    df_joined = pd.merge(df_orders_clean, df_items_clean, on="order_id", how="inner")

    df_gold_kpis = (
        df_joined.groupby("fecha_orden", as_index=False)
        .agg(
            total_ordenes=("order_id", "nunique"),
            total_items_vendidos=("order_item_id", "count"),
            gmv_total=("price", "sum"),
            flete_total=("freight_value", "sum"),
            ingreso_bruto=("total_item_value", "sum"),
            ticket_promedio=("total_item_value", "mean"),
            ordenes_tardias=("es_entrega_tardia", "sum"),
        )
        .sort_values("fecha_orden")
    )
    df_gold_kpis["_gold_updated_at"] = datetime.now(timezone.utc).isoformat()

    guardar_tabla_iceberg(catalog, "gold", "kpi_ventas_diarias", df_gold_kpis)


if __name__ == "__main__":
    construir_silver_y_gold()
