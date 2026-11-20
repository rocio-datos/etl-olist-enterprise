# Olist Enterprise Lakehouse (S3 + Apache Iceberg + Apache Airflow)

Arquitectura de datos End-to-End de nivel producción diseñada bajo el paradigma **Medallion Lakehouse**, incorporando almacenamiento compatible con **AWS S3 (MinIO)**, formato de tabla abierto **Apache Iceberg**, **Data Contracts** con aislamiento en cuarentena y orquestación con **Apache Airflow**.

## Arquitectura del Sistema

```text
[Olist E-Commerce CSVs]
         │
         ▼ (Ingesta diaria particionada: year=YYYY/month=MM/day=DD)
[S3 Landing Bucket: s3://olist-landing/]
         │
         ▼ (Data Quality Gate & Schema Enforcement)
         ├──► Registros corruptos (price <= 0) ──► [s3://olist-quarantine/]
         │
         ▼ Registros válidos + Auditoría (_ingested_at, _source_file)
[Apache Iceberg - Capa BRONZE (s3://olist-warehouse/bronze/)]
         │
         ▼ (Deduplicación, tipado estricto y reglas logísticas)
[Apache Iceberg - Capa SILVER (s3://olist-warehouse/silver/)]
         │
         ▼ (Agregaciones de negocio: GMV, Ticket Promedio, Entregas Tardías)
[Apache Iceberg - Capa GOLD (s3://olist-warehouse/gold/)]
```

## Stack Tecnológico
* **Orquestación:** Apache Airflow 2.8
* **Object Storage:** MinIO (100% compatible con AWS S3 API / `boto3`)
* **Open Table Format:** Apache Iceberg (`pyiceberg` + Tabulario Iceberg REST Catalog)
* **Procesamiento & Calidad:** Python, PyArrow, Pandas, Pytest
* **Infraestructura:** Docker

## Decisiones de Arquitectura (ADRs)
1. **Apache Iceberg sobre Parquet plano:** Garantiza transacciones ACID, evolución de esquema sin reescritura completa y *Time Travel* sobre almacenamiento de objetos S3.
2. **Patrón Quarantine (Resiliencia):** Las anomalías detectadas en origen no detienen el pipeline; se desvían a `s3://olist-quarantine/` con trazabilidad del motivo de rechazo.
3. **Idempotencia:** Todas las cargas emplean sobreescritura atómica controlada y deduplicación por llave primaria (`order_id`), permitiendo re-ejecuciones seguras ante fallos.
