from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "rocio-data-eng",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="olist_enterprise_lakehouse_medallion",
    default_args=default_args,
    description="Pipeline Senior End-to-End: S3 Landing -> Quarantine -> Apache Iceberg (Bronze/Silver/Gold)",
    schedule_interval="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["iceberg", "s3", "medallion", "ecommerce", "tier-1"],
) as dag:

    ingesta_s3_landing = BashOperator(
        task_id="1_ingesta_diaria_s3_landing",
        bash_command="python /opt/airflow/include/extract/simulador_s3.py",
    )

    bronze_y_cuarentena = BashOperator(
        task_id="2_data_contracts_quarantine_y_bronze_iceberg",
        bash_command="python /opt/airflow/include/transform/bronze_iceberg.py",
    )

    silver_y_gold_iceberg = BashOperator(
        task_id="3_transformacion_silver_y_gold_iceberg",
        bash_command="python /opt/airflow/include/transform/silver_gold_iceberg.py",
    )

    ingesta_s3_landing >> bronze_y_cuarentena >> silver_y_gold_iceberg
