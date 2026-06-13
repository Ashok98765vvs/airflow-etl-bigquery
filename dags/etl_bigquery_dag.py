"""etl_bigquery_dag.py

Airflow DAG: API → GCS → BigQuery → dbt → Great Expectations
Author: Ashok Chowdary
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator,
)
from airflow.operators.bash import BashOperator

# ── Default args ─────────────────────────────────────────────────────────────────
default_args = {
    "owner": "ashok",
    "depends_on_past": False,
    "start_date": datetime(2025, 1, 1),
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": True,
    "email": ["ashok98765vvs@gmail.com"],
}

# ── DAG definition ──────────────────────────────────────────────────────────────
with DAG(
    dag_id="etl_api_to_bigquery",
    default_args=default_args,
    description="ETL: Public API -> GCS -> BigQuery -> dbt -> Great Expectations",
    schedule_interval="@daily",
    catchup=False,
    tags=["etl", "bigquery", "dbt", "great-expectations"],
) as dag:

    # Task 1: Extract data from public API and upload to GCS
    def extract_to_gcs(**context):
        """Fetch data from public API and store as JSON in GCS."""
        import requests
        import json
        from google.cloud import storage

        url = "https://data.cityofnewyork.us/resource/nc67-uf89.json?$limit=5000"
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        data = response.json()

        client = storage.Client()
        bucket = client.bucket("your-gcs-bucket")
        blob = bucket.blob(f"raw/nyc_data_{context['ds']}.json")
        blob.upload_from_string(json.dumps(data), content_type="application/json")
        print(f"Uploaded {len(data)} records to GCS")
        return len(data)

    extract_task = PythonOperator(
        task_id="extract_api_to_gcs",
        python_callable=extract_to_gcs,
        provide_context=True,
    )

    # Task 2: Ensure BigQuery dataset exists
    create_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id="create_bq_dataset",
        dataset_id="raw_dataset",
        location="US",
    )

    # Task 3: Load GCS JSON to BigQuery
    load_to_bq = GCSToBigQueryOperator(
        task_id="load_gcs_to_bigquery",
        bucket="your-gcs-bucket",
        source_objects=["raw/nyc_data_{{ ds }}.json"],
        destination_project_dataset_table="raw_dataset.nyc_raw",
        source_format="NEWLINE_DELIMITED_JSON",
        write_disposition="WRITE_APPEND",
        autodetect=True,
    )

    # Task 4: Run dbt transformations
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="cd /opt/airflow/dbt && dbt run --profiles-dir . --target prod",
    )

    # Task 5: Run dbt tests (Great Expectations via dbt-expectations)
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="cd /opt/airflow/dbt && dbt test --profiles-dir . --target prod",
    )

    # ── Task dependencies ───────────────────────────────────────────────────────────
    extract_task >> create_dataset >> load_to_bq >> dbt_run >> dbt_test
