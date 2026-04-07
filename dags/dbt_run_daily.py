from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# Default arguments untuk semua task
default_args = {
    'owner': 'Ivan Martha',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'dbt_northwind_gold_pipeline',
    default_args=default_args,
    description='Pipeline harian untuk pembaruan Gold Layer Northwind',
    schedule_interval='0 2 * * *',  # Jam 02:00 AM setiap hari
    catchup=False,
    tags=['dbt', 'northwind'],
) as dag:

    # 1. Task untuk menjalankan Snapshot (Capture SCD Type 2)
    dbt_snapshot = BashOperator(
        task_id='dbt_snapshot',
        bash_command='cd /opt/airflow/dbt_project && dbt snapshot',
    )

    # 2. Task untuk menjalankan semua model (Silver & Gold)
    dbt_run = BashOperator(
        task_id='dbt_run_models',
        bash_command='cd /opt/airflow/dbt_project && dbt run',
    )

    # 3. Task untuk menjalankan pengujian data (Data Quality)
    dbt_test = BashOperator(
        task_id='dbt_test_data',
        bash_command='cd /opt/airflow/dbt_project && dbt test',
    )

    # 4. Task Opsional: Generate Docs terbaru setiap hari
    dbt_docs = BashOperator(
        task_id='dbt_docs_generate',
        bash_command='cd /opt/airflow/dbt_project && dbt docs generate',
    )

    # Definisi Alur Kerja (Lineage)
    dbt_snapshot >> dbt_run >> dbt_test >> dbt_docs