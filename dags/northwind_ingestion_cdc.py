from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.operators.python import PythonOperator
from snowflake.connector.pandas_tools import write_pandas
from datetime import datetime

TABLES_TO_INGEST = [
    'customer', 'employees', 'inventory_transactions',
    'order_details', 'orders', 'products',
    'purchase_order_details', 'purchase_orders', 'suppliers'
]

default_args = {
    'owner': 'Ivan Martha',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
}

def ingest_to_snowflake_dev(table_name):
    pg_hook = PostgresHook(postgres_conn_id='postgres_northwind')
    sf_hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')

    df = pg_hook.get_pandas_df(f"SELECT * FROM {table_name}")
    df.columns = [x.upper() for x in df.columns]

    if df.empty:
        print(f"⚠️ Tabel {table_name} kosong di Postgres.")
        return

    # ✅ TAMBAHKAN INI — ganti string kosong dengan None (NULL)
    df = df.replace('', None)

    # ✅ TAMBAHKAN INI — paksa kolom numerik jadi float (bukan string)
    for col in df.columns:
        try:
            df[col] = pd.to_numeric(df[col], errors='ignore')
        except Exception:
            pass

    conn = sf_hook.get_conn()
    try:
        success, nchunks, nrows, _ = write_pandas(
            conn=conn,
            df=df,
            table_name=table_name.upper(),
            database='DBT_DEV_DB',
            schema='PUBLIC_1_RAW',
            quote_identifiers=False,
            auto_create_table=True
        )
        if success:
            print(f"✅ {nrows} baris masuk ke DBT_DEV_DB.PUBLIC_1_RAW.{table_name.upper()}")
        else:
            raise Exception(f"❌ write_pandas gagal untuk {table_name.upper()}")
    finally:
        conn.close()

with DAG(
    'data_northwind_ingestion_postgres_snowflake',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    tags=['dev', 'northwind']
) as dag:

    tasks = []
    for table in TABLES_TO_INGEST:
        task = PythonOperator(
            task_id=f'ingest_{table}',
            python_callable=ingest_to_snowflake_dev,
            op_args=[table]
        )
        tasks.append(task)

    # Jalankan sequential
    for i in range(len(tasks) - 1):
        tasks[i] >> tasks[i + 1]