FROM apache/airflow:2.10.4

USER airflow

RUN pip install --no-cache-dir \
    dbt-snowflake==1.7.5 \
    apache-airflow-providers-snowflake==5.7.0 \
    psycopg2-binary==2.9.9