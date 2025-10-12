FROM apache/airflow:3.1.0
USER airflow
RUN pip install --no-cache-dir \
    dbt-core==1.10.13 \
    dbt-snowflake==1.10.2


ENV PATH="/home/airflow/.local/bin:${PATH}"