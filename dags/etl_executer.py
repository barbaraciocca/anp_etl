# -*- coding: utf-8 -*-
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from etl_operator import ANPOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

"""
Airflow DAG that extracts url ANP data.

This DAG extract, loads and tranform the report from the url. 
It also handles the necessary steps to properly treat the data and create a postgres table.

create_table --> anp_operator --> etl_executer

"""

default_args = {
    "owner": 'airflow',
    "retries": 1,
    "execution_timeout": timedelta(seconds=900) #15min
}

with DAG(
        dag_id='etl_executer',
        schedule_interval=timedelta(days=1), 
        start_date=datetime(2023, 10, 30),
        tags=["ETL", "ANP", "Raízen"],
        catchup=False,
        default_args=default_args
    ) as dag:

    create_table = PostgresOperator(
        task_id='create_postgre_table',
        postgres_conn_id='etl_postgres',
        sql="""
            CREATE TABLE IF NOT EXISTS vendas_combustiveis (
                ID SERIAL PRIMARY KEY,
                year_month DATE,
                uf VARCHAR,
                product VARCHAR,
                unit VARCHAR,
                volume DOUBLE PRECISION,
                created_at TIMESTAMP
            );
        """
    )
    anp_operator = ANPOperator(task_id='anp_operator_task')

    execute_anp = PythonOperator(
        task_id='execute_anp',
        python_callable=anp_operator.execute,
        provide_context=True,
    )

    create_table >> anp_operator >> execute_anp