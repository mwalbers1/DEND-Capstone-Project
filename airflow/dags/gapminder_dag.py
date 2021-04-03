from datetime import datetime, timedelta
from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators import (S3ToRedshiftOperator, DataQualityOperator)
import logging
import gapminder_sql_statements

start_date = datetime.utcnow()

args = {
    'owner': 'Michael Albers',
    'start_date': start_date,
    'depends_on_past': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'provide_context': True,
}

REDSHIFT_SCHEMA = 'global'

"""
The gapminder_dag creates the gapminder fact table called global.world_demographics.
It then loads the global.world_demographics table from the gapminder staging tables.
"""

with DAG('gapminder_dag',
    default_args=args,
    description = 'Load gapminder fact table in redshift',
    schedule_interval='0 7 * * *') as gapminder_dag:

    start_task = DummyOperator(
        task_id = 'start_task'
    )

    create_schema = PostgresOperator(
        task_id = 'create_schema',
        postgres_conn_id = "redshift_conn_id",
        sql = gapminder_sql_statements.CREATE_SCHEMA_SQL.format(REDSHIFT_SCHEMA)
    )

    world_demographics_table = PostgresOperator(
        task_id = 'world_demographics_table',
        postgres_conn_id = 'redshift_conn_id',
        sql = gapminder_sql_statements.WORLD_DEMOGRAPHICS_TABLE_SQL.format(REDSHIFT_SCHEMA,REDSHIFT_SCHEMA)
    )

    load_world_demographics_table = PostgresOperator(
        task_id = 'load_world_demographics_table',
        postgres_conn_id = 'redshift_conn_id',
        sql = gapminder_sql_statements.INSERT_WORLD_DEMOGRAPHICS_SQL
    )

    run_quality_check = DataQualityOperator(
        task_id = 'run_quality_check',
        redshift_conn_id="redshift_conn_id",
        table_names = ['global.world_demographics']
    )

    end_task = DummyOperator(
        task_id = 'end_task'
    )

    start_task >> create_schema
    create_schema >> world_demographics_table
    world_demographics_table >> load_world_demographics_table
    load_world_demographics_table >> run_quality_check
    run_quality_check >> end_task
