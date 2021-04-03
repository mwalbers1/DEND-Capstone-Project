from datetime import datetime, timedelta
from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators import (S3ToRedshiftOperator, DataQualityOperator)
import logging
import covid19_sql_statements

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
The covid19_dag is responsible for creating and then loading data into
the Covid-19 fact tables. Each fact table stores daily case data specific to
the fact table region.
"""
with DAG('covid19_dag',
    default_args=args,
    description = 'Load Covid-19 fact tables in redshift',
    schedule_interval='0 7 * * *') as covid19_dag:

    start_task = DummyOperator(
        task_id = 'start_task'
    )

    create_schema = PostgresOperator(
        task_id = 'create_schema',
        postgres_conn_id = "redshift_conn_id",
        sql=covid19_sql_statements.CREATE_SCHEMA_SQL.format(REDSHIFT_SCHEMA)
    )

    covid19_tables = PostgresOperator(
        task_id = 'covid19_tables',
        postgres_conn_id = 'redshift_conn_id',
        sql=covid19_sql_statements.COVID19_FACT_TABLES_SQL.format(REDSHIFT_SCHEMA)
    )

    load_covid19_americas = PostgresOperator(
        task_id = 'load_covid19_americas',
        postgres_conn_id = 'redshift_conn_id',
        sql=covid19_sql_statements.INSERT_COVID19_SQL_STATEMENT.format(REDSHIFT_SCHEMA,'global.covid19_americas','americas')
    )

    load_covid19_asia = PostgresOperator(
        task_id = 'load_covid19_asia',
        postgres_conn_id = 'redshift_conn_id',
        sql=covid19_sql_statements.INSERT_COVID19_SQL_STATEMENT.format(REDSHIFT_SCHEMA,'global.covid19_asia','asia')
    )

    load_covid19_europe = PostgresOperator(
        task_id = 'load_covid19_europe',
        postgres_conn_id = 'redshift_conn_id',
        sql=covid19_sql_statements.INSERT_COVID19_SQL_STATEMENT.format(REDSHIFT_SCHEMA,'global.covid19_europe','europe')
    )

    load_covid19_africa = PostgresOperator(
        task_id = 'load_covid19_africa',
        postgres_conn_id = 'redshift_conn_id',
        sql=covid19_sql_statements.INSERT_COVID19_SQL_STATEMENT.format(REDSHIFT_SCHEMA,'global.covid19_africa','africa')
    )

    covid19_quality_checks = DataQualityOperator(
        task_id='covid19_quality_checks',
        redshift_conn_id="redshift_conn_id",
        table_names = ['global.covid19_asia', 'global.covid19_africa', 'global.covid19_europe', 'global.covid19_americas']
    )

    end_task = DummyOperator(
        task_id = 'end_task'
    )

    start_task >> create_schema
    create_schema >> covid19_tables

    covid19_tables >> load_covid19_asia
    covid19_tables >> load_covid19_africa
    covid19_tables >> load_covid19_europe
    covid19_tables >> load_covid19_americas

    load_covid19_asia >> covid19_quality_checks
    load_covid19_africa >> covid19_quality_checks
    load_covid19_europe >> covid19_quality_checks
    load_covid19_americas >> covid19_quality_checks

    covid19_quality_checks >> end_task
