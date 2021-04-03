# DAG for setting up dimension tables
#
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators import (S3ToRedshiftOperator, DataQualityOperator)

import dimension_sql_statements

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
}

S3_BUCKET_NAME = 'udacity-dend-final-project'
REDSHIFT_SCHEMA = 'global'

"""
Loads country data from three data sources, gapminder, covid-19, and kaggle world-countries into three separate staging tables.
Then merge the three staging tables based on country name pattern matching to load two dimension tables called global.world_countries
and global.country_year
"""
with DAG(dag_id='dimension_dag',
        default_args=args,
        schedule_interval='0 0 * * *') as dimension_dag:

    start_task = DummyOperator(
	    task_id = 'start_task'
	)

    create_schema = PostgresOperator(
        task_id = 'create_schema',
        postgres_conn_id = "redshift_conn_id",
        sql=dimension_sql_statements.CREATE_SCHEMA_SQL.format(REDSHIFT_SCHEMA)
    )

    staging_world_countries_table = PostgresOperator(
        task_id="staging_world_countries_table",
        postgres_conn_id = "redshift_conn_id",
        sql=dimension_sql_statements.STAGING_WORLD_COUNTRIES_TABLE_CREATE.format(REDSHIFT_SCHEMA,REDSHIFT_SCHEMA)
    )

    load_staging_world_countries = S3ToRedshiftOperator(
        task_id = 'load_staging_world_countries',
        table = 'global.staging_world_countries',
        redshift_conn_id = 'redshift_conn_id',
        aws_credentials_id = 'aws_credentials',
        s3_bucket = S3_BUCKET_NAME,
        s3_key = 'world-countries/world-countries.json',
        json_path = 's3://udacity-dend-final-project/world_country_json_path.json'
    )

    staging_covid19_country_table = PostgresOperator(
        task_id = 'staging_covid19_country_table',
        postgres_conn_id = "redshift_conn_id",
        sql=dimension_sql_statements.STAGING_COVID19_COUNTRY_TABLE_CREATE.format(REDSHIFT_SCHEMA,REDSHIFT_SCHEMA)
    )

    load_staging_covid19_country = S3ToRedshiftOperator(
        task_id = 'load_staging_covid19_country',
        table = 'global.staging_covid19_country',
        redshift_conn_id = 'redshift_conn_id',
        aws_credentials_id = 'aws_credentials',
        s3_bucket = S3_BUCKET_NAME,
        s3_key = 'world-countries/Covid19_UID_ISO_FIPS_LookUp_Table.csv'
    )

    gapminder_country_table = PostgresOperator (
        task_id = 'gapminder_country_table',
        postgres_conn_id = "redshift_conn_id",
        sql=dimension_sql_statements.GAPMINDER_COUNTRY_TABLE_CREATE.format(REDSHIFT_SCHEMA,REDSHIFT_SCHEMA)
    )

    load_gapminder_country_table = S3ToRedshiftOperator(
        task_id = 'load_gapminder_country_table',
        table = 'global.gapminder_country',
        redshift_conn_id = 'redshift_conn_id',
        aws_credentials_id = 'aws_credentials',
        s3_bucket = S3_BUCKET_NAME,
        s3_key = 'world-countries/ddf--entities--geo--country.csv'
    )

    run_quality_checks = DataQualityOperator(
        task_id='run_data_quality_checks',
        redshift_conn_id="redshift_conn_id",
        table_names = ['global.staging_world_countries','global.staging_covid19_country','global.gapminder_country']
    )

    world_countries_table = PostgresOperator(
        task_id = 'world_countries_table',
        postgres_conn_id = "redshift_conn_id",
        sql=dimension_sql_statements.COUNTRY_DIMENSION_TABLE_CREATE.format(REDSHIFT_SCHEMA,REDSHIFT_SCHEMA)
    )

    load_world_countries_table = PostgresOperator(
        task_id = "load_world_countries_table",
        postgres_conn_id = "redshift_conn_id",
        sql=dimension_sql_statements.INSERT_WORLD_COUNTRIES_SQL.format(REDSHIFT_SCHEMA)
    )

    country_year_table = PostgresOperator(
        task_id = "country_year_table",
        postgres_conn_id = "redshift_conn_id",
        sql=dimension_sql_statements.COUNTRY_YEAR_TABLE_CREATE.format(REDSHIFT_SCHEMA,REDSHIFT_SCHEMA,REDSHIFT_SCHEMA)
    )

    load_country_year_table = PostgresOperator(
        task_id = "load_country_year_table",
        postgres_conn_id = "redshift_conn_id",
        sql=dimension_sql_statements.INSERT_COUNTRY_YEAR_SQL.format(REDSHIFT_SCHEMA)
    )

    run_quality_check_world_countries = DataQualityOperator(
        task_id='run_quality_check_world_countries',
        redshift_conn_id="redshift_conn_id",
        table_names = ['global.world_countries','global.country_year']
    )

    end_task = DummyOperator(
        task_id = 'end_task'
    )

    start_task >> create_schema
    create_schema >> staging_world_countries_table
    create_schema >>  staging_covid19_country_table
    create_schema >> gapminder_country_table

    staging_world_countries_table >> load_staging_world_countries
    staging_covid19_country_table >> load_staging_covid19_country
    gapminder_country_table >> load_gapminder_country_table

    load_staging_world_countries >> run_quality_checks
    load_staging_covid19_country >> run_quality_checks
    load_gapminder_country_table >> run_quality_checks

    run_quality_checks >> world_countries_table
    world_countries_table >> load_world_countries_table

    load_world_countries_table >> country_year_table
    country_year_table >> load_country_year_table

    load_country_year_table >> run_quality_check_world_countries
    run_quality_check_world_countries >> end_task
