# DAG for loading gapminder data into redshift staging tables
#
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators import (S3ToRedshiftOperator, GapminderOperator, DataQualityOperator)

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
}

S3_BUCKET_NAME = 'udacity-dend-final-project'


REDSHIFT_SCHEMA = 'global'

"""
Each demographic category of gdp, education, government, females, males, and population
has a pre-load task and a load task.  The pre-load tasks reads the raw gapminder data files from
an S3 source folder and then merges them into one file for the demographic category. The load task
will load data from the file created in the pre-load task into a staging table in the AWS Redshift
database.
"""

with DAG(dag_id='gapminder_staging_dag',
        default_args=args,
        schedule_interval='0 1 * * *') as gapminder_staging_dag:

    start_task = DummyOperator(
	    task_id = 'start_task'
	)

    create_schema = PostgresOperator(
        task_id = 'create_schema',
        postgres_conn_id = "redshift_conn_id",
        sql=gapminder_sql_statements.CREATE_SCHEMA_SQL.format(REDSHIFT_SCHEMA)
    )

    # gdp staging
    pre_load_gdp = GapminderOperator(
        task_id = 'pre_load_gdp',
        aws_credentials_id = 'aws_credentials',
        s3_bucket = S3_BUCKET_NAME,
        origin_s3_folder = 'gapminder/gdp',
        destination_s3_folder = 'demographic_test/gdp.csv'
    )

    gdp_staging_table = PostgresOperator(
        task_id="gdp_staging_table",
        postgres_conn_id = "redshift_conn_id",
        sql=gapminder_sql_statements.GDP_STAGING_TABLE_SQL.format(REDSHIFT_SCHEMA,REDSHIFT_SCHEMA)
    )

    load_gdp_staging = S3ToRedshiftOperator(
        task_id = 'load_gdp_staging',
        table = 'global.gdp_staging',
        redshift_conn_id = 'redshift_conn_id',
        aws_credentials_id = 'aws_credentials',
        s3_bucket = S3_BUCKET_NAME,
        s3_key = 'demographic_test/gdp.csv'
    )

    # education staging
    pre_load_education = GapminderOperator(
        task_id = 'pre_load_education',
        aws_credentials_id = 'aws_credentials',
        s3_bucket = S3_BUCKET_NAME,
        origin_s3_folder = 'gapminder/education',
        destination_s3_folder = 'demographic_test/education.csv'
    )

    education_staging_table = PostgresOperator(
        task_id="education_staging_table",
        postgres_conn_id="redshift_conn_id",
        sql=gapminder_sql_statements.EDUCATION_STAGING_TABLE_SQL.format(REDSHIFT_SCHEMA,REDSHIFT_SCHEMA)
    )

    load_education_staging = S3ToRedshiftOperator(
        task_id = 'load_education_staging',
        table = 'global.education_staging',
        redshift_conn_id = 'redshift_conn_id',
        aws_credentials_id = 'aws_credentials',
        s3_bucket = S3_BUCKET_NAME,
        s3_key = 'demographic_test/education.csv'
    )

    # females staging
    pre_load_females = GapminderOperator(
        task_id = 'pre_load_females',
        aws_credentials_id = 'aws_credentials',
        s3_bucket = S3_BUCKET_NAME,
        origin_s3_folder = 'gapminder/females',
        destination_s3_folder = 'demographic_test/females.csv'
    )

    females_staging_table = PostgresOperator(
        task_id="females_staging_table",
        postgres_conn_id="redshift_conn_id",
        sql=gapminder_sql_statements.FEMALE_STAGING_TABLE_SQL.format(REDSHIFT_SCHEMA,REDSHIFT_SCHEMA)
    )

    load_females_staging = S3ToRedshiftOperator(
        task_id = 'load_females_staging',
        table = 'global.female_staging',
        redshift_conn_id = 'redshift_conn_id',
        aws_credentials_id = 'aws_credentials',
        s3_bucket = S3_BUCKET_NAME,
        s3_key = 'demographic_test/females.csv'
    )

    # males staging
    pre_load_males = GapminderOperator(
        task_id = 'pre_load_males',
        aws_credentials_id = 'aws_credentials',
        s3_bucket = S3_BUCKET_NAME,
        origin_s3_folder = 'gapminder/males',
        destination_s3_folder = 'demographic_test/males.csv'
    )

    males_staging_table = PostgresOperator(
        task_id="males_staging_table",
        postgres_conn_id="redshift_conn_id",
        sql=gapminder_sql_statements.MALE_STAGING_TABLE_SQL.format(REDSHIFT_SCHEMA,REDSHIFT_SCHEMA)
    )

    load_males_staging = S3ToRedshiftOperator(
        task_id = 'load_males_staging',
        table = 'global.male_staging',
        redshift_conn_id = 'redshift_conn_id',
        aws_credentials_id = 'aws_credentials',
        s3_bucket = S3_BUCKET_NAME,
        s3_key = 'demographic_test/males.csv'
    )


    # government staging
    pre_load_govt = GapminderOperator(
        task_id = 'pre_load_govt',
        aws_credentials_id = 'aws_credentials',
        s3_bucket = S3_BUCKET_NAME,
        origin_s3_folder = 'gapminder/govt',
        destination_s3_folder = 'demographic_test/govt.csv'
    )

    govt_staging_table = PostgresOperator(
        task_id="govt_staging_table",
        postgres_conn_id="redshift_conn_id",
        sql=gapminder_sql_statements.GOVERNMENT_STAGING_TABLE_SQL.format(REDSHIFT_SCHEMA,REDSHIFT_SCHEMA)
    )

    load_govt_staging = S3ToRedshiftOperator(
        task_id = "load_govt_staging",
        table = 'global.government_staging',
        redshift_conn_id = 'redshift_conn_id',
        aws_credentials_id = 'aws_credentials',
        s3_bucket = S3_BUCKET_NAME,
        s3_key = 'demographic_test/govt.csv'

    )

    # population staging
    pre_load_population = GapminderOperator(
        task_id = 'pre_load_population',
        aws_credentials_id = 'aws_credentials',
        s3_bucket = S3_BUCKET_NAME,
        origin_s3_folder = 'gapminder/population',
        destination_s3_folder = 'demographic_test/population.csv'
    )

    population_staging_table = PostgresOperator(
        task_id="population_staging_table",
        postgres_conn_id="redshift_conn_id",
        sql=gapminder_sql_statements.POPULATION_STAGING_TABLE_SQL.format(REDSHIFT_SCHEMA,REDSHIFT_SCHEMA)
    )

    load_population_staging = S3ToRedshiftOperator(
        task_id = 'load_population_staging',
        table = 'global.population_staging',
        redshift_conn_id = 'redshift_conn_id',
        aws_credentials_id = 'aws_credentials',
        s3_bucket = S3_BUCKET_NAME,
        s3_key = 'demographic_test/population.csv'
    )

    run_quality_checks = DataQualityOperator(
        task_id='run_data_quality_checks',
        redshift_conn_id="redshift_conn_id",
        table_names = ["global.gdp_staging","global.education_staging","global.female_staging","global.male_staging", "global.population_staging","global.government_staging"]
    )

    end_operator = DummyOperator(task_id='Stop_execution')

    start_task >> create_schema

    create_schema >> pre_load_education
    pre_load_education >> education_staging_table
    education_staging_table >> load_education_staging

    create_schema >> pre_load_females
    pre_load_females >> females_staging_table
    females_staging_table >> load_females_staging

    create_schema >> pre_load_males
    pre_load_males >> males_staging_table
    males_staging_table >> load_males_staging

    create_schema >> pre_load_gdp
    pre_load_gdp >> gdp_staging_table
    gdp_staging_table >> load_gdp_staging

    create_schema >> pre_load_govt
    pre_load_govt >> govt_staging_table
    govt_staging_table >> load_govt_staging

    create_schema >> pre_load_population
    pre_load_population >> population_staging_table
    population_staging_table >> load_population_staging

    load_education_staging >> run_quality_checks
    load_females_staging >> run_quality_checks
    load_males_staging >> run_quality_checks
    load_gdp_staging >> run_quality_checks
    load_population_staging >> run_quality_checks
    load_govt_staging >> run_quality_checks

    run_quality_checks >> end_operator
