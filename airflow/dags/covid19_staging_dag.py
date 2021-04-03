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

S3_BUCKET_NAME = 'udacity-dend-final-project'

REDSHIFT_SCHEMA = 'global'


def has_rows(table_name, task_name, prior_task, **kwargs):
    """
    Counts number of rows loaded into covid19 staging table. Checks if no rows loaded and
    compares rowcount of rows loaded with rowcount loaded from previous load task

    args:
        table_name: name of table to validate rowcount on
        task_name: name of the data check task
        prior_task: name of the prior data check task

    """
    prior_count = None
    if prior_task is not None:
        logging.info(f"The prior task is {prior_task}")
        prior_task_instance = kwargs['ti']
        if prior_task_instance is not None:
            logging.info(f"prior_task_instance is not None")
            prior_count = prior_task_instance.xcom_pull(task_ids=prior_task, key='num_records')
            if prior_count is not None:
                logging.info(f"Prior task - {prior_task} loaded {prior_count} rows into {table_name}")

    redshift_hook = PostgresHook("redshift_conn_id")
    records = redshift_hook.get_records(f"SELECT count(*) FROM {table_name}")

    num_records = 0
    if len(records) > 0 and len(records[0]) > 0:
        num_records = records[0][0]

    if num_records < 1:
        raise ValueError(f"Data quality check failed. {table_name} contained 0 rows")

    if prior_count is not None:
        if num_records == prior_count:
            raise ValueError(f"Data quality check failed. Prior task loaded 0 rows")

    logging.info(f"{task_name} completed. There are {num_records} rows in the {table_name} table")
    task_instance = kwargs['ti']
    task_instance.xcom_push('num_records', num_records)

"""
In four steps, load data into the global.staging_covid19_daily_cases table
from the four covid-19 source file folders.
"""


with DAG('covid19_staging_dag',
          default_args=args,
          description='Load daily covid-19 data into redshift staging table',
          schedule_interval='0 1 * * *') as covid19_staging_dag:

    start_task = DummyOperator(
        task_id = 'start_task'
    )

    create_schema = PostgresOperator(
        task_id = 'create_schema',
        postgres_conn_id = "redshift_conn_id",
        sql=covid19_sql_statements.CREATE_SCHEMA_SQL.format(REDSHIFT_SCHEMA)
    )

    covid19_staging_table = PostgresOperator(
        task_id="covid19_staging_table",
        postgres_conn_id='redshift_conn_id',
        sql=covid19_sql_statements.STAGING_COVID19_DAILY_CASES_SQL.format(REDSHIFT_SCHEMA,REDSHIFT_SCHEMA)
    )

    # copy format 1
    load_covid19_format1 = S3ToRedshiftOperator(
        task_id = 'load_covid19_format1',
        table = 'global.staging_covid19_daily_cases(Province_State,Country_Region,Last_Update,Confirmed,Deaths,Recovered)',
        redshift_conn_id = 'redshift_conn_id',
        aws_credentials_id = 'aws_credentials',
        s3_bucket = S3_BUCKET_NAME,
        s3_key = 'covid-19/1'
    )

    data_check_task_1 = PythonOperator(
        task_id="data_check_task_1",
        python_callable=has_rows,
        op_kwargs={'table_name':'global.staging_covid19_daily_cases', 'task_name':'load_covid19_format1', 'prior_task':None}
    )

    # copy format 2
    load_covid19_format2 = S3ToRedshiftOperator(
        task_id = 'load_covid19_format2',
        table = 'global.staging_covid19_daily_cases(Province_State,Country_Region,Last_Update,Confirmed,\
        Deaths,Recovered,Latitude,Longitude)',
        redshift_conn_id = 'redshift_conn_id',
        aws_credentials_id = 'aws_credentials',
        s3_bucket = S3_BUCKET_NAME,
        s3_key = 'covid-19/2'
    )

    data_check_task_2 = PythonOperator(
        task_id="data_check_task_2",
        python_callable=has_rows,
        op_kwargs={'table_name':'global.staging_covid19_daily_cases', 'task_name':'load_covid19_format2', 'prior_task':'data_check_task_1'}
    )

    # copy format 3
    load_covid19_format3 = S3ToRedshiftOperator(
        task_id = 'load_covid19_format3',
        table = 'global.staging_covid19_daily_cases(FIPS,Admin2,Province_State,Country_Region,Last_Update,Latitude,Longitude,\
        Confirmed,Deaths,Recovered,Active,Combined_Key)',
        redshift_conn_id = 'redshift_conn_id',
        aws_credentials_id = 'aws_credentials',
        s3_bucket = S3_BUCKET_NAME,
        s3_key = 'covid-19/3'
    )

    data_check_task_3 = PythonOperator(
        task_id="data_check_task_3",
        python_callable=has_rows,
        op_kwargs={'table_name':'global.staging_covid19_daily_cases', 'task_name':'load_covid19_format3', 'prior_task':'data_check_task_2'}
    )

    # copy format 4
    load_covid19_format4 = S3ToRedshiftOperator(
        task_id = 'load_covid19_format4',
        table = 'global.staging_covid19_daily_cases(FIPS,Admin2,Province_State,Country_Region,Last_Update,\
        Latitude,Longitude,Confirmed,Deaths,Recovered,Active, Combined_Key,Incident_Rate,Case_Fatality_Ratio)',
        redshift_conn_id = 'redshift_conn_id',
        aws_credentials_id = 'aws_credentials',
        s3_bucket = S3_BUCKET_NAME,
        s3_key = 'covid-19/4'
    )

    data_check_task_4 = PythonOperator(
        task_id="data_check_task_4",
        python_callable=has_rows,
        op_kwargs={'table_name':'global.staging_covid19_daily_cases', 'task_name':'load_covid19_format4', 'prior_task':'data_check_task_3'}
    )


    end_operator = DummyOperator(
        task_id = "end_operator"
    )

    start_task >> create_schema
    create_schema >> covid19_staging_table
    covid19_staging_table >> load_covid19_format1

    load_covid19_format1 >> data_check_task_1
    data_check_task_1 >> load_covid19_format2

    load_covid19_format2 >> data_check_task_2
    data_check_task_2 >> load_covid19_format3

    load_covid19_format3 >> data_check_task_3
    data_check_task_3 >> load_covid19_format4

    load_covid19_format4 >> data_check_task_4
    data_check_task_4 >> end_operator
