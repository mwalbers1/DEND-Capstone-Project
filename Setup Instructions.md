# Setup Instructions

1. Update dwh.cfg with AWS access id and secret key

2. Create AWS Redshift Cluster server by running `python project-dwh-redsift.py` at terminal

3. Download all of the sub-folders and files from the data folder and copy into your own S3 bucket

4. Update the S3_BUCKET_NAME constant in the dimension_dag.py, gapminder_staging_dag.py, and covid19_staging_dag.py scripts to the name of your S3 bucket

5. In Airflow control panel, manually run the following DAGs in order

> a. gapminder_staging_dag<br/>
> b. covid19_staging_dag<br/>
> c. dimension_dag<br/>
> d. gapminder_dag<br/>
> e. covid19_dag<br/>
