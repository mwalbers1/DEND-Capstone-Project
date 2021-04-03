# Data Engineering Nanodegree Final Project

## Overview



The daily Covid-19 case data from over 150 countries around the world is loaded into an
AWS Redshift data warehouse.  The demographic data for countries around the world
are loaded into Redshift staging tables. The demographic data includes GDP (Gross Domestic Product),
education, female and male attributes, government, and population data attributes by country
and year for the past 20 years.

The country ISO3 codes and country names are loaded into a Redshift data warehouse staging table.

The purpose of this data warehouse is to compare daily Covid-19 confirmed cases between any two countries and to provide demographic data across many areas in an effort to learn more about the differences and similarities in countries across the education, gdp, sex, government, and population categories to determine whether a relationship or correlation exists with respective to Covid-19 case numbers.

## Datasets

### Gapminder

Gapminder is an independent non-profit organization who's goal is to collect and share demographic data on countries throughout the world with the goal of providing better insights and understanding of world demographic data.  

All Gapminder material is freely available under the Creative Commons Attribution 4.0 International license.
https://www.gapminder.org/free-material/

The gapminder data files were pulled from the GitHub repository at https://github.com/Gapminder/gapminder-offline/tree/development/ddf--gapminder--systema_globalis.

## Covid-19 Daily cases

The Covid-19 case data is from the "COVID-19 Data Repository by the Center for Systems Science and Engineering (CSSE) at Johns Hopkins University".  It is licensed under the Creative Commons Attribution 4.0 International by Johns Hopkins University on behalf of its Center for Systems Science in Engineering. The CSV data files were downloaded from https://github.com/CSSEGISandData/COVID-19/tree/master/csse_covid_19_data/csse_covid_19_daily_reports.

## World countries
A list of countries throughout the world was pulled from Kaggle.com at https://www.kaggle.com/ktochylin/world-countries. This list of countries serves as an independent list of countries for loading the `global.world_countries` dimension table. Because the countries from the `gapminder` and `covid-19` datasets differ, both datasets were cross-referenced with the kaggle world-countries list. The kaggle dataset was used as a way to standardize
the list of countries in the final dimension table.  There were some obscure countries in the gapminder dataset which got filtered out as a result of the reference to the kaggle world countries dataset.

## Data Dictionary
The data dictionary is at <a href="Data%20Dictionary.md" target="_blank">Data Dictionary</a>

## Technologies Used
**AWS (Amazon Web Services) S3**

AWS S3 provides reliable storage for large number of gapminder and covid-19 CSV files. AWS S3 integrates well with Apache Airflow.

**AWS Redshift**

AWS Redshift is a scalable and reasonably priced data warehouse solution for this project. It can store large volumes of data such as the covid-19 dataset. It also offers good performance for the data loads and analytics queries.

**Apache Airflow**

Apache Airflow facilitated the development of independent data pipelines which can be run on a schedule.  Apache Airflow integrates well with AWS S3 and AWS Redshift.  Apache Airflow data pipelines are maintainable and offer a lot of flexibility for scheduling workflows and performing data load tasks.

**Python**

Python was used for writing the Apache Airflow data pipelines

**Pandas**

Pandas was used in the `gapminder_staging_dag` for the data wrangling of the individual gapminder demographic files.  Pandas performs well and provided an efficient way to merge the gapminder data into new dataframes which were then written out to new CSV files.

**Jupyter notebooks**

A Jupyter notebook was used for analysis of the demographic and covid-19 datasets for the United States and Canada.

## Data Model

![](images/dend%20final%20project%20data%20model.jpg)

## Data Pipelines
Refer to <a href="Data%20Pipelines.md" target="_blank">Data Pipelines</a> for a description of the Airflow data pipelines.

## Project Scenerios

### Schedule Airflow Data pipelines for Covid-19 and Gapminder data to run every day at 7 am

#### Gapminder

Both Gapminder DAGs, gapminder_staging_dag and gapminder_dag can support a 7 am run schedule every day.  The Gapminder data is historical in nature.
Many countries will report their data at different times and possibly amend historical measurements as it relates to economic and demographic data. We therefore need to drop all gapminder staging tables and the gapminder fact table and reload. Both gapminder DAGs drop and recreate tables and then loads the complete dataset from it's S3 source bucket.  It is recommended that the gapminder_staging DAG run daily overnight while the gapminder_dag can run at 7 am each morning to reload the data from the staging tables which got refreshed overnight.


#### Covid-19

Because the Covid-19 DAG was written in Ariflow to do an initial load of the Covid-19 history, it will not support a daily 7 am schedule.  The covid19-dag will need to be updated to perform an upsert into the Redshift Covid-19 fact tables.


### The size of the data increases by 100 GB

#### Gapminder

The Gapminder data is historical and can therefore be loaded overnight.  The staging tables are already partitioned horizontally by demographic category.  The Gapminder fact table only pulls data from the last 20 years so it should not be impacted too much by an increase in staging data. The disk space on the Redshift cluster may need to be increased depending on it's current configuration.

#### Covid-19

The Covid-19 dataset is much larger than the Gapminder dataset as it has over 1.3 million records. Because Covid-19 data is updated daily at it's source, John's Hopkins University, the daily Airflow job may be impacted in terms of performance.  The Redshift cluster can be increased by one node initially.  Also, we can revisit the distribution strategy for the Covid-19 fact tables.  Another option is to archive historical Covid-19 staging data off the AWS Redshift cluster and load it to an on-prem PostgreSQL database to alleviate storage costs.

### The database needs to be accessed by 100+ people

The Redshift cluster can scaled up vertically by increasing disk space and the number of CPUs on each node of the cluster.  Horizontal scaling can be achieved by adding additional nodes to the cluster. 

The data warehouse is designed in a way to improve performance when joining data between the fact tables and the `global.world_countries` dimension table.  The dimension table uses a distribution style of ALL and fact tables are setup with a distribution style of EVEN. A sortkey is defined on the date and year columns of each fact table to help in optimizing query performance.

## Analytics

It is widely known that the United State leads the world in total Covid-19 confirmed cases. It as been widely disputed in political circles that the data may be incorrect or that it's due to increased testing.  Some people blame government leadership for not doing enough to protect the people of the United States.  This project does not attempt to resolve these issues, but rather it provides a platform of data to explore demographic characteristics among countries in the four world regions of the Americas, Asia, Africa, and Europe. This project looks at demographic data for the United States and Canada.  Both countries reside in the same region and have large populations and stable economies. Where do similarities and differences exist between the two countries? and does this explain the disparity of Covid-19 cases among the two countries? The analytics queries are in the Jupyter notebook  United_States_and_Canada.ipynb.
