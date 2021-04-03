
CREATE_SCHEMA_SQL = """
CREATE SCHEMA IF NOT EXISTS "{0}";
"""

# Staging table
"""
Create staging table to be loaded from Covid-19 daily cases CSV files via
Redshift COPY command
"""
STAGING_COVID19_DAILY_CASES_SQL = ("""
BEGIN;
DROP TABLE IF EXISTS "{}".staging_covid19_daily_cases;
CREATE TABLE IF NOT EXISTS "{}".staging_covid19_daily_cases (
    FIPS INTEGER DEFAULT 0,
    Admin2 VARCHAR(250) DEFAULT 'n/a',
    Province_State VARCHAR(250) DEFAULT 'n/a',
    Country_Region VARCHAR(250) DEFAULT 'n/a',
    Last_Update VARCHAR(100),
    Latitude NUMERIC(12,6),
    Longitude NUMERIC(12,6),
    Confirmed DECIMAL(10,0),
    Deaths DECIMAL(10,0),
    Recovered DECIMAL(10,0),
    Active DECIMAL(10,0),
    Combined_Key VARCHAR(250) DEFAULT 'n/a',
    Incident_Rate NUMERIC(12,6) DEFAULT -1,
    Case_Fatality_Ratio VARCHAR(100)
);
""")

# Fact tables
"""
Create fact tables for Covid-19 data. Each table stores Covid-19 daily case data
for a specific world region.
"""
COVID19_FACT_TABLES_SQL = ("""
BEGIN;
SET search_path TO {};

CREATE TABLE IF NOT EXISTS covid19_americas
(
	id bigint IDENTITY(0,1),
	country_id integer not null distkey,
	last_update date not null sortkey,
	confirmed bigint,
	deaths bigint,
	recovered bigint,
	active bigint,
    foreign key(country_id) references world_countries
);

CREATE TABLE IF NOT EXISTS covid19_africa
(
	id bigint IDENTITY(0,1),
	country_id integer not null distkey,
	last_update date not null sortkey,
	confirmed bigint,
	deaths bigint,
	recovered bigint,
	active bigint,
    foreign key(country_id) references world_countries
);

CREATE TABLE IF NOT EXISTS covid19_europe
(
	id bigint IDENTITY(0,1),
	country_id integer not null distkey,
	last_update date not null sortkey,
	confirmed bigint,
	deaths bigint,
	recovered bigint,
	active bigint,
    foreign key(country_id) references world_countries
);

CREATE TABLE IF NOT EXISTS covid19_asia
(
	id bigint IDENTITY(0,1),
	country_id integer not null distkey,
	last_update date not null sortkey,
	confirmed bigint,
	deaths bigint,
	recovered bigint,
	active bigint,
    foreign key(country_id) references world_countries
);
END;
""")

"""
First, strip off time part of the last_update column and convert to a Date type.
Then aggregate records by country and last_update and load records for each
region into the respective covid19_[region] tables.
"""
INSERT_COVID19_SQL_STATEMENT = """
BEGIN;
SET search_path TO {};

create temporary table temp_covid19_daily
(
    country_region varchar(250),
    last_update    date,
    confirmed      bigint,
    deaths         bigint,
    recovered      bigint,
    active         bigint

);

insert into temp_covid19_daily (country_region, last_update, confirmed, deaths, recovered, active)
SELECT country_region,
       CAST(TRIM(SPLIT_PART(last_update, ' ', 1)) AS DATE),
       confirmed   as confirmed,
       deaths      as deaths,
       recovered   as recovered,
       active      as active
from staging_covid19_daily_cases
where REGEXP_INSTR(last_update, '([0-9] )') > 0;

insert into temp_covid19_daily (country_region, last_update, confirmed, deaths, recovered, active)
SELECT country_region,
       CAST(TRIM(SPLIT_PART(last_update, 'T', 1)) AS DATE),
       confirmed   as confirmed,
       deaths      as deaths,
       recovered   as recovered,
       active      as active
from staging_covid19_daily_cases
where REGEXP_INSTR(last_update, '([0-9]T)') > 0;

-- Aggregate daily cases by country and day
create temporary table temp_covid19_daily_aggr
(
    country_region varchar(250),
    last_update    date,
    confirmed      bigint,
    deaths         bigint,
    recovered      bigint,
    active         bigint

);

insert into temp_covid19_daily_aggr (country_region, last_update, confirmed, deaths, recovered, active)
SELECT country_region,
       last_update,
       SUM(confirmed)   as confirmed,
       SUM(deaths)      as deaths,
       SUM(recovered)   as recovered,
       SUM(active)      as active
from temp_covid19_daily
group by country_region,last_update;

insert into {}(country_id, last_update, confirmed, deaths, recovered, active)
select wc.country_id,
       last_update,
       confirmed,
       deaths,
       recovered,
       active
from temp_covid19_daily_aggr tmp
         join staging_covid19_country as scc on scc.country_region = tmp.country_region
         join world_countries as wc on wc.iso3 = scc.iso3
where (scc.province_state is null or scc.province_state = '')
and wc.world_4region = '{}';

END;
"""
