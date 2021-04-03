# Sql statements for dimension tables
CREATE_SCHEMA_SQL = """
CREATE SCHEMA IF NOT EXISTS "{0}";
"""

# Create Staging tables
STAGING_WORLD_COUNTRIES_TABLE_CREATE= """
BEGIN;
DROP TABLE IF EXISTS "{}".staging_world_countries;
CREATE TABLE "{}".staging_world_countries (
    id VARCHAR(15) NULL,
    name VARCHAR(250) NULL
)
diststyle all;
END;
"""

STAGING_COVID19_COUNTRY_TABLE_CREATE = """
BEGIN;
DROP TABLE IF EXISTS "{}".staging_covid19_country;
CREATE TABLE "{}".staging_covid19_country (
    uid  INTEGER NULL,
    iso2 VARCHAR(15) NULL,
    iso3 VARCHAR(15) NULL,
    code3 VARCHAR(15) NULL,
    fips INTEGER DEFAULT 0,
    admin2 VARCHAR(250) NULL,
    province_state VARCHAR(250) DEFAULT 'n/a',
    country_region VARCHAR(250),
    lat NUMERIC(12,4) NULL,
    long NUMERIC(12,4) NULL,
    combined_key VARCHAR(250) NULL,
    population INTEGER NULL
 )
 diststyle all;
 END;
"""
GAPMINDER_COUNTRY_TABLE_CREATE = """
BEGIN;
DROP TABLE IF EXISTS "{}".gapminder_country;
CREATE TABLE IF NOT EXISTS "{}".gapminder_country (
    country                text,
    alt_5                  text,
    alternative_1          text,
    alternative_2          text,
    alternative_3          text,
    alternative_4_cdiac    text,
    arb1                   text,
    arb2                   text,
    arb3                   text,
    arb4                   text,
    arb5                   text,
    arb6                   text,
    g77_and_oecd_countries text,
    gapminder_list         text,
    god_id                 text,
    gwid                   text,
    income_groups          text,
    is_country             text,
    iso3166_1_alpha2       text,
    iso3166_1_alpha3       text,
    iso3166_1_numeric      integer,
    iso3166_2              text,
    landlocked             text,
    latitude               numeric,
    longitude              numeric,
    main_religion_2008     text,
    name                   text,
    pandg                  text,
    un_state               text,
    unicode_region_subtag  text,
    upper_case_name        text,
    world_4region          text,
    world_6region          text
)
diststyle all;
END;
"""

# Create Dimension tables
COUNTRY_DIMENSION_TABLE_CREATE = """
BEGIN;
DROP TABLE IF EXISTS "{}".world_countries;
CREATE TABLE IF NOT EXISTS "{}".world_countries (
	country_id BIGINT IDENTITY(0,1),
    iso2 VARCHAR(15),
    iso3 VARCHAR(15),
    name VARCHAR(250),
    landlocked varchar(250),
    latitude NUMERIC(12,4),
    longitude NUMERIC(12,4),
    main_religion_2008 VARCHAR(100) NULL,
    un_state        VARCHAR(100) NULL,
    world_4region   varchar(50) NULL,
    population INTEGER NULL,
    country_region VARCHAR(250) NULL,
    primary key(country_id)
)
diststyle all;
END;
"""

COUNTRY_YEAR_TABLE_CREATE = """
BEGIN;

DROP TABLE IF EXISTS "{}".country_year;
CREATE TABLE if not exists "{}".country_year (
    country_id bigint,
    iso3_upper varchar(15),
    iso3_lower varchar(15),
    year int,
    world_4region varchar(50),
    primary key(country_id, year),
    foreign key(country_id) references "{}".world_countries
)
diststyle all;

END;
"""

# Load Dimension tables
INSERT_COUNTRY_YEAR_SQL = """
BEGIN;
SET search_path TO {};

create temporary table temp_year
(year int);

insert into temp_year values (2000);
insert into temp_year values (2001);
insert into temp_year values (2002);
insert into temp_year values (2003);
insert into temp_year values (2004);
insert into temp_year values (2005);
insert into temp_year values (2006);
insert into temp_year values (2007);
insert into temp_year values (2008);
insert into temp_year values (2009);
insert into temp_year values (2010);
insert into temp_year values (2011);
insert into temp_year values (2012);
insert into temp_year values (2013);
insert into temp_year values (2014);
insert into temp_year values (2015);
insert into temp_year values (2016);
insert into temp_year values (2017);
insert into temp_year values (2018);
insert into temp_year values (2019);
insert into temp_year values (2020);

insert into country_year (country_id,
         iso3_upper,
         iso3_lower,
         year,
         world_4region)
SELECT country_id, upper(iso3), lower(iso3), year, world_4region
from world_countries, temp_year;

END;
"""

INSERT_WORLD_COUNTRIES_SQL = """
BEGIN;
SET search_path TO {};

INSERT INTO world_countries
(
    iso2,
    iso3,
    name,
    landlocked,
    latitude,
    longitude,
    main_religion_2008,
    un_state,
    world_4region,
    population,
    country_region
)
SELECT DISTINCT trim(scc.iso2),
        trim(gc.iso3166_1_alpha3),
        trim(gc.name),
        gc.landlocked,
        gc.latitude,
        gc.longitude,
        gc.main_religion_2008,
        gc.un_state,
        gc.world_4region,
        scc.population,
        scc.country_region
FROM staging_world_countries AS swc
    JOIN gapminder_country AS gc ON
         trim(gc.name) like '%' || trim(swc.name) || '%'
    JOIN staging_covid19_country AS scc ON scc.iso3 = gc.iso3166_1_alpha3
WHERE gc.latitude IS NOT NULL
AND gc.longitude IS NOT NULL
AND (scc.province_state IS NULL OR scc.province_state = '')
AND (scc.iso2 IS NOT NULL AND scc.iso2 <> '')
AND (scc.iso3 IS NOT NULL AND scc.iso3 <> '')
AND length(trim(swc.name)) <= length(trim(gc.name));

create temporary table temp_world_country (
    country_id BIGINT IDENTITY(0,1),
    iso2 VARCHAR(15),
    iso3 VARCHAR(15),
    name VARCHAR(250),
    landlocked varchar(250),
    latitude NUMERIC(12,4),
    longitude NUMERIC(12,4),
    main_religion_2008 VARCHAR(100) NULL,
    un_state        VARCHAR(100) NULL,
    world_4region   varchar(50) NULL,
    population INTEGER NULL,
    country_region VARCHAR(250)
);

INSERT INTO temp_world_country
(
    iso2,
    iso3,
    name,
    landlocked,
    latitude,
    longitude,
    main_religion_2008,
    un_state,
    world_4region,
    population,
    country_region
)
SELECT DISTINCT trim(scc.iso2),
        trim(gc.iso3166_1_alpha3),
        trim(gc.name),
        gc.landlocked,
        gc.latitude,
        gc.longitude,
        gc.main_religion_2008,
        gc.un_state,
        gc.world_4region,
        scc.population,
        scc.country_region
FROM staging_world_countries AS swc
    JOIN gapminder_country AS gc ON
         trim(swc.name) like '%' || trim(gc.name) || '%'
    JOIN staging_covid19_country AS scc ON scc.iso3 = gc.iso3166_1_alpha3
WHERE gc.latitude IS NOT NULL
AND gc.longitude IS NOT NULL
AND (scc.province_state IS NULL OR scc.province_state = '')
AND (scc.iso2 IS NOT NULL AND scc.iso2 <> '')
AND (scc.iso3 IS NOT NULL AND scc.iso3 <> '')
AND length(trim(gc.name)) < length(trim(swc.name));

INSERT into world_countries
(
    iso2,
    iso3,
    name,
    landlocked,
    latitude,
    longitude,
    main_religion_2008,
    un_state,
    world_4region,
    population,
    country_region
)
SELECT iso2,
       iso3,
       name,
       landlocked,
       latitude,
       longitude,
       main_religion_2008,
       un_state,
       world_4region,
       population,
       country_region
FROM temp_world_country
WHERE NOT EXISTS( SELECT 1
            FROM world_countries AS wc
            WHERE wc.iso3 = temp_world_country.iso3 );

END;
"""
