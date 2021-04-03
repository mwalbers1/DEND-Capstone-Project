# gapminder_sql_statements.py

CREATE_SCHEMA_SQL = """
CREATE SCHEMA IF NOT EXISTS "{0}";
"""

GDP_STAGING_TABLE_SQL = """
BEGIN;
DROP TABLE IF EXISTS "{}".gdp_staging;
CREATE TABLE "{}".gdp_staging (
    country_code VARCHAR(150),
    year    INTEGER sortkey,
    per_capita_yr_growth DECIMAL(26,4) NULL,
    total_yr_growth DECIMAL(26,4) NULL,
    pc_growth_next10_yrs DECIMAL(26,4) NULL,
    pc_growth_pct_per_yr DECIMAL(26,4) NULL,
    pc_us_inflation_adj DECIMAL(26,4) NULL,
    pe_us_inflation_adj DECIMAL(26,4) NULL,
    pwh_us_inflation_adj DECIMAL(26,4) NULL,
    income_pp_pc_ppp_inflation_adj DECIMAL(26,4) NULL,
    industry_pct_gdp DECIMAL(26,4) NULL,
    invest_pct_gdp DECIMAL(26,4) NULL,
    military_expend_pct_gdp DECIMAL(26,4) NULL,
    total_gdp_ppp_inflation_adj DECIMAL(26,4) NULL,
    total_gdp_us_inflation_adj DECIMAL(26,4) NULL,
    total_health_spending_pct_gdp DECIMAL(26,4) NULL
)
diststyle even;
"""

EDUCATION_STAGING_TABLE_SQL = """
BEGIN;
DROP TABLE IF EXISTS  "{}".education_staging;
CREATE TABLE  "{}".education_staging (
    country_code VARCHAR(150),
    year    INTEGER sortkey,
    children_oos_primary NUMERIC(15,1) NULL,
    children_oos_primary_female NUMERIC(15,1) NULL,
    children_oos_primary_male NUMERIC(15,1) NULL,
    education_aid_given_pct_of_aid DECIMAL(26,4) NULL,
    mean_yrs_sch_men_15_24 DECIMAL(26,4) NULL,
    mean_yrs_sch_men_25_34 DECIMAL(26,4) NULL,
    mean_yrs_sch_men_25_older DECIMAL(26,4) NULL,
    mean_yrs_sch_men_35_44 DECIMAL(26,4) NULL,
    mean_yrs_sch_men_45_54 DECIMAL(26,4) NULL,
    mean_yrs_sch_men_55_64 DECIMAL(26,4) NULL,
    mean_yrs_sch_men_65_plus DECIMAL(26,4) NULL,
    mean_yrs_sch_women_15_24 DECIMAL(26,4) NULL,
    mean_yrs_sch_women_25_34 DECIMAL(26,4) NULL,
    mean_yrs_sch_women_25_older DECIMAL(26,4) NULL,
    mean_yrs_sch_women_35_44 DECIMAL(26,4) NULL,
    mean_yrs_sch_women_45_54 DECIMAL(26,4) NULL,
    mean_yrs_sch_women_55_64 DECIMAL(26,4) NULL,
    mean_yrs_sch_women_65_plus DECIMAL(26,4) NULL,
    mean_yrs_sch_women_reproductive_15_44 DECIMAL(26,4) NULL,
    mean_yrs_sch_women_pct_men_25_34 DECIMAL(26,4) NULL,
    primary_sch_compl_pct_boys DECIMAL(26,4) NULL,
    primary_sch_compl_pct_girls DECIMAL(26,4) NULL,
    ratio_girls_boys_prim_sec_ed_pct DECIMAL(26,4) NULL
)
diststyle even;
"""

GOVERNMENT_STAGING_TABLE_SQL = """
BEGIN;
DROP TABLE IF EXISTS "{}".government_staging;
CREATE TABLE "{}".government_staging (
    country_code VARCHAR(150),
    year    INTEGER sortkey,
    govt_soc_aid_given_pct_of_aid DECIMAL(26,4) NULL,
    health_spend_total_gov_spend_pct DECIMAL(26,4) NULL,
    health_spend_pp_internl_dollar DECIMAL(26,4) NULL,
    health_spend_pp_us DECIMAL(26,4) NULL,
    share_total_health_spending_pct DECIMAL(26,4) NULL
)
diststyle even;
"""

POPULATION_STAGING_TABLE_SQL = """
BEGIN;
DROP TABLE IF EXISTS "{}".population_staging;
CREATE TABLE "{}".population_staging (
    country_code VARCHAR(150),
    year    INTEGER sortkey,
    female_pop_proj NUMERIC(15,1) NULL,
    male_pop_proj NUMERIC(15,1) NULL,
    age_0_4_yrs_mf_pct DECIMAL(26,4) NULL,
    age_0_4_yrs_female_pct DECIMAL(26,4) NULL,
    age_0_4_yrs_male_pct DECIMAL(26,4) NULL,
    age_0_4_yrs_total_num NUMERIC(15,1) NULL,
    age_10_14_yrs_mf_pct DECIMAL(26,4) NULL,
    age_10_14_yrs_female_pct DECIMAL(26,4) NULL,
    age_10_14_yrs_male_pct DECIMAL(26,4) NULL,
    age_10_14_yrs_total_num NUMERIC(15,1) NULL,
    age_15_19_yrs_mf_pct DECIMAL(26,4) NULL,
    age_15_19_yrs_female_pct DECIMAL(26,4) NULL,
    age_15_19_yrs_male_pct DECIMAL(26,4) NULL,
    age_15_19_yrs_total_num NUMERIC(15,1) NULL,
    age_20_39_yrs_mf_pct DECIMAL(26,4) NULL,
    age_20_39_yrs_female_pct DECIMAL(26,4) NULL,
    age_20_39_yrs_male_pct DECIMAL(26,4) NULL,
    age_20_39_yrs_total_num NUMERIC(15,1) NULL,
    age_40_59_yrs_mf_pct DECIMAL(26,4) NULL,
    age_40_59_yrs_female_pct DECIMAL(26,4) NULL,
    age_40_59_yrs_male_pct DECIMAL(26,4) NULL,
    age_40_59_yrs_total_num NUMERIC(15,1) NULL,
    age_5_9_yrs_mf_pct DECIMAL(26,4) NULL,
    age_5_9_yrs_female_pct DECIMAL(26,4) NULL,
    age_5_9_yrs_male_pct DECIMAL(26,4) NULL,
    age_5_9_yrs_total_num NUMERIC(15,1) NULL,
    age_60_plus_yrs_mf_pct DECIMAL(26,4) NULL,
    age_60_plus_yrs_female_pct DECIMAL(26,4) NULL,
    age_60_plus_yrs_male_pct DECIMAL(26,4) NULL,
    age_60_plus_yrs_total_num NUMERIC(15,1) NULL,
    density_per_sq_km DECIMAL(26,4) NULL,
    growth_annual_pct DECIMAL(26,4) NULL,
    growth_annual_pct_proj DECIMAL(26,4) NULL,
    in_urban_agglom_m_1_mill_pct_of_total DECIMAL(26,4) NULL,
    policies_aid_given_pct_of_aid DECIMAL(26,4) NULL,
    total_population NUMERIC(15,1) NULL,
    total_population_proj NUMERIC(15,1) NULL
)
diststyle even;
"""

FEMALE_STAGING_TABLE_SQL = """
BEGIN;
DROP TABLE IF EXISTS "{}".female_staging;
CREATE TABLE "{}".female_staging (
    country_code VARCHAR(150),
    year    INTEGER sortkey,
    breast_cancer_num_deaths NUMERIC(15,1) NULL,
    breast_cancer_num_new_cases NUMERIC(15,1) NULL,
    family_workers_pct_female_pop DECIMAL(26,4) NULL,
    industry_workers_pct_female_pop DECIMAL(26,4) NULL,
    long_term_unemp_rate_pct DECIMAL(26,4) NULL,
    self_emp_pct_female_emp DECIMAL(26,4) NULL,
    svc_workers_pct_of_female_emp DECIMAL(26,4) NULL,
    age_15_24_emp_rate_pct DECIMAL(26,4) NULL,
    age_15_24_unemp_rate_pct DECIMAL(26,4) NULL,
    age_15_64_labor_force_partic_rate_pct DECIMAL(26,4) NULL,
    age_15plus_emp_rate_pct DECIMAL(26,4) NULL,
    age_15plus_labor_force_partic_rate_pct DECIMAL(26,4) NULL,
    age_15plus_unemp_rate_pct DECIMAL(26,4) NULL,
    age_25_54_labor_force_partic_rate_pct DECIMAL(26,4) NULL,
    age_25_54_unemp_rate_pct DECIMAL(26,4) NULL,
    age_55_64_unemp_rate_pct DECIMAL(26,4) NULL,
    age_65plus_labor_force_partic_rate_pct  DECIMAL(26,4) NULL,
    age_65plus_unemp_rate_pct DECIMAL(26,4) NULL,
    life_expectency NUMERIC(15,1) NULL,
    literacy_rate_pct_15plus DECIMAL(26,4) NULL,
    literacy_rate_pct_15_24 DECIMAL(26,4) NULL,
    ratio_literate_female_male_15_24 DECIMAL(26,4) NULL
)
diststyle even;
"""

MALE_STAGING_TABLE_SQL = """
BEGIN;
DROP TABLE IF EXISTS "{}".male_staging;
CREATE TABLE "{}".male_staging (
    country_code VARCHAR(150),
    year    INTEGER sortkey,
    life_expectancy NUMERIC(15,1) NULL,
    literacy_rate_pct_15plus DECIMAL(26,4) NULL,
    literacy_rate_pct_15_24 DECIMAL(26,4) NULL,
    industry_workers_pct_female_pop DECIMAL(26,4) NULL,
    long_term_unemp_rate_pct DECIMAL(26,4) NULL,
    popul_proj NUMERIC(15,1) NULL,
    salaried_pct_non_agric_male_emp DECIMAL(26,4) NULL,
    self_pct_percent_of_ma_male_emp DECIMAL(26,4) NULL,
    svc_workers_pct_of_male_emp DECIMAL(26,4) NULL,
    age_15_24_emp_rate_pct DECIMAL(26,4) NULL,
    age_15_24_unemp_rate_pct DECIMAL(26,4) NULL,
    age_15_64_labor_force_partic_rate_pct DECIMAL(26,4) NULL,
    age_15plus_emp_rate_pct DECIMAL(26,4) NULL,
    age_15plus_labor_force_partic_rate_pct DECIMAL(26,4) NULL,
    age_15plus_unemp_rate_pct DECIMAL(26,4) NULL,
    age_25_54_labor_force_partic_rate_pct DECIMAL(26,4) NULL,
    age_25_54_unemp_rate_pct DECIMAL(26,4) NULL,
    age_55_64_unemp_rate_pct DECIMAL(26,4) NULL,
    age_65plus_labor_force_partic_rate_pct DECIMAL(26,4) NULL,
    age_65plus_unemp_rate_pct DECIMAL(26,4) NULL,
    prostate_cancer_num_deaths NUMERIC(15,1) NULL,
    cancer_num_new_cases NUMERIC(15,1) NULL
)
diststyle even;
"""

WORLD_DEMOGRAPHICS_TABLE_SQL = """
BEGIN;
CREATE TABLE IF NOT EXISTS "{}".world_demographics
(
    demographic_id                                bigint IDENTITY(0,1),
    country_id                                    integer not null,
    year                                          integer not null sortkey,
    mean_yrs_sch_men_15_24                        decimal(26, 4) null,
    mean_yrs_sch_men_25_older                     decimal(26, 4) null,
    mean_yrs_sch_women_15_24                      decimal(26, 4) null,
    mean_yrs_sch_women_25_older                   decimal(26, 4) null,
    female_age_15plus_emp_rate_pct                decimal(26, 4) null,
    female_age_15plus_labor_force_partic_rate_pct decimal(26, 4) null,
    female_age_15plus_unemp_rate_pct              decimal(26, 4) null,
    female_life_expectency                        numeric(15, 1) null,
    female_literacy_rate_pct_15plus               decimal(26, 4) null,
    male_age_15plus_emp_rate_pct                  decimal(26, 4) null,
    male_age_15plus_labor_force_partic_rate_pct   decimal(26, 4) null,
    male_age_15plus_unemp_rate_pct                decimal(26, 4) null,
    male_life_expectancy                          numeric(15, 1) null,
    male_literacy_rate_pct_15plus                 decimal(26, 4) null,
    health_spend_pp_us                            decimal(26, 4) null,
    health_spend_total_gov_spend_pct              decimal(26, 4) null,
    age_60_plus_yrs_total_num                     decimal(26, 4) null,
    total_population                              numeric(15, 1) null,
    total_gdp_ppp_inflation_adj                   decimal(26, 4) null,
    total_gdp_us_inflation_adj                    decimal(26, 4) null,
    foreign key(country_id) references "{}".world_countries
)
diststyle even;
"""

INSERT_WORLD_DEMOGRAPHICS_SQL = """
BEGIN;
INSERT INTO global.world_demographics (
       country_id,
	   year,
       mean_yrs_sch_men_15_24,
       mean_yrs_sch_men_25_older,
       mean_yrs_sch_women_15_24,
       mean_yrs_sch_women_25_older,
       female_age_15plus_emp_rate_pct,
       female_age_15plus_labor_force_partic_rate_pct,
       female_age_15plus_unemp_rate_pct,
       female_life_expectency,
       female_literacy_rate_pct_15plus,
       male_age_15plus_emp_rate_pct,
       male_age_15plus_labor_force_partic_rate_pct,
       male_age_15plus_unemp_rate_pct,
       male_life_expectancy,
       male_literacy_rate_pct_15plus,
       health_spend_pp_us,
       health_spend_total_gov_spend_pct,
       age_60_plus_yrs_total_num,
       total_population,
       total_gdp_ppp_inflation_adj,
       total_gdp_us_inflation_adj
       )
SELECT
       cy.country_id,
	   cy.year,
       es.mean_yrs_sch_men_15_24,
       es.mean_yrs_sch_men_25_older,
       es.mean_yrs_sch_women_15_24,
       es.mean_yrs_sch_women_25_older,
       fs.age_15plus_emp_rate_pct,
       fs.age_15plus_labor_force_partic_rate_pct,
       fs.age_15plus_unemp_rate_pct,
       fs.life_expectency,
       fs.literacy_rate_pct_15plus,
       ms.age_15plus_emp_rate_pct,
       ms.age_15plus_labor_force_partic_rate_pct,
       ms.age_15plus_unemp_rate_pct,
       ms.life_expectancy,
       ms.literacy_rate_pct_15plus,
       gs.health_spend_pp_us,
       gs.health_spend_total_gov_spend_pct,
       ps.age_60_plus_yrs_total_num,
       ps.total_population,
       gdps.total_gdp_ppp_inflation_adj,
       gdps.total_gdp_us_inflation_adj
from global.country_year cy
left join global.education_staging as es on es.country_code = cy.iso3_lower
    and es.year = cy.year
left join global.female_staging as fs on fs.country_code = cy.iso3_lower
    and fs.year = cy.year
left join global.male_staging as ms on ms.country_code = cy.iso3_lower
    and ms.year = cy.year
left join global.government_staging as gs on gs.country_code = cy.iso3_lower
    and gs.year = cy.year
left join global.population_staging as ps on ps.country_code = cy.iso3_lower
    and ps.year = cy.year
left join global.gdp_staging as gdps on gdps.country_code = cy.iso3_lower
    and gdps.year = cy.year;

END;
"""
