-- Databricks notebook source
create database if not exists jpmia_uv_delta;

-- COMMAND ----------

use jpmia_uv_delta;

-- COMMAND ----------

drop table if exists jpmia_uv_delta.DimCompanies;
create external table jpmia_uv_delta.DimCompanies(
  company_id bigint,
            name string,
           description string,
           company_size int,
           formatted_company_size string,
           state string,
           country string,
           city string,
           zip_code string,
           address string,
           url string,
           ingest_date date
)using delta
location '/mnt/adlsjpmia/jpmiadata/usage_view/DimCompanies'

-- COMMAND ----------

drop table if exists jpmia_uv_delta.DimCompanyIndustries;
create external table jpmia_uv_delta.DimCompanyIndustries(
  company_id bigint,
  industries string,
  ingest_date date
)using delta
location '/mnt/adlsjpmia/jpmiadata/usage_view/DimCompanyIndustries'

-- COMMAND ----------

drop table if exists jpmia_uv_delta.DimCompanySpecialities;
create external table jpmia_uv_delta.DimCompanySpecialities(
  company_id bigint,
  specialities string,
  ingest_date date
)using delta
location '/mnt/adlsjpmia/jpmiadata/usage_view/DimCompanySpecialities'

-- COMMAND ----------

drop table jpmia_uv_delta.FactCompanyEmployeeGrowth;
create external table jpmia_uv_delta.FactCompanyEmployeeGrowth
(
  company_id bigint,
  employee_count bigint,
  follower_count bigint,
  time_recorded string,
  date_time_recorded timestamp,
  date_recorded date,
  total_employees_as_of_now bigint,
  total_followers_as_of_now bigint,
  ingest_date date
)using delta
location '/mnt/adlsjpmia/jpmiadata/usage_view/FactCompanyEmployeeGrowth'

-- COMMAND ----------

drop table if exists jpmia_uv_delta.DimJobBenefits;
create external table jpmia_uv_delta.DimJobBenefits
(
  job_id bigint,
  inferred string,
  type string,
  ingest_date date
)using delta
location '/mnt/adlsjpmia/jpmiadata/usage_view/DimJobBenefits'

-- COMMAND ----------

drop table  if exists jpmia_uv_delta.DimJobIndustries;
create external table jpmia_uv_delta.DimJobIndustries(
  job_id	bigint,
  industry_id bigint,
  industry_name string,
  ingest_date date
)using delta
location '/mnt/adlsjpmia/jpmiadata/usage_view/DimJobIndustries'

-- COMMAND ----------

drop table  if exists jpmia_uv_delta.DimJobSkills;
create external table jpmia_uv_delta.DimJobSkills(
  job_id	bigint,
  skill_abr string,
  skill_name string,
  ingest_date date
)using delta
location '/mnt/adlsjpmia/jpmiadata/usage_view/DimJobSkills'

-- COMMAND ----------



-- COMMAND ----------



-- COMMAND ----------

