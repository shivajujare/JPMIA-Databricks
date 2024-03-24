-- Databricks notebook source
show databases;

-- COMMAND ----------

--DROP database jpmia_delta_bv CASCADE;
create database jpmia_delta_bv;

-- COMMAND ----------

use database jpmia_delta_bv;

-- COMMAND ----------

show tables;

-- COMMAND ----------

drop table if exists jpmia_delta_bv.companies;
create external table jpmia_delta_bv.companies(
            company_id bigint,
            name string,
           description string,
           company_size int,
           state string,
           country string,
           city string,
           zip_code string,
           address string,
           url string,
           ingest_date date
)using delta
location '/mnt/adlsjpmia/jpmiadata/biz_view/companies'

-- COMMAND ----------

drop table if exists jpmia_delta_bv.companies;
create table jpmia_delta_bv.companies(
            company_id bigint,
            name string,
           description string,
           company_size int,
           state string,
           country string,
           city string,
           zip_code string,
           address string,
           url string,
           ingest_date date
)using delta
location '/mnt/adlsjpmia/jpmiadata/biz_view/companies'

-- COMMAND ----------

describe extended jpmia_delta_bv.companies;

-- COMMAND ----------

select * from jpmia_delta_bv.companies;

-- COMMAND ----------

delete from jpmia_delta_bv.companies;

-- COMMAND ----------

describe formatted jpmia_delta_bv.companies  ;

-- COMMAND ----------

select * from jpmia_delta_bv.companies;

-- COMMAND ----------

drop table if exists jpmia_delta_bv.company_industries;
create external table jpmia_delta_bv.company_industries
(
  company_id int,
  industry string,
  ingest_date string
) using delta
location "/mnt/adlsjpmia/jpmiadata/biz_view/company_industries"

-- COMMAND ----------

select * from jpmia_delta_bv.company_industries;

-- COMMAND ----------

select company_id, collect_set(industry) from jpmia_delta_bv.company_industries group by company_id;

-- COMMAND ----------

drop table if exists jpmia_delta_bv.company_specialities;
create external table jpmia_delta_bv.company_specialities
(
  company_id int,
  speciality string,
  ingest_date string
) using delta
location '/mnt/adlsjpmia/jpmiadata/biz_view/company_specialities'

-- COMMAND ----------

select * from jpmia_delta_bv.company_specialities;

-- COMMAND ----------

drop table if exists jpmia_delta_bv.employee_counts;

create external table jpmia_delta_bv.employee_counts
(
  company_id int,
  employee_count int,
  follower_count int,
  time_recorded string,
  date_time_recorded timestamp,
  date_recorded date,
  ingest_date date
) using delta
location '/mnt/adlsjpmia/jpmiadata/biz_view/employee_counts'

-- COMMAND ----------

select * from jpmia_delta_bv.employee_counts;

-- COMMAND ----------

drop table jpmia_delta_bv.job_benefits;

-- COMMAND ----------

--drop table if exists jpmia_delta_bv.job_benefits;
create external table jpmia_delta_bv.job_benefits
(
  job_id bigint,
  inferred int,
  type string,
  ingest_date date
) using delta 
location '/mnt/adlsjpmia/jpmiadata/biz_view/job_benefits'
          

-- COMMAND ----------

describe extended jpmia_delta_bv.job_benefits;

-- COMMAND ----------

alter table jpmia_delta_bv.job_benefits alter column job_id type bigint;

-- COMMAND ----------

refresh table jpmia_delta_bv.job_benefits;


-- COMMAND ----------

select * from jpmia_delta_bv.job_benefits;

-- COMMAND ----------

drop table if exists jpmia_delta_bv.job_industries;
create external table jpmia_delta_bv.job_industries(
  job_id	bigint,
  industry_id bigint,
  ingest_date date
)using delta
location '/mnt/adlsjpmia/jpmiadata/biz_view/job_industries'

-- COMMAND ----------

select * from jpmia_delta_bv.job_industries

-- COMMAND ----------

drop table if exists jpmia_delta_bv.job_skills;
create external table jpmia_delta_bv.job_skills(
  job_id	bigint,
  skill_abr string,
  ingest_date date
)using delta
location '/mnt/adlsjpmia/jpmiadata/biz_view/job_skills'

-- COMMAND ----------

select * from jpmia_delta_bv.job_skills;

-- COMMAND ----------

drop table if exists jpmia_delta_bv.job_salaries;
create external table jpmia_delta_bv.job_salaries
(
  salary_id bigint,
  job_id bigint,
  max_salary float,
  med_salary float,
  min_salary float,
  pay_period string,
  currency string,
  compensation_type string,
  ingest_date date
) using delta
location '/mnt/adlsjpmia/jpmiadata/biz_view/job_salaries'

-- COMMAND ----------

select * from jpmia_delta_bv.job_salaries;

-- COMMAND ----------

drop table if exists jpmia_delta_bv.job_postings;
create external table jpmia_delta_bv.job_postings
(
  job_id bigint, 
  company_id bigint,	
  locations string,	
  applies int,	
  original_listed_time string,	
  remote_allowed string,	
  views int,	
  job_posting_url	string, 
  application_url string,	
  application_type string,	
  expiry string,	
  closed_time string,	
  experience_level string,	
  listed_time string,	
  posting_domain string,	
  sponsored string,	
  work_type string,	
  scraped string,
  ingest_date date
) using delta
location '/mnt/adlsjpmia/jpmiadata/biz_view/job_postings'

-- COMMAND ----------

select * from jpmia_delta_bv.job_postings

-- COMMAND ----------

select distinct work_type from jpmia_delta_bv.job_postings;

-- COMMAND ----------

