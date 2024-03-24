# Databricks notebook source
use database 'referentials';

# COMMAND ----------

create table dbo.industries
(
    industry_id int,
    industry_name VARCHAR(100),
    ingest_date varchar(20)
)

# COMMAND ----------

create table dbo.skills
(
    skill_id int,
    skill_abr VARCHAR(20),
    skill_name VARCHAR(100),
    ingest_date varchar(20)
)

# COMMAND ----------

select * from dbo.industries;