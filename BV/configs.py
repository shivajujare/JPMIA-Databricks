# Databricks notebook source
postings = [
 "full_time_job_postings.csv",
 "other_job_postings.csv",
 "internship_job_postings.csv",
 "contract_job_postings.csv"
]

# COMMAND ----------

source_view = "/mnt/adlsjpmia/jpmiadata/source_view"
usage_view = "/mnt/adlsjpmia/jpmiadata/usage_view"