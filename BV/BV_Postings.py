# Databricks notebook source
#filename = file_name = dbutils.widgets.get("file_name")
fully_qualified_file_name = f"/mnt/adlsjpmia/jpmiadata/source_view/job_postings.csv"
print(fully_qualified_file_name)

# COMMAND ----------

# MAGIC %run "../Common/Utilities"

# COMMAND ----------

#imports
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, LongType
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace, when, current_date
import datetime as dt
from delta import *

# COMMAND ----------

schema = StructType(
                    fields = [
                                StructField("job_id", StringType(), False),
                                StructField("company_id", StringType(), False),
                                StructField("title", StringType(), False),
                                StructField("description", StringType(), False),
                                StructField("max_salary", StringType(), False),
                                StructField("med_salary", StringType(), False),
                                StructField("min_salary", StringType(), False),
                                StructField("pay_period", StringType(), False),
                                StructField("formatted_work_type", StringType(), False),
                                StructField("location", StringType(), False),
                                StructField("applies", IntegerType(), False),
                                StructField("original_listed_time", StringType(), False),
                                StructField("remote_allowed", IntegerType(), False),
                                StructField("views", IntegerType(), False),
                                StructField("job_posting_url", StringType(), False),
                                StructField("application_url", StringType(), False),
                                StructField("application_type", StringType(), False),
                                StructField("expiry", StringType(), False),
                                StructField("closed_time", StringType(), False),
                                StructField("formatted_experience_level", StringType(), False),
                                StructField("skills_desc", StringType(), False),
                                StructField("listed_time", StringType(), False),
                                StructField("posting_domain", StringType(), False),
                                StructField("sponsored", StringType(), False),
                                StructField("work_type", StringType(), False),
                                StructField("currency", StringType(), False),
                                StructField("compensation_type", StringType(), False),
                                StructField("scraped", StringType(), False),
                    ]
                    )

# COMMAND ----------

full_time_pos = spark.read \
      .option("multiline", "true") \
      .option("quote", '"') \
      .option("escape", "\\") \
      .option("escape", '"') \
     .format('csv') \
                .option("header",True) \
                .option("schema", schema) \
                .option("path",fully_qualified_file_name) \
    .load()


# COMMAND ----------

columns_to_drop = ['description','max_salary','med_salary','min_salary','pay_period','skills_desc','work_type','currency','compensation_type']

drop_cols = full_time_pos.drop(*columns_to_drop)

# COMMAND ----------

cols_to_replace_zero = ['company_id','applies','remote_allowed','views']

replace_with_zero = drop_cols.na.fill("0",cols_to_replace_zero)


# COMMAND ----------

replace_with_NA= replace_with_zero.na.fill("N/A",["formatted_experience_level"]) 
add_ing_date = replace_with_NA.withColumn("ingestion_date", current_date().cast("string"))
add_ing_date.show(1)


# COMMAND ----------

postings_delta = DeltaTable.forPath(spark, '/mnt/adlsjpmia/jpmiadata/biz_view/job_postings')

# COMMAND ----------

postings_delta.alias('target') \
    .merge(add_ing_date.alias('source'), \
        "target.job_id = source.job_id and target.work_type = source.formatted_work_type"
        ).whenMatchedUpdate(
            set={
                "target.company_id": "source.company_id",
                "target.title":"source.title",	
                "target.locations": "source.location",	
                "target.applies": "source.applies",	
                "target.original_listed_time": "source.original_listed_time",	
                "target.remote_allowed": "source.remote_allowed",	
                "target.views": "source.views",	
                "target.job_posting_url": "source.job_posting_url", 
                "target.application_url": "source.application_url",	
                "target.application_type": "source.application_type",	
                "target.expiry": "source.expiry",	
                "target.closed_time": "source.closed_time",	
                "target.experience_level": "source.formatted_experience_level",	
                "target.listed_time": "source.listed_time",	
                "target.posting_domain": "source.posting_domain",	
                "target.sponsored": "source.sponsored",	
                "target.scraped": "source.scraped",
                "target.ingest_date": "source.ingestion_date"
            }
        ).whenNotMatchedInsert(
            values={
                "target.job_id": "source.job_id", 
                "target.title":"source.title",
                "target.company_id": "source.company_id",	
                "target.locations": "source.location",	
                "target.applies": "source.applies",	
                "target.original_listed_time": "source.original_listed_time",	
                "target.remote_allowed": "source.remote_allowed",	
                "target.views": "source.views",	
                "target.job_posting_url": "source.job_posting_url", 
                "target.application_url": "source.application_url",	
                "target.application_type": "source.application_type",	
                "target.expiry": "source.expiry",	
                "target.closed_time": "source.closed_time",	
                "target.experience_level": "source.formatted_experience_level",	
                "target.listed_time": "source.listed_time",	
                "target.posting_domain": "source.posting_domain",	
                "target.sponsored": "source.sponsored",	
                "target.work_type": "source.formatted_work_type",	
                "target.scraped": "source.scraped",
                "target.ingest_date": "source.ingestion_date"
            }
        ).execute()

# COMMAND ----------

