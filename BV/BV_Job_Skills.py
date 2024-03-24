# Databricks notebook source
# MAGIC %run "../Common/Utilities"

# COMMAND ----------

#imports
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace, when, current_date
import datetime as dt
from delta import *

# COMMAND ----------

schema = StructType(
                    fields = [
                                StructField("job_id", IntegerType(), False),
                                StructField("skills_abbr", StringType(), False)
                    ]
                    )

# COMMAND ----------

job_skills = read_df_w_schema('csv',schema, '/mnt/adlsjpmia/jpmiadata/source_view/job_skills.csv')

# COMMAND ----------

job_skills_rm_blnk = job_skills.na.fill('N/A')

job_skills_ing_date = job_skills_rm_blnk \
                    .withColumn("ingest_date", current_date().cast("string")) 

job_skills_ing_date.show()

# COMMAND ----------



# COMMAND ----------

#write_df_as_table(job_skills_ing_date, 'delta', '/mnt/adlsjpmiadev/jpmia/biz_view/job_skills', "overwrite", "jpmia_delta_bv.job_skills")

# COMMAND ----------

delta_job_skills = DeltaTable.forPath(spark, "/mnt/adlsjpmia/jpmiadata/biz_view/job_skills")

# COMMAND ----------

display(delta_job_skills.toDF().limit(3))

# COMMAND ----------

delta_job_skills.alias('target') \
    .merge(job_skills_ing_date.alias('source'), "target.job_id == source.job_id and target.skill_abr == source.skill_abr") \
        .whenNotMatchedInsert(
            values= {
                "target.job_id": "source.job_id",
                "target.skill_abr": "source.skill_abr",
                "target.ingest_date": "source.ingest_date"
            }
        ).execute()
        

# COMMAND ----------

