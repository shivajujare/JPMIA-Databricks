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
                                StructField("inferred", StringType(), False),
                                StructField("type", StringType(), False)
                    ]
                    )

# COMMAND ----------

job_benef_df = read_df_w_schema('csv',schema, '/mnt/adlsjpmia/jpmiadata/source_view/benefits.csv')

# COMMAND ----------

job_benef_rm_blnk = job_benef_df.na.fill('N/A')

job_benef_ing_date = job_benef_rm_blnk \
                    .withColumn("job_id",col('job_id').cast("bigint")) \
                    .withColumn("ingest_date", current_date().cast("string"))

job_benef_ing_date.show(5)

# COMMAND ----------

benef_delta = DeltaTable.forPath(spark, '/mnt/adlsjpmia/jpmiadata/biz_view/job_benefits')
benef_delta_df = benef_delta.toDF()
benef_delta_df.show(2)

# COMMAND ----------

benef_delta.alias('target') \
    .merge(job_benef_ing_date.alias('source'), \
            "target.job_id = source.job_id and target.type = source.type"
        ).whenMatchedUpdate(
            condition= "target.inferred != source.inferred",
            set= {
                "target.type": "source.type",
                "target.ingest_date": "source.ingest_date"
            }
        ).whenNotMatchedInsert(
            values= {
                "target.job_id": "source.job_id",
                "target.inferred": "source.inferred",
                "target.type": "source.type",
                "target.ingest_date": "source.ingest_date"
            }
        ).execute()

# COMMAND ----------

