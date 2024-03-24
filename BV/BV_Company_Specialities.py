# Databricks notebook source
# MAGIC %run "../Common/Utilities"

# COMMAND ----------

#imports
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_date
from delta import *


# COMMAND ----------

schema = StructType(
                    fields = [
                                StructField("company_id", IntegerType(), False),
                                StructField("speciality", StringType(), False)
                    ]
                    )

# COMMAND ----------

cmp_spcls = read_df_w_schema('csv',schema, '/mnt/adlsjpmia/jpmiadata/source_view/company_specialities.csv')

# COMMAND ----------

cmp_spcl_rm_blnk = cmp_spcls.na.fill('N/A')
cmp_spcl_ing_date = cmp_spcl_rm_blnk \
                    .withColumn("ingest_date", current_date().cast("string")) 

cmp_spcl_ing_date.show(5)

# COMMAND ----------

comp_spcl_delta = DeltaTable.forPath(spark, "/mnt/adlsjpmia/jpmiadata/biz_view/company_specialities")

# COMMAND ----------

comp_spcl_delta.alias('target') \
    .merge(cmp_spcl_ing_date.alias('source'), \
            "target.company_id = source.company_id \
            and target.speciality = source.speciality"
        ).whenNotMatchedInsert(
            values={
                "target.company_id": "source.company_id",
                "target.speciality": "source.speciality",
                "target.ingest_date": "source.ingest_date"
            }
        ).execute()

# COMMAND ----------

