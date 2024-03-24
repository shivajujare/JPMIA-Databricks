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
                                StructField("company_id", IntegerType(), False),
                                StructField("industry", StringType(), False)
                    ]
                    )

# COMMAND ----------

cmp_inds_srv = read_df_w_schema('csv',schema, '/mnt/adlsjpmia/jpmiadata/source_view/company_industries.csv')


# COMMAND ----------

cmp_ins_rm_blnk_df = cmp_inds_srv.na.fill('N/A')

cmp_ins_ing_date = cmp_ins_rm_blnk_df \
                    .withColumn("ingest_date", current_date().cast("string"))

cmp_ins_ing_date.show(3)

# COMMAND ----------

#write_df_as_table(cmp_ins_ing_date, 'delta', '/mnt/adlsjpmiadev/jpmia/biz_view/company_industries', "overwrite", "jpmia_delta_bv.company_industries")

# COMMAND ----------

comp_indus_delta = DeltaTable.forPath(spark, '/mnt/adlsjpmia/jpmiadata/biz_view/company_industries')
comp_indus_delta.alias('target') \
    .merge(cmp_ins_ing_date.alias('source'), \
            "target.company_id = source.company_id and target.industry = source.industry"
        ).whenNotMatchedInsert(
            values= {
                "target.company_id": "source.company_id",
                "target.industry": "source.industry",
                "target.ingest_date": "source.ingest_date"
            }
        ).execute()

# COMMAND ----------

