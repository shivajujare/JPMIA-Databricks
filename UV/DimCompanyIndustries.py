# Databricks notebook source
# MAGIC %run "../Common/Utilities"

# COMMAND ----------

from pyspark.sql.functions import col, collect_set, translate, concat_ws
from delta import *

# COMMAND ----------

comp_ind_bv = DeltaTable.forName(spark, 'jpmia_delta_bv.company_industries')
comp_ind_df = comp_ind_bv.toDF()
#comp_ind_df.show(2)
agg_comp_ind_df = comp_ind_df.groupby("company_id", "ingest_date") \
                            .agg(collect_set("industry").alias('industries')) \
                            .withColumn("industries", concat_ws(", ", "industries"))
agg_comp_ind_df.show(2,truncate=False)

# COMMAND ----------

comp_ind_uv = DeltaTable.forName(spark, 'jpmia_uv_delta.DimCompanyIndustries')
comp_ind_uv.toDF().show(3)

# COMMAND ----------

comp_ind_uv.alias('target') \
    .merge(agg_comp_ind_df.alias('source'), \
        "target.company_id = source.company_id" \
        ).whenMatchedUpdate(
            condition="target.industries != source.industries",
            set={
                "target.industries": "source.industries",
                "target.ingest_date": "source.ingest_date"
            }
        ).whenNotMatchedInsert(
            values={
                "target.company_id": "source.company_id",
                "target.industries": "source.industries",
                "target.ingest_date": "source.ingest_date"
            }
        ).execute()

# COMMAND ----------

