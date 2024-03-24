# Databricks notebook source
# MAGIC %run "../Common/Utilities"

# COMMAND ----------

from delta import *
from pyspark.sql.functions import col, collect_set, concat_ws

# COMMAND ----------

bv_job_benef = DeltaTable.forName(spark, "jpmia_delta_bv.job_benefits")
bv_job_benef.toDF().show(2)

# COMMAND ----------

agg_job_benef = bv_job_benef.toDF() \
                    .groupby("job_id","inferred","ingest_date") \
                    .agg(concat_ws(",", collect_set("type")).alias('type'))
                
agg_job_benef.show(3, False)

# COMMAND ----------

uv_job_benef = DeltaTable.forName(spark, "jpmia_uv_delta.DimJobBenefits")
uv_job_benef.toDF().show(2)

# COMMAND ----------

uv_job_benef.alias('target') \
    .merge(agg_job_benef.alias('source'), \
        "target.job_id = source.job_id and \
            target.inferred = source.inferred" \
        ).whenMatchedUpdate(
            condition=("target.type != source.type"),
            set={
                "target.type": "source.type"
            }
        ).whenNotMatchedInsert(
            values={
                "target.job_id": "source.job_id",
                "target.ingest_date": "source.ingest_date",
                "target.type": "source.type",
                "target.inferred": "source.inferred" 
            }
        ).execute()

# COMMAND ----------

