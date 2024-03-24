# Databricks notebook source
# MAGIC %run "../Common/Utilities"

# COMMAND ----------

from delta import *
from pyspark.sql.functions import collect_set, concat_ws

# COMMAND ----------

bv_comp_spl = DeltaTable.forName(spark, "jpmia_delta_bv.company_specialities")
#bv_comp_spl.toDF().show(2)
agg_bv_comp_spl = bv_comp_spl.toDF() \
                    .groupby("company_id","ingest_date") \
                    .agg((concat_ws(",",collect_set("speciality")).alias('specialities')))
agg_bv_comp_spl.show(3,False)

# COMMAND ----------

uv_comp_spl = DeltaTable.forName(spark, "jpmia_uv_delta.DimCompanySpecialities")
uv_comp_spl.toDF().show(3)

# COMMAND ----------

uv_comp_spl.alias('target') \
    .merge(agg_bv_comp_spl.alias('source'), \
        "target.company_id = source.company_id"
        ).whenMatchedUpdate(
            condition="target.specialities != source.specialities",
            set={
                "target.company_id": "source.company_id",
                "target.specialities": "source.specialities"
            }
        ).whenNotMatchedInsert(
            values={
                "target.company_id": "source.company_id",
                "target.specialities": "source.specialities",
                "target.ingest_date": "source.ingest_date"
            }
        ).execute()

# COMMAND ----------

