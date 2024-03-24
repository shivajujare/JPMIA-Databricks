# Databricks notebook source
# MAGIC %run "../Common/Utilities"

# COMMAND ----------

from delta import *
from pyspark.sql.functions import col

# COMMAND ----------

bv_job_ind = DeltaTable.forName(spark,"jpmia_delta_bv.job_industries")
job_ind_bv_DF = bv_job_ind.toDF()
job_ind_bv_DF.show(2)

# COMMAND ----------

uv_job_ind = DeltaTable.forName(spark, "jpmia_uv_delta.DimJobIndustries")
uv_job_ind.toDF().show()

# COMMAND ----------

vserver = dbutils.secrets.get(scope="jpmia-kv-scope", key="sql-server-name")
vport = '1433'
vDBName = dbutils.secrets.get(scope="jpmia-kv-scope", key="sql-db-name")
vDBUser = dbutils.secrets.get(scope="jpmia-kv-scope", key="sql-user")
Vpassword = dbutils.secrets.get(scope="jpmia-kv-scope", key="sql-pwd")
vtable = "dbo.industries"
indus_lkp_df = read_sql_table(vserver, vport, vDBName, vDBUser, Vpassword, vtable)
indus_lkp_df.show(2)



# COMMAND ----------

join_df = job_ind_bv_DF.join(indus_lkp_df,job_ind_bv_DF.industry_id == indus_lkp_df.industry_id,"left") \
            .drop(indus_lkp_df.industry_id,indus_lkp_df.ingest_date)
join_df.show(2) 

# COMMAND ----------

uv_job_ind.alias('target') \
    .merge(join_df.alias('source'), \
        "target.job_id = source.job_id and target.industry_id = source.industry_id and target.industry_name != source.industry_name" \
        ).whenMatchedUpdate(
            set={
                "target.industry_name": "source.industry_name",
                "target.ingest_date": "source.ingest_date"
            }
        ).execute()


# COMMAND ----------

uv_job_ind.alias('target') \
    .merge(join_df.alias('source'), \
        "target.industry_id = source.industry_id and target.job_id = source.job_id" \
        ).whenNotMatchedInsert(
            values={
                "target.industry_id": "source.industry_id",
                "target.job_id":"source.job_id",
                "target.industry_name": "source.industry_name",
                "target.ingest_date": "source.ingest_date"
            }
        ).execute()


# COMMAND ----------

