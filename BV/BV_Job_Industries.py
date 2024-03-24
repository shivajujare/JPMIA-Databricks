# Databricks notebook source
# MAGIC %run "../Common/Utilities"

# COMMAND ----------

#imports
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace, when, current_date, lit
import datetime as dt
from delta import *

# COMMAND ----------

vserver = dbutils.secrets.get(scope="jpmia-kv-scope", key="sql-server-name")
vport = '1433'
vDBName = dbutils.secrets.get(scope="jpmia-kv-scope", key="sql-db-name")
vDBUser = dbutils.secrets.get(scope="jpmia-kv-scope", key="sql-user")
Vpassword = dbutils.secrets.get(scope="jpmia-kv-scope", key="sql-pwd")
vtable = "dbo.industries"

indus_lkp_df = read_sql_table(vserver, vport, vDBName, vDBUser, Vpassword, vtable)
indus_lkp_list = [ row.industry_id for row in indus_lkp_df.select("industry_id").collect() ]

# COMMAND ----------

schema = StructType(
                    fields = [
                                StructField("job_id", IntegerType(), False),
                                StructField("industry_id", IntegerType(), False)
                    ]
                    )

# COMMAND ----------

job_indus = read_df_w_schema('csv',schema, '/mnt/adlsjpmia/jpmiadata/source_view/job_industries.csv')

# COMMAND ----------

job_indus_rm_blnk = job_indus.na.fill('N/A')

job_indus_ing_date = job_indus_rm_blnk \
                    .withColumn("ingest_date", current_date().cast("string")) 

job_indus_ing_date.show(2)

# COMMAND ----------

invalid_rows = job_indus_ing_date \
                .filter(~col("industry_id").isin(indus_lkp_list)) 
          

add_reason = invalid_rows.withColumn("reason", lit("invalid industry ids"))
                

add_reason.show()

# COMMAND ----------

#write_df(add_reason, 'csv', 'mnt/adlsjpmiadev/jpmia/rejections/', "overwrite")

# COMMAND ----------

invalid_indus_list = [row.industry_id for row in invalid_rows.select("industry_id").collect()]
filter_invalid_rows = job_indus_ing_date.filter(~col('industry_id').isin(invalid_indus_list))
#write_df_as_table(filter_invalid_rows, 'delta', '/mnt/adlsjpmia/jpmia/biz_view/job_industries', "overwrite", "jpmia_delta_bv.job_industries")

# COMMAND ----------

bv_job_ind = DeltaTable.forName(spark, "jpmia_delta_bv.job_industries")


# COMMAND ----------

indus_lkp_df.show(2)

# COMMAND ----------

filter_invalid_rows.show(2)

# COMMAND ----------

#filter_invalid_rows.show(2)
join_df = filter_invalid_rows.join(indus_lkp_df,filter_invalid_rows.industry_id == indus_lkp_df.industry_id,"left") \
            .drop(indus_lkp_df.industry_id,indus_lkp_df.ingest_date)
filter_invalid_rows.show(2)                

# COMMAND ----------

bv_job_ind.alias('target') \
    .merge(filter_invalid_rows.alias('source'), \
        "target.industry_id = source.industry_id and target.job_id = source.job_id" \
        ).whenNotMatchedInsert(
            values={
                "target.industry_id": "source.industry_id",
                "target.job_id": "source.job_id",
                "target.ingest_date": "source.ingest_date"
            }
        ).execute()

# COMMAND ----------

