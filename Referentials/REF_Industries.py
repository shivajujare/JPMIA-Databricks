# Databricks notebook source
# MAGIC %run "../Common/Utilities"

# COMMAND ----------

#imports
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace, when, current_date
import datetime as dt

# COMMAND ----------

vserver = dbutils.secrets.get(scope="jpmia-kv-scope", key="sql-server-name")
vport = '1433'
vDBName = dbutils.secrets.get(scope="jpmia-kv-scope", key="sql-db-name")
vDBUser = dbutils.secrets.get(scope="jpmia-kv-scope", key="sql-user")
Vpassword = dbutils.secrets.get(scope="jpmia-kv-scope", key="sql-pwd")
vtable = "dbo.industries"

for i in Vpassword:
    print(i)


# COMMAND ----------

schema = 'industry_id Int, industry_name String'

indus_df = read_df_w_schema('csv',schema, '/mnt/adlsjpmiadev/jpmia/source_view/3-11-2024/industries.csv')

# COMMAND ----------

indus_rm_blnk = indus_df.na.fill('N/A')

indus_ing_date = indus_rm_blnk \
                    .withColumn("ingest_date", current_date().cast("string")) 


# COMMAND ----------

write_sql_table(indus_ing_date, vserver, vport, vDBName, vDBUser, Vpassword, vtable)

# COMMAND ----------

