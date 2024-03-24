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
vtable = "dbo.skills"

for i in Vpassword:
    print(i)


# COMMAND ----------

schema = StructType([
                     StructField("skill_id", IntegerType()),
                     StructField("Skills", StructType([
                                                         StructField("skill_abr", StringType()),
                                                         StructField("skill_name", StringType())
                                                     ]))
    
])

# COMMAND ----------

skills_df = read_df_nested_json(schema, '/mnt/adlsjpmiadev/jpmia/source_view/3-11-2024/skills.json')

# COMMAND ----------


result = skills_df.select(
    skills_df["skill_id"].cast(IntegerType()).alias("skill_id"),  # Convert skill_id to IntegerType
    skills_df["Skills.skill_abr"].alias("skill_abr"),
    skills_df["Skills.skill_name"].alias("skill_name")
)

# COMMAND ----------

skills_rm_blnk = result.na.fill('N/A')

skills_ing_date = skills_rm_blnk.withColumn("ingest_date", current_date().cast("string"))


# COMMAND ----------

write_sql_table(skills_ing_date, vserver, vport, vDBName, vDBUser, Vpassword, vtable)

# COMMAND ----------

