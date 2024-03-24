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
                                StructField("name", StringType(), False),
                                StructField("description", StringType(), False),
                                StructField("company_size", IntegerType(), False),
                                StructField("state", StringType(), False),
                                StructField("country", StringType(), False),
                                StructField("city", StringType(), False),
                                StructField("zip_code", StringType(), False),
                                StructField("address", StringType(), False),
                                StructField("url", StringType(), False)
                    ]
                    )

# COMMAND ----------

companies_srv = spark.read \
      .option("multiline", "true") \
      .option("quote", '"') \
      .option("escape", "\\") \
      .option("escape", '"') \
     .format('csv') \
                .option("header",True) \
                .option("schema", schema) \
                .option("path",'/mnt/adlsjpmia/jpmiadata/source_view/companies.csv') \
    .load()

companies_srv.show(2)

# COMMAND ----------

removed_blank = companies_srv.na.fill('N/A')
replace_invalid = removed_blank \
                    .withColumn("state", when(col('state') == '0', 'N/A' ).otherwise(col('state'))) \
                    .withColumn("country", when(col('country') == '0', 'N/A').otherwise(col('country')) ) \
                    .withColumn("city", when(col('city') == '0', 'N/A').otherwise(col('city')) ) \
                    .withColumn("zip_code", when(col('zip_code') == '0', 'N/A').otherwise(col('zip_code')) ) \
                    .withColumn("address", when(col('address').isin('0','-','.'), 'N/A').otherwise(col('address')) ) \
                    .withColumn("ingest_date", current_date().cast("date")) \
                    .withColumn("company_id", col('company_id').cast("bigint")) \
                    .withColumn("company_size",col('company_size').cast("int"))


# COMMAND ----------

#write_df_as_table(replace_invalid.limit(0), 'delta', '/mnt/adlsjpmiadev/jpmia/biz_view/companies/', "overwrite", "jpmia_delta_bv.companies")

# COMMAND ----------

companies_delta = DeltaTable.forName(spark, 'jpmia_delta_bv.companies')
companies_delta_DF = companies_delta.toDF()

# COMMAND ----------

#dbutils.fs.ls("/mnt/adlsjpmia/jpmia/biz_view/companies")

# COMMAND ----------

companies_delta.alias('target') \
    .merge(replace_invalid.alias('source'),
           "target.company_id == source.company_id and ( \
           target.name != source.name or	\
           target.description != source.description or \
           target.company_size != source.company_size or \
           target.state != source.state or \
           target.country != source.country or \
           target.city != source.city or \
           target.zip_code != source.zip_code or \
           target.address != source.address or \
           target.url != source.url )"
    ).whenMatchedUpdate(
        set= {
            "target.name": "source.name",
           "target.description": "source.description",
           "target.company_size": "source.company_size",
           "target.state": "source.state",
           "target.country": "source.country",
           "target.city": "source.city",
           "target.zip_code": "source.zip_code",
           "target.address": "source.address ",
           "target.url": "source.url",
           "target.ingest_date": "source.ingest_date"
        }
    ).execute()

# COMMAND ----------

companies_delta.alias('target') \
    .merge(replace_invalid.alias('source'),
           "target.company_id = source.company_id "
    ).whenNotMatchedInsert(
        values={
            "target.company_id": "source.company_id ",
            "target.name": "source.name",
           "target.description": "source.description",
           "target.company_size": "source.company_size",
           "target.state": "source.state",
           "target.country": "source.country",
           "target.city": "source.city",
           "target.zip_code": "source.zip_code",
           "target.address": "source.address ",
           "target.url": "source.url",
           "target.ingest_date": "source.ingest_date"
        }
    ).execute()

# COMMAND ----------

