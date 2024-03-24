# Databricks notebook source
# MAGIC %run "../Common/Utilities"

# COMMAND ----------

from pyspark.sql.functions import col, when, lit
from delta import *

# COMMAND ----------

bv_comp_DF = spark.read \
                .format('delta') \
                .option('header', True) \
                .table("jpmia_delta_bv.companies") 
                

bv_comp_DF.drop(col('description')).show(2)

# COMMAND ----------

add_format_comp_size = bv_comp_DF \
                        .withColumn("formatted_company_size",when(col('company_size').isin(['0','1','2']), lit('small')) \
                                                            .when(col('company_size').isin(['3','4','5']), lit('medium')) \
                                                            .when(col('company_size') >= '6', lit('large')) \
                                                            .otherwise(lit('N/A'))
                                                                )
add_format_comp_size.drop(col('description')).show(2)

# COMMAND ----------

uv_comp_delta = DeltaTable.forName(spark, 'jpmia_uv_delta.DimCompanies')
uv_comp_delta.toDF().show(2)

# COMMAND ----------

uv_comp_delta.alias('target') \
                .merge(add_format_comp_size.alias('source'), \
                    "target.company_id = source.company_id") \
                .whenMatchedUpdate(
                    set= {
                        "target.company_id": "source.company_id",
                        "target.name": "source.name",
                        "target.description": "source.description",
                        "target.company_size": "source.company_size",
                        "target.formatted_company_size": "source.formatted_company_size",
                        "target.state": "source.state",
                        "target.country": "source.country",
                        "target.city": "source.city",
                        "target.zip_code": "source.zip_code",
                        "target.address": "source.address",
                        "target.url": "source.url",
                        "target.ingest_date": "source.ingest_date"
                    }
                ).whenNotMatchedInsert(
                    values={
                        "target.company_id": "source.company_id",
                        "target.name": "source.name",
                        "target.description": "source.description",
                        "target.company_size": "source.company_size",
                        "target.formatted_company_size": "source.formatted_company_size",
                        "target.state": "source.state",
                        "target.country": "source.country",
                        "target.city": "source.city",
                        "target.zip_code": "source.zip_code",
                        "target.address": "source.address",
                        "target.url": "source.url",
                        "target.ingest_date": "source.ingest_date"
                    }
                ).execute()
                    

# COMMAND ----------

