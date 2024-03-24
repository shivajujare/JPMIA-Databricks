# Databricks notebook source
# MAGIC %run "../Common/Utilities"

# COMMAND ----------

#imports
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, LongType
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace, when, current_date, expr, lit
import datetime as dt
from delta import *

# COMMAND ----------

schema = StructType(
                    fields = [
                                StructField("salary_id", LongType(), False),
                                StructField("job_id", LongType(), False),
                                StructField("max_salary", FloatType(), False),
                                StructField("med_salary", FloatType(), False),
                                StructField("min_salary", FloatType(), False),
                                StructField("pay_period", StringType(), False),
                                StructField("currency", StringType(), False),
                                StructField("compensation_type", StringType(), False)
                    ]
                    )

# COMMAND ----------

job_salaries = read_df_w_schema('csv',schema, "/mnt/adlsjpmia/jpmiadata/source_view/salaries.csv")

# COMMAND ----------

job_sal_rm_blnk = job_salaries.na.fill('N/A')

job_sal_add_med = job_sal_rm_blnk \
                    .withColumn("med_salary", when((col('med_salary') == 'N/A') & \
                                                    (col('max_salary') != 'N/A') & \
                                                    (col('min_salary')!='N/A'), expr("(max_salary + min_salary)/2")) \
                                               .otherwise(col('med_salary')) \
                               )
                   

# COMMAND ----------

job_sal_ing_date = job_sal_add_med \
                    .withColumn("ingest_date", current_date().cast("string")) \
                    .withColumn("job_id", col('job_id').cast("bigint")) \
                    .withColumn("salary_id", col('salary_id').cast("bigint")) \
                    .withColumn("max_salary", col('max_salary').cast("float")) \
                    .withColumn("med_salary", col('med_salary').cast("float")) \
                    .withColumn("min_salary", col('min_salary').cast("float")) \
                    .withColumn("types", col('job_id').cast("int").isNotNull())

                    

job_sal_ing_date.show(4)

# COMMAND ----------

sal_delta = DeltaTable.forPath(spark, '/mnt/adlsjpmia/jpmiadata/biz_view/job_salaries/')

# COMMAND ----------

sal_delta.alias('target') \
    .merge(job_sal_ing_date.alias('source'), \
          "target.job_id = source.job_id and target.salary_id = source.salary_id \
              and (target.max_salary != source.max_salary or \
                   target.med_salary != source.med_salary or \
                   target.min_salary != source.min_salary or \
                   target.pay_period != source.pay_period or \
                   target.currency != source.currency or \
                   target.compensation_type != source.compensation_type \
                   )"
          ).whenMatchedUpdate(
              set= {
                  "target.max_salary": "source.max_salary",
                   "target.med_salary": "source.med_salary",
                   "target.min_salary": "source.min_salary",
                   "target.pay_period": "source.pay_period",
                   "target.currency": "source.currency",
                   "target.compensation_type": "source.compensation_type",
                   "target.ingest_date": "source.ingest_date"
              }
          ).execute()

# COMMAND ----------

sal_delta.alias('target') \
    .merge(job_sal_ing_date.alias('source'), \
          "target.job_id = source.job_id and target.salary_id = source.salary_id"
          ).whenNotMatchedInsert(
              values= {
                  "target.job_id": "source.job_id",
                  "target.salary_id": "source.salary_id",
                  "target.max_salary": "source.max_salary",
                   "target.med_salary": "source.med_salary",
                   "target.min_salary": "source.min_salary",
                   "target.pay_period": "source.pay_period",
                   "target.currency": "source.currency",
                   "target.compensation_type": "source.compensation_type",
                   "target.ingest_date": "source.ingest_date"
              }
          ).execute()

# COMMAND ----------



# COMMAND ----------

