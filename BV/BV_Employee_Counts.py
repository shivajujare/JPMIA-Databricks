# Databricks notebook source
# MAGIC %run "../Common/Utilities"

# COMMAND ----------

#imports
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_date, from_unixtime, to_timestamp, date_format, to_date
import datetime as dt
from delta import DeltaTable

# COMMAND ----------

schema = StructType(
                    fields = [
                                StructField("company_id", IntegerType(), False),
                                StructField("employee_count", IntegerType(), False),
                                StructField("follower_count", IntegerType(), False),
                                StructField("time_recorded", LongType(), False)
                    ]
                    )

# COMMAND ----------

emp_counts = read_df_w_schema('csv',schema, '/mnt/adlsjpmia/jpmiadata/source_view/employee_counts.csv')

# COMMAND ----------

emp_cnt_rm_blnk = emp_counts.na.fill('N/A')

emp_cnt_add_dates = emp_cnt_rm_blnk \
                    .withColumn("date_time_recorded", from_unixtime(col("time_recorded"))) \
                    .withColumn("date_recorded", to_date(col('date_time_recorded'))) \
                    .withColumn("ingest_date", current_date().cast("string"))

emp_cnt_add_dates.show(3)


# COMMAND ----------

emp_cnt_delta = DeltaTable.forPath(spark, '/mnt/adlsjpmia/jpmiadata/biz_view/employee_counts')

# COMMAND ----------

emp_cnt_delta.alias('target') \
    .merge(emp_cnt_add_dates.alias('source'), \
        "target.company_id = source.company_id and \
            target.time_recorded = source.time_recorded"
        ).whenNotMatchedInsert(
            values={
                "target.company_id": "source.company_id",
                "target.employee_count": "source.employee_count",
                "target.follower_count": "source.follower_count",
                "target.time_recorded": "source.time_recorded",
                "target.date_time_recorded": "source.date_time_recorded",
                "target.date_recorded": "source.date_recorded",
                "target.ingest_date": "source.ingest_date"
            }
        ).execute()

# COMMAND ----------

