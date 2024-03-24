# Databricks notebook source
# MAGIC %run "../Common/Utilities"

# COMMAND ----------

from delta import *
from pyspark.sql.functions import col,max, desc
from pyspark.sql.window import Window

# COMMAND ----------

bv_emp_counts = DeltaTable.forName(spark, "jpmia_delta_bv.employee_counts")
#bv_emp_counts.toDF().show(3)
max_window = Window.partitionBy('company_id')
tot_emp_counts = bv_emp_counts.toDF() \
                    .withColumn("total_employees_as_of_now", max("employee_count").over(max_window)) \
                    .withColumn("total_followers_as_of_now",max("follower_count").over(max_window))

tot_emp_counts.filter(col('company_id') == '37486').show(3)

# COMMAND ----------

uv_emp_counts = DeltaTable.forName(spark, "jpmia_uv_delta.FactCompanyEmployeeGrowth")
uv_emp_counts.toDF().show(2)

# COMMAND ----------

uv_emp_counts.alias('target') \
                .merge(tot_emp_counts.alias('source'), \
                    "target.time_recorded = source.time_recorded and \
                    target.company_id = source.company_id  and \
                    target.employee_count = source.employee_count \
                    or target.follower_count = source.follower_count"
                    ).whenNotMatchedInsert(
                        values={
                            "target.company_id": "source.company_id",
                            "target.employee_count": "source.employee_count",
                            "target.follower_count": "source.follower_count",
                            "target.time_recorded": "source.time_recorded",
                            "target.date_time_recorded": "source.date_time_recorded",
                            "target.date_recorded": "source.date_time_recorded",
                            "target.total_employees_as_of_now": "source.total_employees_as_of_now",
                            "target.total_followers_as_of_now": "source.total_followers_as_of_now",
                            "target.ingest_date": "source.ingest_date"
                        }
                    ).execute()

# COMMAND ----------

