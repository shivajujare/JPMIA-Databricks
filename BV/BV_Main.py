# Databricks notebook source
dbutils.notebook.run('BV_Companies',timeout_seconds=300)

# COMMAND ----------

dbutils.notebook.run('BV_Company_Industries',timeout_seconds=300)

# COMMAND ----------

dbutils.notebook.run('BV_Company_Specialities',timeout_seconds=300)

# COMMAND ----------

dbutils.notebook.run('BV_Employee_Counts',timeout_seconds=300)

# COMMAND ----------

dbutils.notebook.run('BV_Job_Benefits',timeout_seconds=300)

# COMMAND ----------

dbutils.notebook.run('BV_Job_Industries',timeout_seconds=300)

# COMMAND ----------

dbutils.notebook.run('BV_Job_Salaries',timeout_seconds=300)

# COMMAND ----------

dbutils.notebook.run('BV_Job_Skills',timeout_seconds=300)

# COMMAND ----------

#%run "./configs"

# COMMAND ----------

#postings = ['full_time_job_postings.csv','other_job_postings.csv']
#for file_name in postings:
 #   print(file_name)
    #dbutils.notebook.run('BV_Postings',0,{"file_name": file_name})

# COMMAND ----------

dbutils.notebook.run('BV_Postings',timeout_seconds=300)

# COMMAND ----------

