# Databricks notebook source
dbutils.notebook.run('DimCompanies',timeout_seconds=300)

# COMMAND ----------

dbutils.notebook.run('DimCompanyIndustries',timeout_seconds=300)

# COMMAND ----------

dbutils.notebook.run('DimCompanySpecialities',timeout_seconds=300)

# COMMAND ----------

dbutils.notebook.run('DimJobBenefits',timeout_seconds=300)

# COMMAND ----------

dbutils.notebook.run('DimJobIndustries',timeout_seconds=300)

# COMMAND ----------

dbutils.notebook.run('DimJobSkills',timeout_seconds=300)

# COMMAND ----------

dbutils.notebook.run('FactEmployeeCounts',timeout_seconds=300)

# COMMAND ----------

