# Databricks notebook source
from datetime import datetime



# COMMAND ----------

# MAGIC %run "../Common/Utilities"

# COMMAND ----------


# Define the ADLS Gen2 path
adls_gen2_path = "/mnt/adlsjpmia/jpmiadata/source_view"

# Get today's date
today_date = datetime.now().strftime("%Y-%m-%d")
print(today_date)

# List files in the folder
files_df = spark.read.option("recursiveFileLookup", "true").format("binaryFile").load(adls_gen2_path)
print(files_df.count())
#files_df.show()



# COMMAND ----------

check = checkFiles(files_df,today_date)
if check == 'False':
    dbutils.notebook.exit("Terminate")
else:
    dbutils.notebook.exit("OK")
#print("not skipped")


# COMMAND ----------

