# JPMIA-Databricks

This Repository contains the modules used to process the data from source_view layer of adls gen 2 storage and store it in Business View and Usage View serving layers according to the business logic. 

Below is the overview of each module

# 1. Common:
    a. Mounting_Storage.py - This notebook has the function defined to mount a storage based on the Storage account and container name. It uses service principle with databrciks backed scopes to mount the container. 
    b. Utilities - This notebook contins the functions defined for reading and writing of data from storage account and sql server and also to validate the no of files received

# 2. Referentials: 
    This module contains the notebooks to refresh the referential data in sql server, these tables will be used as a lookup tables in the data process.

# 3. SQL: 
    This module contains the notebooks used to create the external delta tables and views in databricks database.

# 4. BV: 
    This contains the notebooks which take the data from source_view layer of storage account, process and store in biz_view layer. Below are the 2 main files
        a. BV_Input_Validation - This notebook is run first to check if we received all the required files and  returns "OK" if all received and "Terminate" even if one file is missing. THis return output will be used by data factory to evaluate and run pipelines further or fail
        b. BV_Main - This is the main file which will run all the other notebooks. Data factory will execute this file only when BV input validation output is "OK"

# 5. UV: 
    This contains the notebooks which take the data from biz_view layer, process and load in usage_view layer. 
