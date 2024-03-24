# Databricks notebook source
def mount_adls(container_name, storage_account, client_id, tenant_id, secret):
    configs = {"fs.azure.account.auth.type": "OAuth",
           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
           "fs.azure.account.oauth2.client.id": f"{client_id}",
           "fs.azure.account.oauth2.client.secret": f"{secret}",
           "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}
    
    dbutils.fs.mount(
    source = f"abfss://{container_name}@{storage_account}.dfs.core.windows.net/",
    mount_point = f"/mnt/{storage_account}/{container_name}",
    extra_configs = configs)

# COMMAND ----------

def read_df(vformat, vpath):
    df = spark.read \
                .format(vformat) \
                .option("header",True) \
                .option("inferSchema", True) \
                .option("path",vpath) \
                .load()
    return df

# COMMAND ----------

def read_df_w_schema(vformat,schema, vpath):
    df = spark.read \
                .format(vformat) \
                .option("header",True) \
                .option("schema", schema) \
                .option("path",vpath) \
                .load()
    return df

# COMMAND ----------

def read_df_nested_json(schema, vpath):
    df = spark.read \
                .format('json') \
                .option("schema", schema) \
                .option("multiline",True) \
                .option("path",vpath) \
                .load()
    return df

# COMMAND ----------

def read_sql_table(vserver, vport, vDBName, vDBUser, Vpassword, vtable):
    connectionUrl='jdbc:sqlserver://{}.database.windows.net:{};database={};user={};'.format(vserver,vport,vDBName,vDBUser)

    connectionProperties={
                        'password':Vpassword,
                        'driver':'com.microsoft.sqlserver.jdbc.SQLServerDriver'
                        }
    
    sql_DF = spark.read \
                    .jdbc( url=connectionUrl,
                           table=vtable,
                           properties=connectionProperties
                         )
    return sql_DF



# COMMAND ----------

def write_sql_table(DF, vserver, vport, vDBName, vDBUser, Vpassword, vtable):
    connectionUrl='jdbc:sqlserver://{}.database.windows.net:{};database={};user={};'.format(vserver,vport,vDBName,vDBUser)

    connectionProperties={
                        'password':Vpassword,
                        'driver':'com.microsoft.sqlserver.jdbc.SQLServerDriver'
                        }
    
    sql_DF = DF.write \
                    .mode("overwrite") \
                    .option("truncate", "true") \
                    .jdbc( url=connectionUrl,
                           table=vtable,
                           properties=connectionProperties
                         )



# COMMAND ----------

def write_w_part(df, vformat, vpath, vpartcol, vmode, vtable):
    df.write \
        .option("header",True) \
        .format(vformat) \
        .option("path",vpath) \
        .partitionBy(vpartcol) \
        .mode(vmode) \
        .saveAsTable(vtable )
    return df

# COMMAND ----------

def write_df(df, vformat, vpath, vmode):
    df.write \
        .option("header",True) \
        .format(vformat) \
        .option("path",vpath) \
        .mode(vmode) \
        .save()

# COMMAND ----------

def write_df_as_table(df, vformat, vpath, vmode, table):
    df.write \
        .option("header",True) \
        .format(vformat) \
        .option("path",vpath) \
        .mode(vmode) \
        .saveAsTable(table)

# COMMAND ----------

def checkFiles(files_DF, today_date):
    # Filter files modified today
    files_for_today = files_df.filter(files_df.modificationTime.substr(1, 10) == today_date)

    # Count the number of files received today
    file_count = files_for_today.count()

    if file_count == 9:
        #print(f"Files received for today: {file_count}")
        return "True"
    else:
        #print("No files received for today.")
        return "False"


# COMMAND ----------

