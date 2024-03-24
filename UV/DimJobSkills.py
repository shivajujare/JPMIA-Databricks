# Databricks notebook source
# MAGIC %run "../Common/Utilities"

# COMMAND ----------

from delta import *

# COMMAND ----------

bv_job_skills = DeltaTable.forName(spark, "jpmia_delta_bv.job_skills").toDF()
bv_job_skills.show(3)

# COMMAND ----------

vserver = dbutils.secrets.get(scope="jpmia-kv-scope", key="sql-server-name")
vport = '1433'
vDBName = dbutils.secrets.get(scope="jpmia-kv-scope", key="sql-db-name")
vDBUser = dbutils.secrets.get(scope="jpmia-kv-scope", key="sql-user")
Vpassword = dbutils.secrets.get(scope="jpmia-kv-scope", key="sql-pwd")
vtable = "dbo.skills"
skills_lkp_df = read_sql_table(vserver, vport, vDBName, vDBUser, Vpassword, vtable)
skills_lkp_df.show(3)

# COMMAND ----------

join_df = bv_job_skills.join(skills_lkp_df,bv_job_skills.skill_abr == skills_lkp_df.skill_abr,"left") \
            .drop(skills_lkp_df.skill_id,skills_lkp_df.skill_abr,skills_lkp_df.ingest_date)
join_df.show(2)

# COMMAND ----------

uv_job_skills = DeltaTable.forName(spark, "jpmia_uv_delta.DimJobSkills")
uv_job_skills.toDF().show(2)

# COMMAND ----------

uv_job_skills.alias('target') \
    .merge(join_df.alias('source'), \
        "target.job_id = source.job_id and target.skill_abr = source.skill_abr and target.skill_name != source.skill_name" \
        ).whenMatchedUpdate(
            set={
                "target.skill_name": "source.skill_name",
                "target.ingest_date": "source.ingest_date"
            }
        ).execute()


# COMMAND ----------

uv_job_skills.alias('target') \
    .merge(join_df.alias('source'), \
        "target.skill_abr = source.skill_abr and target.job_id = source.job_id" \
        ).whenNotMatchedInsert(
            values={
                "target.skill_abr": "source.skill_abr", 
                "target.job_id": "source.job_id",
                "target.skill_name": "source.skill_name",
                "target.ingest_date": "source.ingest_date"
            }
        ).execute()


# COMMAND ----------

