# Databricks notebook source
# MAGIC %md
# MAGIC Checklist for tasks:
# MAGIC 1. File extension -- completed
# MAGIC 2. If table doesnt exist -- completed
# MAGIC 3. Least data scanning -- Pending
# MAGIC 4. What if it runs twice -- Pending
# MAGIC 5. is active -- completed
# MAGIC 6. load type -- completed
# MAGIC 7. attribute count DQ check -- Pending
# MAGIC 8. frequency - daily -- Pending
# MAGIC 9. Business logic -- Pending

# COMMAND ----------

# MAGIC %run "../1. includes/configuration_parameterized"

# COMMAND ----------

# MAGIC %run "../1. includes/common_functions_parameterized"
# MAGIC

# COMMAND ----------

display(dbutils.fs.ls("abfss://silver-data@adlsgen2mdftest.dfs.core.windows.net/"))

# COMMAND ----------

# MAGIC %sql
# MAGIC describe metastore

# COMMAND ----------

dbutils.widgets.text("p_EMR", "")
v_EMR  = dbutils.widgets.get("p_EMR")

dbutils.widgets.text("p_directory_for_storing_landing", "")
v_directory  = dbutils.widgets.get("p_directory_for_storing_landing")
#landing

dbutils.widgets.text("p_storage_container_for_landing", "")
v_storage_container_for_landing  = dbutils.widgets.get("p_storage_container_for_landing")
#adlsgen2mdftest

dbutils.widgets.text("p_Practice_ETL_ID", "")
v_Practice_ETL_ID  = dbutils.widgets.get("p_Practice_ETL_ID")
#9997

dbutils.widgets.text("p_ETLBatchID", "")
v_ETLBatchID  = dbutils.widgets.get("p_ETLBatchID")

dbutils.widgets.text("p_ETLBatchTS", "")
v_ETLBatchTS  = dbutils.widgets.get("p_ETLBatchTS")

dbutils.widgets.text("p_ETLOriginalTS", "")
v_ETLOriginalTS  = dbutils.widgets.get("p_ETLOriginalTS")

dbutils.widgets.text("p_practice", "")
v_practice  = dbutils.widgets.get("p_practice")

# COMMAND ----------

create_source_to_landing_control_table()

# COMMAND ----------

# MAGIC %sql
# MAGIC -- to be deleted after code gets productionized
# MAGIC
# MAGIC UPDATE revenue_cycle_dev.metadata.source_to_landing_control_table SET last_modified_date = DATE('2025-01-06'); 
# MAGIC -- only for debugging purpose

# COMMAND ----------

# MAGIC %sql
# MAGIC -- to be deleted after code gets productionized
# MAGIC select * from revenue_cycle_dev.metadata.source_to_landing_control_table -- only for debugging purpose

# COMMAND ----------

source_to_landing_control_table(v_EMR)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- to be deleted after code gets productionized
# MAGIC select * from logging.log_merge
