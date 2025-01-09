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

# MAGIC %run "../1. includes/configuration"

# COMMAND ----------

# MAGIC %run "../1. includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("p_EMR", "")
v_EMR  = dbutils.widgets.get("p_EMR")

# COMMAND ----------

dbutils.widgets.text("p_directory_for_storing_landing", "")
v_directory  = dbutils.widgets.get("p_directory_for_storing_landing")
#landing

# COMMAND ----------

dbutils.widgets.text("p_storage_container_for_landing", "")
v_storage_container_for_landing  = dbutils.widgets.get("p_storage_container_for_landing")
#adlsgen2mdftest

# COMMAND ----------

dbutils.widgets.text("p_Practice_ETL_ID", "")
v_Practice_ETL_ID  = dbutils.widgets.get("p_Practice_ETL_ID")
#9997

# COMMAND ----------

dbutils.widgets.text("p_ETLBatchID", "")
v_ETLBatchID  = dbutils.widgets.get("p_ETLBatchID")

# COMMAND ----------

dbutils.widgets.text("p_ETLBatchTS", "")
v_ETLBatchTS  = dbutils.widgets.get("p_ETLBatchTS")

# COMMAND ----------

dbutils.widgets.text("p_ETLOriginalTS", "")
v_ETLOriginalTS  = dbutils.widgets.get("p_ETLOriginalTS")

# COMMAND ----------

dbutils.widgets.text("p_practice", "")
v_practice  = dbutils.widgets.get("p_practice")

# COMMAND ----------



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



# COMMAND ----------

source_to_landing_control_table(v_EMR)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- to be deleted after code gets productionized
# MAGIC select * from logging.log_merge

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

check_data_existence_in_odbc('mf_facilities')

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

from pyspark.sql.functions import col, collect_list

def create_bronze_table(table_name, schema_name):
    # Fetch metadata from Bronze Layer Control Table
    metadata_df = spark.sql(f"""
        SELECT * 
        FROM hive_metastore.default.meta_source_to_target_table 
        WHERE Target_table = '{table_name}' AND Source_Schema = '{schema_name}' AND Isactive = 1 and EMR = '{v_EMR}' and Source_column_id IS NOT NULL
    """)
    print(display(metadata_df))

    

    # Check if metadata exists for the table
    if metadata_df.count() == 0:
        raise ValueError(f"No active metadata found for table: {schema_name}.{table_name}")

    # Extract column details from metadata
    source_table_name = metadata_df.select("Source_Table_Name").first()["Source_Table_Name"]
    columns = metadata_df.select("Source_column_id", "Source_column_data_type").collect()

    # Build schema dynamically
    # Build schema dynamically, keeping original column names with backticks (` `)
    schema = ", ".join([
        f"`{row['Source_column_id'].strip()}` {row['Source_column_data_type'].strip().upper()}" 
        for row in columns
    ])

    # Add inserted_at column
    schema += ", `inserted_at` TIMESTAMP"

    spark.sql("CREATE catalog IF NOT EXISTS Revenue_cycle_DEV;")
    spark.sql("USE CATALOG Revenue_cycle_DEV;")

    # Build full table name
    table_full_name = f"{schema_name}.{source_table_name}"

    # Check if table already exists
    table_exists = spark._jsparkSession.catalog().tableExists(table_full_name)

    # Create table only if it does not exist
    if not table_exists:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
        spark.sql(f"USE SCHEMA {schema_name}")
        print(f"Creating table {table_full_name}...")
        
        create_table_query = f"""
            CREATE TABLE {table_full_name} (
                {schema}
            ) 
            USING DELTA
            TBLPROPERTIES (
                'delta.columnMapping.mode' = 'name'
            )
        """
        print(create_table_query)  # Debugging the query
        spark.sql(create_table_query)
        print(f"Table {table_full_name} created successfully.")
    else:
        print(f"Table {table_full_name} already exists.")

    print(f"Table creation process completed for {table_full_name}.")

