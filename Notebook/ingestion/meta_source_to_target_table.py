# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS meta_source_to_target_table;
# MAGIC DROP TABLE IF EXISTS bronze.Allregy;
# MAGIC DROP TABLE IF EXISTS bronze.Appointment;
# MAGIC DROP TABLE IF EXISTS silver.Allregy;
# MAGIC DROP TABLE IF EXISTS silver.Appointment;

# COMMAND ----------

dbutils.widgets.text("p_EMR", "")
v_EMR  = dbutils.widgets.get("p_EMR")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS Atehna;

# COMMAND ----------


meta_source_to_target_table_df = spark.read.csv(meta_source_to_target_table_path,header=True, inferSchema=True)
meta_source_to_target_table_df.write.format("delta").mode("overwrite").saveAsTable("meta_source_to_target_table")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from meta_source_to_target_table

# COMMAND ----------

meta_source_to_target_table_df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import col, collect_list

def create_bronze_table(table_name, schema_name):
    # Fetch metadata from Bronze Layer Control Table
    metadata_df = spark.sql(f"""
        SELECT * 
        FROM meta_source_to_target_table 
        WHERE Source_Table_Name = '{table_name}' AND Source_Schema = '{schema_name}' AND Isactive = 1 and EMR = '{v_EMR}' and Source_column_id IS NOT NULL
    """)

    # Check if metadata exists for the table
    if metadata_df.count() == 0:
        raise ValueError(f"No active metadata found for table: {schema_name}.{table_name}")

    # Extract column details from metadata
    columns = metadata_df.select("Target_column_id", "Target_column_data_type").collect()

    # Build schema dynamically
    # Build schema dynamically, keeping original column names with backticks (` `)
    schema = ", ".join([
        f"`{row['Target_column_id'].strip()}` {row['Target_column_data_type'].strip().upper()}" 
        for row in columns
    ])

    # Add inserted_at column
    schema += ", `inserted_at` TIMESTAMP"

    # Build full table name
    table_full_name = f"{schema_name}.{table_name}"

    # Check if table already exists
    table_exists = spark._jsparkSession.catalog().tableExists(table_full_name)

    # Create table only if it does not exist
    if not table_exists:
        print(f"Creating table {table_full_name}...")
        create_table_query = f"""
            CREATE TABLE {table_full_name} (
                {schema}
            ) USING DELTA;
        """
        print(create_table_query)  # Debugging the query
        spark.sql(create_table_query)
        print(f"Table {table_full_name} created successfully.")
    else:
        print(f"Table {table_full_name} already exists.")

    print(f"Table creation process completed for {table_full_name}.")


# COMMAND ----------

create_bronze_table("Allergy", "Bronze")
# create_bronze_table("AppointmentType","Bronze" )
# create_bronze_table(Allregy, Bronze)
# create_bronze_table(Allregy, Bronze)

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------


from pyspark.sql.functions import col, current_timestamp
from delta.tables import DeltaTable

def meta_source_to_target_table(table_name):
    # Fetch metadata from silver layer control table
    metadata_df = spark.sql(f"SELECT * FROM meta_source_to_target_table WHERE source_table_name = '{table_name}' AND record_is_active = 'TRUE'")
    
    if metadata_df.count() == 0:
        raise ValueError(f"No active metadata found for table: {table_name}")

    # Extract metadata details
    emr = v_EMR
    source_table_name = metadata_df.select("Source_Table_Name").first()["Source_Table_Name"]
    target_table_name = metadata_df.select("Target_table").first()["Target_table"]
    # active_columns = metadata_df.select("source_column_name", "target_column_name").collect()
    active_columns = metadata_df.select(
    "Source_column_id", 
    "Source_column_data_type", 
    "Target_column_id"
    "Target_column_data_type", 
    "Isactive"
    ).filter(col("Isactive") == 1).collect()
    source_schema_name = metadata_df.select("Source_Schema").first()["Source_Schema"]
    target_schema_name = metadata_df.select("target_schema_name").first()["target_schema_name"]
    primary_key = metadata_df.select("Primary_key").first()["Primary_key"]
    load_type = metadata_df.select("load_type").first()["load_type"]
    
    # Create schema if it does not exist
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {target_schema_name};")

    # Prepare column mapping
    column_mapping = {row["source_column_name"]: row["target_column_name"] for row in active_columns}
    # print(column_mapping)
    print(f"Column mapping created {column_mapping}")

    # Read source table data
    source_table_full_name = f"{source_schema_name}.{source_table_name}"
    target_table_full_name = f"{target_schema_name}.{target_table_name}"
    
    # Apply column mappings dynamically in temp view
    mapped_columns = ", ".join([f"{src} AS {tgt}" for src, tgt in column_mapping.items()])
    spark.sql(f"CREATE OR REPLACE TEMP VIEW silver_update AS SELECT {mapped_columns} FROM {source_table_full_name};")

    # Check if Target Table Exists
    table_exists = spark._jsparkSession.catalog().tableExists(target_table_full_name)

    # Create Target Table Dynamically if Not Exists
    if not table_exists:
        print(f"Target table {target_table_full_name} does not exist. Creating it...")

        # Generate schema dynamically
        target_schema = ", ".join([
            f"`{row['target_column_name'].strip()}` {row['target_data_type'].strip().upper()}"
            for row in active_columns
        ])

        # Add inserted_at column
        # target_schema += ", `inserted_at` TIMESTAMP"

        create_table_query = f"""
            CREATE TABLE {target_table_full_name} ({target_schema})
            USING DELTA;
        """
        print(create_table_query)
        spark.sql(create_table_query)
        print(f"Target table {target_table_full_name} created successfully.")

    # Handle Incremental or Full Load
    if load_type == "Incremental":
        # Dynamic column mapping for MERGE
        insert_columns = ", ".join([f"t.{col}" for col in column_mapping.values()])
        insert_values = ", ".join([f"s.{col}" for col in column_mapping.values()])

        # Execute MERGE Query
        spark.sql(f"""
        MERGE INTO {target_table_full_name} t
        USING silver_update s
        ON t.{primary_key} = s.{primary_key}
        WHEN NOT MATCHED THEN
        INSERT ({insert_columns})
        VALUES ({insert_values})
        """)
    elif load_type == "Full":
        # Overwrite data for full load
        source_df = spark.sql(f"SELECT * FROM silver_update")
        source_df.write.format("delta").mode("overwrite").saveAsTable(target_table_full_name)
    else:
        raise ValueError(f"Unsupported load type: {load_type}")
    
    print(f"Data loaded successfully into {target_table_full_name} using {load_type} load.")


# COMMAND ----------


bronze_to_silver_table('allergy')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from silver.allergy

# COMMAND ----------

bronze_to_silver_table('circuits')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from silver.circuits

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------


