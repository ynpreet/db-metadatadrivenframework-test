# Databricks notebook source
# MAGIC %run "../1. includes/configuration_parameterized"

# COMMAND ----------

spark.sql(f"USE catalog {v_catalog_name}")

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS bronze_layer_control_table;
# MAGIC DROP TABLE IF EXISTS bronze.allergy;
# MAGIC DROP TABLE IF EXISTS bronze.circuits;

# COMMAND ----------


bronze_control_df = spark.read.csv(v_bronze_layer_control_csv_path,header=True, inferSchema=True)
bronze_control_df.write.format("delta").mode("overwrite").saveAsTable(f"{v_metadata_schema}.{v_bronze_layer_control_table_name}")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from revenue_cycle_dev.metadata.bronze_layer_control_table

# COMMAND ----------

def blob_to_bronze_table(table_name):
    from pyspark.sql.functions import col, current_timestamp, from_utc_timestamp

    print(v_metadata_schema)
    
    # Fetch metadata for the table
    metadata_df = spark.sql(f"""
        SELECT * FROM {v_metadata_schema}.{v_bronze_layer_control_table_name} 
        WHERE table_name = '{table_name}' AND is_active = 'TRUE'
    """)

    display(metadata_df.show())

# COMMAND ----------

blob_to_bronze_table("allergy")

# COMMAND ----------

def blob_to_bronze_table(table_name):
    from pyspark.sql.functions import col, current_timestamp, from_utc_timestamp
    
    # Fetch metadata for the table
    metadata_df = spark.sql(f"""
        SELECT * FROM {v_metadata_schema}.{v_bronze_layer_control_table_name} 
        WHERE table_name = '{table_name}' AND is_active = 'TRUE'
    """)

    display(metadata_df.show())

    # Check if metadata exists
    if metadata_df.count() == 0:
        raise ValueError(f"No active metadata found for table: {table_name}")

    # Extract metadata details
    l_source_file_location = metadata_df.select("source_file_location").first()["source_file_location"]
    l_active_columns = metadata_df.select("source_column_name", "target_column_name").collect()
    active_columns = metadata_df.select(
        "Source_column_id", 
        "Target_column_id",
        "Target_column_data_type",
        "Source_default_value"
    ).collect()

    l_schema = metadata_df.select("schema_name").first()["schema_name"]
    l_load_type = metadata_df.select("load_type").first()["load_type"]

    # Create schema if it doesn't exist
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {l_schema};")

    # Build column mapping
    column_mapping = {row["source_column_name"]: row["target_column_name"] for row in active_columns}

    # Read source file
    bronze_table_df = spark.read.csv(source_file_location, header=True)

    # Apply column mapping dynamically
    bronze_table_df = bronze_table_df.select(
        [col(c).alias(column_mapping[c]) for c in column_mapping.keys()]
    )

    # Add timestamp
    # bronze_table_df = bronze_table_df.withColumn("inserted_at", current_timestamp())
    bronze_table_df = bronze_table_df.withColumn( "inserted_at", from_utc_timestamp(current_timestamp(), "America/Denver") )
    
    # Define full table name
    table_full_name = f"{schema}.{table_name}"

    # Write data based on load type
    if load_type == "incremental":
        # Append data for incremental load
        bronze_table_df.write.format("delta").mode("append").saveAsTable(table_full_name)
    elif load_type == "full":
        # Overwrite data for full load
        bronze_table_df.write.format("delta").mode("overwrite").saveAsTable(table_full_name)
    else:
        raise ValueError(f"Unsupported load type: {load_type}")

    print(f"Data loaded successfully into {table_full_name} using {load_type} load.")



# COMMAND ----------

blob_to_bronze_table('allergy')
blob_to_bronze_table('circuits')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronze.allergy

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronze.circuits

# COMMAND ----------


