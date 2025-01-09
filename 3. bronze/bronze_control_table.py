# Databricks notebook source
# MAGIC %run "../includes/configuration"
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS bronze_layer_control_table;
# MAGIC DROP TABLE IF EXISTS bronze.allergy;
# MAGIC DROP TABLE IF EXISTS bronze.circuits;

# COMMAND ----------


bronze_control_df = spark.read.csv(bronze_control_table_path,header=True, inferSchema=True)
bronze_control_df.write.format("delta").mode("overwrite").saveAsTable("bronze_layer_control_table")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronze_layer_control_table

# COMMAND ----------

def blob_to_bronze_table(table_name):
    from pyspark.sql.functions import col, current_timestamp, from_utc_timestamp
    
    # Fetch metadata for the table
    metadata_df = spark.sql(f"""
        SELECT * FROM bronze_layer_control_table 
        WHERE table_name = '{table_name}' AND is_active = 'TRUE'
    """)

    # Check if metadata exists
    if metadata_df.count() == 0:
        raise ValueError(f"No active metadata found for table: {table_name}")

    # Extract metadata details
    source_file_location = metadata_df.select("source_file_location").first()["source_file_location"]
    active_columns = metadata_df.select("source_column_name", "target_column_name").collect()
    schema = metadata_df.select("schema_name").first()["schema_name"]
    load_type = metadata_df.select("load_type").first()["load_type"]

    # Create schema if it doesn't exist
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema};")

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


