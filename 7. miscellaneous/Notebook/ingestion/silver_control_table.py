# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS silver_layer_control_table;
# MAGIC DROP TABLE IF EXISTS silver.allergy;
# MAGIC DROP TABLE IF EXISTS silver.circuits;
# MAGIC

# COMMAND ----------


silver_control_df = spark.read.csv(silver_control_table_path,header=True, inferSchema=True)
silver_control_df.write.format("delta").mode("overwrite").saveAsTable("silver_layer_control_table")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from silver_layer_control_table

# COMMAND ----------


from pyspark.sql.functions import col, current_timestamp
from delta.tables import DeltaTable

def bronze_to_silver_table(table_name):
    # Fetch metadata from silver layer control table
    metadata_df = spark.sql(f"SELECT * FROM silver_layer_control_table WHERE source_table_name = '{table_name}' AND record_is_active = 'TRUE'")
    
    if metadata_df.count() == 0:
        raise ValueError(f"No active metadata found for table: {table_name}")

    # Extract metadata details
    source_table_name = metadata_df.select("source_table_name").first()["source_table_name"]
    target_table_name = metadata_df.select("target_table_name").first()["target_table_name"]
    # active_columns = metadata_df.select("source_column_name", "target_column_name").collect()
    active_columns = metadata_df.select(
    "source_column_name", 
    "target_column_name", 
    "target_data_type", 
    "record_is_active"
    ).filter(col("record_is_active") == "TRUE").collect()
    source_schema_name = metadata_df.select("source_schema_name").first()["source_schema_name"]
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


