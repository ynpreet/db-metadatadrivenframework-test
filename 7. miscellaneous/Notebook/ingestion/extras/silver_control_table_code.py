# Databricks notebook source
def bronze_to_silver_table(table_name):
    from pyspark.sql.functions import col
    metadata_df=spark.sql(f"select * from silver_layer_control_table where target_table_name = '{table_name}' and record_is_active  = 'TRUE'")
    # source_schema_name = 'bronze' #use this if schema name is fixed
    # target_schema_name = 'silver' #use this if schema name is fixed
    if metadata_df.count() == 0:
        raise ValueError(f"No active metadata found for table: {table_name}")

    # Extract metadata details
    source_table_name = metadata_df.select("source_table_name").first()["source_table_name"]
    target_table_name = metadata_df.select("target_table_name").first()["target_table_name"]
    
    active_columns = metadata_df.select("source_column_name", "target_column_name").collect()
    source_schema_name = metadata_df.select("source_schema_name").first()["source_schema_name"] # use this incase we decide to change layer nomenculature
    target_schema_name = metadata_df.select("target_schema_name").first()["target_schema_name"] # use this incase we decide to change layer nomenculature
    primary_key = metadata_df.select("Primary_key").first()["Primary_key"]
    load_type = metadata_df.select("load_type").first()["load_type"]
    
    
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {target_schema_name};")

    column_mapping = {row["source_column_name"]: row["target_column_name"] for row in active_columns}

    # silver_table_df=spark.read.csv(source_file_location,header=True)
    # silver_table_df = f"{schema_name}.{table_name}"
    silver_table_df = spark.sql(f"select * from {source_schema_name}.{source_table_name}")
    silver_table_df.select([col(c).alias(column_mapping[c]) for c in column_mapping.keys()])
    source_table_full_name = f"{source_schema_name}.{source_table_name}"
    target_table_full_name = f"{target_schema_name}.{target_table_name}"


    # Write data based on load type
    if load_type == "Incremental":
        # Append data for incremental load
        spark.sql(f"Create or replace temp view silver_update  as SELECT * FROM {source_table_full_name};")
        spark.sql(f"MERGE INTO {target_table_full_name} t using silver_update s ON t.{primary_key} = s.{primary_key} WHEN NOT MATCHED THEN INSERT * ")
        # silver_table_df.write.format("delta").mode("append").saveAsTable(target_table_full_name)
    elif load_type == "Full":
        # Overwrite data for full load
        silver_table_df.write.format("delta").mode("overwrite").saveAsTable(target_table_full_name)
    else:
        raise ValueError(f"Unsupported load type: {load_type}")
    
    print(f"Data loaded successfully into {target_table_full_name} using {load_type} load.")

# COMMAND ----------

from pyspark.sql.functions import col
from delta.tables import DeltaTable

def bronze_to_silver_table(table_name):
    # Fetch metadata from silver layer control table
    metadata_df = spark.sql(f"SELECT * FROM silver_layer_control_table WHERE source_table_name = '{table_name}' AND record_is_active = 'TRUE'")
    
    if metadata_df.count() == 0:
        raise ValueError(f"No active metadata found for table: {table_name}")

    # Extract metadata details
    source_table_name = metadata_df.select("source_table_name").first()["source_table_name"]
    target_table_name = metadata_df.select("target_table_name").first()["target_table_name"]
    active_columns = metadata_df.select("source_column_name", "target_column_name").collect()
    source_schema_name = metadata_df.select("source_schema_name").first()["source_schema_name"]
    target_schema_name = metadata_df.select("target_schema_name").first()["target_schema_name"]
    primary_key = metadata_df.select("Primary_key").first()["Primary_key"]
    load_type = metadata_df.select("load_type").first()["load_type"]
    
    # Create schema if it does not exist
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {target_schema_name};")

    # Prepare column mapping
    column_mapping = {row["source_column_name"]: row["target_column_name"] for row in active_columns}

    # Read source table data
    source_table_full_name = f"{source_schema_name}.{source_table_name}"
    target_table_full_name = f"{target_schema_name}.{target_table_name}"
    
    source_df = spark.sql(f"SELECT * FROM {source_table_full_name}")

    # Apply column mappings dynamically
    source_df = source_df.select([col(c).alias(column_mapping[c]) for c in column_mapping.keys()])

    # **Check if Target Table Exists**
    table_exists = spark._jsparkSession.catalog().tableExists(target_table_full_name)

    # **Create Target Table Dynamically if Not Exists Based on Control Table Schema**
    if not table_exists:
        print(f"Target table {target_table_full_name} does not exist. Creating it...")

        # Filter active columns
        # active_columns = metadata_df.filter(col("record_is_active") == "TRUE")
        active_columns = metadata_df.filter(col("record_is_active") == "TRUE").collect()

        # Dynamically generate schema for the target table based on active columns
        # target_schema = ", ".join([
        #     f"{row['target_column_name']} {row['target_data_type']}" 
        #     for row in active_columns.collect()
        # ])
        target_schema = ", ".join([
             f"{row['target_column_name']} {row['target_data_type'].strip().upper()}" 
             for row in active_columns
        ])

        # Generate CREATE TABLE query with active columns only
        create_table_query = f"""
            CREATE TABLE {target_table_name} ({target_schema})
            USING DELTA;
        """
        spark.sql(create_table_query)
        print(f"Target table {target_table_full_name} created successfully.")

    # **Handle Incremental or Full Load**
    if load_type == "Incremental":
        # Use Merge for Incremental Load
        # target_table = DeltaTable.forName(spark, target_table_full_name)
        # target_table.alias("target").merge(
        #     source_df.alias("source"),
        #     f"target.{primary_key} = source.{primary_key}"
        # ).whenMatchedUpdateAll(
        # ).whenNotMatchedInsertAll(
        # ).execute()

        # Append data for incremental load
        spark.sql(f"Create or replace temp view silver_update  as SELECT * FROM {source_table_full_name};")
        spark.sql(f"""
        MERGE INTO {target_table_full_name} t
        USING silver_update s
        ON t.{primary_key} = s.{primary_key}
        WHEN NOT MATCHED THEN
        INSERT (Circuit_Id, Circuit_Reference, Name, Location, Country, Latitude, Longitude, Alt, Url, inserted_at)
        VALUES (s.Circuit_Id, s.Circuit_Reference, s.Name, s.Location, s.Country, s.Latitude, s.Longitude, s.Alt, s.Url, s.inserted_at)
    elif load_type == "Full":
        # Overwrite data for full load
        source_df.write.format("delta").mode("overwrite").saveAsTable(target_table_full_name)
    else:
        raise ValueError(f"Unsupported load type: {load_type}")
    
    print(f"Data loaded successfully into {target_table_full_name} using {load_type} load.")


# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp
from delta.tables import DeltaTable

def bronze_to_silver_table(table_name):
    # Fetch metadata for the table
    metadata_df = spark.sql(f"SELECT * FROM silver_layer_control_table WHERE source_table_name = '{table_name}' AND record_is_active = 'TRUE'")
    
    if metadata_df.count() == 0:
        raise ValueError(f"No active metadata found for table: {table_name}")

    # Extract metadata details
    source_table_name = metadata_df.select("source_table_name").first()["source_table_name"]
    target_table_name = metadata_df.select("target_table_name").first()["target_table_name"]
    active_columns = metadata_df.select("source_column_name", "target_column_name").collect()
    source_schema_name = metadata_df.select("source_schema_name").first()["source_schema_name"]
    target_schema_name = metadata_df.select("target_schema_name").first()["target_schema_name"]
    primary_key = metadata_df.select("Primary_key").first()["Primary_key"]
    load_type = metadata_df.select("load_type").first()["load_type"]
    
    # Create schema if it does not exist
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {target_schema_name};")

    # Prepare column mapping
    column_mapping = {row["source_column_name"]: row["target_column_name"] for row in active_columns}

    # Read source table data
    source_table_full_name = f"{source_schema_name}.{source_table_name}"
    target_table_full_name = f"{target_schema_name}.{target_table_name}"
    
    # Apply column mappings dynamically in temp view
    mapped_columns = ", ".join([f"{src} AS {tgt}" for src, tgt in column_mapping.items()])
    spark.sql(f"CREATE OR REPLACE TEMP VIEW silver_update AS SELECT {mapped_columns}, current_timestamp() AS inserted_at FROM {source_table_full_name};")

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
        target_schema += ", `inserted_at` TIMESTAMP"

        create_table_query = f"""
            CREATE TABLE {target_table_full_name} ({target_schema})
            USING DELTA;
        """
        print(create_table_query)  # Debugging query
        spark.sql(create_table_query)
        print(f"Target table {target_table_full_name} created successfully.")
    else:
        # **ALTER TABLE to ADD Missing Column (`inserted_at`)**
        print(f"Checking if 'inserted_at' column exists in {target_table_full_name}")
        columns_df = spark.sql(f"DESCRIBE TABLE {target_table_full_name}")
        if not columns_df.filter(col("col_name") == "inserted_at").count():
            print(f"Adding 'inserted_at' column to {target_table_full_name}")
            spark.sql(f"ALTER TABLE {target_table_full_name} ADD COLUMNS (inserted_at TIMESTAMP);")
            print(f"'inserted_at' column added successfully.")

    # Handle Incremental or Full Load
    if load_type == "Incremental":
        # Dynamic column mapping for MERGE
        insert_columns = ", ".join([f"t.{col}" for col in column_mapping.values()] + ["t.inserted_at"])
        insert_values = ", ".join([f"s.{col}" for col in column_mapping.values()] + ["s.inserted_at"])

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
        source_df 


# COMMAND ----------

# def bronze_to_silver_table(table_name):
#     from pyspark.sql.functions import col, current_timestamp
#     from delta.tables import DeltaTable
#     # Fetch metadata from silver layer control table
#     metadata_df = spark.sql(f"SELECT * FROM silver_layer_control_table WHERE source_table_name = '{table_name}' AND record_is_active = 'TRUE'")
    
#     if metadata_df.count() == 0:
#         raise ValueError(f"No active metadata found for table: {table_name}")

#     # Extract metadata details
#     source_table_name = metadata_df.select("source_table_name").first()["source_table_name"]
#     target_table_name = metadata_df.select("target_table_name").first()["target_table_name"]
#     active_columns = metadata_df.select("source_column_name", "target_column_name").collect()
#     source_schema_name = metadata_df.select("source_schema_name").first()["source_schema_name"]
#     target_schema_name = metadata_df.select("target_schema_name").first()["target_schema_name"]
#     primary_key = metadata_df.select("Primary_key").first()["Primary_key"]
#     load_type = metadata_df.select("load_type").first()["load_type"]
    
#     # Create schema if it does not exist
#     spark.sql(f"CREATE SCHEMA IF NOT EXISTS {target_schema_name};")

#     # Prepare column mapping
#     column_mapping = {row["source_column_name"]: row["target_column_name"] for row in active_columns}

#     # Read source table data
#     source_table_full_name = f"{source_schema_name}.{source_table_name}"
#     target_table_full_name = f"{target_schema_name}.{target_table_name}"
    
#     # Apply column mappings dynamically in temp view
#     mapped_columns = ", ".join([f"{src} AS {tgt}" for src, tgt in column_mapping.items()])
#     spark.sql(f"CREATE OR REPLACE TEMP VIEW silver_update AS SELECT {mapped_columns} FROM {source_table_full_name};")

#     # Check if Target Table Exists
#     table_exists = spark._jsparkSession.catalog().tableExists(target_table_full_name)

#     # Create Target Table Dynamically if Not Exists
#     if not table_exists:
#         print(f"Target table {target_table_full_name} does not exist. Creating it...")

#         # Generate schema dynamically
#         target_schema = ", ".join([
#             f"`{row['target_column_name'].strip()}` {row['target_data_type'].strip().upper()}"
#             for row in active_columns
#         ])

#         # Add inserted_at column
#         target_schema += ", `inserted_at` TIMESTAMP"

#         create_table_query = f"""
#             CREATE TABLE {target_table_full_name} ({target_schema})
#             USING DELTA;
#         """
#         print(create_table_query)
#         spark.sql(create_table_query)
#         print(f"Target table {target_table_full_name} created successfully.")

#     # Handle Incremental or Full Load
#     if load_type == "Incremental":
#         # Dynamic column mapping for MERGE
#         insert_columns = ", ".join([f"t.{col}" for col in column_mapping.values()])
#         insert_values = ", ".join([f"s.{col}" for col in column_mapping.values()])

#         # Execute MERGE Query
#         spark.sql(f"""
#         MERGE INTO {target_table_full_name} t
#         USING silver_update s
#         ON t.{primary_key} = s.{primary_key}
#         WHEN NOT MATCHED THEN
#         INSERT ({insert_columns})
#         VALUES ({insert_values})
#         """)
#     elif load_type == "Full":
#         # Overwrite data for full load
#         source_df = spark.sql(f"SELECT * FROM silver_update")
#         source_df.write.format("delta").mode("overwrite").saveAsTable(target_table_full_name)
#     else:
#         raise ValueError(f"Unsupported load type: {load_type}")
    
#     print(f"Data loaded successfully into {target_table_full_name} using {load_type} load.")


# COMMAND ----------

# from pyspark.sql.functions import col, current_timestamp
# from delta.tables import DeltaTable

# def bronze_to_silver_table(table_name):
#     # Fetch metadata for the table from the control table
#     metadata_df = spark.sql(f"""
#         SELECT * FROM silver_layer_control_table 
#         WHERE target_column_name = '{table_name}' AND record_is_active = 'TRUE'
#     """)

#     # Check if metadata exists for the given table
#     if metadata_df.count() == 0:
#         raise ValueError(f"No active metadata found for table: {table_name}")

#     # Extract metadata details
#     source_table_name = metadata_df.select("source_table_name").first()["source_table_name"]
#     target_table_name = metadata_df.select("target_table_name").first()["target_table_name"]
#     # active_columns = metadata_df.select("source_column_name", "target_column_name").collect()
#     active_columns = metadata_df.select(
#     "source_column_name", 
#     "target_column_name", 
#     "target_data_type", 
#     "record_is_active"
#     ).filter(col("record_is_active") == "TRUE").collect()
#     source_schema_name = metadata_df.select("source_schema_name").first()["source_schema_name"]
#     target_schema_name = metadata_df.select("target_schema_name").first()["target_schema_name"]
#     primary_key = metadata_df.select("Primary_key").first()["Primary_key"]
#     load_type = metadata_df.select("load_type").first()["load_type"]

#     # Create schema if it does not exist
#     spark.sql(f"CREATE SCHEMA IF NOT EXISTS {target_schema_name};")

#     # Prepare column mapping
#     column_mapping = {row["source_column_name"]: row["target_column_name"] for row in active_columns}

#     # Read source table data
#     source_table_full_name = f"{source_schema_name}.{source_table_name}"
#     target_table_full_name = f"{target_schema_name}.{target_table_name}"

#     # Apply column mappings dynamically and add 'inserted_at'
#     mapped_columns = ", ".join([f"{src} AS {tgt}" for src, tgt in column_mapping.items()])
#     spark.sql(f"""
#         CREATE OR REPLACE TEMP VIEW silver_update AS 
#         SELECT {mapped_columns}
#         FROM {source_table_full_name};
#     """)

#     # Check if Target Table Exists
#     table_exists = spark._jsparkSession.catalog().tableExists(target_table_full_name)

#     # **Create Target Table Dynamically if Not Exists Based on Control Table Schema**
#     if not table_exists:
#         print(f"Target table {target_table_full_name} does not exist. Creating it...")

#         # Generate schema dynamically from metadata
#         target_schema = ", ".join([
#             f"`{row['target_column_name'].strip()}` {row['target_data_type'].strip().upper()}"
#             for row in active_columns
#         ])


#         # Add 'inserted_at' column explicitly
#         target_schema += ", `inserted_at` TIMESTAMP"

#         # Create the target table with the defined schema
#         create_table_query = f"""
#             CREATE TABLE {target_table_full_name} ({target_schema})
#             USING DELTA;
#         """
#         spark.sql(create_table_query)
#         print(f"Target table {target_table_full_name} created successfully.")
#     else:
#         # Validate and Add Missing Columns in Target Table
#         print(f"Validating schema for {target_table_full_name}...")
#         columns_df = spark.sql(f"DESCRIBE TABLE {target_table_full_name}")
#         existing_columns = [row['col_name'] for row in columns_df.collect()]

#         # Prepare required columns based on metadata
#         required_columns = [row["target_column_name"] for row in active_columns] + ["inserted_at"]
#         missing_columns = [col for col in required_columns if col not in existing_columns]

#         # Dynamically add missing columns if needed
#         for col_name in missing_columns:
#             data_type = "TIMESTAMP" if col_name == "inserted_at" else metadata_df.filter(
#                 metadata_df["target_column_name"] == col_name
#             ).select("target_data_type").first()["target_data_type"].strip().upper()
#             print(f"Adding column '{col_name}' with type {data_type}")
#             spark.sql(f"ALTER TABLE {target_table_full_name} ADD COLUMNS ({col_name} {data_type});")
#         print(f"Schema validated successfully for {target_table_full_name}.")

#     # **Handle Incremental or Full Load**
#     if load_type == "Incremental":
#         # Dynamic column mapping for MERGE
#         insert_columns = ", ".join([f"t.{col}" for col in column_mapping.values()] + ["t.inserted_at"])
#         insert_values = ", ".join([f"s.{col}" for col in column_mapping.values()] + ["s.inserted_at"])

#         # Execute MERGE Query
#         spark.sql(f"""
#         MERGE INTO {target_table_full_name} t
#         USING silver_update s
#         ON t.{primary_key} = s.{primary_key}
#         WHEN MATCHED THEN UPDATE SET *
#         WHEN NOT MATCHED THEN INSERT ({insert_columns})
#         VALUES ({insert_values})
#         """)
#     elif load_type == "Full":
#         # Overwrite data for full load
#         source_df = spark.sql(f"SELECT * FROM silver_update")
#         source_df.write.format("delta").mode("overwrite").saveAsTable(target_table_full_name)
#     else:
#         raise ValueError(f"Unsupported load type: {load_type}")
    
#     print(f"Data loaded successfully into {target_table_full_name} using {load_type} load.")

