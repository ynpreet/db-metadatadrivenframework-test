# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %sql
# MAGIC -- DROP TABLE IF EXISTS meta_source_to_target_table;
# MAGIC -- DROP CATALOG AdvMD;
# MAGIC -- DROP CATALOG Athena;
# MAGIC
# MAGIC -- DROP TABLE IF EXISTS AdvMD.Bronze.Allregy;
# MAGIC -- DROP TABLE IF EXISTS Athena.Bronze.Allregy;
# MAGIC DROP TABLE IF EXISTS AdvMD.Silver.allregy;
# MAGIC -- DROP TABLE IF EXISTS Athena.silver.Allregy;
# MAGIC
# MAGIC -- DROP TABLE IF EXISTS AdvMD.Bronze.AppointmentType;
# MAGIC -- DROP TABLE IF EXISTS Athena.Bronze.AppointmentType;
# MAGIC -- DROP TABLE IF EXISTS AdvMD.Silver.AppointmentType;
# MAGIC -- DROP TABLE IF EXISTS Athena.silver.AppointmentType;
# MAGIC

# COMMAND ----------

dbutils.widgets.text("p_EMR", "")
v_EMR  = dbutils.widgets.get("p_EMR")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS Atehna;
# MAGIC CREATE CATALOG IF NOT EXISTS AdvMD;

# COMMAND ----------


meta_source_to_target_table_df = spark.read.csv(meta_source_to_target_table_path,header=True, inferSchema=True)
# df.write.mode("overwrite").option("delta.columnMapping.mode", "name").format("delta").saveAsTable("catalog.schema_name.table_name")
meta_source_to_target_table_df.write.format("delta").option("delta.columnMapping.mode", "name").mode("overwrite").saveAsTable("hive_metastore.default.meta_source_to_target_table")

# COMMAND ----------

athena_allregy_table_path

# COMMAND ----------


# athena_allregy_table_df = spark.read.csv(athena_allregy_table_path,header=True, inferSchema=True)
# athena_allregy_table_df.write.format("delta").option("delta.columnMapping.mode", "name").mode("overwrite").saveAsTable("hive_metastore.default.athena_allregy")

athena_allregy_table_df = spark.read.csv(athena_allregy_table_path, header=True, inferSchema=True)
athena_allregy_table_df.write.format("delta").option("delta.columnMapping.mode", "name").mode("overwrite").saveAsTable("hive_metastore.default.athena_allregy")

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC select * from athena_allregy

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW CATALOGS

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from hive_metastore.default.meta_source_to_target_table

# COMMAND ----------

meta_source_to_target_table_df.printSchema()

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG AdvMD;
# MAGIC SHOW SCHEMAS

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


# COMMAND ----------

create_bronze_table('AppointmentType','Bronze')

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG revenue_cycle_dev;
# MAGIC SHOW TABLES FROM bronze;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC DROP TABLE IF EXISTS bronze.Allergy

# COMMAND ----------



# COMMAND ----------


from pyspark.sql.functions import col, current_timestamp
from delta.tables import DeltaTable

def meta_source_to_target_table(table_name):
    # Fetch metadata from silver layer control table
    metadata_df = spark.sql(f"""SELECT * FROM hive_metastore.default.meta_source_to_target_table  WHERE Target_table = '{table_name}'
                             AND Isactive = 1
                             AND EMR = '{v_EMR}' """
                             )
    print(display(metadata_df))
    
    if metadata_df.count() == 0:
        raise ValueError(f"No active metadata found for table: {table_name}")

    # Extract metadata details
    emr = v_EMR
    source_table_name = metadata_df.select("Source_Table_Name").first()["Source_Table_Name"]
    target_table_name = metadata_df.select("Target_table").first()["Target_table"]
    schema_name = metadata_df.select("Target_Schema").first()["Target_Schema"]
    # active_columns = metadata_df.select("source_column_name", "target_column_name").collect()
    active_columns = metadata_df.select(
    "Source_column_id", 
    "Source_column_data_type", 
    "Target_column_id",
    "Target_column_data_type", 
    "Isactive"
    ).filter(col("Isactive") == 1).filter(col("Source_column_id") != '').collect()
    source_schema_name = metadata_df.select("Source_Schema").first()["Source_Schema"]
    target_schema_name = metadata_df.select("Target_Schema").first()["Target_Schema"]
    # primary_key = metadata_df.select("Primary_key").first()["Primary_key"]
    primary_keys = metadata_df.filter(col("Primary_key") == 1).select("Target_column_id").collect()
    primary_keys = [f"t.`{row['Target_column_id']}` = s.`{row['Target_column_id']}`" for row in primary_keys]
    # Combine primary keys with 'AND' for composite key matching
    on_condition = " AND ".join(primary_keys)
    load_type = metadata_df.select("load_type").first()["load_type"]
    
    # Create Catalog and schema if it does not exist
    spark.sql("CREATE catalog IF NOT EXISTS Revenue_cycle_DEV;")
    spark.sql("USE CATALOG Revenue_cycle_DEV;")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {target_schema_name};")

    # Prepare column mapping
    # column_mapping = {row["Source_column_id"]: row["Target_column_id"] for row in active_columns}
    column_mapping = {f"`{row['Source_column_id']}`": f"`{row['Target_column_id']}`" for row in active_columns}
    # print(column_mapping)
    print(f"Column mapping created {column_mapping}")
    spark.sql("CREATE catalog IF NOT EXISTS Revenue_cycle_DEV;")
    spark.sql("USE CATALOG Revenue_cycle_DEV;")

    # Read source table data
    source_table_full_name = f"{source_schema_name}.{source_table_name}"
    target_table_full_name = f"{target_schema_name}.{target_table_name}"
    
    # Apply column mappings dynamically in temp view
    mapped_columns = ", ".join([f"{src} AS {tgt}" for src, tgt in column_mapping.items()])
    
    # Pring query for reference

    query = f"CREATE OR REPLACE TEMP VIEW silver_update AS SELECT {mapped_columns} FROM {source_table_full_name};"
    print("Pring query for reference: ",query)  # Displays the query instead of executing it

    print("mapped_columns are",mapped_columns)
    spark.sql(f"CREATE OR REPLACE TEMP VIEW silver_update AS SELECT {mapped_columns} FROM {source_table_full_name};")

    # Check if Target Table Exists
    table_exists = spark._jsparkSession.catalog().tableExists(target_table_full_name)

    # Create Target Table Dynamically if Not Exists
    if not table_exists:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
        spark.sql(f"USE SCHEMA {schema_name}")

        # Dynamic column mapping for MERGE
        insert_columns = ", ".join([f"t.{col}" for col in column_mapping.values()])
        insert_values = ", ".join([f"s.{col}" for col in column_mapping.values()])

        print(f"Target table {target_table_full_name} does not exist. Creating it...")

        # Generate schema dynamically
        target_schema = ", ".join([
            f"`{row['Target_column_id'].strip()}` {row['Target_column_data_type'].strip().upper()}"
            for row in active_columns
        ])

        print("The target schema is: ",target_schema)

        # Add inserted_at column
        # target_schema += ", `inserted_at` TIMESTAMP"

        # create_table_query = f"""
        #     CREATE TABLE {target_table_full_name} ({target_schema})
        #     USING DELTA;
        # """
        create_table_query = f"""
            CREATE TABLE {target_table_full_name} (
                {target_schema}
            ) 
            USING DELTA
            TBLPROPERTIES (
                'delta.columnMapping.mode' = 'name'
            )
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
        merge_query = f"""
        MERGE INTO {target_table_full_name} t
        USING silver_update s
        ON {on_condition}
        WHEN NOT MATCHED THEN
        INSERT ({insert_columns})
        VALUES ({insert_values})
        """
    elif load_type == "Full":
        # Overwrite data for full load
        source_df = spark.sql(f"SELECT * FROM silver_update")
        source_df.write.format("delta").mode("overwrite").saveAsTable(target_table_full_name)
    else:
        raise ValueError(f"Unsupported load type: {load_type}")
    
    print(f"Data loaded successfully into {target_table_full_name} using {load_type} load from EMR {v_EMR}.")


# COMMAND ----------

meta_source_to_target_table('Allergy')
# meta_source_to_target_table('AppointmentType')

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG revenue_cycle_dev;
# MAGIC SHOW TABLES FROM silver;

# COMMAND ----------

# from pyspark.sql.functions import col, lit, current_timestamp
# from delta.tables import DeltaTable

# def meta_source_to_target_table(table_name):
#     # Fetch metadata for the table
#     metadata_df = spark.sql(f"""
#         SELECT * FROM hive_metastore.default.meta_source_to_target_table 
#         WHERE source_table_name = '{table_name}' AND Isactive = 1 
#     """)

#     # Check if metadata exists
#     if metadata_df.count() == 0:
#         raise ValueError(f"No active metadata found for table: {table_name}")

#     # Extract metadata details
#     source_table_name = metadata_df.select("Source_Table_Name").first()["Source_Table_Name"]
#     target_table_name = metadata_df.select("Target_table").first()["Target_table"]
#     source_schema_name = metadata_df.select("Source_Schema").first()["Source_Schema"]
#     target_schema_name = metadata_df.select("Target_Schema").first()["Target_Schema"]
#     primary_key = metadata_df.select("Primary_key").first()["Primary_key"]
#     load_type = metadata_df.select("load_type").first()["load_type"]

#     # Create schema if it does not exist
#     spark.sql(f"CREATE SCHEMA IF NOT EXISTS {target_schema_name};")

#     # Prepare column mapping
#     existin_active_columns = metadata_df.filter(col("Isactive") == 1).filter(col("Isactive") == 1).collect()

#     # Prepare mappings for source-to-target
#     column_mapping = {}
#     additional_columns = {}  # Tracks columns that exist only in Silver Table
#     for row in active_columns:
#         source_col = row["Source_column_id"]
#         target_col = row["Target_column_id"]

#         # Handle columns that exist only in Silver
#         if source_col is None:  # Additional columns in Silver
#             additional_columns[target_col] = row["Source_default_value"]
#         else:
#             column_mapping[source_col] = target_col

#     # Read source table data
#     source_table_full_name = f"{source_schema_name}.{source_table_name}"
#     target_table_full_name = f"{target_schema_name}.{target_table_name}"

#     # Read source data and apply column mapping
#     source_df = spark.sql(f"SELECT * FROM {source_table_full_name}")

#     # Map columns from Bronze to Silver
#     mapped_df = source_df.select(
#         *[col(src).alias(tgt) for src, tgt in column_mapping.items()]
#     )

#     # Add additional columns with default values
#     for col_name, default_val in additional_columns.items():
#         if default_val is None or default_val.lower() == "null":
#             mapped_df = mapped_df.withColumn(col_name, lit(None))
#         else:
#             mapped_df = mapped_df.withColumn(col_name, lit(default_val))

#     # Add inserted_at column
#     mapped_df = mapped_df.withColumn("inserted_at", current_timestamp())

#     # Check if Target Table Exists
#     table_exists = spark._jsparkSession.catalog().tableExists(target_table_full_name)

#     # Create Target Table Dynamically if Not Exists
#     if not table_exists:
#         print(f"Target table {target_table_full_name} does not exist. Creating it...")

#         # Generate schema dynamically
#         target_schema = ", ".join([
#             f"`{row['Target_column_id'].strip()}` {row['Target_column_data_type'].strip().upper()}"
#             for row in active_columns
#         ])
#         target_schema += ", `inserted_at` TIMESTAMP"

#         create_table_query = f"""
#             CREATE TABLE {target_table_full_name} ({target_schema})
#             USING DELTA;
#         """
#         spark.sql(create_table_query)
#         print(f"Target table {target_table_full_name} created successfully.")

#     # Handle Incremental or Full Load
#     if load_type == "Incremental":
#         # Merge logic for incremental load
#         target_table = DeltaTable.forName(spark, target_table_full_name)
#         target_table.alias("t").merge(
#             mapped_df.alias("s"),
#             f"t.{primary_key} = s.{primary_key}"
#         ).whenMatchedUpdateAll(
#         ).whenNotMatchedInsertAll(
#         ).execute()
#     elif load_type == "Full":
#         # Overwrite for full load
#         mapped_df.write.format("delta").mode("overwrite").saveAsTable(target_table_full_name)
#     else:
#         raise ValueError(f"Unsupported load type: {load_type}")

#     print(f"Data loaded successfully into {target_table_full_name} using {load_type} load.")


# COMMAND ----------



# COMMAND ----------



# COMMAND ----------


