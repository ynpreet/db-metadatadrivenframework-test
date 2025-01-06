# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %sql
# MAGIC -- DROP TABLE IF EXISTS hive_metastore.default.meta_source_to_target_table;
# MAGIC -- DROP CATALOG AdvMD;
# MAGIC -- DROP CATALOG Athena;
# MAGIC
# MAGIC -- DROP TABLE IF EXISTS AdvMD.Bronze.Allregy;
# MAGIC -- DROP TABLE IF EXISTS Athena.Bronze.Allregy;
# MAGIC -- DROP TABLE IF EXISTS Revenue_cycle_DEV.Silver.allregy;
# MAGIC -- DROP TABLE IF EXISTS Athena.silver.Allregy;
# MAGIC
# MAGIC -- DROP TABLE IF EXISTS AdvMD.Bronze.AppointmentType;
# MAGIC -- DROP TABLE IF EXISTS Athena.Bronze.AppointmentType;
# MAGIC -- DROP TABLE IF EXISTS AdvMD.Silver.AppointmentType;
# MAGIC -- DROP TABLE IF EXISTS revenue_cycle_dev.silver.allergy;
# MAGIC -- DROP TABLE IF EXISTS silver.allergy
# MAGIC -- DROP TABLE IF EXISTS revenue_cycle_dev.bronze.advmd_calc_appointmenttype;
# MAGIC -- DROP TABLE IF EXISTS revenue_cycle_dev.bronze.advmd_allergy;
# MAGIC -- DROP TABLE IF EXISTS revenue_cycle_dev.bronze.advmd_allergy;
# MAGIC
# MAGIC -- DROP TABLE IF EXISTS Athena.silver.AppointmentType;
# MAGIC

# COMMAND ----------

dbutils.widgets.text("p_EMR", "")
v_EMR  = dbutils.widgets.get("p_EMR")

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


meta_source_to_target_table_df = spark.read.csv(meta_source_to_target_table_path,header=True, inferSchema=True)
meta_source_to_target_table_df.write.format("delta").option("delta.columnMapping.mode", "name").mode("overwrite").saveAsTable("hive_metastore.default.meta_source_to_target_table")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from hive_metastore.default.meta_source_to_target_table

# COMMAND ----------

meta_source_to_target_table_df.printSchema()

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

create_bronze_table('Allergy','Bronze')

# COMMAND ----------

create_bronze_table('AppointmentType','Bronze')

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG revenue_cycle_dev;
# MAGIC SHOW TABLES FROM bronze;

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG revenue_cycle_dev;
# MAGIC SHOW TABLES FROM bronze;

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp
from delta.tables import DeltaTable

def meta_source_to_target_table(table_name):
    # Variables for ETL columns
    v_ETLBatchID = 1
    v_ETLBatchTS = "2025-01-01 10:00:00"
    v_ETLOriginalTS = "2025-01-01 10:00:00"
    v_practice = "SKI"

    # Create logging schema and table if not exists
    spark.sql("CREATE SCHEMA IF NOT EXISTS logging;")
    spark.sql("""
            CREATE TABLE IF NOT EXISTS logging.log_merge (
                LogID BIGINT GENERATED BY DEFAULT AS IDENTITY, -- Changed INT to BIGINT
                TableName STRING,
                Practice STRING,
                ETLBatchID INT,
                ETLBatchTS TIMESTAMP,
                StartTime TIMESTAMP,
                EndTime TIMESTAMP
            ) USING DELTA;
    """)

    # Log the start time in MST timezone
    from datetime import datetime
    import pytz
    mst = pytz.timezone('MST')
    start_time = datetime.now(mst)

    # Insert initial log entry
    spark.sql(f"""
        INSERT INTO logging.log_merge (TableName, Practice, ETLBatchID, ETLBatchTS, StartTime)
        VALUES ('{table_name}', '{v_practice}', {v_ETLBatchID}, '{v_ETLBatchTS}', '{start_time}')
    """)

    # Fetch metadata from control table
    metadata_df = spark.sql(f"""SELECT * FROM hive_metastore.default.meta_source_to_target_table  
                             WHERE Target_table = '{table_name}'
                             AND Isactive = 1
                             AND EMR = '{v_EMR}' """
                             )
    # Display metadata for debugging
    print(display(metadata_df))
    
    if metadata_df.count() == 0:
        raise ValueError(f"No active metadata found for table: {table_name}")

    # Check Business Logic
    business_logic = metadata_df.select("Business_logic").first()["Business_logic"]

    # If Business Logic is 1, execute notebook
    if business_logic == 1:
        try:
            # Get notebook path
            notebook_path = metadata_df.select("Notebook").first()["Notebook"]
            
            # Check if notebook path is empty or null
            if not notebook_path or notebook_path.strip() == "":
                raise ValueError(f"Notebook not found for table: {table_name}")

            # Execute the specified notebook
            print(f"Executing notebook: {notebook_path}")
            result = dbutils.notebook.run(notebook_path, 0)  # Timeout is 0 (no timeout)
            print(f"Notebook execution result: {result}")
            return  # Exit the function after notebook execution
        
        except Exception as e:
            # Catch errors related to the notebook execution or missing path
            error_message = f"Error executing notebook for table {table_name}: {str(e)}"
            print(error_message)
            raise ValueError(error_message)

    # If Business Logic is not 1, execute the existing pipeline
    # Extract metadata details
    source_table_name = metadata_df.select("Source_Table_Name").first()["Source_Table_Name"]
    target_table_name = metadata_df.select("Target_table").first()["Target_table"]
    source_schema_name = metadata_df.select("Source_Schema").first()["Source_Schema"]
    target_schema_name = metadata_df.select("Target_Schema").first()["Target_Schema"]
    load_type = metadata_df.select("load_type").first()["load_type"]

    # Fetch active columns
    active_columns = metadata_df.select(
        "Source_column_id", 
        "Target_column_id",
        "Target_column_data_type",
        "Source_default_value"
    ).collect()

    # Fetch primary keys
    primary_keys = metadata_df.filter(col("Primary_key") == 1).select("Target_column_id").collect()
    primary_keys = [f"t.`{row['Target_column_id']}` = s.`{row['Target_column_id']}`" for row in primary_keys]
    on_condition = " AND ".join(primary_keys)

    # Prepare column mappings with forced data type casting and default handling
    mapped_columns = []
    for row in active_columns:
        src_col = row['Source_column_id']  # Source column
        tgt_col = row['Target_column_id']  # Target column
        tgt_type = row['Target_column_data_type']  # Target data type
        default_value = row['Source_default_value']  # Default value

        # Handle ETL Columns using widget values
        if tgt_col == "ETLBatchID":
            cast_expr = f"CAST({v_ETLBatchID} AS {tgt_type}) AS `{tgt_col}`"
        elif tgt_col == "ETLBatchTS":
            cast_expr = f"CAST('{v_ETLBatchTS}' AS {tgt_type}) AS `{tgt_col}`"
        elif tgt_col == "ETLOriginalTS":
            cast_expr = f"CAST('{v_ETLOriginalTS}' AS {tgt_type}) AS `{tgt_col}`"
        elif src_col:  # If source column is not null, use it
            cast_expr = f"CAST(`{src_col}` AS {tgt_type}) AS `{tgt_col}`"
        else:  # If source column is null, use the default value
            if default_value == "Null":  # Set default as empty string ('') if "Null"
                cast_expr = f"CAST('' AS {tgt_type}) AS `{tgt_col}`"
            else:  # Use specified default value
                cast_expr = f"CAST('{default_value}' AS {tgt_type}) AS `{tgt_col}`"

        # Add the expression to mapped columns
        mapped_columns.append(cast_expr)

    # Create full table names
    source_table_full_name = f"{source_schema_name}.{source_table_name}"
    target_table_full_name = f"{target_schema_name}.{target_table_name}"

    # Create catalog and schema if not exists
    spark.sql("CREATE CATALOG IF NOT EXISTS Revenue_cycle_DEV;")
    spark.sql("USE CATALOG Revenue_cycle_DEV;")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {target_schema_name};")

    # Create temp view for source data with forced casting and default handling
    mapped_columns_query = ", ".join(mapped_columns)
    query = f"CREATE OR REPLACE TEMP VIEW silver_update AS SELECT {mapped_columns_query} FROM {source_table_full_name};"
    print("Query for silver_update: ", query) # Only for debugging
    spark.sql(query)

    # Check if target table exists
    table_exists = spark._jsparkSession.catalog().tableExists(target_table_full_name)

    # Create target table dynamically if it doesn't exist
    if not table_exists:
        print(f"Target table {target_table_full_name} does not exist. Creating it...")

        # Generate schema dynamically
        target_schema = ", ".join([
            f"`{row['Target_column_id'].strip()}` {row['Target_column_data_type'].strip().upper()}"
            for row in active_columns
        ])

        # Create table query
        create_table_query = f"""
            CREATE TABLE {target_table_full_name} (
                {target_schema}
            ) 
            USING DELTA
            TBLPROPERTIES (
                'delta.columnMapping.mode' = 'name'
            )
        """
        print("Create Table Query: ", create_table_query) # only for debugging
        spark.sql(create_table_query)
        print(f"Target table {target_table_full_name} created successfully.")

    # Handle Incremental or Full Load
    if load_type == "Incremental":

        # Calculating total number of records in Bronze table
        total_records = spark.sql("SELECT COUNT(*) AS count FROM silver_update").collect()[0]["count"]

        # Count non-matching records that will be inserted from Bronze into Silver table
        non_matching_records_query = f"""
        SELECT COUNT(*) AS count
        FROM silver_update s
        LEFT JOIN {target_table_full_name} t
        ON {on_condition}
        WHERE { " OR ".join([f"t.`{col['Target_column_id']}` IS NULL" for col in active_columns]) }
        """
        non_matching_records = spark.sql(non_matching_records_query).collect()[0]["count"]


        # Prepare column mappings for MERGE without casting
        update_columns = []
        insert_columns = []
        insert_values = []

        for row in active_columns:
            tgt_col = row['Target_column_id']

            # No casting here, directly map columns from silver_update view
            update_columns.append(f"t.`{tgt_col}` = s.`{tgt_col}`")
            insert_columns.append(f"`{tgt_col}`")
            insert_values.append(f"s.`{tgt_col}`")

        # Dynamic MERGE query without casting
        merge_query = f"""
        MERGE INTO {target_table_full_name} t
        USING silver_update s
        ON {on_condition}
        WHEN MATCHED THEN
        UPDATE SET
            {', '.join(update_columns)}
        WHEN NOT MATCHED THEN
        INSERT ({', '.join(insert_columns)})
        VALUES ({', '.join(insert_values)});
        """
        print("Merge Query: ", merge_query)  # Print query only for debugging
        spark.sql(merge_query)

        # Print the results
        print(f"Out of {total_records} records in {source_table_full_name}, {non_matching_records} were inserted into the {target_table_full_name} using {load_type} load from EMR {v_EMR}.")

    elif load_type == "Full":
        source_df = spark.sql(f"SELECT * FROM silver_update")
        source_df.write.format("delta").mode("overwrite").saveAsTable(target_table_full_name)
        # Count inserted records for full load
        inserted_count = source_df.count()
        print(f"{inserted_count} records were inserted into the {target_table_full_name} using {load_type} load from EMR {v_EMR} .")

    else:
        raise ValueError(f"Unsupported load type: {load_type}")
    

    # Log the end time
    end_time = datetime.now(mst)
    spark.sql(f"""
        UPDATE logging.log_merge
        SET EndTime = '{end_time}'
        WHERE TableName = '{table_name}' AND StartTime = '{start_time}'
    """)


# COMMAND ----------

meta_source_to_target_table('Allergy')
# meta_source_to_target_table('AppointmentType')

# COMMAND ----------

meta_source_to_target_table('AppointmentType')

# COMMAND ----------


