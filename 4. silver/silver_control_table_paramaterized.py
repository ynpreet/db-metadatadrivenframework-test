# Databricks notebook source
# MAGIC %run "../1. includes/configuration"

# COMMAND ----------

# MAGIC %run "../1. includes/common_functions"

# COMMAND ----------

# MAGIC %sql
# MAGIC -- DROP TABLE IF EXISTS hive_metastore.default.bronze_to_silver_control_table;
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

dbutils.widgets.text("p_ETLBatchID", "")
v_ETLBatchID  = dbutils.widgets.get("p_ETLBatchID")

dbutils.widgets.text("p_ETLBatchTS", "")
v_ETLBatchTS  = dbutils.widgets.get("p_ETLBatchTS")

dbutils.widgets.text("p_ETLOriginalTS", "")
v_ETLOriginalTS  = dbutils.widgets.get("p_ETLOriginalTS")


dbutils.widgets.text("p_practice", "")
v_practice  = dbutils.widgets.get("p_practice")

# COMMAND ----------


bronze_to_silver_control_table_df = spark.read.csv(bronze_to_silver_control_table_path,header=True, inferSchema=True)
bronze_to_silver_control_table_df.write.format("delta").option("delta.columnMapping.mode", "name").mode("overwrite").saveAsTable("hive_metastore.default.bronze_to_silver_control_table")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from hive_metastore.default.bronze_to_silver_control_table

# COMMAND ----------

bronze_to_silver_control_table_df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import col, collect_list

def create_bronze_table(table_name, schema_name):
    # Fetch metadata from Bronze Layer Control Table
    metadata_df = spark.sql(f"""
        SELECT * 
        FROM hive_metastore.default.bronze_to_silver_control_table 
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

def bronze_to_silver_control_table(table_name):
    # Variables for ETL columns
    v_ETLBatchID = 1
    v_ETLBatchTS = "2025-01-01 10:00:00"
    v_ETLOriginalTS = "2025-01-01 10:00:00"
    v_practice = "SKI"
    layer_name = 'bronze_to_silver'

    # Create logging schema and table if not exists -- followed in Merge to ODS
    spark.sql("CREATE SCHEMA IF NOT EXISTS logging;")
    spark.sql("""
            CREATE TABLE IF NOT EXISTS logging.log_merge (
                LogID BIGINT GENERATED BY DEFAULT AS IDENTITY, 
                layer_name STRING, -- added new column on 1/9/25
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
        INSERT INTO logging.log_merge (layer_name, TableName, Practice, ETLBatchID, ETLBatchTS, StartTime)
        VALUES ('{layer_name}','{table_name}', '{v_practice}', {v_ETLBatchID}, '{v_ETLBatchTS}', '{start_time}')
    """)

    # Fetch metadata from control table
    metadata_df = spark.sql(f"""SELECT * FROM hive_metastore.default.bronze_to_silver_control_table  
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
            # raise ValueError(error_message)
            return

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

# bronze_to_silver_control_table('Allergy')
bronze_to_silver_control_table('AppointmentType')

# COMMAND ----------

# MAGIC %md
# MAGIC sample output:
# MAGIC None
# MAGIC Query for silver_update:  CREATE OR REPLACE TEMP VIEW silver_update AS SELECT CAST(`Allergy Code` AS INT) AS `Allergy Code`, CAST(`LicenseKey` AS INT) AS `BUSINESS_ID`, CAST(`Allergy ID` AS INT) AS `Allergy ID`, CAST(`Allergy Concept Type` AS INT) AS `Allergy Concept Type`, CAST(`Type` AS STRING) AS `Type`, CAST(`Patient ID` AS INT) AS `Patient ID`, CAST(`Allergy Name` AS STRING) AS `Allergy Name`, CAST(`Allergy Onset Date` AS DATE) AS `Onset Date`, CAST(`Allergy Reaction Name` AS STRING) AS `Allergy Reaction Name`, CAST(`Created By` AS STRING) AS `Created By`, CAST(`RxNorm Code` AS STRING) AS `RxNorm Code`, CAST(`Created Datetime` AS TIMESTAMP) AS `Created Datetime`, CAST(`Deactivated By` AS STRING) AS `Deactivated By`, CAST(`Deactivated Datetime` AS TIMESTAMP) AS `Deactivated Datetime`, CAST(`Deleted By` AS STRING) AS `Deleted By`, CAST(`Deleted Datetime` AS TIMESTAMP) AS `Deleted Datetime`, CAST(`Last Modified By` AS STRING) AS `Last Modified By`, CAST(`Reactivated Datetime` AS TIMESTAMP) AS `Reactivated Datetime`, CAST(`Reactivated By` AS STRING) AS `Reactivated By`, CAST(`Last Modified Datetime` AS TIMESTAMP) AS `Last Modified Datetime`, CAST(`Note` AS STRING) AS `Note`, CAST(1 AS INT) AS `ETLBatchID`, CAST('2025-01-01 10:00:00' AS TIMESTAMP) AS `ETLBatchTS`, CAST('2025-01-01 10:00:00' AS TIMESTAMP) AS `ETLOriginalTS`, CAST('' AS INT) AS `Chart ID`, CAST('' AS STRING) AS `CONTEXT_NAME`, CAST('' AS INT) AS `CONTEXT_PARENTCONTEXTID` FROM Bronze.AdvMD_Allergy;
# MAGIC Merge Query:  
# MAGIC         MERGE INTO Silver.Allergy t
# MAGIC         USING silver_update s
# MAGIC         ON t.`BUSINESS_ID` = s.`BUSINESS_ID` AND t.`Allergy ID` = s.`Allergy ID`
# MAGIC         WHEN MATCHED THEN
# MAGIC         UPDATE SET
# MAGIC             t.`Allergy Code` = s.`Allergy Code`, t.`BUSINESS_ID` = s.`BUSINESS_ID`, t.`Allergy ID` = s.`Allergy ID`, t.`Allergy Concept Type` = s.`Allergy Concept Type`, t.`Type` = s.`Type`, t.`Patient ID` = s.`Patient ID`, t.`Allergy Name` = s.`Allergy Name`, t.`Onset Date` = s.`Onset Date`, t.`Allergy Reaction Name` = s.`Allergy Reaction Name`, t.`Created By` = s.`Created By`, t.`RxNorm Code` = s.`RxNorm Code`, t.`Created Datetime` = s.`Created Datetime`, t.`Deactivated By` = s.`Deactivated By`, t.`Deactivated Datetime` = s.`Deactivated Datetime`, t.`Deleted By` = s.`Deleted By`, t.`Deleted Datetime` = s.`Deleted Datetime`, t.`Last Modified By` = s.`Last Modified By`, t.`Reactivated Datetime` = s.`Reactivated Datetime`, t.`Reactivated By` = s.`Reactivated By`, t.`Last Modified Datetime` = s.`Last Modified Datetime`, t.`Note` = s.`Note`, t.`ETLBatchID` = s.`ETLBatchID`, t.`ETLBatchTS` = s.`ETLBatchTS`, t.`ETLOriginalTS` = s.`ETLOriginalTS`, t.`Chart ID` = s.`Chart ID`, t.`CONTEXT_NAME` = s.`CONTEXT_NAME`, t.`CONTEXT_PARENTCONTEXTID` = s.`CONTEXT_PARENTCONTEXTID`
# MAGIC         WHEN NOT MATCHED THEN
# MAGIC         INSERT (`Allergy Code`, `BUSINESS_ID`, `Allergy ID`, `Allergy Concept Type`, `Type`, `Patient ID`, `Allergy Name`, `Onset Date`, `Allergy Reaction Name`, `Created By`, `RxNorm Code`, `Created Datetime`, `Deactivated By`, `Deactivated Datetime`, `Deleted By`, `Deleted Datetime`, `Last Modified By`, `Reactivated Datetime`, `Reactivated By`, `Last Modified Datetime`, `Note`, `ETLBatchID`, `ETLBatchTS`, `ETLOriginalTS`, `Chart ID`, `CONTEXT_NAME`, `CONTEXT_PARENTCONTEXTID`)
# MAGIC         VALUES (s.`Allergy Code`, s.`BUSINESS_ID`, s.`Allergy ID`, s.`Allergy Concept Type`, s.`Type`, s.`Patient ID`, s.`Allergy Name`, s.`Onset Date`, s.`Allergy Reaction Name`, s.`Created By`, s.`RxNorm Code`, s.`Created Datetime`, s.`Deactivated By`, s.`Deactivated Datetime`, s.`Deleted By`, s.`Deleted Datetime`, s.`Last Modified By`, s.`Reactivated Datetime`, s.`Reactivated By`, s.`Last Modified Datetime`, s.`Note`, s.`ETLBatchID`, s.`ETLBatchTS`, s.`ETLOriginalTS`, s.`Chart ID`, s.`CONTEXT_NAME`, s.`CONTEXT_PARENTCONTEXTID`);
# MAGIC         
# MAGIC Out of 0 records in Bronze.AdvMD_Allergy, 0 were inserted into the Silver.Allergy using Incremental load from EMR AdvMD.

# COMMAND ----------

# MAGIC %md
# MAGIC None
# MAGIC Query for silver_update:  CREATE OR REPLACE TEMP VIEW silver_update AS SELECT CAST(`Allergy Code` AS INT) AS `Allergy Code`, CAST(`CONTEXT_ID` AS INT) AS `BUSINESS_ID`, CAST(`CONTEXT_NAME` AS STRING) AS `CONTEXT_NAME`, CAST(`Allergy Concept Type` AS INT) AS `Allergy Concept Type`, CAST(`Allergy ID` AS INT) AS `Allergy ID`, CAST(`CONTEXT_PARENTCONTEXTID` AS INT) AS `CONTEXT_PARENTCONTEXTID`, CAST(`Allergy Name` AS STRING) AS `Allergy Name`, CAST(`Allergy Reaction Name` AS STRING) AS `Allergy Reaction Name`, CAST(`Type` AS STRING) AS `Type`, CAST(`Patient ID` AS INT) AS `Patient ID`, CAST(`Chart ID` AS INT) AS `Chart ID`, CAST(`Created By` AS STRING) AS `Created By`, CAST(`Created Datetime` AS TIMESTAMP) AS `Created Datetime`, CAST(`RxNorm Code` AS STRING) AS `RxNorm Code`, CAST(`Deactivated By` AS STRING) AS `Deactivated By`, CAST(`Deactivated Datetime` AS TIMESTAMP) AS `Deactivated Datetime`, CAST(`Deleted By` AS STRING) AS `Deleted By`, CAST(`Onset Date` AS DATE) AS `Onset Date`, CAST(`Deleted Datetime` AS TIMESTAMP) AS `Deleted Datetime`, CAST(`Note` AS STRING) AS `Note`, CAST(`Reactivated By` AS STRING) AS `Reactivated By`, CAST(`Reactivated Datetime` AS TIMESTAMP) AS `Reactivated Datetime`, CAST(1 AS INT) AS `ETLBatchID`, CAST('2025-01-01 10:00:00' AS TIMESTAMP) AS `ETLBatchTS`, CAST('2025-01-01 10:00:00' AS TIMESTAMP) AS `ETLOriginalTS`, CAST('' AS STRING) AS `Last Modified By`, CAST('' AS TIMESTAMP) AS `Last Modified Datetime` FROM Bronze.Athena_Allergy;
# MAGIC Merge Query:  
# MAGIC         MERGE INTO Silver.Allergy t
# MAGIC         USING silver_update s
# MAGIC         ON t.`BUSINESS_ID` = s.`BUSINESS_ID` AND t.`Allergy ID` = s.`Allergy ID` AND t.`Type` = s.`Type`
# MAGIC         WHEN MATCHED THEN
# MAGIC         UPDATE SET
# MAGIC             t.`Allergy Code` = s.`Allergy Code`, t.`BUSINESS_ID` = s.`BUSINESS_ID`, t.`CONTEXT_NAME` = s.`CONTEXT_NAME`, t.`Allergy Concept Type` = s.`Allergy Concept Type`, t.`Allergy ID` = s.`Allergy ID`, t.`CONTEXT_PARENTCONTEXTID` = s.`CONTEXT_PARENTCONTEXTID`, t.`Allergy Name` = s.`Allergy Name`, t.`Allergy Reaction Name` = s.`Allergy Reaction Name`, t.`Type` = s.`Type`, t.`Patient ID` = s.`Patient ID`, t.`Chart ID` = s.`Chart ID`, t.`Created By` = s.`Created By`, t.`Created Datetime` = s.`Created Datetime`, t.`RxNorm Code` = s.`RxNorm Code`, t.`Deactivated By` = s.`Deactivated By`, t.`Deactivated Datetime` = s.`Deactivated Datetime`, t.`Deleted By` = s.`Deleted By`, t.`Onset Date` = s.`Onset Date`, t.`Deleted Datetime` = s.`Deleted Datetime`, t.`Note` = s.`Note`, t.`Reactivated By` = s.`Reactivated By`, t.`Reactivated Datetime` = s.`Reactivated Datetime`, t.`ETLBatchID` = s.`ETLBatchID`, t.`ETLBatchTS` = s.`ETLBatchTS`, t.`ETLOriginalTS` = s.`ETLOriginalTS`, t.`Last Modified By` = s.`Last Modified By`, t.`Last Modified Datetime` = s.`Last Modified Datetime`
# MAGIC         WHEN NOT MATCHED THEN
# MAGIC         INSERT (`Allergy Code`, `BUSINESS_ID`, `CONTEXT_NAME`, `Allergy Concept Type`, `Allergy ID`, `CONTEXT_PARENTCONTEXTID`, `Allergy Name`, `Allergy Reaction Name`, `Type`, `Patient ID`, `Chart ID`, `Created By`, `Created Datetime`, `RxNorm Code`, `Deactivated By`, `Deactivated Datetime`, `Deleted By`, `Onset Date`, `Deleted Datetime`, `Note`, `Reactivated By`, `Reactivated Datetime`, `ETLBatchID`, `ETLBatchTS`, `ETLOriginalTS`, `Last Modified By`, `Last Modified Datetime`)
# MAGIC         VALUES (s.`Allergy Code`, s.`BUSINESS_ID`, s.`CONTEXT_NAME`, s.`Allergy Concept Type`, s.`Allergy ID`, s.`CONTEXT_PARENTCONTEXTID`, s.`Allergy Name`, s.`Allergy Reaction Name`, s.`Type`, s.`Patient ID`, s.`Chart ID`, s.`Created By`, s.`Created Datetime`, s.`RxNorm Code`, s.`Deactivated By`, s.`Deactivated Datetime`, s.`Deleted By`, s.`Onset Date`, s.`Deleted Datetime`, s.`Note`, s.`Reactivated By`, s.`Reactivated Datetime`, s.`ETLBatchID`, s.`ETLBatchTS`, s.`ETLOriginalTS`, s.`Last Modified By`, s.`Last Modified Datetime`);
# MAGIC         
# MAGIC Out of 0 records in Bronze.Athena_Allergy, 0 were inserted into the Silver.Allergy using Incremental load from EMR Athena.

# COMMAND ----------

# MAGIC %md
# MAGIC None
# MAGIC Query for silver_update:  CREATE OR REPLACE TEMP VIEW silver_update AS SELECT CAST(`Appointment Type Class` AS STRING) AS `Appointment Type Class`, CAST(`BUSINESS_ID` AS INT) AS `BUSINESS_ID`, CAST(`CONTEXT_NAME` AS STRING) AS `CONTEXT_NAME`, CAST(`Appointment Type ID` AS INT) AS `Appointment Type ID`, CAST(`Appointment Type Name` AS STRING) AS `Appointment Type Name`, CAST(`CONTEXT_PARENTCONTEXTID` AS INT) AS `CONTEXT_PARENTCONTEXTID`, CAST(`Appointment Type Short Name` AS STRING) AS `Appointment Type Short Name`, CAST(`Canonical Appointment Type ID` AS INT) AS `Canonical Appointment Type ID`, CAST(`Duration` AS INT) AS `Duration`, CAST(`Patient YN` AS STRING) AS `Patient YN`, CAST(`Created By` AS STRING) AS `Created By`, CAST(`Created Datetime` AS TIMESTAMP) AS `Created Datetime`, CAST(`Deleted By` AS STRING) AS `Deleted By`, CAST(`Deleted Datetime` AS TIMESTAMP) AS `Deleted Datetime`, CAST(1 AS INT) AS `ETLBatchID`, CAST('2025-01-01 10:00:00' AS STRING) AS `ETLBatchTS`, CAST('2025-01-01 10:00:00' AS STRING) AS `ETLOriginalTS` FROM Bronze.Athena_appointmenttype;
# MAGIC Target table Silver.AppointmentType does not exist. Creating it...
# MAGIC Create Table Query:  
# MAGIC             CREATE TABLE Silver.AppointmentType (
# MAGIC                 `Appointment Type Class` STRING, `BUSINESS_ID` INT, `CONTEXT_NAME` STRING, `Appointment Type ID` INT, `Appointment Type Name` STRING, `CONTEXT_PARENTCONTEXTID` INT, `Appointment Type Short Name` STRING, `Canonical Appointment Type ID` INT, `Duration` INT, `Patient YN` STRING, `Created By` STRING, `Created Datetime` TIMESTAMP, `Deleted By` STRING, `Deleted Datetime` TIMESTAMP, `ETLBatchID` INT, `ETLBatchTS` STRING, `ETLOriginalTS` STRING
# MAGIC             ) 
# MAGIC             USING DELTA
# MAGIC             TBLPROPERTIES (
# MAGIC                 'delta.columnMapping.mode' = 'name'
# MAGIC             )
# MAGIC         
# MAGIC Target table Silver.AppointmentType created successfully.
# MAGIC Merge Query:  
# MAGIC         MERGE INTO Silver.AppointmentType t
# MAGIC         USING silver_update s
# MAGIC         ON t.`BUSINESS_ID` = s.`BUSINESS_ID` AND t.`Appointment Type ID` = s.`Appointment Type ID`
# MAGIC         WHEN MATCHED THEN
# MAGIC         UPDATE SET
# MAGIC             t.`Appointment Type Class` = s.`Appointment Type Class`, t.`BUSINESS_ID` = s.`BUSINESS_ID`, t.`CONTEXT_NAME` = s.`CONTEXT_NAME`, t.`Appointment Type ID` = s.`Appointment Type ID`, t.`Appointment Type Name` = s.`Appointment Type Name`, t.`CONTEXT_PARENTCONTEXTID` = s.`CONTEXT_PARENTCONTEXTID`, t.`Appointment Type Short Name` = s.`Appointment Type Short Name`, t.`Canonical Appointment Type ID` = s.`Canonical Appointment Type ID`, t.`Duration` = s.`Duration`, t.`Patient YN` = s.`Patient YN`, t.`Created By` = s.`Created By`, t.`Created Datetime` = s.`Created Datetime`, t.`Deleted By` = s.`Deleted By`, t.`Deleted Datetime` = s.`Deleted Datetime`, t.`ETLBatchID` = s.`ETLBatchID`, t.`ETLBatchTS` = s.`ETLBatchTS`, t.`ETLOriginalTS` = s.`ETLOriginalTS`
# MAGIC         WHEN NOT MATCHED THEN
# MAGIC         INSERT (`Appointment Type Class`, `BUSINESS_ID`, `CONTEXT_NAME`, `Appointment Type ID`, `Appointment Type Name`, `CONTEXT_PARENTCONTEXTID`, `Appointment Type Short Name`, `Canonical Appointment Type ID`, `Duration`, `Patient YN`, `Created By`, `Created Datetime`, `Deleted By`, `Deleted Datetime`, `ETLBatchID`, `ETLBatchTS`, `ETLOriginalTS`)
# MAGIC         VALUES (s.`Appointment Type Class`, s.`BUSINESS_ID`, s.`CONTEXT_NAME`, s.`Appointment Type ID`, s.`Appointment Type Name`, s.`CONTEXT_PARENTCONTEXTID`, s.`Appointment Type Short Name`, s.`Canonical Appointment Type ID`, s.`Duration`, s.`Patient YN`, s.`Created By`, s.`Created Datetime`, s.`Deleted By`, s.`Deleted Datetime`, s.`ETLBatchID`, s.`ETLBatchTS`, s.`ETLOriginalTS`);
# MAGIC         
# MAGIC Out of 0 records in Bronze.Athena_appointmenttype, 0 were inserted into the Silver.AppointmentType using Incremental load from EMR Athena.

# COMMAND ----------

# MAGIC %md
# MAGIC None
# MAGIC Error executing notebook for table AppointmentType: Notebook not found for table: AppointmentType

# COMMAND ----------

bronze_to_silver_control_table('AppointmentType')

# COMMAND ----------


