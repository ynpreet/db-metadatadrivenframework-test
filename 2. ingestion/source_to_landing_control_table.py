# Databricks notebook source
# MAGIC %run "../1. includes/configuration"

# COMMAND ----------

from pyspark.sql.functions import to_date, lit

from datetime import datetime


# COMMAND ----------

def check_data_existence_in_odbc(tab_name):
    server_name = "server-p.database.windows.net"
    database_name = "rco_stage"
    username = "Preet@server-p"  # Use the correct username format
    password = "ABCabc123@"  # Replace with your password
    connection_properties = {
    "user": "Preet@server-p",
    "password": "ABCabc123@",
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    }

    # table_name = "ehr_Allergies"  # Replace with your schema and table name
    # JDBC URL
    url = f"jdbc:sqlserver://{server_name}:1433;database={database_name};user={username};password={password};encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;"
    query = f""" SELECT TABLE_NAME 
                FROM INFORMATION_SCHEMA.TABLES 
                WHERE TABLE_NAME = '{tab_name}'
                """
    df = spark.read.jdbc(url, f"({query}) as table_check", properties=connection_properties)
    # Check if the DataFrame is empty
    if df.count() > 0:
        return True
    else:
        return False


# COMMAND ----------

# def remove_files_from_adls(adls_path):
#     try:
#         dbutils.fs.rm(f"{adls_path}", True)
#         print(f"Cleaned up non-Delta files")
#     except Exception as cleanup_error:
#         print(f"Failed to clean up non-Delta: {str(cleanup_error)}")

# COMMAND ----------

def remove_files_from_adls(adls_path):
    """
    Removes files from the specified ADLS path.

    Parameters:
    adls_path (str): The ADLS path from which files need to be removed.
    """
    try:
        # Validate the path
        if not adls_path.startswith("abfss://"):
            raise ValueError(f"Invalid ADLS path: {adls_path}")

        # Remove the files
        dbutils.fs.rm(adls_path, True)
        print(f"Successfully cleaned up non-Delta files at: {adls_path}")
    except Exception as cleanup_error:
        print(f"Failed to clean up non-Delta files at {adls_path}: {str(cleanup_error)}")

# COMMAND ----------

from datetime import datetime

# Set up parameters
server_name = "server-p.database.windows.net"
database_name = "rco_stage"
username = "Preet@server-p"  # Use the correct username format
password = "ABCabc123@"  # Replace with your password
# table_name = "ehr_Allergies"  # Replace with your schema and table name
# JDBC URL
url = f"jdbc:sqlserver://{server_name}:1433;database={database_name};user={username};password={password};encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;"
# Get the current date

storage_container = "adlsgen2mdftest"
directory = "landing"
current_date_snake_format = datetime.now().strftime('%Y_%m_%d')
current_date_str = datetime.now().strftime('%Y-%m-%d')
current_date = datetime.strptime(current_date_str, "%Y-%m-%d").date()
print(type(current_date))  # Output: <class 'datetime.date'>
print(current_date)        # Output: 2025-01-09


# Define the ADLS Gen2 path to save the results




# Replace with your ADLS Gen2 path



# COMMAND ----------

dbutils.widgets.text("p_EMR", "")
v_EMR  = dbutils.widgets.get("p_EMR")

# COMMAND ----------

dbutils.widgets.text("p_Practice_ETL_ID", "")
v_Practice_ETL_ID  = dbutils.widgets.get("p_Practice_ETL_ID")

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

spark.sql(f"""
DROP TABLE IF EXISTS revenue_cycle_dev.metadata.source_to_landing_control_table;
""")

spark.sql(f"""
CREATE TABLE revenue_cycle_dev.metadata.source_to_landing_control_table
USING DELTA
LOCATION '{source_to_landing_control_table_path}';
""")

# COMMAND ----------

source_to_landing_control_table_df = spark.read.format("csv").load(source_to_landing_control_csv_path,header="True")

source_to_landing_control_table_df.write \
  .mode("overwrite") \
  .option("overwriteSchema", "true") \
      .option("header", "true") \
  .option("path", 'abfss://metadata@adlsgen2mdftest.dfs.core.windows.net/source_to_landing/') \
  .saveAsTable("revenue_cycle_dev.metadata.source_to_landing_control_table") # External table

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE revenue_cycle_dev.metadata.source_to_landing_control_table SET last_modified_date = DATE('2025-01-06'); -- only for debugging purpose

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from revenue_cycle_dev.metadata.source_to_landing_control_table -- only for debugging purpose

# COMMAND ----------

# yet to add number of records inserted -- Pending
# yet to add logic for if functions runs twice on same day. Ideally it should not execute for the same table again

def source_to_landing_control_table(user_emr):

    from datetime import datetime
    metadata_df = spark.sql(f"""select * from revenue_cycle_dev.metadata.source_to_landing_control_table 
    where emr = '{user_emr}'
    -- and source_file_name = "ehr_Allergies"  -- for debugging
    and last_modified_date < Date('{current_date}') -- This helps if function is executed twice
    and is_active = 1
    and frequency = 'Daily' -- Logic for Monthly, Weekly???
    -- and load_type == 'full' -- for debugging
    ;""")


    # Logging stuff
    v_ETLBatchID = 1 #Do not hard code anything
    v_ETLBatchTS = "2025-01-01 10:00:00" #Do not hard code anything
    v_ETLOriginalTS = "2025-01-01 10:00:00" #Do not hard code anything
    v_practice = "SKI" #Do not hard code anything
    layer_name = 'landing_to_bronze' #Do not hard code anything

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


    source_file_name_col = metadata_df.select("source_file_name").collect()
    attributes_name_col = metadata_df.select("attributes_name").collect()
    incremental_key_col = metadata_df.select("incremental_key").collect()
    destination_file_path_col = metadata_df.select("destination_file_path").collect()
    load_type_col = metadata_df.select("load_type").collect()
    destination_file_format_col = metadata_df.select("destination_file_format").collect()
    last_modified_date_col = metadata_df.select("last_modified_date").collect()
    business_logic = metadata_df.select("business_logic").collect()



    for i in range(metadata_df.count()):
        source_file_name=source_file_name_col[i][0]

        from datetime import datetime
        import pytz
        mst = pytz.timezone('MST')
        start_time = datetime.now(mst)

    # Insert initial log entry
        spark.sql(f"""
            INSERT INTO logging.log_merge (layer_name, TableName, Practice, ETLBatchID, ETLBatchTS, StartTime)
            VALUES ('{layer_name}','{source_file_name}', '{v_Practice_ETL_ID}', {v_ETLBatchID}, '{v_ETLBatchTS}', '{start_time}')
        """)

        if check_data_existence_in_odbc(str(source_file_name)) == False:
            print(f"Table {source_file_name} does not exist")

            end_time = datetime.now(mst)
            spark.sql(f"""
                UPDATE logging.log_merge
                SET EndTime = '{end_time}'
                WHERE TableName = '{source_file_name}' AND StartTime = '{start_time}'
            """)
            # Pending: add code to update this in log table
            continue
        
        if business_logic == 1:
            continue # Skipping as of now for business logic = 1


        attributes_name=attributes_name_col[i][0]
        incremental_key=incremental_key_col[i][0]
        destination_file_path=destination_file_path_col[i][0]
        load_type=load_type_col[i][0]
        destination_file_path = destination_file_path.replace('_YYYY_MM_DD', '/'+current_date_snake_format)
        destination_file_path = destination_file_path.replace('Practice_ETL_ID', v_Practice_ETL_ID)
        last_modified_date = last_modified_date_col[i][0]
        destination_file_format = destination_file_format_col[i][0]    
        # incremental_value = last_modified_date  # Value to filter rows
        # Construct the SQL query dynamically

        #loading the data
        if destination_file_format == 'snappy.parquet':
            file_format = "parquet"
            compression = "snappy"
        elif destination_file_format == 'csv':
            file_format = "csv"
            compression = "gzip"


        query_max_last_updated_date = f"SELECT max({incremental_key}) as max_last_updated_date FROM {source_file_name} "
        adls_path = f'abfss://{directory}@{storage_container}.dfs.core.windows.net/{destination_file_path}'
        print("adls_path:",adls_path)
        
        if load_type == 'incremental':
            query = f"SELECT {attributes_name} FROM {source_file_name} where {incremental_key} > '{last_modified_date}'"

            table_for_incremental_load = (spark.read
                        .format("jdbc")
                        .option("url", url)  # Provide the JDBC URL
                        .option("query", query)  # Pass the dynamically constructed query
                        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")  # Specify the JDBC driver
                        .load()
                        )
            max_last_updated_date = (spark.read
                        .format("jdbc")
                        .option("url", url)  # Provide the JDBC URL
                        .option("query", query_max_last_updated_date)  # Pass the dynamically constructed query
                        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")  # Specify the JDBC driver
                        .load()
                        )
            
            max_last_updated_date = max_last_updated_date.collect()[0][0]
            max_last_updated_date_str = max_last_updated_date.strftime('%Y-%m-%d')
            
            
            #loading the data
            table_for_incremental_load.write.mode("overwrite").format(file_format).option("compression", compression).save(adls_path)
                    
            

        elif load_type == 'full':
            remove_files_from_adls(adls_path)
            #     dbutils.fs.rm(f"{adls_path}", True)
            #     print(f"Cleaned up non-Delta files for table: {source_file_name}")
            # except Exception as cleanup_error:
            #     print(f"Failed to clean up non-Delta files for {source_file_name}: {str(cleanup_error)}")
            query = f"SELECT {attributes_name} FROM {source_file_name}"

            table_for_full_load = (spark.read
                        .format("jdbc")
                        .option("url", url)  # Provide the JDBC URL
                        .option("query", query)  # Pass the dynamically constructed query
                        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")  # Specify the JDBC driver
                        .load()
                        )
            
            table_for_full_load.write.mode("overwrite").format(file_format).option("compression", compression).save(adls_path)

        spark.sql(f"UPDATE revenue_cycle_dev.metadata.source_to_landing_control_table SET last_modified_date = '{max_last_updated_date_str}' where source_file_name = '{source_file_name}' and emr = '{v_EMR}' ") 
            # Log the end time
        end_time = datetime.now(mst)
        
        #logging
        spark.sql(f"""
            UPDATE logging.log_merge
            SET EndTime = '{end_time}'
            WHERE TableName = '{source_file_name}' AND StartTime = '{start_time}'
        """)

        print(f"Records from {source_file_name} up to {max_last_updated_date_str} has been successfully written to {adls_path} using {load_type} load")
            
        

# COMMAND ----------

source_to_landing_control_table('AdvMD')

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from logging.log_merge

# COMMAND ----------

# MAGIC %md
# MAGIC 1. File extension
# MAGIC 2. If table doesnt exist
# MAGIC 3. Least data scanning
# MAGIC 4. What if it runs twice
# MAGIC 5. is active
# MAGIC 6. load type
# MAGIC 7. attribute count DQ check
# MAGIC 8. frequency - daily
# MAGIC 9. Business logic

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

