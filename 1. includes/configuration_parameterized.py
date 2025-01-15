# Databricks notebook source
#importing all libraries
from pyspark.sql.types import *
from pyspark.sql.functions import *
from delta.tables import DeltaTable
from datetime import datetime

# COMMAND ----------

# Access keys
spark.conf.set(
    "fs.azure.account.key.adlsgen2mdftest.dfs.core.windows.net",
    "Gjtm4uzocY6ZMlWkA/nXyDFDGm1qUsoWbq6FrK8rGC6yqgUXprByiPkTxmbD6qgpEvHSbHho6JsM+AStgx0pjg==")

# COMMAND ----------

#catalog n

v_catalog_name = "revenue_cycle_dev"

# container names
v_metadata_container = "metadata"
v_landing_container = "landing"
v_bronze_container = "bronze-data"
v_silver_container = "silver"
v_gold_container = "gold"


# schema names

v_metadata_schema = "metadata"
v_landing_schema = "landing"
v_bronze_schema = "bronze-data"
v_silver_schema = "silver"
v_gold_schema = "gold"

#control table variables
v_storage_account_name = "adlsgen2mdftest"
v_container_name = "control-tables"
v_metadata_directory = "metadata" #needs to be replaced by above
v_landing_layer_control_table_file_name = "landing_layer_control_table.csv"
v_bronze_layer_control_table_file_name = "bronze_layer_control_table.csv"
v_silver_layer_control_table_file_name = "silver_layer_control_table.csv"
v_gold_layer_control_table_file_name = "gold_layer_control_table.csv"
v_connection_layer_control_file_name = "connection_layer_control_table.csv"
v_practice_key_layer_control_file_name = "pracetice_key_layer_control_table.csv"


#control table names

v_landing_layer_control_table_name = "landing_layer_control_table"
v_bronze_layer_control_table_name = "bronze_layer_control_table"
v_silver_layer_control_table_name = "silver_layer_control_table"
v_gold_layer_control_table_name = "gold_layer_control_table"
v_connection_layer_control_name = "connection_layer_control_table"
v_practice_key_layer_control_name = "pracetice_key_layer_control_table"




# COMMAND ----------

# File Paths

v_landing_control_csv_path = f"abfss://{v_container_name}@{v_storage_account_name}.dfs.core.windows.net/{v_metadata_directory}/{v_landing_layer_control_table_file_name}"

v_bronze_layer_control_csv_path = f"abfss://{v_container_name}@{v_storage_account_name}.dfs.core.windows.net/{v_metadata_directory}/{v_bronze_layer_control_table_file_name}"


v_silver_layer_control_csv_path = f"abfss://{v_container_name}@{v_storage_account_name}.dfs.core.windows.net/{v_metadata_directory}/{v_silver_layer_control_table_file_name}"


v_gold_layer_control_csv_path = f"abfss://{v_container_name}@{v_storage_account_name}.dfs.core.windows.net/{v_metadata_directory}/{v_gold_layer_control_table_file_name}"


v_connection_layer_control_csv_path = f"abfss://{v_container_name}@{v_storage_account_name}.dfs.core.windows.net/{v_metadata_directory}/{v_connection_layer_control_file_name}"

v_practice_key_layer_control_csv_path = f"abfss://{v_container_name}@{v_storage_account_name}.dfs.core.windows.net/{v_metadata_directory}/{v_practice_key_layer_control_file_name}"



# COMMAND ----------



# COMMAND ----------

# Storage Account and Container Details of the Control Tables and Schemas

# storage_account_name = "adlsgen2mdftest"
# container_name = "control-tables"
# folder_path = "metadata"

# File Paths
# bronze_layer_control_csv_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/{folder_path}/bronze_layer_control_table.csv"
# landing_to_bronze_control_csv_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/{folder_path}/landing_to_bronze_control_table.csv"
# meta_source_to_target_csv_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/{folder_path}/meta_source_to_target_table.csv"
# silver_layer_control_csv_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/{folder_path}/silver_layer_control_table.csv"
# source_to_landing_control_csv_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/{folder_path}/source_to_landing_control_table.csv"
# source_to_landing_control_table_path = f"abfss://metadata@{storage_account_name}.dfs.core.windows.net/source_to_landing/"
# # Print Paths for Verification
# print("Bronze Layer Control Table Path:", bronze_layer_control_csv_path)
# print("Landing to Bronze Control Table Path:", landing_to_bronze_control_csv_path)
# print("Meta Source to Target Table Path:", meta_source_to_target_csv_path)
# print("Silver Layer Control Table Path:", silver_layer_control_csv_path)
# print("Source to Landing Control Table Path:", source_to_landing_control_csv_path)

# COMMAND ----------



# COMMAND ----------

# bronze_storage_account_name = "adlsgen2mdftest"
# bronze_container_name = "control-tables"
# bronze_folder_path = "metadata"
# bronze_file_name = "bronze_layer_control_table.csv"

# # Metadata Table Path
# bronze_control_table_path = f"abfss://{bronze_container_name}@{bronze_storage_account_name}.dfs.core.windows.net/{bronze_folder_path}/{bronze_file_name}"

# COMMAND ----------

# silver_storage_account_name = "adlsgen2mdftest"
# silver_container_name = "control-tables"
# silver_folder_path = "metadata"
# silver_file_name = "silver_layer_control_table.csv"
# silver_control_table_path = f"abfss://{silver_container_name}@{silver_storage_account_name}.dfs.core.windows.net/{silver_folder_path}/{silver_file_name}"

# COMMAND ----------

# # silver_storage_account_name = "adlsgen2mdftest"
# # silver_container_name = "control-tables"
# # silver_folder_path = "metadata"
# meta_source_to_target_table_file_name = "meta_source_to_target_table.csv"
# meta_source_to_target_table_path = f"abfss://{silver_container_name}@{silver_storage_account_name}.dfs.core.windows.net/{silver_folder_path}/{meta_source_to_target_table_file_name}"

# COMMAND ----------


# bronze_storage_account_name = "adlsgen2mdftest"
# bronze_container_name = "bronze-data"
# bronze_folder_path = "bronze"
# athena_allregy_table_file_name = "athena_allergy.csv"
# athena_allregy_table_path = f"abfss://{bronze_container_name}@{bronze_storage_account_name}.dfs.core.windows.net/{bronze_folder_path}/{athena_allregy_table_file_name}"

# COMMAND ----------

# # Details for connection with ODBC
# # Set up parameters
# server_name = "server-p.database.windows.net"
# database_name = "rco_stage"
# username = "Preet@server-p"  # Use the correct username format
# password = "ABCabc123@"  # Replace with your password
# # table_name = "ehr_Allergies"  # Replace with your schema and table name
# # JDBC URL
# url = f"jdbc:sqlserver://{server_name}:1433;database={database_name};user={username};password={password};encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;"

# COMMAND ----------

# # Variables for source_to_landing_control_table

# current_date_snake_format = datetime.now().strftime('%Y_%m_%d')
# current_date_str = datetime.now().strftime('%Y-%m-%d')
# current_date = datetime.strptime(current_date_str, "%Y-%m-%d").date()

# COMMAND ----------


