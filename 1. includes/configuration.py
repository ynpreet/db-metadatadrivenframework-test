# Databricks notebook source
spark.conf.set(
    "fs.azure.account.key.adlsgen2mdftest.dfs.core.windows.net",
    "Gjtm4uzocY6ZMlWkA/nXyDFDGm1qUsoWbq6FrK8rGC6yqgUXprByiPkTxmbD6qgpEvHSbHho6JsM+AStgx0pjg==")

# COMMAND ----------

# Storage Account and Container Details
storage_account_name = "adlsgen2mdftest"
container_name = "control-tables"
folder_path = "metadata"

# File Paths
bronze_layer_control_csv_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/{folder_path}/bronze_layer_control_table.csv"
landing_to_bronze_control_csv_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/{folder_path}/landing_to_bronze_control_table.csv"
meta_source_to_target_csv_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/{folder_path}/meta_source_to_target_table.csv"
silver_layer_control_csv_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/{folder_path}/silver_layer_control_table.csv"
source_to_landing_control_csv_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/{folder_path}/source_to_landing_control_table.csv"
source_to_landing_control_table_path = f"abfss://metadata@{storage_account_name}.dfs.core.windows.net/source_to_landing/"
# Print Paths for Verification
print("Bronze Layer Control Table Path:", bronze_layer_control_csv_path)
print("Landing to Bronze Control Table Path:", landing_to_bronze_control_csv_path)
print("Meta Source to Target Table Path:", meta_source_to_target_csv_path)
print("Silver Layer Control Table Path:", silver_layer_control_csv_path)
print("Source to Landing Control Table Path:", source_to_landing_control_csv_path)

# COMMAND ----------

bronze_storage_account_name = "adlsgen2mdftest"
bronze_container_name = "control-tables"
bronze_folder_path = "metadata"
bronze_file_name = "bronze_layer_control_table.csv"

# Metadata Table Path
bronze_control_table_path = f"abfss://{bronze_container_name}@{bronze_storage_account_name}.dfs.core.windows.net/{bronze_folder_path}/{bronze_file_name}"

# COMMAND ----------

silver_storage_account_name = "adlsgen2mdftest"
silver_container_name = "control-tables"
silver_folder_path = "metadata"
silver_file_name = "silver_layer_control_table.csv"
silver_control_table_path = f"abfss://{silver_container_name}@{silver_storage_account_name}.dfs.core.windows.net/{silver_folder_path}/{silver_file_name}"

# COMMAND ----------

# silver_storage_account_name = "adlsgen2mdftest"
# silver_container_name = "control-tables"
# silver_folder_path = "metadata"
meta_source_to_target_table_file_name = "meta_source_to_target_table.csv"
meta_source_to_target_table_path = f"abfss://{silver_container_name}@{silver_storage_account_name}.dfs.core.windows.net/{silver_folder_path}/{meta_source_to_target_table_file_name}"

# COMMAND ----------


bronze_storage_account_name = "adlsgen2mdftest"
bronze_container_name = "bronze-data"
bronze_folder_path = "bronze"
athena_allregy_table_file_name = "athena_allergy.csv"
athena_allregy_table_path = f"abfss://{bronze_container_name}@{bronze_storage_account_name}.dfs.core.windows.net/{bronze_folder_path}/{athena_allregy_table_file_name}"

# COMMAND ----------

#importing all libraries
from pyspark.sql.types import *
from pyspark.sql.functions import *
from delta.tables import DeltaTable

# COMMAND ----------


