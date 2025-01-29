from pyspark.sql import functions as F
from pyspark.sql.types import *

def create_bronze_tables_from_landing(u_table):

    # Datatype mapping
    data_type_mapping = {
        "int": IntegerType(),
        "varchar": StringType(),
        "date": DateType(),
        "timestamp": TimestampType(),
        "datetime": TimestampType(),
        "real": FloatType(),
        "numeric": DecimalType(),
        "bigint": LongType(),
        "smallint": ShortType(),
        "float": FloatType(),
        "bit": BooleanType(),
        "decimal": DecimalType(),
        "char": StringType(),
        "smallmoney": DecimalType(),
        "tinyint": ShortType(),
        "money": DecimalType(),
        "nvarchar": StringType()
    }

    # Read Bronze control table
    metadata_df = spark.sql(f"""
        SELECT * FROM {v_catalog_name}.{v_metadata_schema}.{v_bronze_layer_control_table_name}
        WHERE is_active = 'TRUE'  
        AND layer_name = '{v_landing_to_bronze}'
        AND target_table_name = '{u_table}' 
        AND emr = '{w_emr}'
    """)

    # Business logic check
    l_business_logic = metadata_df.select("business_logic").first()["business_logic"]
    if str(l_business_logic).strip().lower() == 'true':
        print("This function cannot be executed for tables with business logic.")
        return  

    schema_columns = metadata_df.select(
        "source_column_name", "target_column_name", "target_data_type",
        "target_is_nullable", "is_primary_key", "partition_by"
    ).filter(metadata_df.source_column_name.isNotNull()).collect()

    audit_columns = metadata_df.select("target_column_name", "target_data_type", "target_is_nullable").filter(metadata_df.source_column_name.isNull()).collect()

    # Extract metadata values
    l_source_table_name = metadata_df.select("source_table_name").first()["source_table_name"]
    l_target_schema = metadata_df.select("target_schema").first()["target_schema"]
    l_target_table_name = metadata_df.select("target_table_name").first()["target_table_name"]
    l_load_type = metadata_df.select("load_type").first()["load_type"]

    # File paths
    l_source_dir_path = f"abfss://{v_landing_container}@{v_storage_account_name}.dfs.core.windows.net/{l_target_schema}/"
    l_source_table_path = f"{l_source_dir_path}/{l_source_table_name}*.parquet"

    if not file_exists(l_source_table_path):
        print(f"File not found: {l_source_table_path}")
        return

    # Read parquet data
    df = spark.read.parquet(l_source_table_path)

    # Process schema and rename columns
    for col_info in schema_columns:
        source_col = col_info["source_column_name"]
        target_col = col_info["target_column_name"]
        data_type = col_info["target_data_type"].split('(')[0]
        
        df = df.withColumn(source_col, df[source_col].cast(data_type_mapping[data_type])).withColumnRenamed(source_col, target_col)

    # Adding audit columns
    for col_info in audit_columns:
        target_col = col_info["target_column_name"]
        if target_col == 'insert_at':
            df = df.withColumn(target_col, F.current_timestamp())
        else:
            df = df.withColumn(target_col, F.lit(None))

    # Primary key and partitioning logic
    primary_keys = [col_info["target_column_name"] for col_info in schema_columns if col_info["is_primary_key"] == 'true']
    partition_cols = [col_info["target_column_name"] for col_info in schema_columns if col_info["partition_by"] == 'true']

    table_exists = check_table_exists(f'{v_catalog_name}.{v_bronze_schema}.{l_target_table_name}')

    if not table_exists:
        df.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable(f"{v_catalog_name}.{v_bronze_schema}.{l_target_table_name}")
        print(f"Created table {v_catalog_name}.{v_bronze_schema}.{l_target_table_name}.")

    elif l_load_type.lower() == 'incremental':
        df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(f"{v_catalog_name}.{v_bronze_schema}.{l_target_table_name}")
        print(f"Added incremental records to {v_catalog_name}.{v_bronze_schema}.{l_target_table_name}.")

    elif l_load_type.lower() == 'full':
        df.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable(f"{v_catalog_name}.{v_bronze_schema}.{l_target_table_name}")
        print(f"Overwritten full records in {v_catalog_name}.{v_bronze_schema}.{l_target_table_name}.")

    elif l_load_type.lower() == 'upsert':
        # Create temporary view
        df.createOrReplaceTempView("bronze")

        # Generate join condition using primary keys
        join_condition = " AND ".join([f"target.{pk} = source.{pk}" for pk in primary_keys])

        # Merge query
        merge_query = f"""
        MERGE INTO {v_catalog_name}.{v_bronze_schema}.{l_target_table_name} AS target
        USING bronze AS source
        ON {join_condition}
        WHEN MATCHED THEN 
            UPDATE SET *
        WHEN NOT MATCHED THEN 
            INSERT * 
        """
        spark.sql(merge_query)
        print(f"Upsert operation completed for {v_catalog_name}.{v_bronze_schema}.{l_target_table_name}.")
