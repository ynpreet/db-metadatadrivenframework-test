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

    #Read Bronze control table
    metadata_df = spark.sql(f"""
        SELECT * FROM {v_catalog_name}.{v_metadata_schema}.{v_bronze_layer_control_table_name}
        WHERE is_active = 'TRUE'  and layer_name = '{v_landing_to_bronze}'
        and target_table_name = '{u_table}' and emr = '{w_emr}'
    """)

    #Business logic check
    l_business_logic = metadata_df.select("business_logic").first()["business_logic"]
    if str(l_business_logic).strip().lower() == 'true':
        print("This function can not be executed for tables with business logic")
        return  # Exit the function after notebook execution

    schema_columns = metadata_df.select("source_column_name", "target_column_name", "target_data_type","target_is_nullable","is_primary_key","partition_by").filter(metadata_df.source_column_name.isNotNull())
    schema_columns_list = schema_columns.collect()

    l_source_table_name = metadata_df.select("source_table_name").first()["source_table_name"]


    l_source_schema = metadata_df.select("source_schema").first()["source_schema"]
    l_source_schema = l_source_schema.replace('emr',w_emr)
    l_source_schema = l_source_schema.replace('Practice_ETL_ID',w_practice_etl_id)
    
    l_archive_path = metadata_df.select("archive_path").first()["archive_path"]
    l_archive_path = l_archive_path.replace('emr',w_emr)
    l_archive_path = l_archive_path.replace('Practice_ETL_ID',w_practice_etl_id)



    l_source_table_name = l_source_table_name.replace('Practice_ETL_ID',w_practice_etl_id)
    
    l_source_table_name = l_source_table_name.replace('YYYY-MM-DD', '') # delete this after the pipeline is automated
    # l_source_table_name = l_source_table_name.replace('YYYY-MM-DD',v_current_date_str)

    audit_columns = metadata_df.select("target_column_name", "target_data_type","target_is_nullable").filter(metadata_df.source_column_name.isNull())
    audit_columns_list = audit_columns.collect()

    l_target_schema = metadata_df.select("target_schema").first()["target_schema"]
    l_target_table_name = metadata_df.select("target_table_name").first()["target_table_name"]

    l_load_type = metadata_df.select("load_type").first()["load_type"]
    
    



#=============================== PLEASE DELETE BELOW CODE after the pipelines is automated =================================

    if w_emr.lower() == 'advmd':
        l_source_dir_path = f"abfss://{v_landing_container}@{v_storage_account_name}.dfs.core.windows.net/{l_source_schema}/"
        l_archive_dir_path=f"abfss://{v_archive_container}@{v_storage_account_name}.dfs.core.windows.net/{l_archive_path}/"

    
#=============================== PLEASE DELETE ABOVE CODE after the pipelines is automated =================================    

    if w_emr.lower() == 'athena':
        x = l_target_table_name
        x = x.split('_')
        x[0] = x[0].capitalize()
        x[1] = x[1].upper()
        x = '_'.join(x)
        l_source_schema = l_source_schema.replace('target_table_name',x)
        l_archive_path = l_archive_path.replace('target_table_name',x)

        l_source_dir_path = f"abfss://{v_landing_container}@{v_storage_account_name}.dfs.core.windows.net/{l_source_schema}/"
        l_source_table_name = l_source_table_name.replace('target_table_name',x)
    
    try:
        dbutils.fs.ls(l_source_dir_path)
    except:
        print(f"Table {l_target_table_name} could not be created. Because {l_source_dir_path} does not exist in the {v_landing_container} container")
        spark.sql(f"""
            INSERT INTO {v_catalog_name}.{v_logging_schema}.{v_db_activity_log_table} 
            (practice_etl_id, adf_pipeline_run_id, pipeline_name, emr, target_schema, target_table_name, activity_name, activity_status, record_count, batch_log_id, batch_log_ts, error_detail, error_log)
            VALUES ('{w_practice_etl_id}','{w_adf_pipeline_run_id}', '{v_notebook_name}', '{w_emr}', '{l_target_schema}', '{l_target_table_name}', '{l_load_type}', 'Failed', 0, '{w_etl_batch_id}', '{w_etl_batch_ts}', 'File not found', '{l_target_table_name}')
        """)
        return

    expected_prefix = l_source_table_name
    all_files = [file.name for file in dbutils.fs.ls(l_source_dir_path) if file.name.startswith(expected_prefix)][0]
    source_table_location = f"abfss://{v_landing_container}@{v_storage_account_name}.dfs.core.windows.net/{l_source_schema}/{all_files}"
    l_archive_file_location = f"abfss://{v_archive_container}@{v_storage_account_name}.dfs.core.windows.net/{l_archive_path}/{all_files}"


    #source_table_location = f"abfss://{v_landing_container}@{v_storage_account_name}.dfs.core.windows.net/{l_source_schema}/{l_source_table_name}" #uncomment this once pipeline is automated
   
    # print(source_table_location) # for debugging

    if file_exists(source_table_location) == False:
        print(f"File not found for {all_files}")
        spark.sql(f"""
            INSERT INTO {v_catalog_name}.{v_logging_schema}.{v_db_activity_log_table} 
            (practice_etl_id,adf_pipeline_run_id, pipeline_name, emr, target_schema, target_table_name, activity_name, activity_status, record_count, batch_log_id, batch_log_ts, error_detail, error_log)
            VALUES ('{w_practice_etl_id}','{w_adf_pipeline_run_id}', '{v_notebook_name}', '{w_emr}', '{l_target_schema}', '{l_target_table_name}', '{l_load_type}', 'Failed', 0, '{w_etl_batch_id}', '{w_etl_batch_ts}', 'File not found', '{l_target_table_name}')
        """)
        return

    # try:
    #     df = spark.read.parquet(source_table_location)
    # except Exception as e:
    #     print(f"Error reading parquet file {source_table_location}:" , str(e).split("\n")[0])
    #     spark.sql(f"""
    #         INSERT INTO {v_catalog_name}.{v_logging_schema}.{v_db_activity_log_table} 
    #         (practice_etl_id,adf_pipeline_run_id, pipeline_name, emr, target_schema, target_table_name, activity_name, activity_status, record_count, batch_log_id, batch_log_ts, error_detail, error_log)
    #         VALUES ('{w_practice_etl_id}', '{w_adf_pipeline_run_id}','{v_notebook_name}', '{w_emr}', '{l_target_schema}', '{l_target_table_name}', '{l_load_type}', 'Failed', 0, '{w_etl_batch_id}', '{w_etl_batch_ts}', 'File not found', '{l_target_table_name}')
    #     """)
    #     return
    
    df= spark.read.parquet(source_table_location)


    l_partition_columns_list = []
    l_primary_Key_columns_list = []
    for i in range(len(schema_columns_list)):
        source_column_name = schema_columns_list[i]["source_column_name"]
        target_column_name = schema_columns_list[i]["target_column_name"]
        target_data_type = schema_columns_list[i]["target_data_type"]
        target_is_nullable = schema_columns_list[i]["target_is_nullable"]
        primary_key_column = schema_columns_list[i]["is_primary_key"]
        partition_by_column = schema_columns_list[i]["partition_by"]
        # print(source_column_name, target_column_name, target_data_type, target_is_nullable, primary_key_column, partition_by_column) #for debugging purpose
        if str(target_is_nullable).strip().lower() == 'no':
            if df.filter(df[source_column_name].isNull()).count() > 0:
                # print("The column " + source_column_name + " has"+df.filter(df[source_column_name].isNull()).count()," nulls . So filtering out the null values") #for debugging purpose
                df.filter(df[source_column_name].isNull()).drop()
            # else:
            #     print("The column " + source_column_name + " has no nulls") #For debugging purpose

        if str(partition_by_column).strip().lower() == 'true':
            l_partition_columns_list.append(target_column_name) #creating list of all columns to be paritioned

        if str(primary_key_column).strip().lower() == 'true':
            l_primary_Key_columns_list.append(target_column_name) #creating list of all columns to be paritioned

        df = df.withColumn(source_column_name, df[source_column_name].cast(data_type_mapping[target_data_type.split('(')[0]]))          

        df = df.withColumnRenamed(source_column_name, target_column_name)

    for i in range(len(audit_columns_list)):
        target_column_name = audit_columns_list[i]["target_column_name"]
        target_data_type = audit_columns_list[i]["target_data_type"]
        target_is_nullable = audit_columns_list[i]["target_is_nullable"]
        if target_column_name == 'insert_at':
            df = df.withColumn(target_column_name, F.current_timestamp())
        elif target_column_name == 'file_name':
            df = df.withColumn(target_column_name, F.lit(all_files)) # replace it with source_table_name once its automated
        elif target_column_name == 'etl_batch_id':
            df = df.withColumn(target_column_name, F.lit(w_etl_batch_id)) # replace it with source_table_name once its automated
        elif target_column_name == 'etl_batch_ts':
            df = df.withColumn(target_column_name, F.lit(w_etl_batch_ts)) # replace it with source_table_name once its automated
        elif target_column_name == 'etl_original_ts':
            df = df.withColumn(target_column_name, F.lit(w_etl_batch_ts)) # replace it with source_table_name once its automated
        else:
            df = df.withColumn(target_column_name, F.lit(None))


    #Checking duplicates in table:

    # print(l_primary_Key_columns_list)
    #Uncomment the below code once you have added all primary keys
    # duplicate_count = df.groupBy(*l_primary_Key_columns_list).count().filter("count > 1").count()

    # if duplicate_count > 0:
    #     print(f"Duplicate records found in {l_target_table_name}. Deleting duplicates.")
    #     df = df.dropDuplicates(l_primary_Key_columns_list)
    # else:
    #     print(f"No duplicate records found in {l_target_table_name}.")

    #Check if table already exists
    table_exists = check_table_exists(f'{v_catalog_name}.{v_bronze_schema}.{l_target_table_name}')


    if table_exists:
        print(f"Table {l_target_table_name} already exists.")
        if l_load_type == "incremental":
            # Append data for incremental load
            df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(f"{v_catalog_name}.{v_bronze_schema}.{l_target_table_name}")

            l_record_count = int(df.count())

            
            # Inserting the status in log table
            spark.sql(f"""
                INSERT INTO {v_catalog_name}.{v_logging_schema}.{v_db_activity_log_table} 
                (practice_etl_id,adf_pipeline_run_id, pipeline_name, emr, target_schema, target_table_name, activity_name, activity_status, record_count, batch_log_id, batch_log_ts, error_detail, error_log)
                VALUES ('{w_practice_etl_id}', '{w_adf_pipeline_run_id}','{v_notebook_name}', '{w_emr}', '{l_target_schema}', '{l_target_table_name}', '{l_load_type}', 'Success', {l_record_count}, '{w_etl_batch_id}', '{w_etl_batch_ts}', '{l_target_table_name}', '{l_target_table_name}')
            """)
            print("Added", format_large_number(l_record_count),f"records to {v_catalog_name}.{v_bronze_schema}.{l_target_table_name} by {l_load_type} load")

        elif l_load_type == "full":
            # Overwrite data for full load
            df.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable(f"{v_catalog_name}.{v_bronze_schema}.{l_target_table_name}")
            l_record_count = int(df.count())

            
            # Inserting the status in log table
            spark.sql(f"""
                INSERT INTO {v_catalog_name}.{v_logging_schema}.{v_db_activity_log_table} 
                (practice_etl_id,adf_pipeline_run_id, pipeline_name, emr, target_schema, target_table_name, activity_name, activity_status, record_count, batch_log_id, batch_log_ts, error_detail, error_log)
                VALUES ('{w_practice_etl_id}','{w_adf_pipeline_run_id}', '{v_notebook_name}', '{w_emr}', '{l_target_schema}', '{l_target_table_name}', '{l_load_type}', 'Success', {l_record_count}, '{w_etl_batch_id}', '{w_etl_batch_ts}', '{l_target_table_name}', '{l_target_table_name}')
            """)
            print("Added", format_large_number(l_record_count),f"records to {v_catalog_name}.{v_bronze_schema}.{l_target_table_name} by {l_load_type} load")
    else:
        print(f"Table {l_target_table_name} does not exist. Creating the table with partioning.")
        if l_load_type.lower() == 'incremental':
            # df= df.repartition(*l_partition_columns_list)
            df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(f"{v_catalog_name}.{v_bronze_schema}.{l_target_table_name}")
            # .partitionBy(*l_partition_columns_list)
            l_record_count = int(df.count())
            # Inserting the status in log table
            spark.sql(f"""
                INSERT INTO {v_catalog_name}.{v_logging_schema}.{v_db_activity_log_table} 
                (practice_etl_id,adf_pipeline_run_id, pipeline_name, emr, target_schema, target_table_name, activity_name, activity_status, record_count, batch_log_id, batch_log_ts, error_detail, error_log)
                VALUES ('{w_practice_etl_id}','{w_adf_pipeline_run_id}', '{v_notebook_name}', '{w_emr}', '{l_target_schema}', '{l_target_table_name}', '{l_load_type}', 'Success', {l_record_count}, '{w_etl_batch_id}', '{w_etl_batch_ts}', '{l_target_table_name}', '{l_target_table_name}')
            """)
            print(f"Created table {v_catalog_name}.{v_bronze_schema}.{l_target_table_name} with records", format_large_number(l_record_count),f"by {l_load_type} load")
        elif l_load_type.lower() == 'full':
            # df= df.repartition(*l_partition_columns_list)
            df.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable(f"{v_catalog_name}.{v_bronze_schema}.{l_target_table_name}")
            l_record_count = int(df.count())
            # .partitionBy(*l_partition_columns_list)
            spark.sql(f"""
                INSERT INTO {v_catalog_name}.{v_logging_schema}.{v_db_activity_log_table} 
                (practice_etl_id, adf_pipeline_run_id, pipeline_name, emr, target_schema, target_table_name, activity_name, activity_status, record_count, batch_log_id, batch_log_ts, error_detail, error_log)
                VALUES ('{w_practice_etl_id}', '{w_adf_pipeline_run_id}','{v_notebook_name}', '{w_emr}', '{l_target_schema}', '{l_target_table_name}', '{l_load_type}', 'Success', {l_record_count}, '{w_etl_batch_id}', '{w_etl_batch_ts}', '{l_target_table_name}', '{l_target_table_name}')
            """)
            print(f"Created table {v_catalog_name}.{v_bronze_schema}.{l_target_table_name} with records", format_large_number(l_record_count),f"by {l_load_type} load")
    
    print("Checking if all records are present in the table.")
    print(f"Records in landing file",df.count())
    # print(f"select count(*) from {v_catalog_name}.{v_bronze_schema}.{l_target_table_name} where file_name = '{all_files}'")
    print("Records in table",spark.sql(f"select count(*) from {v_catalog_name}.{v_bronze_schema}.{l_target_table_name} where file_name = '{all_files}'").collect()[0][0])
    table_count =spark.sql(f"select count(*) from {v_catalog_name}.{v_bronze_schema}.{l_target_table_name} where file_name = '{all_files}'").collect()[0][0]
    # print(table_count)
    if df.count() == table_count:
        print(f"All records from landing are present in {v_catalog_name}.{v_bronze_schema}.{l_target_table_name}. Now moving file from landing to archive.")
        # dbutils.fs.mv(source_table_location, l_archive_file_location)
        print(f"Moved {l_target_table_name} file from landing to archive.")
    else:
        print(f"Not all records from landing are present in {v_catalog_name}.{v_bronze_schema}.{l_target_table_name}. Please execute it again.")
