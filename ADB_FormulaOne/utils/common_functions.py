# Databricks notebook source
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, DateType, ArrayType, MapType
from pyspark.sql.functions import lit, col, current_date, concat, sum, count, when, rank, desc, countDistinct
from datetime import datetime
from pyspark.sql.window import Window

# COMMAND ----------

def create_csv_df(input_location, schema):
    """
    This function is used for creating spark df on csv file location
    :input_location: provide input csv fiel location
    :schema: provide input schema
    :rturn: spark dataframe  
    """
    return spark.read.csv(input_location, header= True, schema=schema)

# COMMAND ----------

def rearrange_column_list(input_df, partition_col):
    column_lst = []
    for column_nm in input_df.schema.names:
        if column_nm != partition_col:
            column_lst.append(column_nm)
    column_lst.append(partition_col)
    output_df = input_df.select(column_lst)
    return output_df

# COMMAND ----------

def increment_data(input_df, partition_column, db_name, table_name):
    output_df = rearrange_column_list(input_df, partition_column)

    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    if (spark._jsparkSession.catalog().tableExists(f"{db_name}.{table_name}")):
        output_df.write.mode('overwrite').insertInto(f"{db_name}.{table_name}")
    else:
        output_df.write.mode('overwrite').partitionBy(partition_column).format('parquet').saveAsTable(f"{db_name}.{table_name}")

# COMMAND ----------

def merge_delta_table(db_name, table_name, folder_path, input_df, merge_condition, partition_column):
    spark.conf.set("spark.databricks.optimizer.dynamicPartitionPruning","true")
    from delta.tables import DeltaTable
    if (spark._jsparkSession.catalog().tableExists(f"{db_name}.{table_name}")):
        deltaTable = DeltaTable.forPath(spark, f"{folder_path}/{table_name}")
        deltaTable.alias("target").merge(input_df.alias("source"), merge_condition)\
            .whenMatchedUpdateAll()\
            .whenNotMatchedInsertAll()\
            .execute()
    else: 
        input_df.write.mode("overwrite").partitionBy(partition_column).format("delta").saveAsTable(f"{db_name}.{table_name}")
