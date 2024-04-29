# Databricks notebook source
# MAGIC %md
# MAGIC 1. Read CSV File
# MAGIC 2. Apply schema for it
# MAGIC 3. Rename and Remove column based on the requirements

# COMMAND ----------

# MAGIC %run ../utils/common_functions

# COMMAND ----------

input_schema = StructType(
    [
        StructField("raceId", IntegerType()),
        StructField("driverId", IntegerType()),
        StructField("lap", IntegerType()),
        StructField("position", IntegerType()),
        StructField("time", StringType()),
        StructField("milliseconds", IntegerType()),
    ]
)

# COMMAND ----------

df = create_csv_df("/mnt/formulaoneadb/raw/lap_times/", input_schema)
df.display()

# COMMAND ----------

# Rename column and add New column
df = df.withColumnRenamed("raceId", "race_id").withColumnRenamed("driverId", "driver_id").withColumn("ingest_dt", lit(current_date()))
df.display()

# COMMAND ----------

# df.write.mode('overwrite').parquet('/mnt/formulaoneadb/processed/lap_times')

# COMMAND ----------

# dbutils.notebook.exit("success")

# COMMAND ----------

df.write.mode('overwrite').format('parquet').saveAsTable('f1_processed.laptimes_tbl')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_processed.laptimes_tbl;
