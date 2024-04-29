# Databricks notebook source
# MAGIC %md
# MAGIC 1. Read CSV File
# MAGIC 2. Apply schema for it
# MAGIC 3. Rename and Remove column based on the requirements

# COMMAND ----------

# MAGIC %run ../utils/common_functions

# COMMAND ----------

# Defining_schema
input_schema = StructType(
    [
        StructField("raceId", IntegerType()),
        StructField("driverId", IntegerType()),
        StructField("stop", IntegerType()),
        StructField("lap", IntegerType()),
        StructField("time", StringType()),
        StructField("duration", StringType()),
        StructField("milliseconds", IntegerType()), 
    ]
)

# COMMAND ----------

df = spark.read.json("/mnt/formulaoneadb/raw/pit_stops.json", input_schema, multiLine=True)
df.display()

# COMMAND ----------

# Rename column and add New column
df = df.withColumnRenamed("raceId", "race_id").withColumnRenamed("driverId", "driver_id").withColumn("ingest_dt", lit(current_date()))
df.display()

# COMMAND ----------

# df.write.mode('overwrite').parquet('/mnt/formulaoneadb/processed/pit_stops')

# COMMAND ----------

# dbutils.notebook.exit("success")

# COMMAND ----------

df.write.mode('overwrite').format('parquet').saveAsTable('f1_processed.pitstops_tbl')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_processed.pitstops_tbl;
