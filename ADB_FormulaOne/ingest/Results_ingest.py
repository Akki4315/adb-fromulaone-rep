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
        StructField("resultId", IntegerType()),
        StructField("raceId", IntegerType()),
        StructField("driverId", IntegerType()),
        StructField("constructorId", IntegerType()),
        StructField("number", IntegerType()),
        StructField("grid", IntegerType()),
        StructField("position", IntegerType()),
        StructField("positionText", StringType()),
        StructField("positionOrder", IntegerType()),
        StructField("points", FloatType()),
        StructField("laps", IntegerType()),
        StructField("time", StringType()),
        StructField("milliseconds", IntegerType()),
        StructField("fastestLap", IntegerType()),
        StructField("rank", IntegerType()),
        StructField("fastestLapTime", StringType()),
        StructField("fastestLapSpeed", StringType()),
        StructField("statusId", IntegerType()),
    ]
)

# COMMAND ----------

df = spark.read.json("/mnt/formulaoneadb/raw/results.json", schema=input_schema)
df.display()

# COMMAND ----------

# Rename column and add New column
df = df.withColumnRenamed("resultId", "result_id")\
    .withColumnRenamed("raceId", "race_id")\
    .withColumnRenamed("driverId", "driver_id")\
    .withColumnRenamed("constructorId", "constructor_id")\
    .withColumnRenamed("statusId", "status_id").withColumn("ingest_dt", lit(current_date()))
df.display()

# COMMAND ----------

# df.write.mode("overwrite").parquet("/mnt/formulaoneadb/processed/results")

# COMMAND ----------

# dbutils.notebook.exit("success")

# COMMAND ----------

df.write.mode('overwrite').format('parquet').saveAsTable('f1_processed.results_tbl')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_processed.results_tbl;
