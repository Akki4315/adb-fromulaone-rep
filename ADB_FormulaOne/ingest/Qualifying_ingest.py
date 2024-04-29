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
        StructField("qualifyId", IntegerType()),
        StructField("raceId", IntegerType()),
        StructField("constructorId", IntegerType()),
        StructField("driverId", IntegerType()),
        StructField("number", IntegerType()),
        StructField("position", IntegerType()),
        StructField("q1", StringType()),
        StructField("q2", StringType()),
        StructField("q3", StringType()),   
    ]
)

# COMMAND ----------

df = spark.read.json("/mnt/formulaoneadb/raw/qualifying", schema=input_schema, multiLine=True)
df.display()

# COMMAND ----------

# Rename column and add New column
df = df.withColumnRenamed("qualifyId", "qualify_id")\
    .withColumnRenamed("raceId", "race_id")\
    .withColumnRenamed("driverId", "driver_id")\
    .withColumnRenamed("constructorId", "constructor_id")\
    .withColumn("ingest_dt", lit(current_date()))
df.display()

# COMMAND ----------

# df.write.mode("overwrite").parquet("/mnt/formulaoneadb/processed/qualifying")

# COMMAND ----------

# dbutils.notebook.exit("success")

# COMMAND ----------

df.write.mode('overwrite').format('parquet').saveAsTable('f1_processed.qualifying_tbl')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_processed.qualifying_tbl;
