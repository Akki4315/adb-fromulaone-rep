# Databricks notebook source
# MAGIC %md
# MAGIC 1. Read CSV File
# MAGIC 2. Apply schema for it
# MAGIC 3. Rename and Remove column based on the requirements

# COMMAND ----------

dbutils.widgets.text("source_name", "")
source_name = dbutils.widgets.get("source_name")

# COMMAND ----------

dbutils.widgets.text("file_date", "2021-03-21")
file_dt = dbutils.widgets.get("file_date")

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

df = spark.read.json(f"/mnt/raw/{file_dt}/{source_name}", schema=input_schema, multiLine=True)

# COMMAND ----------

# Rename column and add New column
df = df.withColumnRenamed("qualifyId", "qualify_id")\
    .withColumnRenamed("raceId", "race_id")\
    .withColumnRenamed("driverId", "driver_id")\
    .withColumnRenamed("constructorId", "constructor_id")\
    .withColumn("ingest_dt", lit(current_date()))\
    .withColumn("file_date", lit(file_dt))
df.display()

# COMMAND ----------

increment_data(df, "race_id", "f1_processed", "qualifying_tbl")

# COMMAND ----------

dbutils.notebook.exit("success")

# COMMAND ----------

# MAGIC %sql
# MAGIC select race_id, count(1) from f1_processed.qualifying_tbl
# MAGIC group by race_id
# MAGIC order by race_id desc;
