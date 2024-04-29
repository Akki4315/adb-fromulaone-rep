# Databricks notebook source
# MAGIC %md
# MAGIC 1. Read JSON File
# MAGIC 2. Apply schema for it
# MAGIC 3. Rename and Remove column based on the requirements

# COMMAND ----------

dbutils.widgets.text("source_name", "")
source_name = dbutils.widgets.get("source_name")

# COMMAND ----------

dbutils.widgets.text("file_date", "2021-03-21")
file_dt = dbutils.widgets.get("file_date")

# COMMAND ----------

spark.read.json(f"/mnt/formulaoneadb/raw/{file_dt}/{source_name}.json").createOrReplaceTempView('result_cutout')

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

df = spark.read.json(f"/mnt/formulaoneadb/raw/{file_dt}/{source_name}.json", schema=input_schema)
df.display()

# COMMAND ----------

# Rename column and add New column
df = df.withColumnRenamed("resultId", "result_id")\
    .withColumnRenamed("raceId", "race_id")\
    .withColumnRenamed("driverId", "driver_id")\
    .withColumnRenamed("constructorId", "constructor_id")\
    .withColumnRenamed("statusId", "status_id")\
    .withColumn("ingest_dt", lit(current_date()))\
    .withColumn("file_date", lit(file_dt))

# COMMAND ----------

# MAGIC %md
# MAGIC ####Incremental data loading: Approch 1

# COMMAND ----------

# for race_id_list in df.select('race_id').distinct().collect():
#     if (spark._jsparkSession.catalog().tableExists("f1_processed.results_tbl")):
#         spark.sql(f"alter table f1_processed.results_tbl drop if exists partition (race_id = {race_id_list.race_id})")

# COMMAND ----------

# df.write.mode('append').partitionBy('race_id').format('parquet').saveAsTable('f1_processed.results_tbl')

# COMMAND ----------

# MAGIC %md
# MAGIC ####Incremental data loading: Approch 2

# COMMAND ----------

increment_data(df, "race_id", "f1_processed", "results_tbl")

# COMMAND ----------

dbutils.notebook.exit("success")

# COMMAND ----------

# MAGIC %sql
# MAGIC select race_id, count(1) from f1_processed.results_tbl
# MAGIC group by race_id
# MAGIC order by race_id desc;
