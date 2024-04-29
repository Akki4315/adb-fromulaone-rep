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

df = spark.read.json(f"/mnt/raw/{file_dt}/{source_name}.json", input_schema, multiLine=True)

# COMMAND ----------

# Rename column and add New column
df = df.withColumnRenamed("raceId", "race_id").withColumnRenamed("driverId", "driver_id").withColumn("ingest_dt", lit(current_date())).withColumn("file_date", lit(file_dt))

# COMMAND ----------

# increment_data(df, "race_id", "f1_processed", "pitstops_tbl")

# COMMAND ----------

merge_condition = ("target.race_id = source.race_id and target.driver_id = source.driver_id and target.stop = source.stop")
folder_path = "mnt/processed"
merge_delta_table("f1_processed", "pitstop_delta_tbl", folder_path, df, merge_condition, "race_id")

# COMMAND ----------

dbutils.notebook.exit("success")

# COMMAND ----------

# MAGIC %sql
# MAGIC select race_id, count(1) from f1_processed.pitstop_delta_tbl
# MAGIC group by race_id
# MAGIC order by race_id desc;
