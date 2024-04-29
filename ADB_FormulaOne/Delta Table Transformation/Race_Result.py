# Databricks notebook source
dbutils.widgets.text("file_date","2021-03-21")
file_dt = dbutils.widgets.get("file_date")

# COMMAND ----------

# MAGIC %run ../utils/common_functions

# COMMAND ----------

driver_df = spark.read.parquet("/mnt/processed/drivers_tbl/").withColumnRenamed("number","driver_number").withColumnRenamed("nationality","driver_nationality")
constructor_df = spark.read.parquet("/mnt/processed/constructors_tbl/").withColumnRenamed("name", "team")
pitstop_df = spark.read.parquet("/mnt/processed/pitstops_tbl/").withColumnRenamed("stop","pits")
race_df = spark.read.parquet("/mnt/processed/races_tbl/").withColumnRenamed("name","race_name").withColumnRenamed("date", "race_date").withColumnRenamed("year", "race_year")
circuit_df = spark.read.parquet("/mnt/processed/circuits_tbl/").withColumnRenamed("location", "circuit_location")
result_df = spark.read.parquet("/mnt/processed/results_tbl/").withColumnRenamed("time", "race_time").withColumnRenamed("race_id","result_race_id").filter(f"file_date = '{file_dt}'").withColumnRenamed("file_date", "result_file_date")


# COMMAND ----------

race_circuit_df = race_df.join(circuit_df, "circuit_id", "inner").select(
    "race_id", "race_year", "race_name", "race_date", "circuit_location"
)

# COMMAND ----------

race_result_df = (
    result_df.join(race_circuit_df, race_circuit_df.race_id == result_df.result_race_id, "left")
    .join(driver_df, "driver_id", "left")
    .join(constructor_df, "constructor_id", "left")
    #.join(pitstop_df, "race_id", "left")
)

# COMMAND ----------

final_df= race_result_df.select("race_id", "race_year", "race_name","race_date","circuit_location", "driver_nationality", "driver_name", "driver_number", "team", "grid", "fastestLap", "race_time", "points", "position", "result_file_date").withColumn("created_dt", current_date()).withColumnRenamed("result_file_date", "file_date")
final_df.display()


# COMMAND ----------

increment_data(final_df, "race_id", "f1_presentation", "race_result_tbl")

# COMMAND ----------

dbutils.notebook.exit("success")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_presentation.race_result_tbl
# MAGIC where race_year = 2021;
