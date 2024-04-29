# Databricks notebook source
# MAGIC %run ../utils/common_functions

# COMMAND ----------

driver_df = spark.read.parquet("/mnt/formulaoneadb/processed/drivers/").withColumnRenamed("number","driver_number").withColumnRenamed("nationality","driver_nationality").withColumn("name", concat("forename", lit(" "), "surname"))
constructor_df = spark.read.parquet("/mnt/formulaoneadb/processed/constructors/").withColumnRenamed("name", "team")
pitstop_df = spark.read.parquet("/mnt/formulaoneadb/processed/pit_stops/").withColumnRenamed("stop","pits")
result_df = spark.read.parquet("/mnt/formulaoneadb/processed/results/").withColumnRenamed("time", "race_time")
race_df = spark.read.parquet("/mnt/formulaoneadb/processed/races").withColumnRenamed("name","race_name").withColumnRenamed("date", "race_date").withColumnRenamed("year", "race_year")
circuit_df = spark.read.parquet("/mnt/formulaoneadb/processed/circuits").withColumnRenamed("location", "circuit_location")


# COMMAND ----------

race_circuit_df = race_df.join(circuit_df, "circuit_id", "inner").select(
    "race_id", "race_year", "race_name", "race_date", "circuit_location"
)

# COMMAND ----------

race_result_df = (
    result_df.join(race_circuit_df, "race_id", "left")
    .join(driver_df, "driver_id", "left")
    .join(constructor_df, "constructor_id", "left")
    #.join(pitstop_df, "race_id", "left")
)

# COMMAND ----------

final_df= race_result_df.select("race_year", "race_name","race_date","circuit_location", "driver_nationality", "name", "driver_number", "team", "grid", "fastestLap", "race_time", "points", "position").withColumn("created_dt", current_date())
final_df.display()


# COMMAND ----------

# final_df.write.mode("overwrite").parquet("/mnt/formulaoneadb/presentation/Race_result")

# COMMAND ----------

# dbutils.notebook.exit("success")

# COMMAND ----------

final_df.write.mode("overwrite").format('parquet').saveAsTable("f1_presentation.race_result_tbl")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_presentation.race_result_tbl;
