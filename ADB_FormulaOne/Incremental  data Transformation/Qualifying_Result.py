# Databricks notebook source
dbutils.widgets.text("file_date","2021-03-21")
file_dt = dbutils.widgets.get("file_date")

# COMMAND ----------

# MAGIC %run ../utils/common_functions

# COMMAND ----------

driver_df = spark.read.parquet("/mnt/formulaoneadb/mng_tbl/processed/drivers_tbl/").withColumnRenamed("number","driver_number").withColumnRenamed("nationality","driver_nationality")
qualifying_df = spark.read.parquet("/mnt/formulaoneadb/mng_tbl/processed/qualifying_tbl/").withColumnRenamed("q1","qualifying1").withColumnRenamed("q2","qualifying2").withColumnRenamed("q3","qualifying3").filter(f"file_date = '{file_dt}'")
constructor_df = spark.read.parquet("/mnt/formulaoneadb/mng_tbl/processed/constructors_tbl/").withColumnRenamed("name", "team")
race_df = spark.read.parquet("/mnt/formulaoneadb/mng_tbl/processed/races_tbl/").withColumnRenamed("name","race_name").withColumnRenamed("date", "race_date").withColumnRenamed("year", "race_year")
circuit_df = spark.read.parquet("/mnt/formulaoneadb/mng_tbl/processed/circuits_tbl/").withColumnRenamed("location", "circuit_location")

# COMMAND ----------

race_circuit_df = race_df.join(circuit_df, "circuit_id", "inner")\
    .select("race_id","race_year", "race_name","race_date", "circuit_location")

race_circuit_df.display()

# COMMAND ----------

quali_result_df = (
    qualifying_df.join(race_circuit_df, "race_id", "inner")
    .join(driver_df, "driver_id", "inner")
    .join(constructor_df, "constructor_id", "inner")
)

# COMMAND ----------

final_df = quali_result_df.select(qualifying_df.race_id, "race_year", "race_name","race_date","circuit_location", "driver_nationality", "driver_name", "driver_number", "team","qualifying1","qualifying2","qualifying3").withColumn("created_dt", current_date())
final_df.display()

# COMMAND ----------

increment_data(final_df, "race_id", "f1_presentation", "quali_result_tbl")

# COMMAND ----------

dbutils.notebook.exit("success")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_presentation.quali_result_tbl
# MAGIC where race_year = 2021;
