# Databricks notebook source
# MAGIC %run ../utils/common_functions

# COMMAND ----------

driver_df = spark.read.parquet("/mnt/formulaoneadb/processed/drivers/").withColumnRenamed("number","driver_number").withColumnRenamed("nationality","driver_nationality").withColumn("name", concat("forename", lit(" "), "surname"))
qualifying_df = spark.read.parquet("/mnt/formulaoneadb/processed/qualifying/").withColumnRenamed("q1","qualifying1").withColumnRenamed("q2","qualifying2").withColumnRenamed("q3","qualifying3")
constructor_df = spark.read.parquet("/mnt/formulaoneadb/processed/constructors/").withColumnRenamed("name", "team")
race_df = spark.read.parquet("/mnt/formulaoneadb/processed/races/").withColumnRenamed("name","race_name").withColumnRenamed("date", "race_date").withColumnRenamed("year", "race_year")
circuit_df = spark.read.parquet("/mnt/formulaoneadb/processed/circuits").withColumnRenamed("location", "circuit_location")

# COMMAND ----------

race_circuit_df = race_df.join(circuit_df, "circuit_id", "inner")\
    .select("race_id","race_year", "race_name","race_date", "circuit_location")

# COMMAND ----------

quali_result_df = (
    qualifying_df.join(race_circuit_df, "race_id", "inner")
    .join(driver_df, "driver_id", "inner")
    .join(constructor_df, "constructor_id", "inner")
)

# COMMAND ----------

final_df = quali_result_df.select("race_year", "race_name","race_date","circuit_location", "driver_nationality", "name", "driver_number", "team","qualifying1","qualifying2","qualifying3").withColumn("created_dt", current_date())

# COMMAND ----------

final_df.filter("race_year == 2020 and race_name =='Abu Dhabi Grand Prix'").orderBy("qualifying1","qualifying2","qualifying3").display()

# COMMAND ----------

# final_df.write.mode("overwrite").parquet("/mnt/formulaoneadb/presentation/Qualifying_result")

# COMMAND ----------

# dbutils.notebook.exit("success")

# COMMAND ----------

final_df.write.mode("overwrite").format('parquet').saveAsTable("f1_presentation.qualify_result_tbl")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_presentation.qualify_result_tbl;
