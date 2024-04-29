# Databricks notebook source
# MAGIC %run ../utils/common_functions

# COMMAND ----------

driver_standings_df = spark.read.parquet("/mnt/formulaoneadb/presentation/Race_result/").select("race_year", "driver_nationality", "name", "team", "points","position")

# COMMAND ----------

driver_standings_df = driver_standings_df.groupBy("race_year","driver_nationality","name","team").agg(sum(col("points")).alias("total_points"), count(when(col("position")==1, True)).alias("wins"))
driver_standings_df.display()

# COMMAND ----------

window_spec = Window.partitionBy("race_year").orderBy(desc("total_points"), desc("wins"))

# COMMAND ----------

final_df = driver_standings_df.withColumn("driver_standing", rank().over(window_spec)).select("driver_standing","race_year","driver_nationality","name","team","wins","total_points")
final_df.display()

# COMMAND ----------

# final_df.write.mode("overwrite").parquet("/mnt/formulaoneadb/presentation/Driver_standing/")

# COMMAND ----------

# dbutils.notebook.exit("success")

# COMMAND ----------

final_df.write.mode("overwrite").format('parquet').saveAsTable("f1_presentation.driver_standing_tbl")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_presentation.driver_standing_tbl;
