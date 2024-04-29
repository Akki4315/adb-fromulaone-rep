# Databricks notebook source
# MAGIC %run ../utils/common_functions

# COMMAND ----------

race_result_df = spark.read.parquet("/mnt/formulaoneadb/presentation/Race_result/").select("race_year", "team", "points", "position")

# COMMAND ----------

race_result_df = race_result_df.groupBy("race_year", "team").agg(sum("points").alias("total_points"), count(when(col("position")==1, True)).alias("wins"))

# COMMAND ----------

window_spec = Window.partitionBy("race_year").orderBy(desc("total_points"), desc("wins"))

# COMMAND ----------

final_df = race_result_df.withColumn("constructor_standing", rank().over(window_spec)).select("constructor_standing","race_year","team","wins","total_points")
final_df.filter("constructor_standing == 1 and team = 'Mercedes'").display()

# COMMAND ----------

# final_df.write.mode("overwrite").parquet("/mnt/formulaoneadb/presentation/Constructor_standing")

# COMMAND ----------

# dbutils.notebook.exit("success")

# COMMAND ----------

final_df.write.mode("overwrite").format('parquet').saveAsTable("f1_presentation.cons_standing_tbl")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_presentation.cons_standing_tbl
