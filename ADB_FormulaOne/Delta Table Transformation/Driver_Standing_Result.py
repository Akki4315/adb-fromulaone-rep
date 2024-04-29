# Databricks notebook source
# MAGIC %md
# MAGIC ####Create Driver Standing Table:(with incremental load approch)

# COMMAND ----------

dbutils.widgets.text("file_date","2021-03-21")
file_dt = dbutils.widgets.get("file_date")

# COMMAND ----------

# MAGIC %run ../utils/common_functions

# COMMAND ----------

race_result_list = spark.read.parquet("/mnt/presentation/race_result_tbl/")\
  .filter(f"file_date = '{file_dt}'")\
  .select("race_year").distinct()\
  .collect()

# COMMAND ----------

race_year_lst = []
for race_year in race_result_list:
    race_year_lst.append(race_year.race_year)

# COMMAND ----------

race_result_df= spark.read.parquet("/mnt/presentation/race_result_tbl/")\
    .filter(col("race_year").isin(race_year_lst))

race_result_df.display()

# COMMAND ----------

driver_standings_df = race_result_df.groupBy("race_year","driver_nationality","driver_name","team").agg(sum(col("points")).alias("total_points"), count(when(col("position")==1, True)).alias("wins"))
driver_standings_df.display()

# COMMAND ----------

window_spec = Window.partitionBy("race_year").orderBy(desc("total_points"), desc("wins"))

# COMMAND ----------

final_df = driver_standings_df.withColumn("driver_standing", rank().over(window_spec)).select("driver_standing","race_year","driver_nationality","driver_name","team","wins","total_points")
final_df.display()

# COMMAND ----------

increment_data(final_df, "race_year","f1_presentation","driver_standing_tbl")

# COMMAND ----------

dbutils.notebook.exit("success")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_presentation.driver_standing_tbl;
