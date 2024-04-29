# Databricks notebook source
# MAGIC %run ../utils/adf_common_functions

# COMMAND ----------

df = spark.read.json("/mnt/formulaoneadf/bronze/results/20240427/")
df.printSchema()

# COMMAND ----------

result_df = df.select('*', explode('MRData.RaceTable.Races').alias('flatten_races'))\
              .select('*', explode('flatten_races.Results').alias('flatten_result'))\
              .select(col('flatten_races.date').alias('race_date'),
                      col('flatten_races.season').alias('season_id'),
                      col('flatten_races.round').alias('round_id'),
                      col('flatten_races.Circuit.circuitId').alias('circuit_id'),
                      col('flatten_result.Constructor.constructorId').alias('constructor_id'),
                      col('flatten_result.Driver.driverId').alias('driver_id'),
                      col('flatten_result.FastestLap.Time.time').alias('fast_lap'),
                      col('flatten_result.Time.time').alias('race_time'),
                      col('flatten_result.FastestLap.rank').alias('rank'),
                      col('flatten_result.grid').alias('grid'),
                      col('flatten_result.laps').alias('laps'),
                      col('flatten_result.position').alias('result_position'),
                      col('flatten_result.points').alias('points'),
                      col('flatten_result.positionText').alias('position_text'),
                      col('flatten_result.status').alias('status'))

result_df.display()

# COMMAND ----------

result_df.write.mode("overwrite").parquet(f"/mnt/formulaoneadf/silver/results/{transformation_dt}/")

# COMMAND ----------

dbutils.notebook.exit("success")

# COMMAND ----------

spark.read.parquet(f"/mnt/formulaoneadf/silver/results/{transformation_dt}/").count()
