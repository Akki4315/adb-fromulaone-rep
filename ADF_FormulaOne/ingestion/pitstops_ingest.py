# Databricks notebook source
# MAGIC %run ../utils/adf_common_functions

# COMMAND ----------

df = spark.read.json(f"/mnt/formulaoneadf/bronze/pitstops/20240427/")
df.printSchema()

# COMMAND ----------

pitstop_df = df.select('*', explode('MRData.RaceTable.Races').alias('flatten_races'))\
              .select('*', explode('flatten_races.Pitstops').alias('flatten_pitstop'))\
              .select(col('flatten_pitstop.driverId').alias('driver_id'),
                      col('flatten_races.date').alias('race_date'),
                      col('flatten_pitstop.duration').alias('duration'),
                      col('flatten_pitstop.lap').alias('lap'),
                      col('flatten_pitstop.stop').alias('stop'),
                      col('flatten_pitstop.time').alias('time'))
pitstop_df.count()

# COMMAND ----------

pitstop_df.write.mode("overwrite").parquet(f"/mnt/formulaoneadf/silver/pitstops/{transformation_dt}/")

# COMMAND ----------

dbutils.notebook.exit("success")

# COMMAND ----------

spark.read.parquet(f"/mnt/formulaoneadf/silver/pitstops/{transformation_dt}/").display()
