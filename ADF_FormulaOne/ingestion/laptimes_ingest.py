# Databricks notebook source
# MAGIC %run ../utils/adf_common_functions

# COMMAND ----------

df = spark.read.json("/mnt/formulaoneadf/bronze/laptimes/20240427/")
df.printSchema()

# COMMAND ----------

laptimes_df = df.select(explode('MRData.RaceTable.races').alias('flatten_races'))\
    .select('*', explode('flatten_races.Laps').alias('flatten_laps'))\
        .select('*', explode('flatten_laps.Timings').alias('flatten_Timing'))\
        .select(col("flatten_races.date").alias("race_date"),
                col("flatten_Timing.driverId").alias('driver_id'),
                col("flatten_laps.number").alias('lap'),
                col("flatten_Timing.position").alias('position'),
                col("flatten_Timing.time").alias('time'))
laptimes_df.display()

# COMMAND ----------

# Rename column and add New column
laptimes_df = laptimes_df.withColumn("ingest_dt", lit(current_date))
laptimes_df.count()

# COMMAND ----------

laptimes_df.write.mode("overwrite").parquet(f"/mnt/formulaoneadf/silver/lap_times/{transformation_dt}/")

# COMMAND ----------

dbutils.notebook.exit("success")

# COMMAND ----------

spark.read.parquet(f"/mnt/formulaoneadf/silver/lap_times/{transformation_dt}/").count()
