# Databricks notebook source
# MAGIC %run ../utils/adf_common_functions

# COMMAND ----------

cons_standing_df = spark.read.parquet(f"/mnt/formulaoneadf/gold/Race_Result/{transformation_dt}").select('season','race_name','race_date','location','driver_nationality','driver_name','team_name','result_position','points')

cons_standing_df.display()

# COMMAND ----------

cons_standing_df = cons_standing_df.groupBy('season','team_name').agg(sum('points').alias('points'), count(when(col('result_position')== 1, True )).alias('wins'))
cons_standing_df.display()

# COMMAND ----------

windowspec = Window.partitionBy('season').orderBy(desc('points'), desc('wins'))

# COMMAND ----------

final_df = cons_standing_df.withColumn('constructor_standing', rank().over(windowspec))\
    .select('season', 'constructor_standing', 'team_name', 'points', 'wins')

final_df.display()

# COMMAND ----------

final_df.write.mode('overwrite').parquet(f"/mnt/formulaoneadf/gold/Constructor_Standing_Result/{transformation_dt}/")

# COMMAND ----------

dbutils.notebook.exit("success")

# COMMAND ----------

spark.read.parquet(f"/mnt/formulaoneadf/gold/Constructor_Standing_Result/{transformation_dt}").count()
