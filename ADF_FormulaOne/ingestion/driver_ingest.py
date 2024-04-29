# Databricks notebook source
# MAGIC %run ../utils/adf_common_functions

# COMMAND ----------

df = spark.read.json("/mnt/formulaoneadf/bronze/drivers/20240427/")
df.printSchema()

# COMMAND ----------

driver_df = df.select(explode('MRData.DriverTable.Drivers').alias('flatten_driver'))\
              .select(col('flatten_driver.driverId').alias('driver_ref'),
                      col('flatten_driver.permanentNumber').alias('driver_no'),
                      concat((col('flatten_driver.givenName')), lit(' '), (col('flatten_driver.familyName'))).alias('driver_name'),
                      col('flatten_driver.dateOfBirth').alias('driver_dob'),
                      col('flatten_driver.nationality').alias('driver_nationality'))
driver_df.display()

# COMMAND ----------

driver_df.write.mode("overwrite").parquet(f"/mnt/formulaoneadf/silver/drivers/{transformation_dt}/")

# COMMAND ----------

dbutils.notebook.exit("success")

# COMMAND ----------

spark.read.parquet(f"/mnt/adfformulaone/silver/drivers/{transformation_dt}/").display()
