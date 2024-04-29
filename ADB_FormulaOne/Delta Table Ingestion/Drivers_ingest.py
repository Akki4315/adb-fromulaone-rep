# Databricks notebook source
# MAGIC %md
# MAGIC 1. Read JSON File
# MAGIC 2. Apply schema for it
# MAGIC 3. Rename and Remove column based on the requirements

# COMMAND ----------

dbutils.widgets.text("source_name", "")
source_name = dbutils.widgets.get("source_name")

# COMMAND ----------

dbutils.widgets.text("file_date", "2021-03-21")
file_dt = dbutils.widgets.get("file_date")

# COMMAND ----------

# MAGIC %run ../utils/common_functions

# COMMAND ----------

# Defining_schema
input_schema = StructType(
    [
        StructField("driverId", IntegerType()),
        StructField("driverRef", StringType()),
        StructField("number", IntegerType()),
        StructField("code", StringType()),
        StructField("name", StructType([StructField("forename",StringType()),
                                        StructField("surname", StringType())
                                        ])),
        StructField("dob", DateType()),
        StructField("nationality", StringType()),
        StructField("url", StringType()),
    ]
)

# COMMAND ----------

df = spark.read.json(f"/mnt/raw/{file_dt}/{source_name}.json", input_schema)
df.display()

# COMMAND ----------

df = df.select("*", concat((col("name.forename")), lit(" "), (col("name.surname"))).alias("driver_name"))\
.drop("name")
df.display()

# COMMAND ----------

# Rename column and add New column
df = df.withColumnRenamed("driverRef", "driver_ref").withColumnRenamed("driverId", "driver_id").withColumn("ingest_dt", lit(current_date())).withColumn("file_date", lit(file_dt))
df.display()

# COMMAND ----------

df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.driver_delta_tbl")

# COMMAND ----------

# increment_data(df, "driver_id", "f1_processed", "drivers_tbl")

# COMMAND ----------

dbutils.notebook.exit("success")

# COMMAND ----------

# MAGIC %sql
# MAGIC select driver_id, count(1) from f1_processed.driver_delta_tbl
# MAGIC group by driver_id
# MAGIC order by driver_id desc
# MAGIC ;
