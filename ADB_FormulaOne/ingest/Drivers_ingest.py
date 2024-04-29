# Databricks notebook source
# MAGIC %md
# MAGIC 1. Read CSV File
# MAGIC 2. Apply schema for it
# MAGIC 3. Rename and Remove column based on the requirements

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

df = spark.read.json("/mnt/formulaoneadb/raw/drivers.json", input_schema)
df.display()

# COMMAND ----------

df = df.select("*", col("name.forename").alias("forename"), col("name.surname").alias("surname"))\
.drop("name")
df.display()

# COMMAND ----------

# Rename column and add New column
df = df.withColumnRenamed("driverRef", "driver_ref").withColumnRenamed("driverId", "driver_id").withColumn("ingest_dt", lit(current_date()))
df.display()

# COMMAND ----------

# df.write.mode('overwrite').parquet('/mnt/formulaoneadb/processed/drivers')

# COMMAND ----------

# dbutils.notebook.exit("success")

# COMMAND ----------

df.write.mode('overwrite').format('parquet').saveAsTable('f1_processed.drivers_tbl')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_processed.drivers_tbl;
