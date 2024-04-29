# Databricks notebook source
# MAGIC %md
# MAGIC 1. Read CSV File
# MAGIC 2. Apply schema for it
# MAGIC 3. Rename and Remove column based on the requirements

# COMMAND ----------

# MAGIC %run ../utils/common_functions

# COMMAND ----------

input_schema = StructType(
    [
        StructField("raceId", IntegerType()),
        StructField("year", IntegerType()),
        StructField("round", IntegerType()),
        StructField("circuitId", IntegerType()),
        StructField("name", StringType()),
        StructField("date", DateType()),
        StructField("time", StringType()),
        StructField("url", StringType()),
        StructField("fp1_date", DateType()),
        StructField("fp1_time", StringType()),
        StructField("fp2_date", DateType()),
        StructField("fp2_time", StringType()),
        StructField("fp3_date", DateType()),
        StructField("fp3_time", StringType()),
        StructField("quali_date", DateType()),
        StructField("quali_time", StringType()),
        StructField("sprint_date", DateType()),
        StructField("sprint_time", StringType()),  
    ]
)

# COMMAND ----------

df = create_csv_df("/mnt/formulaoneadb/raw/races.csv", input_schema)
df.display()

# COMMAND ----------

# Rename column and add New column
df = df.withColumnRenamed("raceId", "race_id").withColumnRenamed("circuitId", "circuit_id").withColumn("ingest_dt", lit(current_date()))
df.display()

# COMMAND ----------

# df.write.mode('overwrite').parquet('/mnt/formulaoneadb/processed/races')

# COMMAND ----------

# dbutils.notebook.exit("success")

# COMMAND ----------

df.write.mode('overwrite').format('parquet').saveAsTable('f1_processed.races_tbl')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_processed.races_tbl;
