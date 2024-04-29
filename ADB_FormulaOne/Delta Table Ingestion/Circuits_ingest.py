# Databricks notebook source
# MAGIC %md
# MAGIC 1. Read CSV File
# MAGIC 2. Apply schema for it
# MAGIC 3. Rename and Remove column based on the requirements

# COMMAND ----------

dbutils.widgets.text("source_name", "circuits")
source_name = dbutils.widgets.get("source_name")

# COMMAND ----------

dbutils.widgets.text("file_date", "2021-03-21")
file_dt = dbutils.widgets.get("file_date")

# COMMAND ----------

# MAGIC %run ../utils/common_functions

# COMMAND ----------

input_schema = StructType(
    [
        StructField("circuitId", IntegerType()),
        StructField("circuitRef", StringType()),
        StructField("name", StringType()),
        StructField("location", StringType()),
        StructField("country", StringType()),
        StructField("lat", FloatType()),
        StructField("lng", FloatType()),
        StructField("alt", IntegerType()),
        StructField("url", StringType()),
    ]
)

# COMMAND ----------

df = create_csv_df(f"/mnt/raw/{file_dt}/{source_name}.csv", input_schema)

# COMMAND ----------

# Rename column and add New column
df = df.withColumnRenamed("circuitId", "circuit_id").withColumnRenamed("circuitRef", "circuit_ref").withColumn("ingest_dt", lit(current_date())).withColumn("file_date", lit(file_dt))
df.display()

# COMMAND ----------

df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.circuit_delta_tbl")

# COMMAND ----------

# increment_data(df, 'circuit_id', 'f1_processed', 'circuits_tbl')

# COMMAND ----------

dbutils.notebook.exit("success")

# COMMAND ----------

# MAGIC %sql
# MAGIC select circuit_id, count(1) from f1_processed.circuit_delta_tbl
# MAGIC group by circuit_id
# MAGIC order by circuit_id desc;
