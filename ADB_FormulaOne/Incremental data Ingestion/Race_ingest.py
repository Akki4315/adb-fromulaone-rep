# Databricks notebook source
# MAGIC %md
# MAGIC 1. Read CSV File
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
    ]
)

# COMMAND ----------

df = create_csv_df(f"/mnt/formulaoneadb/raw/{file_dt}/{source_name}.csv", input_schema)
df.display()

# COMMAND ----------

# Rename column and add New column
df = df.withColumnRenamed("raceId", "race_id").withColumnRenamed("circuitId", "circuit_id").withColumn("ingest_dt", lit(current_date())).withColumn("file_date", lit(file_dt))
df.display()

# COMMAND ----------

increment_data(df, "race_id", "f1_processed", "races_tbl")

# COMMAND ----------

dbutils.notebook.exit("success")

# COMMAND ----------

# MAGIC %sql
# MAGIC select race_id, count(1) from f1_processed.races_tbl
# MAGIC group by race_id
# MAGIC order by race_id desc;
