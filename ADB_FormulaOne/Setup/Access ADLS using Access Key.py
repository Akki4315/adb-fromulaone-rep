# Databricks notebook source
# MAGIC %md
# MAGIC ####Accessing ADLS in Databricks using Access Key

# COMMAND ----------

#assigning the secrets key into a varible
access_key = dbutils.secrets.get('bwt session','kv-formulaoneproject1-accesskey')

# COMMAND ----------

# configuration for accessing the azure storage account into databrick notebook
spark.conf.set(
    "fs.azure.account.key.formulaoneproject1.dfs.core.windows.net", access_key)

# COMMAND ----------

#dbutils.fs.ls("abfss://<container-name>@<storage-account-name>.dfs.core.windows.net/<path-to-data>")
display(dbutils.fs.ls("abfss://bronze@formulaoneproject1.dfs.core.windows.net/"))


# COMMAND ----------

df = spark.read.csv("abfss://test@formulaoneproject1.dfs.core.windows.net/input_data/emp_data.csv", header = True)
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Capabilities of dbutils.secrets methods

# COMMAND ----------

dbutils.secrets.help()

# COMMAND ----------

# to findout the all secret scope name list 
dbutils.secrets.listScopes()

# COMMAND ----------

# to listdown the all secrets from the specific secret scope
dbutils.secrets.list('bwt session')

# COMMAND ----------

# to get & use the secrets into notebooks
dbutils.secrets.get('bwt session','kv-formulaoneproject1-accesskey')

# COMMAND ----------

dbutils.secrets.getBytes('bwt session','kv-formulaoneproject1-accesskey')
