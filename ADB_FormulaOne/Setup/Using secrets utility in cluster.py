# Databricks notebook source
# MAGIC %md
# MAGIC ### Using a secret utility in cluster
# MAGIC ###### we can store the secrets in cluster configuration instead of adding in specific notebook. the advantage of this approch is we don't need to add the secrets configuration in every notebook, by using the same cluster we can access the secrets values to all the notebooks attached to it. but, there is drawback- everyone using that cluster can access to that secrets. so to avoid that we need set cluster access configuration to specific user only. 

# COMMAND ----------

# MAGIC %md
# MAGIC #### Steps to add secrets in cluster
# MAGIC 1. Go to Cluster and click on Edit and drop down advance option
# MAGIC 2. Go to the spark config window and add the secrets config as follows:
# MAGIC 3. example: for accessing the storage account using access key from cluster - fs.azure.account.key.formulaoneproject1.dfs.core.windows.net {{secrets/<secret scope name>/<secret key name>}}

# COMMAND ----------

# access key config code
fs.azure.account.key.formulaoneproject1.dfs.core.windows.net {{secrets/bwt session/kv-formulaoneproject1-accesskey}}
