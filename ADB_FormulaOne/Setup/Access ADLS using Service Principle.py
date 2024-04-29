# Databricks notebook source
# MAGIC %md
# MAGIC ####Accessing ADLS from Databricks using Service Principle

# COMMAND ----------

# MAGIC %md
# MAGIC ### to generate service principle: 
# MAGIC ###### go to azure portal--> search 'Microsoft Intra Id' and select-->App Registration-->New Registarion--> give specific name (like sp_bwtsession)--> keep remaining field by as it is--> click register --> copy Application_id & Dirctory_id  which require for configuration--> then go to Certificate & secrets--> click on New Client Id--> fill the description (for which purpose service principle going to use) & expiry of the service principle--> click Add. then copy the secret value and save it in service_credential variable. 
# MAGIC
# MAGIC ###### then again go to the specific container location--> select access control (IAM)--> click on Add Role Assignment--> in role select 'Storage Blob Data Contributor' then click Next --> click select member--> search and select service principle which created during app registration (sp_bwtsession). then click next--> review & assign. after creating role assignment, it will show in role assignment tab.
# MAGIC
# MAGIC ##### after completing all above process run the below code or run whole notebook on a single click (Run all command)

# COMMAND ----------

#secret scope name is key-vault-secrets
application_id = dbutils.secrets.get("key-vault-secrets","kv-formulaoneproject-application-id")
directory_id = dbutils.secrets.get("key-vault-secrets","kv-formulaoneproject-directory-id")
service_credential = dbutils.secrets.get("key-vault-secrets","kv-formulaoneproject-service-credential")

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.f1adlsgen2sa.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.f1adlsgen2sa.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.f1adlsgen2sa.dfs.core.windows.net", application_id)
spark.conf.set("fs.azure.account.oauth2.client.secret.f1adlsgen2sa.dfs.core.windows.net", service_credential)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.f1adlsgen2sa.dfs.core.windows.net", f"https://login.microsoftonline.com/{directory_id}/oauth2/token")

# COMMAND ----------

#dbutils.fs.ls("abfss://<container-name>@<storage-account-name>.dfs.core.windows.net/<path-to-data>")
display(dbutils.fs.ls("abfss://formulaoneadf@f1adlsgen2sa.dfs.core.windows.net/bronze/"))
