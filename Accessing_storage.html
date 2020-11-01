# Databricks notebook source
# MAGIC %md
# MAGIC # Accessing storage in Databricks
# MAGIC ---
# MAGIC ## 0. Prerequisites
# MAGIC ### 0.1. Azure App registration
# MAGIC 
# MAGIC 1. [Create an Azure AD application and service principal that can access resources](https://docs.microsoft.com/en-us/azure/azure-resource-manager/resource-group-create-service-principal-portal). Note the following properties:
# MAGIC   - application-id: An ID that uniquely identifies the application
# MAGIC   - directory-id: An ID that uniquely identifies the Azure AD instance
# MAGIC   - storage-account-name: The name of the storage account
# MAGIC   - service-credential: A string that the application uses to prove its identity   
# MAGIC   
# MAGIC 2. Register the service principal, granting the correct role assignment, such as Storage Blob Data Contributor, on the Azure Data Lake Storage Gen2 account.
# MAGIC 
# MAGIC ### 0.2. Adding scoped secrets
# MAGIC To add a secret and a scope, this needs to be completed using the [Databricks CLI](https://docs.databricks.com/dev-tools/cli/index.html).
# MAGIC 
# MAGIC Once signed in via the CLI, issue the following commands:
# MAGIC 1. `databricks secrets create-scope --scope Analysts`
# MAGIC 2. `databricks secrets put --scope Analysts --key SPID --string-value "Service Principal ID"` (Application Client ID)
# MAGIC 3. `databricks secrets put --scope Analysts --key SPKey --string-value "Service Principal Secret Key"`
# MAGIC 4. `databricks secrets put --scope Analysts --key DirectoryID --string-value "Azure Directory ID"`

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Mount Azure Data Lake Storage Gen2 filesystem
# MAGIC This only needs to be done once per cluster

# COMMAND ----------

# service principal details
client_id     = dbutils.secrets.get(scope='Analysts', key='SPID')  # 'cb90462d-aaae-4038-ac60-b8e090d404bb'
client_secret = dbutils.secrets.get(scope='Analysts', key='SPKey')  # '-nGdTkdSa-TN-4Fr7z0aBX3.k4Dct54SOX'
directory_id  = dbutils.secrets.get(scope='Analysts', key='DirectoryID')  # '600fc753-881c-4179-a38f-27842e71dc98'

# data lake container details
account_name = 'storagejamesleslie'
container_name = 'ecovacs'

configs = {'fs.azure.account.auth.type': 'OAuth',
           'fs.azure.account.oauth.provider.type': 'org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider',
           'fs.azure.account.oauth2.client.id': client_id,
           'fs.azure.account.oauth2.client.secret': client_secret,
           'fs.azure.account.oauth2.client.endpoint': f'https://login.microsoftonline.com/{directory_id}/oauth2/token'}

# Optionally, you can add <directory-name> to the source URI of your mount point.
dbutils.fs.mount(
    source = f'abfss://{container_name}@{account_name}.dfs.core.windows.net/',
    mount_point = f'/mnt/{container_name}',
    extra_configs = configs)

# list all items in container
dbutils.fs.ls(f'/mnt/{container_name}')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Unmount a mount point

# COMMAND ----------

# unmount container
dbutils.fs.unmount(f'/mnt/{container_name}')

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Access directly with service principal and OAuth 2.0
# MAGIC All code below needs to be run every time

# COMMAND ----------

# service principal details
client_id     = dbutils.secrets.get(scope='Analysts', key='SPID')  # 'cb90462d-aaae-4038-ac60-b8e090d404bb'
client_secret = dbutils.secrets.get(scope='Analysts', key='SPKey')  # '-nGdTkdSa-TN-4Fr7z0aBX3.k4Dct54SOX'
directory_id  = dbutils.secrets.get(scope='Analysts', key='DirectoryID')  # '600fc753-881c-4179-a38f-27842e71dc98'

# update spark configs
spark.conf.set('fs.azure.account.auth.type', 'OAuth')
spark.conf.set('fs.azure.account.oauth.provider.type', 'org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider')
spark.conf.set('fs.azure.account.oauth2.client.id', client_id)
spark.conf.set('fs.azure.account.oauth2.client.secret', client_secret)
spark.conf.set('fs.azure.account.oauth2.client.endpoint', f'https://login.microsoftonline.com/{directory_id}/oauth2/token')

# list all items in container / directory
dbutils.fs.ls('abfss://ecovacs@storagejamesleslie.dfs.core.windows.net/BestBuy/')

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Using dbutils

# COMMAND ----------

dbutils.fs.help()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Listing files / directories

# COMMAND ----------

# Mount the "transactions container"
dbutils.fs.mount(
    source = f'abfss://transactions@{account_name}.dfs.core.windows.net/',
    mount_point = f'/mnt/transactions',
    extra_configs = configs)

# list all mounted directories
dbutils.fs.ls('/mnt')

# COMMAND ----------

# list all items in the 2020 directory
dbutils.fs.ls('/mnt/transactions/2020')

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Creating new directories

# COMMAND ----------

# create a new directory
dbutils.fs.mkdirs('/mnt/transactions/2020/p14')

# check that it appears in the mount point
dbutils.fs.ls('/mnt/transactions/2020')

# COMMAND ----------

# does it appear in the data lake too?
dbutils.fs.ls('abfss://transactions@storagejamesleslie.dfs.core.windows.net/2020/')

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Remove directory

# COMMAND ----------

# remove the new directory and all its contents
dbutils.fs.rm('/mnt/transactions/2020/p14', recurse=True)

# is it gone?
dbutils.fs.ls('abfss://transactions@storagejamesleslie.dfs.core.windows.net/2020/')