# Databricks notebook source
configs = {"fs.azure.account.auth.type": "OAuth",
"fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
"fs.azure.account.oauth2.client.id": "625a1007-0406-427d-a342-0e215d195e5f",
"fs.azure.account.oauth2.client.secret": 'd6C8Q~tvUPspMOr9RJqfBNJn792N4ewl9RqRScfV',
"fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/19a059ea-a65b-4856-9a0d-0391038281f0/oauth2/token"}

dbutils.fs.mount(
source = "abfss://tokyo-olympic-data@tokyoolympichelmi2.dfs.core.windows.net", # contrainer@storageacc
mount_point = "/mnt/tokyoolymic",
extra_configs = configs)

# COMMAND ----------

# MAGIC %fs
# MAGIC ls "/mnt/tokyoolymic"
# MAGIC

# COMMAND ----------

athletes = spark.read.format("csv").option("header", "true").load("/mnt/tokyoolymic/raw-data/athletes.csv")
coaches = spark.read.format("csv").option("header", "true").load("/mnt/tokyoolymic/raw-data/coaches.csv")
medals = spark.read.format("csv").option("header", "true").load("/mnt/tokyoolymic/raw-data/medals.csv")
entriesgender = spark.read.format("csv").option("header", "true").load("/mnt/tokyoolymic/raw-data/entriesgender.csv")
teams = spark.read.format("csv").option("header", "true").load("/mnt/tokyoolymic/raw-data/teams.csv")

athletes.show()
coaches.show()
medals.show()
entriesgender.show()
teams.show()

# COMMAND ----------

athletes.printSchema()
coaches.printSchema()
medals.printSchema()
entriesgender.printSchema()
coaches.printSchema()

# COMMAND ----------


from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType, DoubleType, BooleanType, DateType
entriesgender = entriesgender.withColumn("Female",col("Female").cast(IntegerType()))\
    .withColumn("Male",col("Male").cast(IntegerType()))\
    .withColumn("Total",col("Total").cast(IntegerType()))


# COMMAND ----------

entriesgender.printSchema()

# COMMAND ----------



# COMMAND ----------

# MAGIC %fs
# MAGIC ls "/mnt/tokyoolymic"

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

athletes = spark.read.option("header", "true").option("inferSchema", "true").csv("/mnt/tokyoolymic/raw-data/athletes.csv")
athletes.show()
athletes.printSchema()

# COMMAND ----------

medals = spark.read.option("header", "true").option("inferSchema", "true").csv("/mnt/tokyoolymic/raw-data/medals.csv")
medals.show()
medals.printSchema()

# COMMAND ----------

coaches = spark.read.option("header", "true").option("inferSchema", "true").csv("/mnt/tokyoolymic/raw-data/coaches.csv")
coaches.show()
coaches.printSchema()

teams = spark.read.option("header", "true").option("inferSchema", "true").csv("/mnt/tokyoolymic/raw-data/teams.csv")
teams.show()
teams.printSchema()

entriesgender = spark.read.option("header", "true").option("inferSchema", "true").csv("/mnt/tokyoolymic/raw-data/entriesgender.csv")
entriesgender.show()
entriesgender.printSchema()



# COMMAND ----------

#Find the top countries with the highest number of goal medals
top_gold_medal_countries = medals.orderBy("Gold", ascending=False).select("Team_Country","Gold").show()

# COMMAND ----------



# COMMAND ----------


