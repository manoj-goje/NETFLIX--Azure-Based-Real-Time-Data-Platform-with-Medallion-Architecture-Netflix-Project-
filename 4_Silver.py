# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import*

# COMMAND ----------

df  = spark.read.format("delta")\
    .option("header", True)\
        .option("inferSchema", True)\
        .load("abfss://bronze@netfilxprojectproject.dfs.core.windows.net/netflix_titles")

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC fixing null

# COMMAND ----------

df.display()

# COMMAND ----------

df = df.fillna({"duration_minutes": 0 , "duration_seasons" : 1})

# COMMAND ----------

df.display()

# COMMAND ----------

df = df.withColumn("duration_minutes",col("duration_minutes").cast(IntegerType()))\
    .withColumn("duration_seasons",col("duration_seasons").cast(IntegerType()))

# COMMAND ----------

df.printSchema

# COMMAND ----------

display(df)

# COMMAND ----------

df = df.withColumn("shortTitile",split(col("title"), ":")[0])

# COMMAND ----------

df.display()

# COMMAND ----------

df = df.withColumn("rating",split(col("rating"), "-").getItem(0))



# COMMAND ----------

df.display()

# COMMAND ----------



df = df.withColumn(
    "typeFlag",
    when(col("type") == "Movie", 1).when(col("type") == "Movie", 2).otherwise(0)
)
display(df)

# COMMAND ----------

from pyspark.sql.window import Window

# COMMAND ----------

df = df.withColumn("duration_ranking",dense_rank().over(Window.orderBy(col("duration_minutes").desc())))

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC if we not have able to understand spark over() then we can use our sql query for this but for performing sql quiries on dataframe we have to convert df to sql views

# COMMAND ----------

df.createOrReplaceTempView("netflix_view")

# COMMAND ----------

df_sql = spark.sql("select * from netflix_view")

# COMMAND ----------

display(df_sql)

# COMMAND ----------

df_vis = df.groupBy("type").agg(count("type").alias("total_count"))

# COMMAND ----------

df_vis.display()

# COMMAND ----------

df.write.format("delta")\
    .mode("overwrite")\
    .option("path","abfss://silver@netfilxprojectproject.dfs.core.windows.net/netflix_titles")\
    .save()

# COMMAND ----------

