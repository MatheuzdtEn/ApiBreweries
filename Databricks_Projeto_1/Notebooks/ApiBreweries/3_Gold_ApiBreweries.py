# Databricks notebook source
from pyspark.sql.functions import count

# COMMAND ----------

# Lê os dados:
df_prata = spark.read.table("prata.beers")

# Cria a visualização agregada retornando a contagem de tipo/estado:
df_final = df_prata.groupBy("state", "brewery_type").agg(count("*").alias("count")).orderBy("count", ascending=False)

# COMMAND ----------

# Verifica o schema e cria se nao existir:
if not spark.catalog._jcatalog.databaseExists("ouro"):
    spark.sql("CREATE DATABASE ouro")

# Salvar particionado como Delta na camada 'prata'
df_final.write.mode("overwrite").option("mergeSchema", "true").format("delta").saveAsTable("ouro.beers")

# Confirmando que foi criado
spark.catalog.listTables("prata")
