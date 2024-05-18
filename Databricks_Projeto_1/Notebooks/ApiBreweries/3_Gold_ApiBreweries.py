# Databricks notebook source
from pyspark.sql.functions import count, col

# COMMAND ----------

# Lê os dados:
df_prata = spark.read.table("prata.beers")

# Cria a visualização agregada retornando a contagem de tipo/estado:
df_final = df_prata.groupBy("brewery_type", "state").agg(count(col("state")).alias("Contagem"))

# COMMAND ----------

# Verifica o schema e cria se nao existir:
if not spark.catalog._jcatalog.databaseExists("ouro"):
    spark.sql("CREATE DATABASE ouro")

# Salvar particionado como Delta na camada 'prata'
df_final.write.mode("overwrite").format("delta").saveAsTable("ouro.beers")
