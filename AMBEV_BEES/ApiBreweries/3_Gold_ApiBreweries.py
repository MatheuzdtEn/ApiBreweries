# Databricks notebook source
from pyspark.sql.functions import count, row_number
from pyspark.sql.window import Window

# COMMAND ----------

# Lê os dados:
df_prata = spark.read.table("prata.beers")

# COMMAND ----------

# Cria uma visualização agregada com a quantidade de cervejarias por tipo e localização:
df_aggregated = df_prata.groupBy("brewery_type", "city").agg(count("id").alias("count"))

# Vai Adicionar uma coluna de classificação para ordenar os resultados:
window_spec = Window.partitionBy("brewery_type").orderBy(df_aggregated["count"].desc())
df_ranked = df_aggregated.withColumn("rank", row_number().over(window_spec))

# Seleciona apenas as linhas classificadas como 1 (maior contagem) para cada tipo de cervejaria:
df_final = df_ranked.filter(df_ranked["rank"] == 1).drop("rank")

# COMMAND ----------

# Verifica o schema e cria se nao existir:
if not spark.catalog._jcatalog.databaseExists("ouro"):
    spark.sql("CREATE DATABASE ouro")

# Salvar particionado como Delta na camada 'prata'
df_final.write.mode("overwrite").format("delta").saveAsTable("ouro.beers")
