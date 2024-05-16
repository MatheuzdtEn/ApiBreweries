# Databricks notebook source
# Especifique o caminho completo da tabela no formato "banco_de_dados.tabela"
table_name = "bronze.beers"

# Leia os dados da tabela no esquema bronze
df_read = spark.read.table(table_name)

# Verifique se o esquema 'bronze' existe, e crie se não existir
if not spark.catalog._jcatalog.databaseExists("prata"):
    spark.sql("CREATE DATABASE prata")

# Salvar particionado como Delta na camada 'prata'
df_read.write.mode("overwrite").partitionBy("city").format("delta").saveAsTable("prata.beers")

# COMMAND ----------

df_read.display()
