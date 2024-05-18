# Databricks notebook source
# Caminho da tabela na camada bronze:
table_name = "bronze.beers"

# Lê os dados da tabela no esquema bronze
df_read = spark.read.table(table_name)

# Verifica se o esquema 'prata' existe, e cria:
if not spark.catalog._jcatalog.databaseExists("prata"):
    spark.sql("CREATE DATABASE prata")

# Salvando com particionado na camada 'prata':
df_read.write.mode("overwrite").partitionBy("state").format("delta").saveAsTable("prata.beers")

# Confirmando que foi criado
spark.catalog.listTables("prata")
