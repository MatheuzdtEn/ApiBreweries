# Databricks notebook source
from pyspark.sql.types import StructType, StructField, StringType

# COMMAND ----------

import requests

url = "https://api.openbrewerydb.org/breweries"

# Fazer a requisição à API e obter os dados
response = requests.get(url)

if response.status_code == 200:
    data = response.json()
else:
    print("Erro ao obter os dados da API:", response.status_code)
    data = None

# COMMAND ----------

# Define o esquema com o tipo de dados apropriado
schema = StructType([
    StructField("id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("brewery_type", StringType(), True),
    StructField("street", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("postal_code", StringType(), True),
    StructField("country", StringType(), True),
    StructField("longitude", StringType(), True),
    StructField("latitude", StringType(), True),
    StructField("phone", StringType(), True),
    StructField("website_url", StringType(), True),
])

# Cria o DataFrame usando o esquema especificado
df = spark.createDataFrame(data, schema)

# COMMAND ----------

# Verifica se o esquema 'bronze' existe, e cria se não existir
if not spark.catalog._jcatalog.databaseExists("bronze"):
    spark.sql("CREATE DATABASE bronze")

# Salve a tabela 'beers' no esquema:
table_name = "bronze.beers"
df.write.mode("overwrite").saveAsTable(table_name)

# Confirme que a tabela foi criada
spark.catalog.listTables("bronze")

