# Databricks notebook source
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import regexp_replace
import requests

# COMMAND ----------

#url da API
url = "https://api.openbrewerydb.org/breweries"

# Fazer a requisição à API e obter os dados
response = requests.get(url)

if response.status_code == 200:
    data = response.json()
else:
    print("Erro ao obter os dados da API:", response.status_code)
    data = None

# COMMAND ----------

# Define o esquema manualmente, garantindo o tipo de schema necessário:
schema = StructType([
    StructField("id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("brewery_type", StringType(), True),
    StructField("address_1", StringType(), True),
    StructField("address_2", StringType(), True),
    StructField("address_3", StringType(), True),
    StructField("street", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state_province", StringType(), True),
    StructField("postal_code", StringType(), True),
    StructField("country", StringType(), True),
    StructField("longitude", StringType(), True),
    StructField("latitude", StringType(), True),
    StructField("phone", StringType(), True),
    StructField("website_url", StringType(), True),
    StructField("state", StringType(), True),
])

# Cria o DataFrame usando o esquema especificado:
df = spark.createDataFrame(data, schema)

# Garante a qualidade das colunas que serão utilizadas:
df = df.withColumn("state", regexp_replace("state", " ", "")) \
       .withColumn("brewery_type", regexp_replace("brewery_type", " ", ""))

# COMMAND ----------

# Verificando se o esquema 'bronze' existe, e cria se não existir:
if not spark.catalog._jcatalog.databaseExists("bronze"):
    spark.sql("CREATE DATABASE bronze")

# Salvando a tabela 'beers' no esquema bronze:
table_name = "bronze.beers"
df.write.mode("overwrite").saveAsTable(table_name)

# Confirmando que a tabela foi criada
spark.catalog.listTables("bronze")
