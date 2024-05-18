# Databricks notebook source
import requests
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import regexp_replace

# COMMAND ----------

# URL da API
url = "https://api.openbrewerydb.org/breweries"

def DadosDaCervejarias(base_url):
    all_data = []
    page = 1

    while True:
        # Faz a requisição à API com paginação
        response = requests.get(f'{base_url}?page={page}&per_page=50')
        if response.status_code == 200:
            data = response.json()
            if not data:  # Se não houver mais dados, sai do loop
                break
            all_data.extend(data)
            page += 1
        else:
            print("Erro ao obter os dados da API:", response.status_code)
    if not all_data:
        print("Nenhum dado foi recuperado.")
        return None
    
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
    df = spark.createDataFrame(all_data, schema)

    # Garante a qualidade das colunas que serão utilizadas:
    df = df.withColumn("state", regexp_replace("state", " ", "")) \
           .withColumn("brewery_type", regexp_replace("brewery_type", " ", ""))
    
    # Contabiliza a quantidade de linhas ingeridas
    num_rows = df.count()
    print(f"Quantidade de linhas ingeridas: {num_rows}")
    return df

# COMMAND ----------

# Executa a funcao:
df = DadosDaCervejarias(url)

# COMMAND ----------

# Verificando se o esquema 'bronze' existe, e cria se não existir:
if not spark.catalog._jcatalog.databaseExists("bronze"):
    spark.sql("CREATE DATABASE bronze")

# Salvando a tabela 'beers' no esquema bronze:
table_name = "bronze.beers"
df.write.mode("overwrite").saveAsTable(table_name)

# Confirmando que a tabela foi criada
spark.catalog.listTables("bronze")
