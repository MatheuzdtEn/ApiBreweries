# Databricks notebook source
import requests
import json

# COMMAND ----------

# URL base da API do Databricks
base_url = "https://adb-1314509360849583.3.azuredatabricks.net/api/2.0"

# Token de acesso
token = "dsapiad615f0f444ed1fd0b49e86ee97dc807"

# Caminho para o arquivo JSON no Databricks File System (DBFS)
workspace_path = "/Workspace/Repos/matheusglreis_96@hotmail.com/ApiBreweries/Workflows/Pipe_ApiBreweries/Pipe_ApiBreweries.json"

# Leitura do arquivo JSON
# Se o arquivo JSON estiver no seu sistema local, use o caminho do sistema de arquivos local
# Se o arquivo estiver no DBFS, você precisará baixá-lo primeiro. Para simplificação, vamos assumir que ele está no local.
with open("Pipe_ApiBreweries.json", "r") as f:
    job_config = json.load(f)

# Cabeçalho de autenticação
headers = {
    "Authorization": f"Bearer {token}",
    "Content-Type": "application/json"
}

# Endpoint para criar o job
create_job_url = f"{base_url}/jobs/create"

# Requisição para criar o job
response = requests.get(create_job_url, headers=headers, json=job_config)

# Verifique a resposta
if response.status_code == 200:
    print("Job criado com sucesso!")
    print(response.json())
else:
    print(f"Erro: {response.status_code} - {response.text}")
