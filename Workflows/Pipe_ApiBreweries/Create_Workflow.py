# Databricks notebook source
import requests
import json

# COMMAND ----------

# MAGIC %run /Workspace/Workflows/Variavel

# COMMAND ----------


# Caminho para o arquivo JSON no Databricks File System (DBFS)
workspace_path = "./Pipe_ApiBreweries.json"

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
create_job_url = f"{base_url}/api/2.0/jobs/create"

# Requisição para criar o job
response = requests.post(create_job_url, headers=headers, json=job_config)

# Verifique a resposta
if response.status_code == 200:
    print("Job criado com sucesso!")
    print(response.json())
else:
    print(f"Erro: {response.status_code} - {response.text}")
