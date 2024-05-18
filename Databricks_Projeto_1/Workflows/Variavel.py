# Databricks notebook source
# MAGIC %run ./Create_Workflow

# COMMAND ----------

#Host_url
base_url = "<<SUA_URL_AQUI>>"

# Token de acesso: Definicôes --> Programador --> Gerar Token
token = "<<SEU_TOKEN_AQUI>>"

# Seu email, certifique de estar registrado no ambiente. 
username = "<<SEU_EMAIL_AQUI>>"

# COMMAND ----------

create_job(username, token, base_url)
