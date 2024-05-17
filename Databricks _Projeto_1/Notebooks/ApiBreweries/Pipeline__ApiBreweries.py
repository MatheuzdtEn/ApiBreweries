# Databricks notebook source
path_notebook_databricks = './1_Bronze_ApiBreweries'
timeout_seconds = 1200
sent_vars = {'catalog': 'AMBEV','notebook':'1_Bronze_ApiBreweries', 'database': "bronze" }
dbutils.notebook.run( path_notebook_databricks , timeout_seconds , sent_vars )

# COMMAND ----------

path_notebook_databricks = './2_Silver_ApiBreweries'
timeout_seconds = 1200
sent_vars = {'catalog': 'AMBEV','catalog_bruto':'2_Silver_ApiBreweries', 'database': "prata" }
dbutils.notebook.run( path_notebook_databricks , timeout_seconds , sent_vars )

# COMMAND ----------

path_notebook_databricks = './3_Gold_ApiBreweries'
timeout_seconds = 1200
sent_vars = {'catalog': 'AMBEV','notebook':'3_Gold_ApiBreweries', 'database': "ouro" }
dbutils.notebook.run( path_notebook_databricks , timeout_seconds , sent_vars )
