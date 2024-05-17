# Databricks notebook source
import json
import requests

def create_job(email, token, base_url):
    # JSON definido como uma string e atribuído a uma variável
    json_config = """
    {
      "name": "Pipe_ApiBreweries",
      "email_notifications": {
        "no_alert_for_skipped_runs": false
      },
      "webhook_notifications": {},
      "timeout_seconds": 0,
      "schedule": {
        "quartz_cron_expression": "17 1 5 * * ?",
        "timezone_id": "America/Sao_Paulo",
        "pause_status": "UNPAUSED"
      },
      "max_concurrent_runs": 1,
      "tasks": [
        {
          "task_key": "Pipe_ApiBreweries",
          "run_if": "ALL_SUCCESS",
          "notebook_task": {
            "notebook_path": "/Workspace/AMBEV/ApiBreweries/Pipeline__ApiBreweries",
            "source": "WORKSPACE"
          },
          "existing_cluster_id": "0516-195718-ycordw6n",
          "max_retries": 2,
          "min_retry_interval_millis": 900000,
          "retry_on_timeout": false,
          "timeout_seconds": 0,
          "email_notifications": {
            "on_failure": [
              "{email}"
            ]
          },
          "notification_settings": {
            "no_alert_for_skipped_runs": false,
            "no_alert_for_canceled_runs": false,
            "alert_on_last_attempt": true
          },
          "webhook_notifications": {}
        }
      ],
      "queue": {
        "enabled": true
      },
      "run_as": {
        "user_name": "{email}"
      }
    }
    """
    # Substituir placeholders no JSON pela variável email
    json_config = json_config.replace("{email}", email)

    # Carregar a string JSON em um dicionário Python
    job_config = json.loads(json_config)

    # Cabeçalho de autenticação
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

    # Endpoint para criar o job
    create_job_url = f"{base_url}/api/2.0/jobs/create"

    # Requisição para criar o job
    response = requests.post(create_job_url, headers=headers, json=job_config)

    # Verificar a resposta
    if response.status_code == 200:
        print("Job criado com sucesso!")
        return response.json()
    else:
        print(f"Erro: {response.status_code} - {response.text}")
        return None

