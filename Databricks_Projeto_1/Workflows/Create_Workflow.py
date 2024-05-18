# Databricks notebook source
import json
import requests

def create_job(username, token, base_url):
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
        "quartz_cron_expression": "49 0 4 * * ?",
        "timezone_id": "America/Sao_Paulo",
        "pause_status": "UNPAUSED"
      },
      "max_concurrent_runs": 1,
      "tasks": [
        {
          "task_key": "BeerPipeApiBreweries",
          "run_if": "ALL_SUCCESS",
          "notebook_task": {
            "notebook_path": "Databricks_Projeto_1/Notebooks/ApiBreweries/Pipeline__ApiBreweries",
            "source": "GIT"
          },
          "existing_cluster_id": "0516-195718-ycordw6n",
          "max_retries": 2,
          "min_retry_interval_millis": 0,
          "retry_on_timeout": false,
          "timeout_seconds": 0,
          "email_notifications": {
            "on_failure": [
              "{username}"
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
      "git_source": {
        "git_url": "https://github.com/MatheuzdtEn/ApiBreweries.git",
        "git_provider": "gitHub",
        "git_branch": "main"
      },
      "queue": {
        "enabled": true
      },
      "run_as": {
        "user_name": "{username}"
      }
    }
    """
    # Substituir placeholders no JSON pelo nome de usuário
    json_config = json_config.replace("{username}", username)

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
        # Obtendo o ID do job recém-criado
        job_id = response.json().get("job_id")
        # Iniciando o trigger do job recém-criado
        start_job_url = f"{base_url}/api/2.0/jobs/run-now"
        start_response = requests.post(start_job_url, headers=headers, json={"job_id": job_id})
        if start_response.status_code == 200:
            print("Trigger do job iniciado com sucesso!")
        else:
            print(f"Erro ao iniciar trigger do job: {start_response.status_code} - {start_response.text}")
    else:
        print(f"Erro ao criar o job: {response.status_code} - {response.text}")

    return response.json() if response.status_code == 200 else None
