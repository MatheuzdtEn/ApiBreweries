from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator
import pandas as pd
import requests
import os

# Função para adicionar a coluna 'Data_processada'
def DataDeProcessamento(dataframe):
    df = dataframe
    # Obter a data atual
    dt = datetime.now()
    # Formatar a data
    format_dt = dt.strftime('%Y%m%d')
    # Adicionar a coluna 'data processada'
    df = df.assign(dt_process=format_dt)
    return df

# Extrai os dados da API com paginação
def extrair_dados_da_api():
    url_base = "https://api.openbrewerydb.org/breweries"
    todos_os_dados = []
    pagina = 1

    while True:
        resposta = requests.get(f'{url_base}?page={pagina}&per_page=50')
        if resposta.status_code == 200:
            dados = resposta.json()
            if not dados:
                break
            todos_os_dados.extend(dados)
            pagina += 1
        else:
            print("Erro ao buscar dados da API:", resposta.status_code)
            break
    if not todos_os_dados:
        print("Nenhum dado recuperado.")
        return None
    df = pd.DataFrame.from_dict(todos_os_dados)
    return df

# Persistir dados na camada bronze
def persistir_dados_bronze(ti):
    df = ti.xcom_pull(task_ids='extrair_dados_da_api')
    dt = datetime.now().strftime('%Y%m%d')
    caminho = f"datalake/bronze/{dt}.json"
    os.makedirs(os.path.dirname(caminho), exist_ok=True)
    df.to_json(caminho, orient='records', lines=True)
    print(f"Dados persistidos na camada bronze em {caminho}")

# Ler dados da camada bronze
def ler_dados_bronze():
    dt = datetime.now().strftime('%Y%m%d')
    caminho = f'datalake/bronze/{dt}.json'
    if not os.path.exists(caminho):
        raise FileNotFoundError(f"Caminho {caminho} não existe")
    print(f"Lendo dados de {caminho}")
    esquema = {
        "id": str,
        "name": str,
        "brewery_type": str,
        "address_1": str,
        "address_2": str,
        "address_3": str,
        "city": str,
        "state_province": str,
        "postal_code": str,
        "country": str,
        "longitude": str,
        "latitude": str,
        "phone": str,
        "website_url": str,
        "state": str,
        "street": str
    }
    df = pd.read_json(caminho, dtype=esquema, lines=True)
    return df

# Transformar dados
def transformar_dados(ti):
    df = ti.xcom_pull(task_ids='ler_dados_bronze')
    df = DataDeProcessamento(df)
    df['country'] = df['country'].str.replace(' ', '_')
    df['state'] = df['state'].str.replace(' ', '_')
    return df

# Persistir dados na camada silver
def persistir_dados_silver(ti):
    df = ti.xcom_pull(task_ids='transformar_dados')
    # Garantir que o DataFrame contenha as colunas necessárias
    colunas_necessarias = ['dt_process']
    for col in colunas_necessarias:
        if col not in df.columns:
            raise ValueError(f"Coluna {col} está faltando no DataFrame")

    # Garantir que o diretório de destino exista
    diretorio_destino = 'datalake/silver'
    os.makedirs(diretorio_destino, exist_ok=True)

    # Persistir o DataFrame como um arquivo Parquet particionado usando pyarrow
    print(f"Dados a serem persistidos em {diretorio_destino} com colunas de partição {colunas_necessarias}")
    print(df.head())
    df.to_parquet(diretorio_destino, partition_cols=colunas_necessarias, engine='pyarrow', compression='gzip')

    # Verificar se o arquivo foi escrito corretamente
    caminho_parquet = f'{diretorio_destino}/dt_process={df["dt_process"].iloc[0]}'
    if os.path.exists(caminho_parquet):
        print(f"Dados persistidos com sucesso em {caminho_parquet}")
    else:
        raise FileNotFoundError(f"Falha ao escrever dados em {caminho_parquet}")

# Ler dados da camada silver
def ler_dados_silver():
    diretorio_raiz = 'datalake/silver'  # Caminho raiz onde estão as subpastas
    # Lista para armazenar dataframes
    dataframes = []
    # Percorrer todas as subpastas e arquivos
    for subdir, dirs, files in os.walk(diretorio_raiz):
        for file in files:
            if file.endswith('.parquet'):
                caminho_arquivo = os.path.join(subdir, file)
                print(f"Lendo arquivo: {caminho_arquivo}")
                try:
                    # Ler o arquivo parquet e adicionar à lista de dataframes
                    df = pd.read_parquet(caminho_arquivo, engine='pyarrow')
                    dataframes.append(df)
                except Exception as e:
                    print(f"Erro ao ler {caminho_arquivo}: {e}")

    if not dataframes:
        raise ValueError("Nenhum dataframe para concatenar. Verifique os arquivos de entrada.")
    
    # Concatenar todos os dataframes em um único dataframe
    df_final = pd.concat(dataframes, ignore_index=True)
    return df_final

# Agregar dados da cervejaria por localização (state_province, brewery_type) e salvar diretamente na camada gold
def agregar_dados_por_localizacao(ti):
    df = ti.xcom_pull(task_ids='ler_dados_silver')
    vw_localizacao = df.groupby(['state_province', 'brewery_type'])['id'].count().reset_index()
    vw_localizacao.columns = ['state_province', 'brewery_type', 'number']
    vw_localizacao = vw_localizacao.sort_values(by='number', ascending=False)
    vw_localizacao = DataDeProcessamento(vw_localizacao)

    # Salvando o resultado:
    vw_localizacao.to_parquet('datalake/gold/vw_GroupyBy_TipoLocalizacao.parquet', engine='pyarrow', compression='gzip')
    print(f"Dados agregados salvos com sucesso em 'datalake/gold/vw_GroupyBy_TipoLocalizacao.parquet'")


# Definição do DAG
with DAG('pipeline_Cervejaria', start_date=datetime(2024, 4, 15, 6, 0, 0), schedule_interval='@daily', catchup=False) as dag:
    
    # Tarefa para extrair dados da API
    tarefa_extrair_dados_da_api = PythonOperator(
        task_id='extrair_dados_da_api',
        python_callable=extrair_dados_da_api
    )
    
    # Tarefa para persistir dados na camada bronze
    tarefa_persistir_dados_bronze = PythonOperator(
        task_id='persistir_dados_bronze',
        python_callable=persistir_dados_bronze
    )
    
    # Tarefa para ler dados da camada bronze
    tarefa_ler_dados_bronze = PythonOperator(
        task_id='ler_dados_bronze',
        python_callable=ler_dados_bronze
    )
    
    # Tarefa para transformar dados
    tarefa_transformar_dados = PythonOperator(
        task_id='transformar_dados',
        python_callable=transformar_dados
    )
    
    # Tarefa para persistir dados na camada silver
    tarefa_persistir_dados_silver = PythonOperator(
        task_id='persistir_dados_silver',
        python_callable=persistir_dados_silver
    )
    
    # Tarefa para ler dados da camada silver
    tarefa_ler_dados_silver = PythonOperator(
        task_id='ler_dados_silver',
        python_callable=ler_dados_silver
    )
    
    # Tarefa para agregar dados por localização e salvar diretamente na camada gold
    tarefa_agregar_dados_por_TipoLocalizacao = PythonOperator(
        task_id='agregar_dados_por_localizacao',
        python_callable=agregar_dados_por_localizacao
    )
    
    # Orquestração
    tarefa_extrair_dados_da_api >> tarefa_persistir_dados_bronze >> tarefa_ler_dados_bronze >> tarefa_transformar_dados >> tarefa_persistir_dados_silver >> tarefa_ler_dados_silver >> tarefa_agregar_dados_por_TipoLocalizacao
