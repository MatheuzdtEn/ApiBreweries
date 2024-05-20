# BEES Data Engineering – Breweries (AIRFLOW-DOCKER)

## Objetivo do Projeto

O objetivo deste projeto é implementar uma pipeline de ETL (Extract, Transform, Load) usando Apache Airflow para automatizar a coleta, transformação e armazenamento de dados. O pipeline extrai dados da API Brewery, transforma e persiste os dados processados em um data lake em três camadas: bronze, silver e gold.

## Estrutura do Projeto:

#### Camada Bronze
- **Extrair Dados da API**: A pipeline extrai dados da API Brewery com paginação.

- **Persistir Dados na Camada Bronze**: Os dados extraídos são salvos em arquivos JSON na camada bronze.

#### Camada Silver
- **Ler Dados da Camada Bronze**: A pipeline lê os dados salvos na camada bronze.

- **Transformar Dados**: Os dados são transformados, adicionando uma coluna de data de processamento e substituindo espaços em branco por underscores nas colunas `country` e `state`.

- **Persistir Dados na Camada Silver**: Os dados transformados são salvos em arquivos Parquet particionados pela coluna `dt_process`.

#### Camada Gold
- **Ler Dados da Camada Silver**: A pipeline lê os dados salvos na camada silver.

- **Agregar Dados por Localização**: Os dados são agregados por `state` e `brewery_type`, contando o número de cervejarias por tipo e localização.

- **Persistir Dados na Camada Gold**: Os dados agregados são salvos em arquivos Parquet na camada gold.


##### O script do código esta disponivel em DAGS ---> `pipeline_Cervejaria.py`

# COMO DAR INICIO:



### Pré-requisitos

- **Docker**: Certifique-se de ter o Docker instalado em sua máquina.
- **Editor de código**: Utilizado o VSCODE para analise do código.
### Passos para Execução

1. **Clone o repositório**:
    ```sh
    git clone https://github.com/MatheuzdtEn/ApiBreweries.git
    
    ```
2. **Inicie o Airflow com Docker**:
    - Abra o terminal (cmd) e execute o comando, para dar inicio ao ambiente:
      ```sh
      docker-compose up airflow-init
      ```
    - Após a finalização do comando anterior, garantindo que o ambiente está Ok:
      ```sh
      docker-compose up
      ```

3. **Acesse o Airflow**:
    - Abra o navegador e acesse `http://localhost:8080`.
    - Utilize as credenciais:
        - Usuário: `airflow`
        - Senha: `airflow`

4. **Execute o DAG**:
    - Procure pelo pipeline com o nome `pipeline_Cervejaria`.
    - Ative e execute o DAG.

2. **Finalize o Docker**:
    - Abra o terminal (cmd) e execute o comando, para dar fim ao ambiente:
      ```sh
      docker-compose down
      ```
   


