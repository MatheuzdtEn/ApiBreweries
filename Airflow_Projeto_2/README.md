# BEES Data Engineering – Breweries (AIRFLOW-DOCKER)

## Objetivo do Projeto

O objetivo deste projeto é implementar uma pipeline de ETL (Extract, Transform, Load) usando Apache Airflow para automatizar a coleta, transformação e armazenamento de dados. O pipeline extrai dados da API Brewery, transforma e persiste os dados processados em um data lake em três camadas: bronze, silver e gold.

## Estrutura do Projeto:

#### Camada Bronze
- **Extrair Dados da API**: O pipeline extrai dados da API Brewery com paginação.

- **Persistir Dados na Camada Bronze**: Os dados extraídos são salvos em arquivos JSON na camada bronze.

#### Camada Silver
- **Ler Dados da Camada Bronze**: A pipeline lê os dados salvos na camada bronze.

- **Transformar Dados**: Os dados são transformados, adicionando uma coluna de data de processamento e substituindo espaços em branco por underscores nas colunas `country` e `state`.

- **Persistir Dados na Camada Silver**: Os dados transformados são salvos em arquivos Parquet particionados pela coluna `dt_process` e `state`.

#### Camada Gold
- **Ler Dados da Camada Silver**: O pipeline lê os dados salvos na camada silver.

- **Agregar Dados por Localização**: Os dados são agregados por `state` e `brewery_type`, contando o número de cervejarias por tipo e localização.

- **Persistir Dados na Camada Gold**: Os dados agregados são salvos em arquivos Parquet na camada gold.


##### O script do código esta disponivel em DAGS ---> `pipeline_Cervejaria.py`

# COMO DAR INICIO:



### Pré-requisitos

- **Docker**: Certifique-se de ter o Docker instalado em sua máquina.
- **Editor de código**: Qualquer editor de código para visualização.
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
    - Após a finalização do comando anterior, inicie e execute os serviços definidos do docker-compose.yml:
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
      
Pipeline criado e executado com sucesso.

![image](https://github.com/MatheuzdtEn/ApiBreweries/assets/106482156/ca7b8580-88de-40e3-9531-1da3e4352644)



Dados persistidos com sucesso.

![image](https://github.com/MatheuzdtEn/ApiBreweries/assets/106482156/db01f00c-478d-49bb-95e9-b7aa657a5f71)




5. **Finalize o Docker**:
    - Abra o terminal (cmd) e execute o comando, para dar fim ao ambiente:
      ```sh
      docker-compose down
      ```

# Explicação dos códigos:

#### - Função Global:
DataDeProcessamento - Utilizada para identiicação do dia de processamento.

![image](https://github.com/MatheuzdtEn/ApiBreweries/assets/106482156/193946e6-3ba0-455c-b898-64142b57d96e)


### 1. BRONZE:
#### - extrair_dados_da_api - Extrai a API paginada.
![image](https://github.com/MatheuzdtEn/ApiBreweries/assets/106482156/3c0d61b1-073c-41e4-8e51-58e844feef25)


#### - persistir_dados_bronze - Salva os dados na camada Bronze
![image](https://github.com/MatheuzdtEn/ApiBreweries/assets/106482156/02c42115-a8da-440f-b043-ead5682a6d51)

### 2.PRATA
#### - ler_dados_bronze - lê dados na bronze, garantindo os schemas corretamente.

![image](https://github.com/MatheuzdtEn/ApiBreweries/assets/106482156/7e215326-9c6e-45a1-80ab-00365eab8708)

#### - transformar_dados - Garante que não existe espacamento indevidos, inclui a coluna de data.
![image](https://github.com/MatheuzdtEn/ApiBreweries/assets/106482156/d84f79f0-f8ae-47be-ba1d-b219376bc195)

#### - persistir_dados_silver - Dados carregados na SILVER. Fiz a inclusão da verificação das colunas que serão particionadas na gravação.
![image](https://github.com/MatheuzdtEn/ApiBreweries/assets/106482156/a7cb69e4-84bd-4ee1-be8d-841e29a57196)

### 3.GOLD
#### - ler_dados_silver - Leitura dos dados da PRATA. Incluso no código a leitura de toda pasta e subpasta da Silver, garantindo que o todas as partições serão lidas.
![image](https://github.com/MatheuzdtEn/ApiBreweries/assets/106482156/1e055eed-927c-4165-b925-3e40f9a2e3fa)

#### - agregar_dados_por_localizacao - GroupBy realizado por ['state', 'brewery_type]
![image](https://github.com/MatheuzdtEn/ApiBreweries/assets/106482156/5c04c02e-7257-43d1-8d39-f9930971f719)

### 4.DAG 
#### - Orquestração paralela, agendada diariamente as 6:00 AM
![image](https://github.com/MatheuzdtEn/ApiBreweries/assets/106482156/016c5347-3ca5-4bdf-b42d-c2a61e316d9b)







   


