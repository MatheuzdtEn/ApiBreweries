# BEES Data Engineering – Breweries (DATABRICKS)

## Objetivo
Este projeto visa consumir dados da API Brewery, transformá-los e armazená-los em um data lake seguindo a arquitetura de medallion com três camadas: bronze, prata e ouro. A solução inclui um workflow completo criado no Databricks, utilizando notebooks para cada camada do data lake.

## Estrutura do Projeto
O projeto está dividido em três notebooks, cada um representando uma camada da arquitetura de medallion:
1. **Bronze Layer**: Armazena os dados brutos da API.
- **Descrição**: Este notebook consome dados da API Open Brewery DB e os armazena na camada bronze do data lake.
- **Armazenamento**: Os dados são armazenados no catálogo do Databricks sob o nome `bronze.beers`.

2. **Silver Layer**: Transforma os dados para um formato colunar e particiona por localização.
- **Descrição**: Este notebook transforma os dados da camada bronze, convertendo-os para o formato delta e particionando por localização.
- **Transformações**: Limpeza e normalização dos dados, remoção de duplicatas e particionamento.
- **Armazenamento**: Os dados transformados são armazenados no catálogo do Databricks sob o nome `silver.beers`.
  
3. **Gold Layer**: Cria uma visualização agregada com a quantidade de cervejarias por tipo e localização.
- **Descrição**: Este notebook cria uma visualização agregada a partir dos dados da camada prata, calculando a quantidade de cervejarias por tipo e localização.
- **Transformações**: Agregação dos dados por tipo e localização.
- **Armazenamento**: A visualização agregada é armazenada no catálogo do Databricks sob o nome `gold.beers`.

### Notebooks Auxiliares:
- **Pipeline__ApiBreweries.py**: Este notebook fica responsável por executar os três notebooks das camadas, possibilitando uma visão completa do pipeline.
- **Create_Workflow.py**: Este notebook contém o Script para criar o workflow no ambiente Databricks. (Utiliza-se das API's do databricks para encontrar o ID_Cluster, criar o Job e iniciar o Job)
- **Variavel.py**: Este notebook será utilizado pelo usuário para incluir os parâmetros de criação do Workflow.


##COMO DAR INICIO:

### 1. Criar um Ambiente Databricks
- Certifique-se de ter um ambiente Databricks configurado. Se você ainda não tem, siga as instruções (https://docs.databricks.com/getting-started/index.html) para criar um.

- Em sua conta Azure, procure por Databricks, clique em criar.

![image](https://github.com/MatheuzdtEn/ApiBreweries/assets/106482156/6bca7aa8-bc11-4dc8-a83a-df3c5ca28a51)


### 2. Criar um Cluster
- Caso ainda não tenha um cluster criado, vá até a seção Clusters e crie um novo cluster. As instruções para criar um cluster podem ser encontradas.(https://docs.databricks.com/clusters/create.html).

- Caminho para criação do cluster
  
![image](https://github.com/MatheuzdtEn/ApiBreweries/assets/106482156/03b1eab4-9cd7-4e9a-9845-900b3286d9d6)

### 3. Clonar o Repositório do GitHub
- Vá até a seção Repos no Databricks e clone este repositório:
    git clone: https://github.com/MatheuzdtEn/ApiBreweries.git

- Caminho para criação do Repos:
  
![image](https://github.com/MatheuzdtEn/ApiBreweries/assets/106482156/8cff904e-ca4b-42f9-b533-d6b4700427f6)

### 4. Vá até o notebook Variavel.py e coloque as variáveis/Credenciais para criação do Job no WORKFLOW.

- Variaveis:

  - `URL Base do seu databricks`
![image](https://github.com/MatheuzdtEn/ApiBreweries/assets/106482156/99212dcf-48f1-45cf-b06f-649ab6107f21)


  - `Token, disponivel no menu do usuário na aba programador`
 

![image](https://github.com/MatheuzdtEn/ApiBreweries/assets/106482156/fe93502d-a036-44d9-9af5-e3d72e33dfd0)

  - `Username, email do usuário`
 

![image](https://github.com/MatheuzdtEn/ApiBreweries/assets/106482156/e6a22145-5261-4377-b424-4ab63ad07e25)


Obs: Caso não tenha seguido os passos 1 e 2. Certifique-se de ter as permissões necessários do usuário.

### 5. Executar o Notebook Variavel.py

### 6. Verificar os Resultados
- Após a conclusão do workflow, os dados estarão disponíveis no catálogo do Databricks com os seguintes nomes:
    - `bronze.beers`
    - `silver.beers`
    - `gold.beers`
    - 
- Todos os dados estão no formato Delta para otimizar a leitura e consulta. Os logs também ficam disponíveis para visualizaçã e podem ser vistos no catálogo.

- O workflow cobre todas as etapas desde a extração dos dados da API até a criação da visualização agregada e possibilita realizar 
o monitoramento e Alerta de falha.Em caso de falha será enviado um e-mail para o usuário,  três tentativas serão realizadas.Acompanhe também o tempo de execução do pipeline.

- Todos os notebooks foram testados e estão funcionando conforme o esperado.

