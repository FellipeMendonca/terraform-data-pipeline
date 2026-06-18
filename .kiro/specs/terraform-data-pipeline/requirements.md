# Requirements Document

## Introduction

Este documento define os requisitos para uma Pipeline de Dados implementada com Terraform na AWS. A pipeline utiliza AWS Lambda para ingestão de dados da PokeAPI (pokeapi.co), AWS Glue para transformações seguindo a Arquitetura Medalhão (Bronze, Silver e Gold), AWS Step Functions para orquestração e GitHub Actions para CI/CD. Toda a infraestrutura é provisionada como código utilizando Terraform.

## Glossary

- **Pipeline**: Fluxo automatizado de ingestão, transformação e armazenamento de dados
- **Lambda_Function**: Função serverless AWS Lambda responsável pela ingestão de dados da PokeAPI
- **PokeAPI**: API REST pública (pokeapi.co) utilizada como fonte de dados externa da pipeline
- **S3_Bucket**: Serviço de armazenamento de objetos da AWS utilizado para persistir os dados em cada camada
- **Glue_Job**: Job do AWS Glue responsável por executar transformações de dados entre camadas
- **Glue_Catalog**: Catálogo de metadados do AWS Glue que registra tabelas e schemas dos dados
- **Step_Function**: Máquina de estados do AWS Step Functions que orquestra a execução da pipeline
- **Bronze_Layer**: Primeira camada da Arquitetura Medalhão contendo dados brutos ingeridos da fonte externa
- **Silver_Layer**: Segunda camada da Arquitetura Medalhão contendo dados limpos e padronizados
- **Gold_Layer**: Terceira camada da Arquitetura Medalhão contendo dados agregados e prontos para consumo
- **GitHub_Actions**: Plataforma de CI/CD do GitHub utilizada para deploy automatizado da infraestrutura
- **Branch_Main**: Branch principal do repositório Git responsável por gerenciar a infraestrutura do Ambiente_Prd
- **Branch_Develop**: Branch de desenvolvimento do repositório Git responsável por gerenciar a infraestrutura do Ambiente_Dev
- **Terraform**: Ferramenta de Infraestrutura como Código utilizada para provisionar todos os recursos AWS
- **Terraform_Workspace**: Recurso nativo do Terraform que permite gerenciar múltiplos estados de infraestrutura isolados dentro do mesmo código, utilizado para separar ambientes (dev, prd) na mesma conta AWS
- **Ambiente_Dev**: Ambiente de desenvolvimento da pipeline, identificado pelo workspace "dev" no Terraform
- **Ambiente_Prd**: Ambiente de produção da pipeline, identificado pelo workspace "prd" no Terraform
- **Terraform_Tfvars**: Arquivo de variáveis do Terraform (terraform.tfvars) que define valores específicos por ambiente
- **Medallion_Architecture**: Padrão de arquitetura de dados com camadas Bronze, Silver e Gold
- **Pytest**: Framework de testes unitários Python utilizado para validar o código da pipeline
- **Chispa**: Biblioteca Python para assertions de igualdade em DataFrames PySpark nos testes unitários
- **SparkSession_Local**: Sessão Spark executada localmente (local[*]) para testes unitários sem necessidade de cluster

## Requirements

### Requisito 1: Ingestão de Dados da PokeAPI via Lambda

**User Story:** Como engenheiro de dados, eu quero que uma função Lambda acesse dados da PokeAPI e salve no S3, para que os dados brutos de Pokémon estejam disponíveis para transformação.

#### Critérios de Aceitação

1. WHEN a Step_Function inicia a execução da pipeline, THE Lambda_Function SHALL realizar requisições HTTP GET ao endpoint /api/v2/pokemon da PokeAPI (pokeapi.co), percorrendo todas as páginas disponíveis até obter a lista completa de Pokémon
2. WHEN a Lambda_Function obtém dados da PokeAPI com sucesso, THE Lambda_Function SHALL salvar os dados em formato JSON no S3_Bucket na Bronze_Layer, com um arquivo por Pokémon obtido
3. WHEN a Lambda_Function salva dados no S3_Bucket, THE Lambda_Function SHALL organizar os arquivos utilizando particionamento por data no formato ano/mês/dia (YYYY/MM/DD) baseado na data de execução
4. IF a PokeAPI retorna um erro HTTP (status 4xx ou 5xx) ou não responde dentro de 30 segundos por requisição, THEN THE Lambda_Function SHALL registrar o erro no CloudWatch incluindo o endpoint chamado e o código de erro, e realizar até 3 tentativas com intervalo exponencial antes de marcar o Pokémon como falho
5. IF a Lambda_Function atinge 80% do seu limite de tempo de execução configurado, THEN THE Lambda_Function SHALL salvar no S3_Bucket os dados dos Pokémon já obtidos com sucesso, registrar no CloudWatch quais Pokémon não foram processados, e retornar status de falha parcial para a Step_Function
6. THE Lambda_Function SHALL respeitar os rate limits da PokeAPI realizando no máximo 100 requisições por minuto, com intervalo mínimo de 500 milissegundos entre requisições consecutivas
7. IF a Lambda_Function falha ao obter dados de um ou mais Pokémon individuais após as tentativas de retry, THEN THE Lambda_Function SHALL continuar processando os demais Pokémon, salvar os dados obtidos com sucesso na Bronze_Layer, e retornar para a Step_Function a lista de Pokémon que falharam

### Requisito 2: Transformação Bronze para Silver

**User Story:** Como engenheiro de dados, eu quero que os dados brutos sejam limpos e padronizados, para que a camada Silver contenha dados confiáveis e consistentes.

#### Critérios de Aceitação

1. WHEN a ingestão na Bronze_Layer é concluída com sucesso, THE Glue_Job SHALL iniciar a transformação dos dados da Bronze_Layer para a Silver_Layer
2. THE Glue_Job SHALL remover registros duplicados durante a transformação Bronze para Silver, utilizando o identificador único do Pokémon (id) como chave de deduplicação
3. THE Glue_Job SHALL padronizar tipos de dados durante a transformação Bronze para Silver, convertendo campos numéricos (height, weight, base_experience) para tipo inteiro, campos de texto (name) para lowercase, e campos de lista (types, abilities) para arrays tipados
4. WHEN a transformação Bronze para Silver é concluída, THE Glue_Job SHALL salvar os dados resultantes no S3_Bucket na Silver_Layer em formato Parquet
5. IF o Glue_Job de transformação Bronze para Silver falha, THEN THE Glue_Job SHALL registrar no CloudWatch a etapa de transformação que falhou, a mensagem de exceção e o timestamp do erro, e retornar status de falha para a Step_Function
6. THE Glue_Job SHALL tratar valores nulos ou inválidos durante a transformação Bronze para Silver, substituindo campos numéricos nulos por 0 e campos de texto nulos por string vazia
7. THE Glue_Job SHALL organizar os dados na Silver_Layer utilizando particionamento por tipo primário do Pokémon

### Requisito 3: Transformação Silver para Gold

**User Story:** Como engenheiro de dados, eu quero que os dados limpos sejam agregados e enriquecidos, para que a camada Gold contenha dados prontos para consumo analítico.

#### Critérios de Aceitação

1. WHEN a transformação para Silver_Layer é concluída com sucesso, THE Glue_Job SHALL iniciar a transformação dos dados da Silver_Layer para a Gold_Layer
2. THE Glue_Job SHALL aplicar agregações aos dados de Pokémon durante a transformação Silver para Gold, incluindo agrupamento por tipo de Pokémon e cálculo de estatísticas sumarizadas (contagem, média, mínimo e máximo dos atributos numéricos)
3. WHEN a transformação Silver para Gold é concluída, THE Glue_Job SHALL salvar os dados resultantes no S3_Bucket na Gold_Layer em formato Parquet, organizados por chave de agregação
4. IF o Glue_Job de transformação Silver para Gold falha, THEN THE Glue_Job SHALL registrar no CloudWatch a etapa de transformação que falhou, a mensagem de exceção e o timestamp do erro, e retornar status de falha para a Step_Function
5. THE Glue_Job SHALL garantir que os dados na Gold_Layer não contenham registros duplicados resultantes do processo de agregação

### Requisito 4: Registro no Catálogo do Glue

**User Story:** Como engenheiro de dados, eu quero que todas as camadas de dados estejam registradas no Catálogo do Glue, para que os dados sejam descobríveis e consultáveis via Athena.

#### Critérios de Aceitação

1. THE Terraform SHALL criar tabelas no Glue_Catalog para cada camada da Medallion_Architecture (Bronze, Silver e Gold), configurando o formato de serialização como JSON para a Bronze_Layer e Parquet para a Silver_Layer e Gold_Layer
2. WHEN um Glue_Job conclui uma transformação com sucesso, THE Glue_Job SHALL atualizar as colunas e tipos de dados da tabela correspondente no Glue_Catalog para refletir o schema dos dados escritos no S3_Bucket
3. THE Terraform SHALL configurar as tabelas no Glue_Catalog com colunas de partição por ano, mês e dia, correspondendo à estrutura de diretórios utilizada no S3_Bucket
4. THE Terraform SHALL configurar exatamente um database no Glue_Catalog para agrupar todas as tabelas da pipeline
5. IF a atualização de schema no Glue_Catalog falha durante a execução do Glue_Job, THEN THE Glue_Job SHALL registrar o erro e retornar status de falha para a Step_Function

### Requisito 5: Orquestração via Step Functions

**User Story:** Como engenheiro de dados, eu quero que a execução da pipeline seja orquestrada por uma Step Function, para que o fluxo seja controlado, monitorável e resiliente a falhas.

#### Critérios de Aceitação

1. THE Step_Function SHALL orquestrar a execução sequencial das etapas: ingestão Lambda, transformação Bronze-Silver e transformação Silver-Gold
2. WHEN uma etapa da pipeline falha, THE Step_Function SHALL interromper a execução das etapas subsequentes e transitar para um estado de falha que registra no CloudWatch o nome da etapa falha, a mensagem de erro e o timestamp
3. THE Step_Function SHALL expor status de execução consultável via API (em execução, concluída, falha) com timestamp de início e duração de cada etapa
4. WHEN a Step_Function conclui todas as etapas com sucesso, THE Step_Function SHALL transitar para um estado de sucesso que registra no CloudWatch o timestamp de conclusão e a duração total da execução
5. IF uma etapa reporta falha, THEN THE Step_Function SHALL disponibilizar no output do estado de falha informações de diagnóstico incluindo o nome da etapa que falhou, a mensagem de erro retornada e o ARN da execução para rastreabilidade
6. THE Step_Function SHALL configurar timeout máximo de execução para cada etapa individual, sendo 15 minutos para a Lambda de ingestão e 60 minutos para cada Glue_Job de transformação

### Requisito 6: Infraestrutura como Código com Terraform

**User Story:** Como engenheiro de DevOps, eu quero que toda a infraestrutura seja definida como código com Terraform, para que o ambiente seja reproduzível, versionável e auditável.

#### Critérios de Aceitação

1. THE Terraform SHALL definir todos os recursos AWS necessários para a pipeline (Lambda, S3, Glue, Step Functions, IAM) sem nenhum recurso criado manualmente fora do código
2. THE Terraform SHALL utilizar módulos separados para cada componente da pipeline (módulo Lambda, módulo S3, módulo Glue, módulo Step Functions, módulo IAM), cada um com seu próprio diretório contendo main.tf, variables.tf e outputs.tf
3. THE Terraform SHALL configurar políticas IAM com princípio de menor privilégio, criando roles dedicadas para cada serviço (Lambda, Glue, Step Functions) com permissões restritas apenas aos recursos específicos que cada serviço necessita acessar
4. THE Terraform SHALL parametrizar configurações via variáveis para permitir reutilização em diferentes ambientes (dev e prd), incluindo no mínimo: nome do ambiente, região AWS, nome do bucket S3 e configurações de retenção
5. THE Terraform SHALL armazenar o estado remoto (remote state) em um backend S3 com DynamoDB para locking, configurado no bloco backend do arquivo principal
6. THE Terraform SHALL utilizar Terraform_Workspace para isolar os estados de infraestrutura dos ambientes dev e prd dentro da mesma conta AWS

### Requisito 7: CI/CD com GitHub Actions

**User Story:** Como engenheiro de DevOps, eu quero que o deploy da infraestrutura seja automatizado via GitHub Actions com mapeamento de branches para ambientes, para que mudanças na branch DEVELOP afetem apenas o ambiente DEV e mudanças na branch MAIN afetem apenas o ambiente PRD.

#### Critérios de Aceitação

1. WHEN um pull request é aberto ou atualizado com novos commits direcionado à Branch_Main, THE GitHub_Actions SHALL selecionar o Terraform_Workspace "prd" e o arquivo prd.tfvars, executar terraform plan e publicar o resultado como comentário no pull request
2. WHEN um pull request é aberto ou atualizado com novos commits direcionado à Branch_Develop, THE GitHub_Actions SHALL selecionar o Terraform_Workspace "dev" e o arquivo dev.tfvars, executar terraform plan e publicar o resultado como comentário no pull request
3. WHEN um merge é realizado na Branch_Main, THE GitHub_Actions SHALL selecionar o Terraform_Workspace "prd" e executar terraform apply com o arquivo prd.tfvars para provisionar a infraestrutura no Ambiente_Prd
4. WHEN um merge é realizado na Branch_Develop, THE GitHub_Actions SHALL selecionar o Terraform_Workspace "dev" e executar terraform apply com o arquivo dev.tfvars para provisionar a infraestrutura no Ambiente_Dev
5. WHEN uma execução do pipeline é iniciada (pull request ou merge), THE GitHub_Actions SHALL executar validação de formato (terraform fmt -check) e validação de configuração (terraform validate) antes de executar terraform plan ou terraform apply
6. THE GitHub_Actions SHALL utilizar credenciais AWS configuradas como secrets do repositório para autenticação em todas as operações Terraform
7. IF o terraform plan ou apply falha, THEN THE GitHub_Actions SHALL reportar o erro como comentário no pull request ou no log do workflow e interromper a execução do pipeline sem aplicar mudanças na infraestrutura
8. IF a validação de formato (terraform fmt -check) ou a validação de configuração (terraform validate) falha, THEN THE GitHub_Actions SHALL interromper o pipeline e reportar os erros de validação antes de executar terraform plan ou apply

### Requisito 8: Testes Unitários do Código Python

**User Story:** Como engenheiro de dados, eu quero que todo o código Python possua testes unitários, para que a qualidade e a confiabilidade do código sejam garantidas antes do deploy.

#### Critérios de Aceitação

1. THE Lambda_Function SHALL ter código organizado em funções com responsabilidade única (uma função por operação: fetch de dados, escrita no S3, tratamento de erros) para facilitar testes unitários
2. WHEN uma execução do pipeline de CI/CD é iniciada, THE GitHub_Actions SHALL executar testes unitários Python (pytest) como etapa obrigatória antes do deploy
3. WHEN testes unitários falham, THE GitHub_Actions SHALL interromper o pipeline de CI/CD e reportar os nomes dos testes que falharam com suas mensagens de erro
4. THE Lambda_Function SHALL ter cobertura de testes unitários para todas as funções de lógica de negócio, incluindo cenários de sucesso, erros HTTP, timeout e rate limiting
5. THE Lambda_Function SHALL utilizar mocks (unittest.mock) para simular chamadas a serviços externos (PokeAPI, S3) nos testes unitários, sem dependência de rede ou infraestrutura
6. THE Glue_Job SHALL separar a lógica de transformação em funções puras que recebem DataFrames e retornam DataFrames, sem dependência de contexto Glue
7. THE Glue_Job SHALL ter suas funções de transformação testadas utilizando SparkSession local (local[*]) com datasets de no mínimo 10 registros representativos por cenário de teste
8. THE Glue_Job SHALL utilizar a biblioteca chispa para assertions de igualdade em DataFrames nos testes unitários

### Requisito 9: Armazenamento S3 com Separação por Camadas

**User Story:** Como engenheiro de dados, eu quero que o armazenamento S3 seja organizado por camadas da Arquitetura Medalhão, para que os dados de cada estágio de processamento estejam claramente separados.

#### Critérios de Aceitação

1. THE Terraform SHALL criar prefixos ou buckets separados no S3_Bucket para cada camada: Bronze_Layer, Silver_Layer e Gold_Layer
2. THE Terraform SHALL configurar lifecycle policies no S3_Bucket com regras de transição ou expiração específicas para cada camada, sendo que cada camada deve possuir ao menos uma regra de lifecycle com período de retenção definido em dias
3. THE Terraform SHALL habilitar versionamento no S3_Bucket e configurar uma regra de lifecycle para expirar versões não-correntes após um número definido de dias
4. THE Terraform SHALL configurar criptografia server-side (SSE-S3 ou SSE-KMS) para todos os dados armazenados no S3_Bucket
5. THE Terraform SHALL bloquear acesso público ao S3_Bucket configurando Block Public Access com todas as quatro opções habilitadas (BlockPublicAcls, IgnorePublicAcls, BlockPublicPolicy, RestrictPublicBuckets)

### Requisito 10: Suporte Multi-Ambiente com Terraform Workspaces

**User Story:** Como engenheiro de DevOps, eu quero que a mesma conta AWS suporte dois ambientes isolados (dev e prd) utilizando Terraform workspaces com mapeamento direto para branches Git, para que eu possa testar mudanças no ambiente de desenvolvimento (via branch DEVELOP) antes de aplicar em produção (via branch MAIN) sem duplicar código.

#### Critérios de Aceitação

1. THE Terraform SHALL utilizar a diretiva workspace para criar e gerenciar dois ambientes distintos (dev e prd) na mesma conta AWS, onde o workspace "dev" é controlado pela Branch_Develop e o workspace "prd" é controlado pela Branch_Main
2. THE Terraform SHALL manter um arquivo Terraform_Tfvars específico para cada ambiente (dev.tfvars e prd.tfvars), contendo as configurações diferenciadas de cada ambiente incluindo nome do ambiente, prefixos de nomenclatura de recursos e configurações de capacidade
3. THE Terraform SHALL utilizar a variável terraform.workspace para compor nomes de recursos AWS, garantindo que recursos do Ambiente_Dev e do Ambiente_Prd não colidam em nomenclatura dentro da mesma conta
4. THE Terraform SHALL configurar o backend de estado remoto com chave de estado separada por workspace, garantindo isolamento completo do state entre Ambiente_Dev e Ambiente_Prd
5. WHEN o workspace selecionado é "dev", THE Terraform SHALL aplicar as configurações definidas no arquivo dev.tfvars, incluindo dimensionamento reduzido de recursos (menor memória Lambda, menor número de DPUs Glue) adequado para desenvolvimento e testes
6. WHEN o workspace selecionado é "prd", THE Terraform SHALL aplicar as configurações definidas no arquivo prd.tfvars, incluindo dimensionamento adequado para cargas de produção
7. THE Terraform SHALL parametrizar tags de recursos AWS incluindo uma tag "Environment" com valor derivado do workspace ativo (dev ou prd) para identificação e controle de custos por ambiente
8. THE Terraform SHALL garantir que buckets S3, databases do Glue_Catalog e nomes de Step_Function incluam o identificador do ambiente (dev ou prd) derivado do workspace, evitando conflitos de nomenclatura na mesma conta AWS
