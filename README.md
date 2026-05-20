# Project Lake House

## Visão Geral

O **Project Lake House** é um projeto educacional de Engenharia de Dados construído no Databricks. A proposta é mostrar, de ponta a ponta, como sair da ingestão de fontes externas e chegar em dados organizados para análise.

A proposta aqui é servir como material prático para quem quer estudar fundamentos de Data Engineering, com foco em:

- Arquitetura Medalhão (Bronze, Silver, Gold)
- Ingestão incremental e controle de estado
- Pipelines de transformação com Delta Live Tables
- Orquestração com Databricks Asset Bundles
- Automação com CI/CD (GitHub Actions)
- Integração com Apache Airflow
- Boas práticas de governança e qualidade de dados

---

## Por que este projeto existe?

Este repositório foi criado para servir como material de estudo e referência prática para quem deseja:

- Entender como estruturar um Data Lake no Databricks
- Aprender sobre arquitetura por camadas (Bronze, Silver, Gold)
- Ver exemplos reais de ingestão de APIs públicas
- Compreender padrões de controle incremental
- Aprender sobre separação de ambientes (dev/prod)
- Entender orquestração com Asset Bundles e Airflow
- Ter um projeto documentado para incluir no portfólio

Tudo foi construído pensando em ser legível, escalável e fácil de entender, mesmo para quem está começando na área.

---

## Galeria Visual Geral (Airflow + GitHub + Databricks)

Esta seção centraliza os principais registros visuais do projeto para facilitar a leitura técnica e a apresentação (ex.: portfólio e LinkedIn).

### 1) Repositório no GitHub

![GitHub Repository](open_meteo/docs/images/linkedin/github_repo.png)

**O que mostra:** estrutura geral do repositório, branch ativa e último commit relevante.

### 2) Pull Request técnico consolidando ajustes

![GitHub Pull Request](open_meteo/docs/images/linkedin/github_pr_33.png)

**O que mostra:** PR com descrição técnica das mudanças de orquestração e documentação.

### 3) Pipeline de CI/CD no GitHub Actions

![GitHub Actions](open_meteo/docs/images/linkedin/github_actions_open_meteo.png)

**O que mostra:** histórico de execuções do workflow `open_meteo-ci-cd.yml`.

### 4) Airflow - Visão geral da plataforma

![Airflow Home](open_meteo/docs/images/linkedin/airflow_home.png)

**O que mostra:** saúde dos componentes, status de dags e histórico de execuções.

### 5) Airflow - DAG de produção (overview)

![Airflow DAG Overview](open_meteo/docs/images/linkedin/airflow_dag_prod.png)

**O que mostra:** DAG `open_meteo_databricks_prod` ativa e monitorada.

### 6) Airflow - Histórico de runs da DAG de produção

![Airflow DAG Runs](open_meteo/docs/images/linkedin/airflow_dag_runs_prod.png)

**O que mostra:** execuções recentes com rastreabilidade de sucesso.

### 7) Databricks - Jobs + Arquitetura (snapshot visual)

![Databricks Snapshot Visual](open_meteo/docs/images/linkedin/databricks_visual_report.png)

**O que mostra:** consolidado visual com dados reais de jobs, pipelines, taxa de sucesso e fluxo de arquitetura.

### Capturas Originais do Databricks Workspace

### 8) Workspace - raiz do usuario no Databricks

![Workspace Root](docs/images/project_data_lake/workspace_root.png)

**O que mostra:** estrutura inicial do workspace com o repositório `project_lake_house` vinculado.

### 9) Workspace - repositório `project_lake_house`

![Workspace Repo](docs/images/project_data_lake/workspace_repo.png)

**O que mostra:** visão interna do repositório no Databricks Repos com os domínios de dados.

### 10) Job `carros_dev` - execuções e tasks

![Job Carros Dev](docs/images/project_data_lake/carros_dev.png)

**O que mostra:** painel de runs, tasks, detalhes do job, parâmetros e compute serverless.

### 11) Job `pokemon_dev` - execuções e tasks

![Job Pokemon Dev](docs/images/project_data_lake/pokemon_dev.png)

**O que mostra:** histórico de execuções do bundle pokemon com detalhamento por task.

### 12) Bundle `carros` dentro do workspace

![Bundle Carros](docs/images/project_data_lake/asset_bundle_carros.png)

**O que mostra:** estrutura de pastas/arquivos do asset bundle `carros` no Databricks.

### 13) Bundle `pokemon` dentro do workspace

![Bundle Pokemon](docs/images/project_data_lake/asset_bundle_pokemon.png)

**O que mostra:** estrutura de pastas/arquivos do asset bundle `pokemon` no Databricks.

### 14) Unity Catalog - visão de catálogos e schemas

![Databricks Catalog](docs/images/project_data_lake/catalog.png)

**O que mostra:** navegação no Catalog Explorer com os catálogos por ambiente e os schemas dos domínios do projeto.

### 15) Workflows - jobs e pipelines no Databricks

![Databricks Jobs and Pipelines](docs/images/project_data_lake/jobs_pipelines.png)

**O que mostra:** lista consolidada dos workflows (jobs e DLT pipelines), facilitando validação de governança e organização operacional.

### 16) Job Databricks - execução mais recente

![Databricks Job Open Meteo Prod](docs/images/project_data_lake/open_meteo_prod.png)

**O que mostra:** painel de execução mais recente de job no Databricks, com status de run, trilha de monitoramento e contexto operacional do ambiente.

### Atualizacao de imagens do `project_data_lake`

Sempre que adicionar novas fotos em `C:\Users\Arthur\Desktop\project_data_lake`, execute:

`powershell -ExecutionPolicy Bypass -File scripts/sync_project_data_lake_images.ps1`

<!-- START:PROJECT_DATA_LAKE_IMAGES -->
### Galeria automatica - project_data_lake

> Bloco atualizado automaticamente por `scripts/sync_project_data_lake_images.ps1`.

| Arquivo | Preview |
| --- | --- |
| `asset_bundle_carros.png` | ![asset bundle carros](docs/images/project_data_lake/asset_bundle_carros.png) |
| `asset_bundle_pokemon.png` | ![asset bundle pokemon](docs/images/project_data_lake/asset_bundle_pokemon.png) |
| `camara_deputados_prod.png` | ![camara deputados prod](docs/images/project_data_lake/camara_deputados_prod.png) |
| `camara_deputados_prod_run.png` | ![camara deputados prod run](docs/images/project_data_lake/camara_deputados_prod_run.png) |
| `carros_dev.png` | ![carros dev](docs/images/project_data_lake/carros_dev.png) |
| `carros_prod.png` | ![carros prod](docs/images/project_data_lake/carros_prod.png) |
| `carros_prod_timeline.png` | ![carros prod timeline](docs/images/project_data_lake/carros_prod_timeline.png) |
| `catalog.png` | ![catalog](docs/images/project_data_lake/catalog.png) |
| `databricks_job_execucao_recente.png` | ![databricks job execucao recente](docs/images/project_data_lake/databricks_job_execucao_recente.png) |
| `ibge_prod.png` | ![ibge prod](docs/images/project_data_lake/ibge_prod.png) |
| `jobs_pipelines.png` | ![jobs pipelines](docs/images/project_data_lake/jobs_pipelines.png) |
| `open_meteo_prod.png` | ![open meteo prod](docs/images/project_data_lake/open_meteo_prod.png) |
| `pokemon_dev.png` | ![pokemon dev](docs/images/project_data_lake/pokemon_dev.png) |
| `pokemon_prod.png` | ![pokemon prod](docs/images/project_data_lake/pokemon_prod.png) |
| `workspace_repo.png` | ![workspace repo](docs/images/project_data_lake/workspace_repo.png) |
| `workspace_root.png` | ![workspace root](docs/images/project_data_lake/workspace_root.png) |

<!-- END:PROJECT_DATA_LAKE_IMAGES -->
### Mapa rápido por domínio

- `carros`: ingestão de dados da Tabela FIPE.
- `pokemon`: ingestão e modelagem de dados da PokeAPI.
- `ibge`: ingestão de divisão territorial oficial do Brasil.
- `open_meteo`: ingestão meteorológica, DLT e orquestração externa via Airflow.
- `camara_deputados`: ingestão de dados políticos (deputados, partidos, despesas, votações).

### Onde aprofundar cada parte

- Visão detalhada do Open Meteo: `open_meteo/README.md`
- Stack de orquestração Airflow: `open_meteo/airflow/README.md`
- Material visual Databricks: `open_meteo/docs/databricks_visual.md`
- Roteiro pronto para post: `open_meteo/docs/linkedin_post.md`

---
## Arquitetura Medalhão

O projeto segue a **Arquitetura Medalhão**, que organiza os dados em três camadas progressivas de refinamento:

### Camada Bronze (Dados Brutos)

**Propósito**: Armazenamento dos dados no formato mais próximo da fonte original.

**Características**:
- Dados brutos, sem transformações complexas
- Preservação histórica completa
- Permite reprocessamento futuro
- Tabelas prefixadas com `raw_`
- Controle de ingestão incremental quando aplicável

**Catálogos**:
- `bronze_dev` (Desenvolvimento)
- `bronze_prod` (Produção)

### Camada Silver (Dados Tratados)

**Propósito**: Limpeza, padronização e enriquecimento dos dados brutos.

**Características**:
- Limpeza de valores nulos e inconsistências
- Padronização de tipos de dados
- Remoção de duplicidades
- Normalização de strings
- Enriquecimento com chaves de relacionamento
- Tabelas prefixadas com `cleaned_` ou `tb_`

**Catálogos**:
- `silver_dev` (Desenvolvimento)
- `silver_prod` (Produção)

### Camada Gold (Dados Analíticos)

**Propósito**: Modelagem dimensional para consumo analítico e business intelligence.

**Características**:
- Modelagem dimensional (Star Schema / Kimball)
- Tabelas de fatos e dimensões
- Surrogate keys (chaves substitutas) com xxhash64()
- Agregações e métricas de negócio
- Otimizações para performance (Liquid Clustering, Auto Optimize)
- Quality constraints com `EXPECT ... ON VIOLATION DROP ROW`
- Materialized Views (DLT) para refresh automático
- Tabelas prefixadas com `dim_` (dimensões) ou `fact_` (fatos)

**Status da Implementação**:
-  **Câmara dos Deputados**: 6 dimensões + 2 fatos (completo)
-  **IBGE**: 5 dimensões hierárquicas (completo)
-  **Open Meteo**: 2 dimensões + 3 fatos (completo)
-  **Pokémon**: 6 dimensões + 2 bridge tables (completo)
-  **Carros**: 4 dimensões + 1 fato (em desenvolvimento)

**Notebooks DLT Gold Criados**:
1. `camara_deputados/src/dlt_camara_deputados_gold.sql` - Star Schema política
2. `ibge/src/dlt_ibge_gold.sql` - Hierarquia geográfica
3. `open_meteo/src/dlt_open_meteo_gold.sql` - Dados meteorológicos dimensionais
4. `pokemon/src/dlt_pokemon_gold.sql` - Modelo dimensional Pokémon
5. `carros/src/dlt_carros_gold.sql` - Modelo FIPE dimensional (em desenvolvimento)

**Padrões Técnicos Implementados**:
- **Surrogate Keys**: Geradas com `xxhash64(CAST(id AS STRING))` para garantir unicidade
- **Variáveis DLT**: Uso de `${silver_catalog}` para referenciar origem Silver
- **Clustering Automático**: `CLUSTER BY AUTO` para otimização de queries
- **Auto Optimize**: Delta Lake otimização de escrita e compactação automática
- **Quality Constraints**: Validações com `EXPECT` para garantir qualidade
- **Documentação Inline**: Comments em todas as colunas e tabelas

**Catálogos**:
- `gold_dev` (Desenvolvimento) - 31 tabelas criadas
- `gold_prod` (Produção) - 31 tabelas criadas

**Schemas (Data Marts)**:
- `dm_camara_deputados` - 8 tabelas (6 dims + 2 facts)
- `dm_ibge` - 5 tabelas (5 dims hierárquicas)
- `dm_open_meteo` - 5 tabelas (2 dims + 3 facts)
- `dm_pokemon` - 8 tabelas (6 dims + 2 bridges)
- `dm_carros` - 5 tabelas (4 dims + 1 fact) [em desenvolvimento]

**Catálogos**:
- `gold_dev` (Desenvolvimento)
- `gold_prod` (Produção)

---

## Estrutura de Catálogos e Schemas

### Convenção de Nomenclatura

**Catálogos**:
```
{camada}_{ambiente}

Exemplos:
- bronze_dev / bronze_prod
- silver_dev / silver_prod
- gold_dev / gold_prod
├── dm_camara_deputados (Câmara dos Deputados - Star Schema Política)
│   ├── dim_deputado (Deputados federais)
│   ├── dim_partido (Partidos políticos)
│   ├── dim_tempo (Calendário temporal)
│   ├── dim_tipo_despesa (Tipos de despesas parlamentares)
│   ├── dim_fornecedor (Fornecedores)
│   ├── dim_orgao (Órgãos legislativos - Plenário/Comissões)
│   ├── fact_despesas (Despesas da cota parlamentar)
│   └── fact_votacoes (Votações em Plenário e Comissões)
│
├── dm_ibge (IBGE - Hierarquia Geográfica do Brasil)
│   ├── dim_regiao (5 macroregiões)
│   ├── dim_estado (27 estados + DF)
│   ├── dim_municipio (5.570 municípios com hierarquia completa)
│   ├── dim_distrito (Distritos)
│   └── dim_subdistrito (Subdistritos)
│
├── dm_open_meteo (Open Meteo - Dados Meteorológicos Dimensionais)
│   ├── dim_localidade (27 capitais brasileiras)
│   ├── dim_tempo_meteo (Calendário temporal meteorológico)
│   ├── fact_historical_weather (Dados históricos de temperatura/precipitação)
│   ├── fact_air_quality (Índices de qualidade do ar)
│   └── fact_daily_forecast (Previsão diária do tempo)
│
├── dm_pokemon (Pokémon - Modelo Dimensional PokeAPI)
│   ├── dim_pokemon (Pokémon com atributos)
│   ├── dim_ability (Habilidades)
│   ├── dim_nature (Naturezas)
│   ├── dim_growth_rate (Taxas de crescimento)
│   ├── dim_item (Itens do jogo)
│   ├── dim_location (Localizações)
│   ├── bridge_pokemon_ability (Relacionamento N:N Pokémon-Habilidades)
│   └── bridge_pokemon_type (Relacionamento N:N Pokémon-Tipos)
│
└── dm_carros (Carros - Tabela FIPE Dimensional) [em desenvolvimento]
    ├── dim_marca (Marcas de veículos)
    ├── dim_modelo (Modelos por marca)
    ├── dim_ano_modelo (Anos/versões)
    ├── dim_tempo_fipe (Meses de referência)
    └── fact_preco_fipe (Preços médios de veículos)

---

## Asset Bundles

O projeto utiliza **Databricks Asset Bundles** para gerenciar cinco pipelines de dados independentes. Cada bundle representa um domínio de negócio específico com suas próprias fontes de dados e transformações.

### 1. Bundle: Carros (Tabela FIPE)

**Fonte de Dados**: API Tabela FIPE - Fundação Instituto de Pesquisas Econômicas

**Descrição**: Pipeline de ingestão de preços médios de veículos no mercado brasileiro.

**Camadas**: Bronze → Silver (em desenvolvimento) → Gold (planejado)

**Ingestões Bronze (5 tasks sequenciais)**:
1. `ingest_marcas` - Marcas de veículos (VW, Fiat, Ford, etc.)
2. `ingest_modelos` - Modelos por marca
3. `ingest_anos` - Anos/versões disponíveis
4. `ingest_referencias` - Meses de referência dos preços
5. `ingest_fipe` - Preços finais consolidados

**Pipeline DLT**:
- Pipeline de transformação Bronze → Silver (em desenvolvimento)

**Agendamento**: Configurável via Asset Bundle

**Localização**: `/project_lake_house/carros/`

**README específico**: `carros/README.md`

---

### 2. Bundle: Pokemon (PokeAPI)

**Fonte de Dados**: PokeAPI (https://pokeapi.co/)

**Descrição**: Pipeline completo de dados do universo Pokémon, incluindo pokémons, habilidades, itens, localizações e versões de jogos.

**Camadas**: Bronze → Silver (em desenvolvimento)

**Ingestões Bronze (17 tasks)**:
1. `ingest_pokedex` - Pokédex disponíveis
2. `ingest_pokemon` - Dados detalhados de Pokémon
3. `ingest_abilities` - Habilidades dos Pokémon
4. `ingest_pokemon_genders` - Informações de gênero
5. `ingest_pokemon_growth_rates` - Taxas de crescimento
6. `ingest_pokemon_location_areas` - Áreas onde aparecem
7. `ingest_pokemon_natures` - Naturezas dos Pokémon
8. `ingest_pokemon_pokeathlon_stats` - Estatísticas Pokeathlon
9. `ingest_egg_groups` - Grupos de ovos para reprodução
10. `ingest_locations` - Localizações no jogo
11. `ingest_items` - Itens disponíveis
12. `ingest_item_attributes` - Atributos dos itens
13. `ingest_versions` - Versões de jogos Pokémon

**Pipeline DLT**:
- `pokemon_pipeline` - Transformação Bronze → Silver

**Agendamento**: Diário (horário configurável)

**Localização**: `/project_lake_house/pokemon/`

**README específico**: `pokemon/README.md`

---

### 3. Bundle: IBGE (API Localidades)

**Fonte de Dados**: API IBGE Localidades (https://servicodados.ibge.gov.br/api/docs/localidades)

**Descrição**: Pipeline de divisões territoriais do Brasil, desde macrorregiões até subdistritos.

**Camadas**: Bronze → Silver

**Ingestões Bronze (12 tasks hierárquicas)**:
1. `ingest_regioes` - 5 macrorregiões
2. `ingest_estados` - 27 estados + DF
3. `ingest_mesorregioes` - 137 mesorregiões
4. `ingest_microrregioes` - 558 microrregiões
5. `ingest_regioes_intermediarias` - 134 regiões intermediárias
6. `ingest_regioes_imediatas` - 510 regiões imediatas
7. `ingest_municipios` - 5.570 municípios
8. `ingest_distritos` - Aproximadamente 10.000 distritos
9. `ingest_subdistritos` - Aproximadamente 600 subdistritos
10. `ingest_regioes_metropolitanas` - Aproximadamente 80 RMs
11. `ingest_aglomeracoes_urbanas` - Aproximadamente 50 aglomerações
12. `ingest_regioes_integradas_desenvolvimento` - 3 RIDEs

**Pipeline DLT**:
- `ibge_pipeline` - Transformação Bronze → Silver

**Agendamento**: Diário (horário configurável)

**Localização**: `/project_lake_house/ibge/`

**README específico**: `ibge/README.md`

---

### 4. Bundle: Open Meteo (Dados Meteorológicos)

**Fonte de Dados**: Open-Meteo API (https://open-meteo.com/)

**Descrição**: Pipeline de dados meteorológicos das capitais brasileiras, incluindo dados históricos, previsão do tempo e qualidade do ar.

**Camadas**: Bronze → Silver

**Ingestões Bronze (5 tasks)**:
1. `ingest_tabela_auxiliar_localidades` - Tabela auxiliar com as 27 capitais brasileiras
2. `ingest_geocoding_localidades` - Coordenadas geográficas das capitais
3. `ingest_historical_weather` - Dados históricos de temperatura, precipitação, vento
4. `ingest_air_quality` - Índices de qualidade do ar
5. `ingest_daily_forecast` - Previsão diária do tempo

**Pipeline DLT**:
- `dlt_open_meteo` - Transformação Bronze → Silver

**Orquestração**: Apache Airflow (externo ao Databricks)

**Observação importante**: Este bundle tem `pause_status: PAUSED` no databricks.yml porque a execução é gerenciada por um Apache Airflow externo. O CI/CD apenas faz o deploy do bundle, mas NÃO executa o job automaticamente.

**Localização**: `/project_lake_house/open_meteo/`

**README específico**: `open_meteo/README.md`

---

### 5. Bundle: Câmara dos Deputados

**Fonte de Dados**: API de Dados Abertos da Câmara dos Deputados (https://dadosabertos.camara.leg.br/)

**Descrição**: Pipeline de dados políticos do Brasil, incluindo deputados federais, partidos políticos, despesas parlamentares e votações.

**Camadas**: Bronze → Silver (em desenvolvimento)

**Ingestões Bronze (4 tasks principais)**:
1. `ingest_deputados` - Deputados federais ativos
2. `ingest_partidos` - Partidos políticos registrados
3. `ingest_despesas` - Cota parlamentar (despesas)
4. `ingest_votacoes` - Votações do plenário e comissões

**Pipeline DLT**:
- `camara_deputados_pipeline` - Transformação Bronze → Silver

**Agendamento**: Diário às 06:00 AM (America/Sao_Paulo)

**Localização**: `/project_lake_house/camara_deputados/`

**README específico**: `camara_deputados/README.md`

---

## Orquestração: Databricks Asset Bundles vs Apache Airflow

O projeto demonstra duas abordagens diferentes de orquestração:

### Databricks Asset Bundles (4 bundles)

**Bundles orquestrados pelo Databricks**:
- carros
- pokemon
- ibge
- camara_deputados

**Como funciona**:
1. Jobs são definidos em arquivos YAML (`.job.yml`)
2. Tasks são encadeadas com dependências
3. Execução automática via schedule configurável
4. Status do job: `PAUSED` (dev) ou `UNPAUSED` (prod)
5. CI/CD via GitHub Actions faz deploy e pode executar o job

**Vantagens**:
- Tudo integrado no Databricks
- Sem dependências externas
- Monitoramento nativo
- Retry automático

### Apache Airflow (1 bundle)

**Bundle orquestrado pelo Airflow**:
- open_meteo

**Como funciona**:
1. O bundle é deployado no Databricks via Asset Bundle
2. O job fica com status `PAUSED` permanentemente
3. Um DAG do Airflow (externo) dispara a execução do job via API do Databricks
4. O Airflow gerencia o schedule, retry e monitoramento
5. CI/CD via GitHub Actions faz apenas o deploy (não executa)

**Vantagens**:
- Orquestração centralizada de múltiplas ferramentas
- DAGs complexos com lógica condicional
- Integração com outros sistemas (não só Databricks)
- Visibilidade unificada

**Por que usar Airflow para open_meteo?**

Este bundle serve como demonstração de como integrar Databricks com ferramentas externas de orquestração, um cenário comum em empresas que já possuem Airflow para gerenciar workflows complexos envolvendo múltiplas plataformas.

---

## Ambientes (Dev e Prod)

O projeto mantém dois ambientes completamente isolados para garantir segurança e qualidade:

### Ambiente de Desenvolvimento (dev)

**Propósito**: Testes, desenvolvimento e experimentação

**Características**:
- Jobs com status `PAUSED` por padrão (execução manual)
- Dados podem ser recriados ou modificados livremente
- Ambiente para validação de código e lógica
- Não afeta dados de produção
- Deploy automático na branch `dev`

**Catálogos**:
- bronze_dev
- silver_dev
- gold_dev

**Workspace path**: `/Users/{user}/.bundle/{bundle_name}/dev/`

### Ambiente de Produção (prod)

**Propósito**: Dados oficiais e consumo por aplicações

**Características**:
- Jobs com status `UNPAUSED` (execução automática conforme schedule)
- Dados críticos e controlados
- Requer permissões específicas
- Monitoramento e alertas configurados
- Deploy automático na branch `prod`

**Catálogos**:
- bronze_prod
- silver_prod
- gold_prod

**Workspace path**: `/Users/{user}/.bundle/{bundle_name}/prod/`

### Isolamento entre Ambientes

Os ambientes são isolados através de:

1. **Catálogos separados** (bronze_dev vs bronze_prod)
2. **Workspaces separados** no bundle
3. **Variáveis de ambiente** diferentes
4. **Status de execução** diferente (PAUSED vs UNPAUSED)
5. **Branches Git** separadas (dev vs prod)


## Variáveis de Ambiente e Tokens

O projeto utiliza um sistema robusto de variáveis de ambiente e tokens para garantir flexibilidade, segurança e isolamento entre ambientes.

### Variáveis dos Asset Bundles

Cada bundle define suas variáveis no arquivo `databricks.yml`, que são substituídas em tempo de execução conforme o ambiente (dev/prod).

#### Exemplo: carros/databricks.yml

```yaml
variables:
  environment:
    description: "Environment name (dev or prod)"
    default: dev

  catalog:
    description: "Catalog de destino"
    default: bronze_dev

  schema:
    description: "Schema de destino"
    default: ds_carros

  silver_catalog:
    description: "Catalog de destino da silver"
    default: silver_dev

  pause_status:
    description: "Job schedule status"
    default: PAUSED

  pipeline_development:
    description: "Pipeline development mode flag"
    default: true
```

#### Como as Variáveis Funcionam

**No arquivo databricks.yml**, as variáveis são referenciadas usando a sintaxe `${var.nome_variavel}`:

```yaml
resources:
  jobs:
    carros_job:
      name: carros_${var.environment}
      schedule:
        pause_status: ${var.pause_status}
      tasks:
        - task_key: ingest_marcas
          notebook_task:
            notebook_path: ../src/ingest_marcas.py
            base_parameters:
              catalog: ${var.catalog}
              schema: ${var.schema}
```

**Nos notebooks**, os parâmetros são recebidos via `dbutils.widgets`:

```python
# O notebook recebe o valor da variável
dbutils.widgets.text("catalog", "bronze_prod")
used_catalog = dbutils.widgets.get("catalog")  # Resultado: "bronze_dev" ou "bronze_prod"

dbutils.widgets.text("schema", "ds_carros")
used_schema = dbutils.widgets.get("schema")  # Resultado: "ds_carros"

# Uso prático
tabela_destino = f"{used_catalog}.{used_schema}.raw_marcas"
# Resultado dev: "bronze_dev.ds_carros.raw_marcas"
# Resultado prod: "bronze_prod.ds_carros.raw_marcas"
```

### Targets (Ambientes)

Os **targets** definem configurações específicas por ambiente no `databricks.yml`:

```yaml
targets:
  dev:
    mode: development
    default: true
    workspace:
      host: https://dbc-414ad402-adeb.cloud.databricks.com
      root_path: /Workspace/Users/${workspace.current_user.userName}/.bundle/${bundle.name}/${bundle.target}
    variables:
      environment: dev
      catalog: bronze_dev
      schema: ds_carros
      silver_catalog: silver_dev
      pause_status: PAUSED
      pipeline_development: true

  prod:
    mode: production
    workspace:
      host: https://dbc-414ad402-adeb.cloud.databricks.com
      root_path: /Workspace/Users/${workspace.current_user.userName}/.bundle/${bundle.name}/${bundle.target}
    variables:
      environment: prod
      catalog: bronze_prod
      schema: ds_carros
      silver_catalog: silver_prod
      pause_status: UNPAUSED
      pipeline_development: false
```

**Diferenças entre Dev e Prod**:

| Variável | Dev | Prod | Propósito |
|----------|-----|------|-----------|
| `catalog` | bronze_dev | bronze_prod | Isola dados brutos |
| `silver_catalog` | silver_dev | silver_prod | Isola dados tratados |
| `pause_status` | PAUSED | UNPAUSED | Controla execução automática |
| `pipeline_development` | true | false | Modo de desenvolvimento do DLT |

### Tokens e Autenticação

O projeto utiliza tokens para autenticação segura em diferentes contextos:

#### 1. Token do Databricks (Local)

**Uso**: CLI local para deploy e execução manual de bundles

**Configuração**:
```bash
databricks configure --token

# Será solicitado:
# Databricks Host (should begin with https://): https://dbc-414ad402-adeb.cloud.databricks.com
# Token: dapi********************************  (token oculto)
```

**Onde é armazenado**: `~/.databrickscfg` (arquivo local protegido)

**Formato do arquivo**:
```ini
[DEFAULT]
host = https://dbc-414ad402-adeb.cloud.databricks.com
token = dapi********************************
```

**Importante**: Nunca commite este arquivo no Git! Ele contém credenciais sensíveis.

#### 2. GitHub Secrets (CI/CD)

**Uso**: Workflows do GitHub Actions para deploy automático

**Configuração**: No repositório GitHub, vá em **Settings → Secrets and variables → Actions**

**Secrets configurados**:

- **DATABRICKS_HOST**
  - Valor: `https://dbc-414ad402-adeb.cloud.databricks.com`
  - Uso: URL do workspace Databricks
  - Visível em: Workflows como `${{ secrets.DATABRICKS_HOST }}`

- **DATABRICKS_TOKEN**
  - Valor: `dapi********************************` (oculto)
  - Uso: Autenticação da CLI nos workflows
  - Visível em: Workflows como `${{ secrets.DATABRICKS_TOKEN }}`

**Exemplo no workflow**:
```yaml
jobs:
  deploy:
    runs-on: ubuntu-latest
    env:
      DATABRICKS_HOST: ${{ secrets.DATABRICKS_HOST }}
      DATABRICKS_TOKEN: ${{ secrets.DATABRICKS_TOKEN }}
    steps:
      - name: Deploy bundle
        run: databricks bundle deploy -t prod --auto-approve
```

#### 3. Token do Airflow (Bundle open_meteo)

**Uso**: DAG do Airflow dispara jobs no Databricks via API

**Como funciona**:

1. **No Airflow**, o token é armazenado como **Airflow Connection**:
   ```python
   # Exemplo de configuração no Airflow
   Connection ID: databricks_default
   Connection Type: Databricks
   Host: https://dbc-414ad402-adeb.cloud.databricks.com
   Token: dapi********************************  (oculto)
   ```

2. **O DAG usa o DatabricksRunNowOperator**:
   ```python
   from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
   
   run_job = DatabricksRunNowOperator(
       task_id='run_open_meteo_job',
       databricks_conn_id='databricks_default',  # Usa o token configurado
       job_id=12345,  # ID do job open_meteo no Databricks
       notebook_params={
           'catalog': 'bronze_prod',
           'schema': 'ds_open_meteo'
       }
   )
   ```

3. **O Airflow faz uma chamada HTTP à API do Databricks**:
   ```bash
   POST https://dbc-414ad402-adeb.cloud.databricks.com/api/2.1/jobs/run-now
   Authorization: Bearer dapi********************************
   Content-Type: application/json
   
   {
     "job_id": 12345,
     "notebook_params": {
       "catalog": "bronze_prod",
       "schema": "ds_open_meteo"
     }
   }
   ```

**Por que o job fica PAUSED no Databricks?**

- O bundle `open_meteo` tem `pause_status: PAUSED` no `databricks.yml`
- Isso garante que o job NÃO seja executado automaticamente pelo schedule do Databricks
- A execução é controlada exclusivamente pelo Airflow via API

### Fluxo Completo de Variáveis

Aqui está como as variáveis fluem desde o bundle até a execução:

```
1. databricks.yml (Definição)
   ├── variables: { catalog: bronze_dev }
   └── targets:
       └── dev: { catalog: bronze_dev }
       └── prod: { catalog: bronze_prod }

2. Deploy (Substituição)
   $ databricks bundle deploy -t prod
   
   Substitui: ${var.catalog} → "bronze_prod"
   
3. Job YAML (Gerado)
   tasks:
     - task_key: ingest_marcas
       notebook_task:
         base_parameters:
           catalog: "bronze_prod"  ← Valor substituído

4. Execução (Notebook)
   dbutils.widgets.get("catalog")
   
   Retorna: "bronze_prod"  ← Recebido como parâmetro

5. Uso no Código
   tabela_destino = f"{catalog}.{schema}.raw_marcas"
   
   Resultado: "bronze_prod.ds_carros.raw_marcas"
```

### Boas Práticas de Segurança

#### O que NUNCA fazer:

- Commitar tokens ou credenciais no Git
- Expor tokens em logs ou prints
- Compartilhar tokens por email ou chat
- Usar o mesmo token para dev e prod
- Hardcoded de valores sensíveis no código

#### O que SEMPRE fazer:

- Usar GitHub Secrets para CI/CD
- Usar Airflow Connections para integração externa
- Rotacionar tokens periodicamente
- Usar tokens de service principal para produção (não pessoais)
- Configurar permissões mínimas necessárias (least privilege)
- Revogar tokens quando não forem mais necessários

#### Exemplo de código ERRADO (inseguro):

```python
# ❌ NUNCA FAÇA ISSO!
token = "dapi1234567890abcdef"  # Hardcoded no código
catalog = "bronze_prod"  # Hardcoded no código
```

#### Exemplo de código CORRETO (seguro):

```python
# ✅ CORRETO - Usa parâmetros
catalog = dbutils.widgets.get("catalog")  # Recebe via parâmetro
schema = dbutils.widgets.get("schema")    # Recebe via parâmetro

# Token não aparece no código - autenticação é automática via Databricks
```

### Testando Variáveis Localmente

Para testar diferentes ambientes localmente:

```bash
# Validar configuração dev
cd carros
databricks bundle validate -t dev

# Validar configuração prod
databricks bundle validate -t prod

# Ver valores resolvidos (sem fazer deploy)
databricks bundle validate -t dev --var catalog=bronze_dev

# Deploy em dev
databricks bundle deploy -t dev

# Deploy em prod
databricks bundle deploy -t prod
```

### Troubleshooting

**Problema**: Job executando no catálogo errado

**Solução**: Verificar se o parâmetro está sendo passado corretamente:
```yaml
# No resources/*.job.yml
tasks:
  - task_key: ingest_marcas
    notebook_task:
      base_parameters:
        catalog: ${var.catalog}  # ← Verificar esta linha
        schema: ${var.schema}
```

**Problema**: Token inválido no CI/CD

**Solução**: Verificar secrets no GitHub:
1. Acesse Settings → Secrets and variables → Actions
2. Verifique se `DATABRICKS_TOKEN` está configurado
3. Gere um novo token no Databricks se necessário
4. Atualize o secret no GitHub

**Problema**: Airflow não consegue disparar o job

**Solução**: Verificar configuração do Airflow Connection:
1. Acesse Airflow UI → Admin → Connections
2. Verifique o `databricks_default` connection
3. Teste a conexão
4. Verifique se o token tem permissão para executar o job

---

---

## Controle de Ingestão Incremental

Todos os bundles utilizam um sistema de controle para evitar reprocessamento desnecessário de dados:

### Tabela de Controle

**Localização**: `{catalog}.{schema}.controle_ingestao` ou `tabela_controle`

**Estrutura genérica**:
```sql
CREATE TABLE controle_ingestao (
    id INT,                    -- ID fixo = 1
    raw_{entidade} INT,        -- Último ID processado
    ... (um campo para cada endpoint/tabela)
)
```

### Como Funciona

1. **Antes da ingestão**: O notebook consulta o último ID/data processado
2. **Durante a ingestão**: Filtra apenas registros novos (ID > último_processado)
3. **Após a ingestão**: Atualiza o controle com o novo último ID/data

### Exemplo de Código

```python
# 1. Busca último ID processado
ctrl_id = spark.sql(
    f"""SELECT raw_pokemon_name 
        FROM {tabela_controle} 
        WHERE id = 1"""
).collect()

ultimo_id = int(ctrl_id[0][0]) if ctrl_id else 0

# 2. Filtra apenas novos registros
if pokemon_id <= ultimo_id:
    continue  # Pula registro já processado

# 3. Atualiza controle
spark.sql(f"""
    UPDATE {tabela_controle}
    SET raw_pokemon_name = {novo_ultimo_id}
    WHERE id = 1
""")
```

### Benefícios

- Evita duplicação de dados
- Reduz tempo de execução
- Reduz custos de processamento
- Facilita retomada após falhas
- Permite rastreabilidade

---

## Padrões de Desenvolvimento

### Estrutura de um Asset Bundle

```
nome_bundle/
├── databricks.yml                   # Configuração principal
├── resources/
│   ├── {nome}_job.job.yml          # Definição do job
│   └── {nome}_etl.pipeline.yml     # Definição do pipeline DLT
├── src/
│   ├── ingest_{entidade1}.py       # Notebooks de ingestão
│   ├── ingest_{entidade2}.py
│   ├── dlt_{nome}.sql              # Transformações DLT (SQL)
│   └── ...
├── tests/
│   ├── conftest.py                 # Configuração de testes
│   └── *_test.py                   # Testes unitários
├── fixtures/                        # Dados de teste
├── .vscode/                         # Configurações VS Code
├── .gitignore
└── README.md                        # Documentação do bundle
```

### Padrão de Notebooks de Ingestão

Todos os notebooks de ingestão seguem esta estrutura:

**Célula 1 - Imports**:
```python
import requests
import json
import time
from datetime import datetime
from delta.tables import DeltaTable
```

**Célula 2 - Configuração de parâmetros**:
```python
dbutils.widgets.text("catalog", "bronze_prod")
used_catalog = dbutils.widgets.get("catalog")

dbutils.widgets.text("schema", "ds_{dominio}")
used_schema = dbutils.widgets.get("schema")

tabela_controle = f"{used_catalog}.{used_schema}.controle_ingestao"
tabela_destino = f"{used_catalog}.{used_schema}.raw_{entidade}"
```

**Célula 3 - Busca último ID processado**:
```python
ctrl_id = spark.sql(
    f"SELECT raw_{entidade} FROM {tabela_controle} WHERE id = 1"
).collect()

ultimo_id = int(ctrl_id[0][0]) if ctrl_id else 0
```

**Célula 4 - Ingestão com filtro incremental**:
```python
API_URL = "https://api.exemplo.com/endpoint?limit=100"
all_rows = []

while API_URL:
    response = requests.get(API_URL)
    dados = response.json()
    
    for item in dados.get("results", []):
        item_id = int(item.get("id"))
        
        # Filtro incremental
        if item_id <= ultimo_id:
            continue
        
        row = (item_id, item.get("name"))
        all_rows.append(row)
        
        time.sleep(0.1)  # Rate limiting
    
    API_URL = dados.get("next")
```

**Célula 5 - Gravação e atualização**:
```python
if all_rows:
    df = spark.createDataFrame(all_rows, ["id", "name"])
    df.write.mode("append").saveAsTable(tabela_destino)
    
    novo_ultimo_id = all_rows[-1][0]
    spark.sql(f"""
        UPDATE {tabela_controle}
        SET raw_{entidade} = {novo_ultimo_id}
        WHERE id = 1
    """)
else:
    print("Nenhum novo registro para inserir.")
```

### Padrão de Pipelines DLT

Os pipelines de transformação Bronze → Silver seguem este padrão:

```sql
-- Limpeza e padronização
CREATE OR REFRESH STREAMING LIVE TABLE cleaned_{entidade}
AS
SELECT
    CAST(id AS BIGINT) AS id,
    TRIM(LOWER(name)) AS name,
    CAST(value AS DOUBLE) AS value,
    CURRENT_TIMESTAMP() AS processed_at
FROM STREAM(LIVE.raw_{entidade})
WHERE id IS NOT NULL
```

---

## CI/CD com GitHub Actions

O projeto utiliza GitHub Actions para automatizar validação e deploy dos bundles.

### Estrutura de Workflows

Cada bundle possui seu próprio workflow:

```
.github/workflows/
├── carros-ci-cd.yml
├── pokemon-ci-cd.yml
├── ibge-ci-cd.yml
├── open_meteo-ci-cd.yml
└── camara_deputados-ci-cd.yml
```

### Fluxo de CI/CD

**Branch `dev` (Desenvolvimento)**:
1. Push ou Pull Request dispara o workflow
2. Instala dependências (Python, Databricks CLI, uv)
3. Valida autenticação com Databricks
4. Valida o bundle (`databricks bundle validate -t dev`)
5. Se commit direto (não PR): faz deploy (`databricks bundle deploy -t dev --auto-approve`)

**Branch `prod` (Produção)**:
1. Push ou Pull Request dispara o workflow
2. Instala dependências
3. Valida autenticação
4. Valida o bundle (`databricks bundle validate -t prod`)
5. Se commit direto: faz deploy (`databricks bundle deploy -t prod --auto-approve`)

**Exceção - Open Meteo**: O workflow NÃO executa o job automaticamente (apenas deploy), pois a execução é gerenciada pelo Airflow.

### Secrets Necessários

Configure os seguintes secrets no GitHub:

- `DATABRICKS_HOST`: URL do workspace (ex: https://dbc-414ad402-adeb.cloud.databricks.com)
- `DATABRICKS_TOKEN`: Token de acesso pessoal do Databricks

---

## Tecnologias Utilizadas

### Plataforma e Infraestrutura

- **Databricks**: Plataforma de lakehouse unificada
- **Delta Lake**: Armazenamento ACID sobre Data Lake
- **Unity Catalog**: Governança centralizada de dados
- **Databricks Asset Bundles**: Orquestração declarativa
- **Apache Airflow**: Orquestração externa (bundle open_meteo)

### Processamento e Transformação

- **Delta Live Tables (DLT)**: Pipelines gerenciados de transformação
- **PySpark**: Processamento distribuído
- **Spark SQL**: Queries e transformações

### Linguagens

- **Python**: Notebooks de ingestão
- **SQL**: Transformações DLT e queries
- **YAML**: Configuração de bundles e workflows

### Automação e DevOps

- **GitHub**: Controle de versão
- **GitHub Actions**: CI/CD
- **Databricks CLI**: Interação programática com Databricks

### APIs Externas

- API Tabela FIPE (preços de veículos)
- PokeAPI (dados Pokémon)
- API IBGE Localidades (geografia do Brasil)
- Open-Meteo API (dados meteorológicos)
- API Câmara dos Deputados (dados políticos)

---

## Como Usar Este Projeto

### Pré-requisitos

1. **Conta no Databricks** (Community Edition ou paga)
2. **Databricks CLI** instalado:
   ```bash
   pip install databricks-cli
   ```
3. **Autenticação configurada**:
   ```bash
   databricks configure --token
   # Informar Host e Token
   ```
4. **Git** configurado

### Clonar o Repositório

```bash
git clone <url_do_repositorio>
cd project_lake_house
```

### Deploy de um Bundle (Desenvolvimento)

```bash
cd carros  # ou pokemon, ibge, open_meteo, camara_deputados
databricks bundle deploy -t dev
```

### Deploy em Produção

```bash
cd carros
databricks bundle deploy -t prod
```

### Executar Job Manualmente

```bash
cd carros
databricks bundle run carros_job -t dev
```

### Validar Configuração

```bash
cd carros
databricks bundle validate
```

### Acompanhar Execução

1. Acesse o Databricks workspace
2. Navegue para **Workflows → Jobs**
3. Localize o job (ex: `carros_dev`, `pokemon_prod`)
4. Acompanhe status e logs de cada task

---

## Boas Práticas Implementadas

### Governança de Dados

- Separação de ambientes (dev/prod)
- Catálogos isolados por camada e ambiente
- Unity Catalog para controle de acesso
- Auditoria via Delta Lake (time travel)

### Qualidade de Dados

- Ingestão incremental com controle de estado
- Validação de integridade referencial
- Deduplicação de registros
- Limpeza de valores nulos
- Padronização de tipos

### Performance

- Delta Lake para otimização automática
- Serverless para escalabilidade
- Particionamento de tabelas grandes (quando necessário)
- Streaming quando aplicável

### Confiabilidade

- Retry automático em falhas (max_retries: 1)
- Notificações por email em erros
- Rate limiting em chamadas de API (0.1-0.2s)
- Tratamento de erros HTTP

### Desenvolvimento

- Estrutura padronizada de bundles
- Notebooks parametrizados via widgets
- Testes unitários (pasta tests/)
- Documentação inline e READMEs
- Código legível e comentado

### CI/CD

- GitHub Actions para deploy automático
- Validação antes de deploy
- Deploy separado por ambiente
- Versionamento de código

---

## Estrutura do Repositório

```
project_lake_house/
├── README.md                        # Este arquivo
├── LICENSE
│
├── .github/
│   └── workflows/                   # GitHub Actions
│       ├── carros-ci-cd.yml
│       ├── pokemon-ci-cd.yml
│       ├── ibge-ci-cd.yml
│       ├── open_meteo-ci-cd.yml
│       └── camara_deputados-ci-cd.yml
│
├── carros/                          # Bundle FIPE
│   ├── README.md
│   ├── databricks.yml
│   ├── resources/
│   ├── src/
│   ├── tests/
│   └── fixtures/
│
├── pokemon/                         # Bundle PokeAPI
│   ├── README.md
│   ├── databricks.yml
│   ├── resources/
│   ├── src/
│   ├── tests/
│   └── fixtures/
│
├── ibge/                            # Bundle IBGE
│   ├── README.md
│   ├── databricks.yml
│   ├── resources/
│   ├── src/
│   ├── tests/
│   └── fixtures/
│
├── open_meteo/                      # Bundle Open-Meteo (Airflow)
│   ├── README.md
│   ├── databricks.yml
│   ├── resources/
│   ├── src/
│   ├── tests/
│   └── fixtures/
│
├── camara_deputados/                # Bundle Câmara dos Deputados
│   ├── README.md
│   ├── databricks.yml
│   ├── resources/
│   ├── src/
│   ├── tests/
│   └── fixtures/
│
├── dm_carros/                       # Data Mart Carros (planejado)
├── dm_pokemon/                      # Data Mart Pokemon (planejado)
├── dm_ibge/                         # Data Mart IBGE (planejado)
└── dm_open_meteo/                   # Data Mart Open Meteo (planejado)
```

---

## Casos de Uso e Análises Possíveis

### Carros (FIPE)

- Análise de tendências de preços por marca/modelo
- Identificação de veículos com maior valorização
- Comparação de preços entre marcas
- Análise temporal de mercado automotivo

### Pokemon

- Análise de distribuição de tipos de Pokémon
- Estatísticas de habilidades mais comuns
- Estudo de evolução e taxas de crescimento
- Mapeamento de localizações por versão do jogo

### IBGE

- Análise de distribuição populacional
- Hierarquia territorial do Brasil
- Mapeamento de regiões metropolitanas
- Estudo de divisões administrativas

### Open Meteo

- Análise climática das capitais brasileiras
- Comparação de temperaturas entre regiões
- Estudo de qualidade do ar
- Previsões meteorológicas

### Câmara dos Deputados

- Transparência de gastos parlamentares
- Análise de votações por partido
- Identificação de padrões de despesas
- Ranking de deputados por atividade legislativa

---

## Roadmap e Melhorias Futuras

### Curto Prazo

- Completar camada Silver para Carros e Pokemon
- Implementar camada Gold para IBGE e Open Meteo
- Adicionar testes unitários completos
- Melhorar documentação de cada bundle

### Médio Prazo

- Implementar data quality expectations no DLT
- Criar dashboards de monitoramento
- Adicionar novos bundles (BCB, outros)
- Implementar alertas proativos

### Longo Prazo

- Pipelines de Machine Learning
- API de consumo de dados
- Data observability completa
- Disaster recovery e backup

---

## Documentação Adicional

### README por Bundle

Cada bundle possui sua própria documentação detalhada:

- `carros/README.md` - Detalhes da API FIPE
- `pokemon/README.md` - Detalhes da PokeAPI
- `ibge/README.md` - Detalhes da API IBGE
- `open_meteo/README.md` - Detalhes da Open-Meteo API e integração Airflow
- `camara_deputados/README.md` - Detalhes da API Câmara dos Deputados

### Links Úteis

**Documentação Databricks**:
- [Databricks Asset Bundles](https://docs.databricks.com/dev-tools/bundles/)
- [Delta Live Tables](https://docs.databricks.com/delta-live-tables/)
- [Unity Catalog](https://docs.databricks.com/data-governance/unity-catalog/)
- [Databricks Workflows](https://docs.databricks.com/workflows/)

**APIs Utilizadas**:
- [Tabela FIPE](https://veiculos.fipe.org.br/)
- [PokeAPI](https://pokeapi.co/)
- [API IBGE Localidades](https://servicodados.ibge.gov.br/api/docs/localidades)
- [Open-Meteo](https://open-meteo.com/)
- [API Câmara dos Deputados](https://dadosabertos.camara.leg.br/swagger/api.html)

**Conceitos e Arquitetura**:
- [Medallion Architecture](https://www.databricks.com/glossary/medallion-architecture)
- [Delta Lake](https://delta.io/)
- [Lakehouse Architecture](https://www.databricks.com/product/data-lakehouse)

---

## Contribuindo

Este é um projeto educacional aberto. Sugestões e melhorias são bem-vindas!

### Como Contribuir

1. Fork o repositório
2. Crie uma branch para sua feature (`git checkout -b feature/nova-feature`)
3. Commit suas mudanças (`git commit -m 'Adiciona nova feature'`)
4. Push para a branch (`git push origin feature/nova-feature`)
5. Abra um Pull Request

### Diretrizes

- Mantenha o padrão de estrutura dos bundles existentes
- Documente todas as mudanças no README apropriado
- Adicione testes quando aplicável
- Siga as convenções de nomenclatura estabelecidas

---

## Segurança e Privacidade

### Dados Utilizados

Todos os dados ingeridos são de APIs públicas e abertas:
- Nenhum dado sensível ou pessoal identificável
- Sem credenciais ou informações confidenciais
- Dados utilizados apenas para fins educacionais

### Boas Práticas de Segurança

- Tokens e credenciais armazenados em GitHub Secrets
- Nunca commitar credenciais no código
- Uso de Unity Catalog para controle de acesso
- Separação de ambientes dev/prod

### LGPD (Lei Geral de Proteção de Dados)

Mesmo sendo um projeto educacional com dados públicos, o projeto segue princípios de:
- Minimização de dados
- Transparência
- Uso responsável da informação

---

## Suporte e Contato

**Autor**: Arthur Delacorte  
**Email**: delacortearthur@gmail.com  
**Workspace Databricks**: https://dbc-414ad402-adeb.cloud.databricks.com  
**Objetivo**: Projeto educacional e de portfólio

---

## Licença

Consulte o arquivo [LICENSE](LICENSE) para detalhes sobre a licença do projeto.

---

## Agradecimentos

Este projeto foi desenvolvido como material de estudo e demonstração prática de Engenharia de Dados. Agradecimentos especiais:

- Databricks pela plataforma incrível
- Comunidade open source pelas APIs públicas utilizadas
- Todos que contribuírem com sugestões e melhorias

---

**Última Atualização**: 07 de Maio de 2026  
**Versão**: 4.2 - Documentação geral com galeria visual completa e capturas originais do Databricks
