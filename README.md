# Project Lake House

## Visão Geral

O **Project Lake House** é uma arquitetura de dados moderna implementada no Databricks que centraliza a ingestão, transformação e modelagem de dados de múltiplas fontes externas. O projeto segue os princípios da **Arquitetura Medalhão** (Medallion Architecture) e utiliza **Databricks Asset Bundles** para orquestração, deploy e governança de dados.

**Objetivo**: Criar um repositório centralizado de dados variados, tratados e estruturados, prontos para análises, machine learning e business intelligence.

---

## Arquitetura Medalhão (Medallion Architecture)

A arquitetura do projeto é baseada em três camadas progressivas de refinamento de dados:

### Camada Bronze (Raw Data)

**Propósito**: Ingestão e armazenamento de dados brutos das fontes externas

**Características**:
* Dados no formato mais próximo possível da fonte original
* Mínimo ou nenhum processamento aplicado
* Preservação histórica completa dos dados
* Tabelas prefixadas com `raw_`
* Ingestão incremental com controle de estado

**Catálogos**:
* `bronze_dev` - Ambiente de desenvolvimento
* `bronze_prod` - Ambiente de produção

**Schemas**:
* `ds_carros` - Dados da Tabela FIPE (preços de veículos)
* `ds_pokemon` - Dados da PokeAPI (universo Pokémon)
* `ds_ibge` - Dados do IBGE (divisões territoriais do Brasil)
* `ds_bcb` - Dados do Banco Central do Brasil (planejado)

### Camada Silver (Cleaned Data)

**Propósito**: Dados limpos, validados e padronizados

**Características**:
* Limpeza de valores nulos e inconsistências
* Padronização de tipos de dados e formatos
* Deduplicação de registros
* Normalização de strings
* Validação de integridade referencial
* Enriquecimento com chaves de relacionamento

**Catálogos**:
* `silver_dev` - Ambiente de desenvolvimento
* `silver_prod` - Ambiente de produção

**Schemas**:
* `ds_carros` - Dados FIPE tratados
* `ds_pokemon` - Dados Pokémon tratados
* `ds_ibge` - Dados IBGE tratados

### Camada Gold (Business-Level Data)

**Propósito**: Dados modelados dimensionalmente para consumo analítico

**Características**:
* Modelagem dimensional (Star Schema / Snowflake)
* Tabelas de fatos e dimensões
* Agregações e métricas de negócio
* Otimizações para performance de queries
* Dados prontos para BI e dashboards

**Catálogos**:
* `gold_dev` - Ambiente de desenvolvimento
* `gold_prod` - Ambiente de produção

**Schemas**:
* `dm_carros` - Data Mart de carros (dimensões e fatos)
* `dm_pokemon` - Data Mart de Pokémon (planejado)
* `dm_ibge` - Data Mart de geografia (planejado)

---

## Estrutura de Catálogos e Schemas

### Convenção de Nomenclatura

**Catálogos**:
```
{camada}_{ambiente}

Exemplos:
- bronze_dev
- silver_dev
- gold_dev
- bronze_prod
- silver_prod
- gold_prod
```

**Schemas**:
```
{tipo}_{dominio}

Tipos:
- ds_ (data source) - Para Bronze e Silver
- dm_ (data mart) - Para Gold

Exemplos:
- ds_carros
- ds_pokemon
- ds_ibge
- dm_carros
- dm_pokemon
```

**Tabelas Bronze**:
```
raw_{entidade}

Exemplos:
- raw_marcas
- raw_pokemon_name
- raw_municipios
```

**Tabelas Silver**:
```
tb_{entidade} ou dim_{entidade}

Exemplos:
- tb_marcas
- tb_pokemon
- tb_municipios
```

**Tabelas Gold**:
```
dim_{dimensao} - Tabelas de dimensão
fato_{metrica} - Tabelas de fato

Exemplos:
- dim_marca
- dim_modelo
- fato_preco_fipe
```

### Hierarquia Completa

```
bronze_dev (Catálogo)
├── ds_carros (Schema)
│   ├── controle_ingestao (Tabela de controle)
│   ├── raw_marcas
│   ├── raw_modelos
│   ├── raw_anos
│   ├── raw_referencias
│   └── raw_fipe
├── ds_pokemon (Schema)
│   ├── controle_ingestao
│   ├── raw_pokemon_name
│   ├── raw_pokemon_abilities
│   ├── raw_items
│   └── ... (13 tabelas)
└── ds_ibge (Schema)
    ├── raw_regioes
    ├── raw_ufs
    ├── raw_municipios
    └── ... (12 tabelas)

silver_dev (Catálogo)
├── ds_carros (Schema)
│   ├── tb_marcas
│   ├── tb_modelos
│   └── tb_fipe
├── ds_pokemon (Schema)
│   └── tb_pokemon
└── ds_ibge (Schema)
    └── dim_municipio

gold_dev (Catálogo)
├── dm_carros (Schema)
│   ├── dim_marca
│   ├── dim_modelo
│   └── fato_preco_fipe
└── (outros data marts planejados)
```

---

## Asset Bundles

O projeto utiliza **Databricks Asset Bundles** para gerenciar três pipelines de dados independentes:

### 1. Bundle Carros

**Fonte de Dados**: Tabela FIPE (Fundação Instituto de Pesquisas Econômicas)

**Descrição**: Pipeline completo de ingestão e transformação de preços médios de veículos no mercado brasileiro.

**Camadas**: Bronze → Silver → Gold (arquitetura completa)

**Tasks de Ingestão**: 5 sequenciais
* ingest_marcas - Marcas de veículos
* ingest_modelos - Modelos por marca
* ingest_anos - Anos disponíveis
* ingest_referencias - Meses de referência
* ingest_fipe - Preços finais

**Pipelines DLT**:
* carros_pipeline_silver - Transformação Bronze → Silver
* carros_pipeline_gold - Transformação Silver → Gold

**Agendamento**: Diário às 01:30 AM (America/Sao_Paulo)

**Documentação**: [carros/README.md](carros/README.md)

### 2. Bundle Pokemon

**Fonte de Dados**: PokeAPI (https://pokeapi.co/)

**Descrição**: Pipeline de ingestão e transformação de dados do universo Pokémon, incluindo Pokémon, habilidades, itens, localizações e versões de jogos.

**Camadas**: Bronze → Silver

**Tasks de Ingestão**: 13 sequenciais
* ingest_pokedex - Pokédex disponíveis
* ingest_pokemon - Dados detalhados de Pokémon
* ingest_abilities - Habilidades
* ingest_pokemon_genders - Gêneros
* ingest_pokemon_growth_rates - Taxas de crescimento
* ingest_pokemon_location_areas - Áreas de localização
* ingest_pokemon_natures - Naturezas
* ingest_pokemon_pokeathlon_stats - Stats Pokeathlon
* ingest_egg_groups - Grupos de ovos
* ingest_locations - Localizações
* ingest_items - Itens do jogo
* ingest_items_attributes - Atributos de itens
* ingest_versions - Versões de jogos

**Pipelines DLT**:
* pokemon_pipeline - Transformação Bronze → Silver

**Agendamento**: Diário à meia-noite (00:00 AM - America/Sao_Paulo)

**Documentação**: [pokemon/README.md](pokemon/README.md)

### 3. Bundle IBGE

**Fonte de Dados**: API IBGE Localidades

**Descrição**: Pipeline de ingestão e transformação de divisões territoriais do Brasil, desde regiões até subdistritos.

**Camadas**: Bronze → Silver

**Tasks de Ingestão**: 12 sequenciais (hierárquicas)
* ingest_regioes - 5 macrorregiões
* ingest_estados - 27 estados + DF
* ingest_mesorregioes - 137 mesorregiões
* ingest_microrregioes - 558 microrregiões
* ingest_regioes_intermediarias - 134 regiões intermediárias
* ingest_regioes_imediatas - 510 regiões imediatas
* ingest_municipios - 5.570 municípios
* ingest_distritos - Aproximadamente 10.000 distritos
* ingest_subdistritos - Aproximadamente 600 subdistritos
* ingest_regioes_metropolitanas - Aproximadamente 80 RMs
* ingest_aglomeracoes_urbanas - Aproximadamente 50 aglomerações
* ingest_regioes_integradas_desenvolvimento - 3 RIDEs

**Pipelines DLT**:
* ibge_pipeline - Transformação Bronze → Silver

**Agendamento**: Diário às 22:00 (10 PM - America/Sao_Paulo)

**Documentação**: [ibge/README.md](ibge/README.md)

---

## Ambientes (Dev e Prod)

O projeto mantém dois ambientes isolados para garantir segurança e qualidade:

### Ambiente de Desenvolvimento (dev)

**Propósito**: Testes, desenvolvimento e experimentação

**Características**:
* Jobs pausados por padrão (execução manual)
* Dados podem ser recriados ou modificados livremente
* Ambiente para validação de código e lógica
* Não afeta dados de produção

**Catálogos**:
* bronze_dev
* silver_dev
* gold_dev

**Ativação de jobs**: Manual via CLI ou interface Databricks

### Ambiente de Produção (prod)

**Propósito**: Dados oficiais e consumo por aplicações

**Características**:
* Jobs agendados automaticamente
* Dados críticos e controlados
* Requer permissões específicas
* Monitoramento e alertas configurados

**Catálogos**:
* bronze_prod
* silver_prod
* gold_prod

**Ativação de jobs**: Automática conforme agendamento

### Isolamento de Ambientes

Os ambientes são completamente isolados através de:

**1. Catálogos separados**:
```yaml
dev: bronze_dev, silver_dev, gold_dev
prod: bronze_prod, silver_prod, gold_prod
```

**2. Workspaces separados no bundle**:
```yaml
dev:
  root_path: /Workspace/Users/{user}/.bundle/{nome}/{target}
  
prod:
  root_path: /Workspace/Users/{user}/.bundle/{nome}/{target}
```

**3. Variáveis de ambiente**:
```yaml
variables:
  environment: dev ou prod
  catalog: bronze_dev ou bronze_prod
  schema: ds_{dominio}
  pause_status: PAUSED (dev) ou UNPAUSED (prod)
```

---

## Controle de Ingestão Incremental

Todos os bundles utilizam um sistema de controle de ingestão incremental para evitar reprocessamento desnecessário:

### Tabela de Controle

**Localização**: `{catalog}.{schema}.controle_ingestao`

**Estrutura**:
```sql
CREATE TABLE controle_ingestao (
    id INT,                              -- ID único do controle (sempre 1)
    raw_pokemon_name INT,                -- Último ID processado de pokemon
    raw_pokemon_number_dex INT,          -- Último número Pokédex processado
    raw_pokemon_abilities INT,           -- Último ID de abilities
    raw_pokemon_versions INT,            -- Último ID de versions
    ... (um campo para cada endpoint)
)
```

**Funcionamento**:

1. Antes da ingestão: O notebook consulta o último ID processado
2. Durante a ingestão: Filtra apenas registros com ID > último_processado
3. Após a ingestão: Atualiza o controle com o novo último ID

**Exemplo de código**:
```python
# Busca último ID
ctrl_id = spark.sql(
    f"SELECT raw_pokemon_name FROM {tabela_controle} WHERE id = 1"
).collect()

ultimo_id = int(ctrl_id[0][0]) if ctrl_id else 0

# Filtra dados incrementais
if pokemon_id <= ultimo_id:
    continue  # Pula registro já processado

# Atualiza controle
spark.sql(f"""
    UPDATE {tabela_controle}
    SET raw_pokemon_name = {novo_ultimo_id}
    WHERE id = 1
""")
```

---

## Padrões de Desenvolvimento

### Estrutura de um Asset Bundle

```
nome_bundle/
├── databricks.yml              # Configuração principal do bundle
├── resources/
│   ├── {nome}_job.job.yml     # Definição do job
│   └── {nome}_etl.pipeline.yml # Definição dos pipelines DLT
├── src/
│   ├── ingest_{entidade1}.py  # Notebooks de ingestão
│   ├── ingest_{entidade2}.py
│   ├── dlt_{nome}.sql         # Transformações DLT
│   └── ...
├── tests/
│   ├── conftest.py            # Configuração de testes
│   └── *_test.py              # Testes unitários
├── fixtures/                   # Dados de teste
├── .vscode/                    # Configurações VS Code
├── .gitignore
└── README.md                   # Documentação do bundle
```

### Padrão de Notebooks de Ingestão

Todos os notebooks de ingestão seguem uma estrutura consistente:

**Célula 1**: Imports
```python
import requests
import json
import time
from datetime import datetime
```

**Célula 2**: Configuração de widgets e parâmetros
```python
dbutils.widgets.text("catalog", "bronze_prod")
used_catalog = dbutils.widgets.get("catalog")

dbutils.widgets.text("schema", "ds_{dominio}")
used_schema = dbutils.widgets.get("schema")

tabela_controle = f"{used_catalog}.{used_schema}.controle_ingestao"
tabela_destino = f"{used_catalog}.{used_schema}.raw_{entidade}"
```

**Célula 3**: Busca último ID processado
```python
ctrl_id = spark.sql(
    f"SELECT raw_{entidade} FROM {tabela_controle} WHERE id = 1"
).collect()

ultimo_id = int(ctrl_id[0][0]) if ctrl_id else 0
```

**Célula 4**: Ingestão com filtro incremental
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

**Célula 5**: Gravação e atualização de controle
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

### Padrão de Jobs

Todos os jobs seguem uma estrutura de dependências sequenciais:

```yaml
tasks:
  - task_key: ingest_entidade1
    notebook_task:
      notebook_path: ../src/ingest_entidade1.py
    max_retries: 1

  - task_key: ingest_entidade2
    depends_on:
      - task_key: ingest_entidade1
    notebook_task:
      notebook_path: ../src/ingest_entidade2.py
    max_retries: 1

  - task_key: refresh_pipeline
    depends_on:
      - task_key: ingest_entidade1
      - task_key: ingest_entidade2
    pipeline_task:
      pipeline_id: ${resources.pipelines.pipeline_name.id}
    max_retries: 1
```

---

## Tecnologias Utilizadas

### Databricks

**Databricks Asset Bundles**:
* Orquestração de workflows
* Deploy declarativo via YAML
* Versionamento e governança
* Isolamento de ambientes

**Delta Live Tables (DLT)**:
* Pipelines de transformação gerenciados
* Atualização automática de dependências
* Expectativas e qualidade de dados
* Serverless para escalabilidade

**Delta Lake**:
* Armazenamento ACID sobre Data Lake
* Time travel (viagem no tempo)
* Versionamento de dados
* Otimizações automáticas (compaction, Z-order)

**Unity Catalog**:
* Governança centralizada de dados
* Controle de acesso fino (RBAC)
* Linhagem de dados
* Auditoria e compliance

**Databricks Workflows**:
* Orquestração serverless de jobs
* Retry automático em falhas
* Notificações por email
* Monitoramento integrado

### Linguagens e Frameworks

**Python**:
* PySpark para processamento distribuído
* Requests para chamadas HTTP
* Pandas para manipulação de dados

**SQL**:
* Spark SQL para transformações
* Delta Live Tables SQL
* Queries analíticas

**YAML**:
* Configuração de bundles
* Definição de jobs e pipelines
* Parametrização de ambientes

---

## Como Usar

### Pré-requisitos

**1. Databricks CLI instalado localmente**:
```bash
pip install databricks-cli
```

**2. Autenticação configurada**:
```bash
databricks configure --token

# Informar:
# Host: https://dbc-414ad402-adeb.cloud.databricks.com
# Token: [seu token de acesso pessoal]
```

**3. Git configurado** (para CI/CD):
```bash
git clone <url_do_repositorio>
cd project_lake_house
```

### Deploy de um Bundle

**Desenvolvimento**:
```bash
cd carros  # ou pokemon, ou ibge
databricks bundle deploy -t dev
```

**Produção**:
```bash
cd carros
databricks bundle deploy -t prod
```

### Executar um Job Manualmente

**Desenvolvimento**:
```bash
cd carros
databricks bundle run carros_job -t dev
```

**Produção**:
```bash
cd carros
databricks bundle run carros_job -t prod
```

### Validar Configuração

Antes de fazer deploy, valide a configuração:
```bash
cd carros
databricks bundle validate
```

### Acompanhar Execução

Após executar um job, acesse o Databricks workspace:
* Navegue para Workflows → Jobs
* Localize o job pelo nome (ex: carros_dev ou carros_prod)
* Acompanhe o status e logs de cada task

---

## Boas Práticas Implementadas

### Governança de Dados

* Separação de ambientes (dev/prod)
* Catálogos isolados por camada e ambiente
* Unity Catalog para controle de acesso
* Auditoria de alterações via Delta Lake

### Qualidade de Dados

* Ingestão incremental com controle de estado
* Validação de integridade referencial
* Deduplicação de registros
* Limpeza de dados nulos e inconsistentes

### Performance

* Delta Lake para otimização automática
* Serverless para escalabilidade
* Particionamento de tabelas grandes
* Z-ordering em colunas de filtro

### Confiabilidade

* Retry automático em falhas (max_retries: 1)
* Notificações por email em erros
* Rate limiting em chamadas de API
* Tratamento de erros HTTP

### Desenvolvimento

* Estrutura padronizada de bundles
* Notebooks parametrizados via widgets
* Testes unitários (pasta tests/)
* Documentação inline e READMEs

### CI/CD

* GitHub Actions para deploy automático
* Validação antes de merge
* Deploy separado por ambiente
* Versionamento de código

---

## Estrutura de Pastas

```
project_lake_house/
├── README.md                   # Este arquivo (documentação principal)
├── LICENSE                     # Licença do projeto
│
├── .github/
│   └── workflows/              # GitHub Actions para CI/CD
│       ├── carros-ci-cd.yml
│       ├── pokemon-ci-cd.yml
│       └── ibge-ci-cd.yml
│
├── carros/                     # Bundle FIPE/Carros
│   ├── README.md
│   ├── databricks.yml
│   ├── pyproject.toml
│   ├── resources/
│   │   ├── carros_job.job.yml
│   │   └── carros_etl.pipeline.yml
│   ├── src/
│   │   ├── ingest_marcas.py
│   │   ├── ingest_modelos.py
│   │   ├── ingest_anos.py
│   │   ├── ingest_referencias.py
│   │   ├── ingest_fipe.py
│   │   ├── dlt_carros.sql
│   │   └── dlt_carros_gold.sql
│   ├── tests/
│   ├── fixtures/
│   └── .vscode/
│
├── pokemon/                    # Bundle PokeAPI
│   ├── README.md
│   ├── databricks.yml
│   ├── resources/
│   │   ├── pokemon.job.yml
│   │   └── pokemon.pipeline.yml
│   ├── src/
│   │   ├── ingest_pokedex.py
│   │   ├── ingest_pokemon.py
│   │   ├── ingest_abilities.py
│   │   ├── ingest_pokemon_genders.py
│   │   ├── ingest_pokemon_growth_rates.py
│   │   ├── ingest_pokemon_location_areas.py
│   │   ├── ingest_pokemon_natures.py
│   │   ├── ingest_pokemon_pokeathlon_stats.py
│   │   ├── ingest_egg_groups.py
│   │   ├── ingest_locations.py
│   │   ├── ingest_items.py
│   │   ├── ingest_items_attributes.py
│   │   ├── ingest_versions.py
│   │   └── dlt_pokemon.sql
│   ├── tests/
│   ├── fixtures/
│   └── .vscode/
│
├── ibge/                       # Bundle IBGE
│   ├── README.md
│   ├── databricks.yml
│   ├── resources/
│   │   ├── ibge_job.job.yml
│   │   └── ibge_etl.pipeline.yml
│   ├── src/
│   │   ├── ingest_regioes.py
│   │   ├── ingest_estados.py
│   │   ├── ingest_mesorregioes.py
│   │   ├── ingest_microrregioes.py
│   │   ├── ingest_regioes_intermediarias.py
│   │   ├── ingest_regioes_imediatas.py
│   │   ├── ingest_municipios.py
│   │   ├── ingest_distritos.py
│   │   ├── ingest_subdistritos.py
│   │   ├── ingest_regioes_metropolitanas.py
│   │   ├── ingest_aglomeracoes_urbanas.py
│   │   ├── ingest_regioes_integradas_desenvolvimento.py
│   │   └── dlt_ibge.sql
│   ├── tests/
│   ├── fixtures/
│   └── .vscode/
│
├── dm_carros/                  # Data Mart Carros (planejado)
├── dm_pokemon/                 # Data Mart Pokemon (planejado)
└── dm_ibge/                    # Data Mart IBGE (planejado)
```

---

## Notificações e Monitoramento

Todos os jobs estão configurados para enviar notificações em caso de falha:

**Email de notificação**: delacortearthur@gmail.com

**Eventos notificados**:
* Falha em qualquer task do job
* Timeout de execução
* Erros de permissão
* Falhas de conectividade com APIs

**Logs disponíveis**:
* Databricks Workflows → Jobs → [Nome do Job]
* Logs de cada task individual
* Métricas de execução (tempo, recursos)

---

## Roadmap e Melhorias Futuras

### Curto Prazo

* Implementar camada Gold para Pokemon e IBGE
* Adicionar testes unitários completos
* Configurar monitoramento proativo (alertas)
* Documentar queries SQL dos pipelines DLT

### Médio Prazo

* Adicionar novo bundle BCB (Banco Central do Brasil)
* Implementar data quality expectations no DLT
* Criar dashboards de monitoramento
* Automatizar geração de documentação

### Longo Prazo

* Machine Learning pipelines
* API de consumo de dados
* Data observability completa
* Disaster recovery e backup

---

## Referências e Links Úteis

**Documentação Databricks**:
* [Asset Bundles](https://docs.databricks.com/dev-tools/bundles/)
* [Delta Live Tables](https://docs.databricks.com/delta-live-tables/)
* [Unity Catalog](https://docs.databricks.com/data-governance/unity-catalog/)
* [Workflows](https://docs.databricks.com/workflows/)

**Fontes de Dados**:
* [Tabela FIPE](https://veiculos.fipe.org.br/)
* [PokeAPI](https://pokeapi.co/)
* [API IBGE Localidades](https://servicodados.ibge.gov.br/api/docs/localidades)

**Arquitetura**:
* [Medallion Architecture](https://www.databricks.com/glossary/medallion-architecture)
* [Delta Lake](https://delta.io/)
* [Lakehouse Architecture](https://www.databricks.com/product/data-lakehouse)

---

## Contato e Manutenção

**Mantido por**: delacortearthur@gmail.com  
**Workspace**: https://dbc-414ad402-adeb.cloud.databricks.com  
**Última atualização**: 2024-04-14

---

## Licença

Consulte o arquivo [LICENSE](LICENSE) para detalhes sobre a licença do projeto.
