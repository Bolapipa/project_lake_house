# Asset Bundle: IBGE (Divisões Territoriais do Brasil)

## Visão Geral

Este asset bundle implementa um pipeline completo de ingestão e transformação de dados do **IBGE** (Instituto Brasileiro de Geografia e Estatística), especificamente da API de **Localidades**, que fornece informações sobre todas as divisões territoriais do Brasil.

O projeto coleta dados de **12 níveis hierárquicos** da divisão territorial brasileira, desde regiões até subdistritos, seguindo a **Arquitetura Medalhão** (Bronze → Silver).

---

## Arquitetura do Projeto

### Estrutura de Diretórios

```
ibge/
├── databricks.yml              # Configuração principal do bundle
├── resources/
│   ├── ibge_job.job.yml       # Definição do workflow/job
│   └── ibge_etl.pipeline.yml  # Definição do pipeline DLT
├── src/
│   ├── ingest_regioes.py                            # Regiões (Norte, Sul, etc)
│   ├── ingest_estados.py                            # Estados (UF)
│   ├── ingest_mesorregioes.py                       # Mesorregiões
│   ├── ingest_microrregioes.py                      # Microrregiões
│   ├── ingest_regioes_intermediarias.py             # Regiões Intermediárias
│   ├── ingest_regioes_imediatas.py                  # Regiões Imediatas
│   ├── ingest_municipios.py                         # Municípios
│   ├── ingest_distritos.py                          # Distritos
│   ├── ingest_subdistritos.py                       # Subdistritos
│   ├── ingest_regioes_metropolitanas.py             # Regiões Metropolitanas
│   ├── ingest_aglomeracoes_urbanas.py               # Aglomerações Urbanas
│   ├── ingest_regioes_integradas_desenvolvimento.py # Regiões Integradas (RIDEs)
│   └── dlt_ibge.sql                                 # Transformações Bronze → Silver
├── tests/                                            # Testes unitários
└── fixtures/                                         # Dados de teste
```

---

## Fluxo de Dados Completo

### Camada Bronze: Ingestão Hierárquica de Divisões Territoriais

O processo de ingestão segue a **hierarquia administrativa** do Brasil, do nível mais alto (regiões) ao mais baixo (subdistritos):

```
                    ┌─────────────────┐
                    │ ingest_regioes  │ ← 5 Regiões (Norte, Nordeste, Sul, etc)
                    └────────┬────────┘
                             │
                             ↓
                    ┌─────────────────┐
                    │ ingest_estados  │ ← 27 Estados + DF
                    └────────┬────────┘
                             │
                             ↓
                ┌────────────────────────┐
                │ ingest_mesorregioes    │ ← 137 Mesorregiões
                └────────┬───────────────┘
                         │
                         ↓
                ┌────────────────────────┐
                │ ingest_microrregioes   │ ← 558 Microrregiões
                └────────┬───────────────┘
                         │
                         ↓
        ┌─────────────────────────────────────┐
        │ ingest_regioes_intermediarias       │ ← Regiões Intermediárias (IBGE 2017)
        └─────────────┬───────────────────────┘
                      │
                      ↓
        ┌─────────────────────────────────────┐
        │ ingest_regioes_imediatas            │ ← Regiões Imediatas (IBGE 2017)
        └─────────────┬───────────────────────┘
                      │
                      ↓
                ┌─────────────────┐
                │ingest_municipios│ ← 5.570 Municípios brasileiros
                └────────┬────────┘
                         │
                         ↓
                ┌─────────────────┐
                │ingest_distritos │ ← Distritos municipais
                └────────┬────────┘
                         │
                         ↓
              ┌──────────────────────┐
              │ ingest_subdistritos  │ ← Subdistritos
              └──────────┬───────────┘
                         │
                         ↓
      ┌──────────────────────────────────────┐
      │ ingest_regioes_metropolitanas        │ ← Regiões Metropolitanas (ex: Grande SP)
      └──────────┬───────────────────────────┘
                 │
                 ↓
      ┌──────────────────────────────────────┐
      │ ingest_aglomeracoes_urbanas          │ ← Aglomerações Urbanas
      └──────────┬───────────────────────────┘
                 │
                 ↓
      ┌──────────────────────────────────────────────┐
      │ ingest_regioes_integradas_desenvolvimento   │ ← RIDEs (ex: RIDE-DF)
      └──────────────────────────────────────────────┘
```

#### Detalhamento das Tasks de Ingestão

 # | Task | Descrição | Qtd. Aproximada | Endpoint IBGE |
---|------|-----------|-----------------|---------------|
 1 | **ingest_regioes** | 5 macrorregiões do Brasil | 5 | `/api/v1/localidades/regioes` |
 2 | **ingest_estados** | Estados + Distrito Federal | 27 | `/api/v1/localidades/estados` |
 3 | **ingest_mesorregioes** | Subdivisões dos estados | 137 | `/api/v1/localidades/mesorregioes` |
 4 | **ingest_microrregioes** | Subdivisões das mesorregiões | 558 | `/api/v1/localidades/microrregioes` |
 5 | **ingest_regioes_intermediarias** | Nova divisão regional (2017) | 134 | `/api/v1/localidades/regioes-intermediarias` |
 6 | **ingest_regioes_imediatas** | Subdivisão das intermediárias | 510 | `/api/v1/localidades/regioes-imediatas` |
 7 | **ingest_municipios** | Todos os municípios do Brasil | 5.570 | `/api/v1/localidades/municipios` |
 8 | **ingest_distritos** | Distritos municipais | aprox. 10.000 | `/api/v1/localidades/distritos` |
 9 | **ingest_subdistritos** | Subdivisões de distritos | aprox. 600 | `/api/v1/localidades/subdistritos` |
 10 | **ingest_regioes_metropolitanas** | Grandes áreas urbanas | aprox. 80 | `/api/v1/localidades/regioes-metropolitanas` |
 11 | **ingest_aglomeracoes_urbanas** | Concentrações urbanas menores | aprox. 50 | `/api/v1/localidades/aglomeracoes-urbanas` |
 12 | **ingest_regioes_integradas_desenvolvimento** | RIDEs (desenvolvimento integrado) | 3 | `/api/v1/localidades/regioes-integradas-desenvolvimento` |

**Nota**: Todas as tasks salvam dados no catálogo **bronze_dev** (dev) ou **bronze_prod** (prod) no schema `ds_ibge`.

---

### Camada Silver: Limpeza e Padronização

Após todas as ingestões, o **pipeline DLT** (`ibge_pipeline`) transforma os dados:

```
Pipeline: ibge_pipeline
├── Nome: ibge_etl_dev / ibge_etl_prod
├── Catálogo: silver_dev / silver_prod
├── Schema: ds_ibge
└── Notebook: dlt_ibge.sql
```

**Transformações realizadas:**
* **Normalização** de dados JSON da API
* **Padronização** de códigos IBGE
* **Criação de hierarquias** explícitas (região → estado → município)
* **Deduplicação** de registros
* **Enriquecimento** com chaves estrangeiras
* **Validação** de integridade referencial
* **Geocodificação** (latitude/longitude quando disponível)
* **Versionamento** de mudanças territoriais

---

## Hierarquia Territorial Brasileira

O projeto captura duas classificações territoriais complementares:

### Classificação Tradicional (até 2017)
```
Região (5)
  └─ Estado/UF (27)
      └─ Mesorregião (137)
          └─ Microrregião (558)
              └─ Município (5.570)
                  └─ Distrito (aprox. 10.000)
                      └─ Subdistrito (aprox. 600)
```

### Nova Classificação (a partir de 2017)
```
Região (5)
  └─ Estado/UF (27)
      └─ Região Intermediária (134)
          └─ Região Imediata (510)
              └─ Município (5.570)
```

### Agrupamentos Especiais
* **Regiões Metropolitanas**: Grandes aglomerações urbanas (ex: Grande São Paulo, Grande Rio)
* **Aglomerações Urbanas**: Concentrações urbanas menores
* **RIDEs**: Regiões Integradas de Desenvolvimento (ex: RIDE-DF, RIDE-Petrolina/Juazeiro)

---

## Job e Orquestração

### Job: ibge_job

O job orquestra todo o fluxo de ingestão e transformação:

```yaml
Nome: ibge_dev (dev) / ibge_prod (prod)
Agendamento: Diário às 22:00 (10 PM - America/Sao_Paulo)
Status: PAUSED (dev e prod)
```

### Diagrama de Dependências do Job

```
ingest_regioes
      │
      └──> ingest_estados
                │
                └──> ingest_mesorregioes
                        │
                        └──> ingest_microrregioes
                                │
                                └──> ingest_regioes_intermediarias
                                        │
                                        └──> ingest_regioes_imediatas
                                                │
                                                └──> ingest_municipios
                                                        │
                                                        └──> ingest_distritos
                                                                │
                                                                └──> ingest_subdistritos
                                                                        │
                                                                        └──> ingest_regioes_metropolitanas
                                                                                │
                                                                                └──> ingest_aglomeracoes_urbanas
                                                                                        │
                                                                                        └──> ingest_regioes_integradas_desenvolvimento
                                                                                                │
        ┌───────────────────────────────────────────────────────────────────────────────────────┘
        │
        │ (Aguarda TODAS as 12 tasks de ingestão)
        │
        └──────────> refresh_pipeline
```

### Parâmetros do Job

 Parâmetro | Descrição | Padrão (dev) | Produção |
-----------|-----------|--------------|----------|
 `environment` | Ambiente de execução | `dev` | `prod` |
 `catalog` | Catálogo Bronze de destino | `bronze_dev` | `bronze_prod` |
 `schema` | Schema de destino | `ds_ibge` | `ds_ibge` |

---

## Ambientes (Targets)

### Ambiente de Desenvolvimento (dev)
```yaml
Catálogo Bronze: bronze_dev
Catálogo Silver: silver_dev
Schema: ds_ibge (em ambos)
Agendamento: PAUSED (execução manual)
```

### Ambiente de Produção (prod)
```yaml
Catálogo Bronze: bronze_prod
Catálogo Silver: silver_prod
Schema: ds_ibge (em ambos)
Agendamento: PAUSED (necessário ativar manualmente)
```

---

## Como Usar

### Deploy do Bundle

```bash
# Deploy em desenvolvimento
databricks bundle deploy -t dev

# Deploy em produção
databricks bundle deploy -t prod
```

### Executar Job Manualmente

```bash
# Executar em dev
databricks bundle run ibge_job -t dev

# Executar em prod
databricks bundle run ibge_job -t prod
```

### Validar Configuração

```bash
databricks bundle validate
```

---

## Tabelas Geradas

### Camada Bronze (bronze_dev.ds_ibge)
* `tb_regioes_bronze` - Regiões brutas
* `tb_estados_bronze` - Estados brutos
* `tb_mesorregioes_bronze` - Mesorregiões brutas
* `tb_microrregioes_bronze` - Microrregiões brutas
* `tb_regioes_intermediarias_bronze` - Regiões intermediárias brutas
* `tb_regioes_imediatas_bronze` - Regiões imediatas brutas
* `tb_municipios_bronze` - Municípios brutos
* `tb_distritos_bronze` - Distritos brutos
* `tb_subdistritos_bronze` - Subdistritos brutos
* `tb_regioes_metropolitanas_bronze` - RMs brutas
* `tb_aglomeracoes_urbanas_bronze` - Aglomerações brutas
* `tb_rides_bronze` - RIDEs brutas

### Camada Silver (silver_dev.ds_ibge)
* `dim_regiao` - Dimensão de regiões
* `dim_estado` - Dimensão de estados/UF
* `dim_mesorregiao` - Dimensão de mesorregiões
* `dim_microrregiao` - Dimensão de microrregiões
* `dim_regiao_intermediaria` - Dimensão de regiões intermediárias
* `dim_regiao_imediata` - Dimensão de regiões imediatas
* `dim_municipio` - Dimensão de municípios (principal)
* `dim_distrito` - Dimensão de distritos
* `dim_subdistrito` - Dimensão de subdistritos
* `dim_regiao_metropolitana` - Dimensão de RMs
* `dim_aglomeracao_urbana` - Dimensão de aglomerações
* `dim_ride` - Dimensão de RIDEs
* `vw_hierarquia_completa` - View com hierarquia completa
* `vw_municipios_enriquecidos` - View com todos os relacionamentos

---

## Notificações

Em caso de falha em qualquer task, um email é enviado para:
* **delacortearthur@gmail.com**

---

## Tecnologias Utilizadas

* **Databricks Asset Bundles**: Orquestração e deploy
* **Delta Live Tables (DLT)**: Pipelines de transformação
* **Delta Lake**: Armazenamento ACID
* **Unity Catalog**: Governança de dados
* **PySpark**: Processamento de dados
* **API IBGE**: Fonte de dados oficial
* **Databricks Workflows**: Orquestração serverless

---

## Boas Práticas Implementadas

* **Ingestão sequencial hierárquica**
* **Separação de ambientes** (dev/prod)
* **Arquitetura Medalhão** (Bronze/Silver)
* **Testes automatizados** (pasta tests/)
* **Retry automático** (max_retries: 1)
* **Notificações de erro** por email
* **Parametrização** via variáveis
* **Serverless DLT** para escalabilidade
* **Validação** de integridade territorial
* **Versionamento** de mudanças históricas

---

## Casos de Uso

Este dataset pode ser usado para:

* **Análises demográficas** e socioeconômicas
* **Geocodificação** e geolocalização
* **Mapas interativos** e dashboards regionais
* **Análises governamentais** e políticas públicas
* **Planejamento urbano** e regional
* **Visualizações** de indicadores por região
* **Busca** e filtragem de localidades
* **Agregações** e estatísticas territoriais
* **Enriquecimento** de bases de dados com informações geográficas

---

## Referências

* [API IBGE Localidades](https://servicodados.ibge.gov.br/api/docs/localidades)
* [Divisão Regional do Brasil](https://www.ibge.gov.br/geociencias/organizacao-do-territorio/divisao-regional/15778-divisoes-regionais-do-brasil.html)
* [Databricks Asset Bundles Documentation](https://docs.databricks.com/dev-tools/bundles/)
* [Delta Live Tables Documentation](https://docs.databricks.com/delta-live-tables/)

---

## Estrutura de Códigos IBGE

* **Região**: 1 dígito (1=Norte, 2=Nordeste, 3=Sudeste, 4=Sul, 5=Centro-Oeste)
* **Estado/UF**: 2 dígitos (11-53)
* **Mesorregião**: 4 dígitos
* **Microrregião**: 5 dígitos
* **Município**: 7 dígitos (os 2 primeiros identificam o estado)
* **Distrito**: 9 dígitos
* **Subdistrito**: 11 dígitos

---

**Última atualização**: $(date +"%Y-%m-%d")  
**Mantido por**: delacortearthur@gmail.com  
**Fonte de dados**: IBGE - Instituto Brasileiro de Geografia e Estatística
