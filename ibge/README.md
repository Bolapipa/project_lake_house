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

| # | Task | Descrição | Qtd. Aproximada | Endpoint IBGE |
|---|------|-----------|-----------------|---------------|
| 1 | **ingest_regioes** | 5 macrorregiões do Brasil | 5 | `/api/v1/localidades/regioes` |
| 2 | **ingest_estados** | Estados + Distrito Federal | 27 | `/api/v1/localidades/estados` |
| 3 | **ingest_mesorregioes** | Subdivisões dos estados | 137 | `/api/v1/localidades/mesorregioes` |
| 4 | **ingest_microrregioes** | Subdivisões das mesorregiões | 558 | `/api/v1/localidades/microrregioes` |
| 5 | **ingest_regioes_intermediarias** | Nova divisão regional (2017) | 134 | `/api/v1/localidades/regioes-intermediarias` |
| 6 | **ingest_regioes_imediatas** | Subdivisão das intermediárias | 510 | `/api/v1/localidades/regioes-imediatas` |
| 7 | **ingest_municipios** | Todos os municípios do Brasil | 5.570 | `/api/v1/localidades/municipios` |
| 8 | **ingest_distritos** | Distritos municipais | aprox. 10.000 | `/api/v1/localidades/distritos` |
| 9 | **ingest_subdistritos** | Subdivisões de distritos | aprox. 600 | `/api/v1/localidades/subdistritos` |
| 10 | **ingest_regioes_metropolitanas** | Grandes áreas urbanas | aprox. 80 | `/api/v1/localidades/regioes-metropolitanas` |
| 11 | **ingest_aglomeracoes_urbanas** | Concentrações urbanas menores | aprox. 50 | `/api/v1/localidades/aglomeracoes-urbanas` |
| 12 | **ingest_regioes_integradas_desenvolvimento** | RIDEs (desenvolvimento integrado) | 3 | `/api/v1/localidades/regioes-integradas-desenvolvimento` |

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

## Variáveis de Ambiente e Parâmetros

### Variáveis do databricks.yml

O arquivo `databricks.yml` define as seguintes variáveis de ambiente:

```yaml
variables:
  environment:
    description: "Environment name (dev or prod)"
    default: dev

  catalog:
    description: "Catálogo Bronze de destino"
    default: bronze_dev

  schema:
    description: "Schema de destino"
    default: ds_ibge

  silver_catalog:
    description: "Catálogo Silver de destino"
    default: silver_dev

  pause_status:
    description: "Job schedule status"
    default: PAUSED

  pipeline_development:
    description: "Pipeline development mode flag"
    default: true
```

### Configuração por Target

#### Target: dev (Desenvolvimento)

```yaml
targets:
  dev:
    mode: development
    default: true
    variables:
      environment: dev
      catalog: bronze_dev
      schema: ds_ibge
      silver_catalog: silver_dev
      pause_status: PAUSED
      pipeline_development: true
```

#### Target: prod (Produção)

```yaml
targets:
  prod:
    mode: production
    variables:
      environment: prod
      catalog: bronze_prod
      schema: ds_ibge
      silver_catalog: silver_prod
      pause_status: PAUSED  # Requer ativação manual
      pipeline_development: false
```

### Parâmetros dos Notebooks

Todos os notebooks de ingestão recebem os seguintes parâmetros via `dbutils.widgets`:

| Parâmetro | Tipo | Descrição | Exemplo |
|-----------|------|-----------|---------|
| `catalog` | String | Catálogo de destino | `bronze_dev` ou `bronze_prod` |
| `schema` | String | Schema de destino | `ds_ibge` |

**Exemplo de uso no notebook**:
```python
dbutils.widgets.text("catalog", "bronze_prod")
used_catalog = dbutils.widgets.get("catalog")

dbutils.widgets.text("schema", "ds_ibge")
used_schema = dbutils.widgets.get("schema")

tabela_destino = f"{used_catalog}.{used_schema}.raw_municipios"
```

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
* `raw_regioes` - Regiões brutas
* `raw_ufs` - Estados brutos
* `raw_mesorregioes` - Mesorregiões brutas
* `raw_microrregioes` - Microrregiões brutas
* `raw_regioes_intermediarias` - Regiões intermediárias brutas
* `raw_regioes_imediatas` - Regiões imediatas brutas
* `raw_municipios` - Municípios brutos
* `raw_distritos` - Distritos brutos
* `raw_subdistritos` - Subdistritos brutos
* `raw_regioes_metropolitanas` - RMs brutas
* `raw_aglomeracoes_urbanas` - Aglomerações brutas
* `raw_regioes_integradas_desenvolvimento` - RIDEs brutas

### Camada Silver (silver_dev.ds_ibge)
* `cleaned_regioes` - Dimensão de regiões
* `cleaned_estados` - Dimensão de estados/UF
* `cleaned_mesorregioes` - Dimensão de mesorregiões
* `cleaned_microrregioes` - Dimensão de microrregiões
* `cleaned_regioes_intermediarias` - Dimensão de regiões intermediárias
* `cleaned_regioes_imediatas` - Dimensão de regiões imediatas
* `cleaned_municipios` - Dimensão de municípios (principal)
* `cleaned_distritos` - Dimensão de distritos
* `cleaned_subdistritos` - Dimensão de subdistritos
* `cleaned_regioes_metropolitanas` - Dimensão de RMs
* `cleaned_aglomeracoes_urbanas` - Dimensão de aglomerações
* `cleaned_regioes_integradas_desenvolvimento` - Dimensão de RIDEs

---

## Métricas de Volume e Performance

### Volume de Dados Estimado

| Camada | Tabelas | Registros Totais | Tamanho Aprox. |
|--------|---------|------------------|----------------|
| Bronze | 12 tabelas | ~17.000 registros | ~5 MB |
| Silver | 12 tabelas | ~17.000 registros | ~4 MB (otimizado) |

### Detalhamento por Tabela (Bronze)

| Tabela | Registros | Descrição |
|--------|-----------|-----------|
| `raw_regioes` | 5 | 5 macrorregiões |
| `raw_ufs` | 27 | Estados + DF |
| `raw_mesorregioes` | 137 | Mesorregiões |
| `raw_microrregioes` | 558 | Microrregiões |
| `raw_regioes_intermediarias` | 134 | Regiões intermediárias |
| `raw_regioes_imediatas` | 510 | Regiões imediatas |
| `raw_municipios` | 5.570 | Todos os municípios |
| `raw_distritos` | ~10.000 | Distritos municipais |
| `raw_subdistritos` | ~600 | Subdistritos |
| `raw_regioes_metropolitanas` | ~80 | RMs |
| `raw_aglomeracoes_urbanas` | ~50 | Aglomerações |
| `raw_regioes_integradas_desenvolvimento` | 3 | RIDEs |

### Performance de Execução

| Task | Tempo Médio | Registros Processados |
|------|-------------|------------------------|
| `ingest_regioes` | ~5 segundos | 5 |
| `ingest_estados` | ~10 segundos | 27 |
| `ingest_mesorregioes` | ~30 segundos | 137 |
| `ingest_microrregioes` | ~1 minuto | 558 |
| `ingest_regioes_intermediarias` | ~30 segundos | 134 |
| `ingest_regioes_imediatas` | ~1 minuto | 510 |
| `ingest_municipios` | ~5 minutos | 5.570 |
| `ingest_distritos` | ~10 minutos | ~10.000 |
| `ingest_subdistritos` | ~2 minutos | ~600 |
| `ingest_regioes_metropolitanas` | ~30 segundos | ~80 |
| `ingest_aglomeracoes_urbanas` | ~20 segundos | ~50 |
| `ingest_regioes_integradas_desenvolvimento` | ~5 segundos | 3 |
| **Pipeline Silver** | ~5 minutos | ~17.000 registros |
| **Total (primeira execução)** | ~25-30 minutos | - |
| **Total (incremental)** | ~5-10 minutos | Apenas novos dados |

**Nota**: Tempos podem variar conforme disponibilidade da API IBGE. Dados geográficos raramente mudam.

---

## Exemplos de Queries SQL

### 1. Hierarquia Completa: Região → Estado → Município

```sql
SELECT 
    r.nome AS regiao,
    COUNT(DISTINCT e.id) AS qtd_estados,
    COUNT(DISTINCT m.id) AS qtd_municipios,
    STRING_AGG(DISTINCT e.sigla, ', ') AS siglas_estados
FROM silver_prod.ds_ibge.cleaned_regioes r
JOIN silver_prod.ds_ibge.cleaned_estados e ON r.id = e.regiao_id
JOIN silver_prod.ds_ibge.cleaned_municipios m ON e.id = m.microrregiao_mesorregiao_UF_regiao_id
GROUP BY r.id, r.nome
ORDER BY r.id;
```

### 2. Top 10 Estados com Mais Municípios

```sql
SELECT 
    e.sigla AS uf,
    e.nome AS estado,
    r.nome AS regiao,
    COUNT(m.id) AS qtd_municipios,
    ROUND(COUNT(m.id) * 100.0 / (SELECT COUNT(*) FROM silver_prod.ds_ibge.cleaned_municipios), 2) AS percentual_total
FROM silver_prod.ds_ibge.cleaned_estados e
JOIN silver_prod.ds_ibge.cleaned_regioes r ON e.regiao_id = r.id
LEFT JOIN silver_prod.ds_ibge.cleaned_municipios m 
    ON e.id = m.microrregiao_mesorregiao_UF_regiao_id
GROUP BY e.id, e.sigla, e.nome, r.nome
ORDER BY qtd_municipios DESC
LIMIT 10;
```

### 3. Regiões Metropolitanas e Seus Municípios

```sql
SELECT 
    rm.nome AS regiao_metropolitana,
    e.sigla AS uf,
    COUNT(DISTINCT m.id) AS qtd_municipios,
    STRING_AGG(m.nome, ', ') AS municipios
FROM silver_prod.ds_ibge.cleaned_regioes_metropolitanas rm
JOIN bronze_prod.ds_ibge.raw_regioes_metropolitanas rm_raw ON rm.id = rm_raw.id
JOIN silver_prod.ds_ibge.cleaned_municipios m 
    ON rm_raw.municipios LIKE CONCAT('%', CAST(m.id AS STRING), '%')  -- Simplificação
JOIN silver_prod.ds_ibge.cleaned_estados e ON m.microrregiao_mesorregiao_UF_regiao_id = e.id
GROUP BY rm.id, rm.nome, e.sigla
ORDER BY qtd_municipios DESC;
```

### 4. Comparação: Classificação Antiga vs Nova (2017)

```sql
WITH dados_municipios AS (
    SELECT 
        m.id AS municipio_id,
        m.nome AS municipio,
        e.sigla AS uf,
        -- Classificação antiga
        mic.nome AS microrregiao,
        mes.nome AS mesorregiao,
        -- Classificação nova (2017)
        ri.nome AS regiao_imediata,
        rint.nome AS regiao_intermediaria
    FROM silver_prod.ds_ibge.cleaned_municipios m
    JOIN silver_prod.ds_ibge.cleaned_estados e 
        ON m.microrregiao_mesorregiao_UF_regiao_id = e.id
    LEFT JOIN silver_prod.ds_ibge.cleaned_microrregioes mic 
        ON m.microrregiao_id = mic.id
    LEFT JOIN silver_prod.ds_ibge.cleaned_mesorregioes mes 
        ON mic.mesorregiao_id = mes.id
    LEFT JOIN silver_prod.ds_ibge.cleaned_regioes_imediatas ri 
        ON m.regiao_imediata_id = ri.id
    LEFT JOIN silver_prod.ds_ibge.cleaned_regioes_intermediarias rint 
        ON ri.regiao_intermediaria_id = rint.id
)
SELECT 
    uf,
    COUNT(DISTINCT microrregiao) AS qtd_microrregioes,
    COUNT(DISTINCT mesorregiao) AS qtd_mesorregioes,
    COUNT(DISTINCT regiao_imediata) AS qtd_regioes_imediatas,
    COUNT(DISTINCT regiao_intermediaria) AS qtd_regioes_intermediarias
FROM dados_municipios
GROUP BY uf
ORDER BY uf;
```

### 5. Densidade de Municípios por Região

```sql
WITH contagem AS (
    SELECT 
        r.nome AS regiao,
        COUNT(DISTINCT e.id) AS qtd_estados,
        COUNT(DISTINCT m.id) AS qtd_municipios
    FROM silver_prod.ds_ibge.cleaned_regioes r
    JOIN silver_prod.ds_ibge.cleaned_estados e ON r.id = e.regiao_id
    JOIN silver_prod.ds_ibge.cleaned_municipios m 
        ON e.id = m.microrregiao_mesorregiao_UF_regiao_id
    GROUP BY r.id, r.nome
)
SELECT 
    regiao,
    qtd_estados,
    qtd_municipios,
    ROUND(qtd_municipios * 1.0 / qtd_estados, 2) AS municipios_por_estado,
    ROUND(qtd_municipios * 100.0 / (SELECT SUM(qtd_municipios) FROM contagem), 2) AS percentual_brasil
FROM contagem
ORDER BY qtd_municipios DESC;
```

### 6. Buscar Município e Sua Hierarquia Completa

```sql
-- Exemplo: buscar informações de "Campinas"
WITH municipio_selecionado AS (
    SELECT id, nome
    FROM silver_prod.ds_ibge.cleaned_municipios
    WHERE LOWER(nome) LIKE '%campinas%'
    LIMIT 1
)
SELECT 
    m.id AS codigo_ibge,
    m.nome AS municipio,
    e.sigla AS uf,
    e.nome AS estado,
    r.nome AS regiao,
    mic.nome AS microrregiao,
    mes.nome AS mesorregiao,
    ri.nome AS regiao_imediata,
    rint.nome AS regiao_intermediaria
FROM silver_prod.ds_ibge.cleaned_municipios m
JOIN silver_prod.ds_ibge.cleaned_estados e 
    ON m.microrregiao_mesorregiao_UF_regiao_id = e.id
JOIN silver_prod.ds_ibge.cleaned_regioes r 
    ON e.regiao_id = r.id
LEFT JOIN silver_prod.ds_ibge.cleaned_microrregioes mic 
    ON m.microrregiao_id = mic.id
LEFT JOIN silver_prod.ds_ibge.cleaned_mesorregioes mes 
    ON mic.mesorregiao_id = mes.id
LEFT JOIN silver_prod.ds_ibge.cleaned_regioes_imediatas ri 
    ON m.regiao_imediata_id = ri.id
LEFT JOIN silver_prod.ds_ibge.cleaned_regioes_intermediarias rint 
    ON ri.regiao_intermediaria_id = rint.id
WHERE m.id = (SELECT id FROM municipio_selecionado);
```

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

## Casos de Uso e Análises Possíveis

### 1. Geocodificação e Enriquecimento de Dados

**Objetivo**: Adicionar informações geográficas a datasets existentes

**Aplicações**:
* Normalizar endereços com código IBGE
* Enriquecer bases de clientes com hierarquia territorial
* Validar localidades em formulários
* Criar agregações por região/estado/município

### 2. Análises Demográficas

**Objetivo**: Estudar distribuição populacional e territorial

**Análises**:
* Densidade populacional por região
* Urbanização (RMs vs demais municípios)
* Tamanho médio de municípios por estado
* Identificar vazios demográficos

### 3. Planejamento Governamental

**Objetivo**: Subsidiar políticas públicas

**Aplicações**:
* Mapear áreas de atuação de programas sociais
* Planejar distribuição de recursos por região
* Análise de cobertura de serviços públicos
* Identificar regiões prioritárias

### 4. Business Intelligence Geográfico

**Objetivo**: Análises de mercado e expansão de negócios

**Aplicações**:
* Identificar gaps de cobertura regional
* Planejar expansão para novas regiões
* Análise de concorrência por localidade
* Segmentação de clientes por hierarquia territorial

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

## Troubleshooting

### Problema: Ingestão de distritos muito lenta

**Causa**: Grande volume de dados (~10.000 registros)

**Solução**: 
* Este é o endpoint mais pesado, é esperado levar ~10 minutos
* Considere aumentar o timeout da task se necessário
* Avaliar processar em lotes menores

---

### Problema: Mudanças territoriais não refletidas

**Causa**: Dados geográficos raramente mudam, mas ocorrem fusões/desmembramentos

**Solução**:
* Executar job manualmente após comunicados oficiais do IBGE
* Manter histórico de mudanças com versionamento
* Validar integridade referencial após atualizações

---

## Referências

* [API IBGE Localidades](https://servicodados.ibge.gov.br/api/docs/localidades)
* [Divisão Regional do Brasil](https://www.ibge.gov.br/geociencias/organizacao-do-territorio/divisao-regional/15778-divisoes-regionais-do-brasil.html)
* [Databricks Asset Bundles Documentation](https://docs.databricks.com/dev-tools/bundles/)
* [Delta Live Tables Documentation](https://docs.databricks.com/delta-live-tables/)

---

**Última atualização**: 25 de Abril de 2026  
**Versão**: 3.0 - Documentação expandida com variáveis, queries e métricas  
**Mantido por**: delacortearthur@gmail.com  
**Fonte de dados**: IBGE - Instituto Brasileiro de Geografia e Estatística
