# Asset Bundle: Open Meteo (Dados Meteorológicos)

## Visão Geral

Este asset bundle implementa um pipeline completo de ingestão e transformação de dados meteorológicos das **27 capitais brasileiras** utilizando a **Open-Meteo API** (https://open-meteo.com/), uma API gratuita de dados meteorológicos históricos e de previsão.

O projeto coleta dados de **5 endpoints diferentes** da API:
* **Tabela Auxiliar de Localidades**: Lista hardcoded das 27 capitais
* **Geocoding**: Coordenadas geográficas das capitais
* **Historical Weather**: Dados históricos de temperatura, precipitação, vento
* **Air Quality**: Índices de qualidade do ar (PM2.5, PM10, NO2, O3)
* **Daily Forecast**: Previsão do tempo para os próximos dias

**Diferencial deste Bundle**: É orquestrado externamente pelo **Apache Airflow**, demonstrando integração entre Databricks e ferramentas externas de orquestração.

---

## Galeria Visual (Airflow + GitHub)

As imagens abaixo foram capturadas em **07/05/2026** para documentar os pontos mais importantes do projeto de forma visual e didatica.

### 1) Airflow - Home da plataforma

![Airflow Home](docs/images/linkedin/airflow_home.png)

**O que mostra:** saude dos componentes (`MetaDatabase`, `Scheduler`, `Triggerer`, `Dag Processor`) e historico de execucoes com predominio de sucesso.

### 2) Airflow - Overview da DAG de producao

![Airflow DAG Overview](docs/images/linkedin/airflow_dag_prod.png)

**O que mostra:** DAG `open_meteo_databricks_prod` ativa, com `max_active_runs=1`, `latest run` com sucesso e visao geral de estabilidade.

### 3) Airflow - Historico de runs da DAG de producao

![Airflow DAG Runs](docs/images/linkedin/airflow_dag_runs_prod.png)

**O que mostra:** execucoes manuais e agendadas com status `Success`, incluindo os testes finais de validacao da orquestracao.

### 4) GitHub - Visao do repositorio

![GitHub Repository](docs/images/linkedin/github_repo.png)

**O que mostra:** estrutura do `project_lake_house` e ultimo commit relacionado ao Airflow no dominio `open_meteo`.

### 5) GitHub - Pull Request de sincronizacao

![GitHub Pull Request](docs/images/linkedin/github_pr_33.png)

**O que mostra:** PR `#33` (`prod -> dev`) com descricao tecnica das mudancas de orquestracao Airflow.

### 6) GitHub Actions - Pipeline de CI/CD do open_meteo

![GitHub Actions](docs/images/linkedin/github_actions_open_meteo.png)

**O que mostra:** historico de execucoes do workflow `CI/CD open_meteo`, reforcando rastreabilidade e governanca de deploy.

---
## Arquitetura do Projeto

### Estrutura de Diretórios

```
open_meteo/
├── databricks.yml              # Configuração principal do bundle
├── resources/
│   ├── open_meteo.job.yml     # Definição do workflow/job
│   └── dlt_open_meteo.pipeline.yml # Definição do pipeline DLT
├── src/
│   ├── ingest_tabela_auxiliar_localidades.py  # Cria tabela auxiliar com 27 capitais
│   ├── ingest_geocoding_localidades.py        # Busca coordenadas geográficas
│   ├── ingest_historical_weather.py           # Dados históricos meteorológicos
│   ├── ingest_air_quality.py                  # Qualidade do ar
│   ├── ingest_daily_forecast.py               # Previsão diária
│   └── dlt_open_meteo.sql                     # Transformações Bronze → Silver
├── tests/                      # Testes unitários
└── fixtures/                   # Dados de teste
```

---

## Fluxo de Dados Completo

### Camada Bronze: Ingestão de Dados Meteorológicos

O processo de ingestão segue uma **hierarquia de dependências**, começando pela tabela auxiliar de localidades:

```
┌────────────────────────────────────┐
│ ingest_tabela_auxiliar_localidades│ ← Cria lista das 27 capitais brasileiras
└───────────────┬────────────────────┘
                │
                ↓
┌─────────────────────────────────┐
│ ingest_geocoding_localidades    │ ← Busca lat/long de cada capital
└───────────────┬─────────────────┘
                │
                ├──────────────────────────┐
                │                          │
                ↓                          ↓
┌──────────────────────────┐  ┌─────────────────────┐
│ingest_historical_weather │  │ ingest_air_quality  │
└───────────┬──────────────┘  └──────────┬──────────┘
            │                            │
            └──────────┬─────────────────┘
                       │
                       ↓
            ┌──────────────────────┐
            │ingest_daily_forecast │
            └──────────────────────┘
```

#### Detalhamento das Tasks de Ingestão

| # | Task | Descrição | Fonte de Dados |
|---|------|-----------|----------------|
| 1 | **ingest_tabela_auxiliar_localidades** | Cria tabela hardcoded com as 27 capitais + UF + região | Dados estáticos |
| 2 | **ingest_geocoding_localidades** | Busca latitude e longitude de cada capital | Open-Meteo Geocoding API |
| 3 | **ingest_historical_weather** | Temperatura, precipitação, umidade, vento (histórico) | Open-Meteo Historical Weather API |
| 4 | **ingest_air_quality** | PM2.5, PM10, NO2, O3, SO2, CO (qualidade do ar) | Open-Meteo Air Quality API |
| 5 | **ingest_daily_forecast** | Previsão de 7 dias (temp máx/mín, precipitação) | Open-Meteo Forecast API |

**Nota**: Todas as tasks salvam dados no catálogo **bronze_dev** (dev) ou **bronze_prod** (prod) no schema `ds_open_meteo`.

---

### Camada Silver: Limpeza e Padronização

Após a ingestão, o **pipeline DLT** (`dlt_open_meteo`) transforma os dados:

```
Pipeline: dlt_open_meteo
├── Catálogo: silver_dev / silver_prod
├── Schema: ds_open_meteo
└── Notebook: dlt_open_meteo.sql
```

**Transformações realizadas:**
* **Limpeza** de valores nulos e inconsistentes
* **Padronização** de tipos de dados (datas, floats, strings)
* **Deduplicação** de registros duplicados por capital/data
* **Validação** de coordenadas geográficas
* **Enriquecimento** com dados regionais (Norte, Sul, Sudeste, etc)
* **Criação de métricas agregadas** (médias por região, totais mensais)

---

## Dados Coletados

### Localidades (27 Capitais Brasileiras)

| Capital | UF | Região |
|---------|-----|--------|
| Manaus | AM | Norte |
| Belém | PA | Norte |
| Palmas | TO | Norte |
| Boa Vista | RR | Norte |
| Macapá | AP | Norte |
| Porto Velho | RO | Norte |
| Rio Branco | AC | Norte |
| Salvador | BA | Nordeste |
| Fortaleza | CE | Nordeste |
| Recife | PE | Nordeste |
| São Luís | MA | Nordeste |
| Natal | RN | Nordeste |
| João Pessoa | PB | Nordeste |
| Maceió | AL | Nordeste |
| Aracaju | SE | Nordeste |
| Teresina | PI | Nordeste |
| São Paulo | SP | Sudeste |
| Rio de Janeiro | RJ | Sudeste |
| Belo Horizonte | MG | Sudeste |
| Vitória | ES | Sudeste |
| Curitiba | PR | Sul |
| Florianópolis | SC | Sul |
| Porto Alegre | RS | Sul |
| Brasília | DF | Centro-Oeste |
| Goiânia | GO | Centro-Oeste |
| Cuiabá | MT | Centro-Oeste |
| Campo Grande | MS | Centro-Oeste |

### Variáveis Meteorológicas Coletadas

**Historical Weather (Dados Históricos)**:
* `temperature_2m` - Temperatura a 2 metros (°C)
* `precipitation` - Precipitação (mm)
* `relative_humidity_2m` - Umidade relativa (%)
* `wind_speed_10m` - Velocidade do vento a 10m (km/h)
* `pressure_msl` - Pressão atmosférica ao nível do mar (hPa)

**Air Quality (Qualidade do Ar)**:
* `pm2_5` - Partículas finas (μg/m³)
* `pm10` - Partículas inaláveis (μg/m³)
* `no2` - Dióxido de nitrogênio (μg/m³)
* `o3` - Ozônio (μg/m³)
* `so2` - Dióxido de enxofre (μg/m³)
* `co` - Monóxido de carbono (μg/m³)

**Daily Forecast (Previsão)**:
* `temperature_2m_max` - Temperatura máxima (°C)
* `temperature_2m_min` - Temperatura mínima (°C)
* `precipitation_sum` - Total de precipitação (mm)
* `wind_speed_10m_max` - Velocidade máxima do vento (km/h)

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
    default: ds_open_meteo

  silver_catalog:
    description: "Catálogo Silver de destino"
    default: silver_dev

  pause_status:
    description: "Job schedule status (sempre PAUSED para Airflow)"
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
      schema: ds_open_meteo
      silver_catalog: silver_dev
      pause_status: PAUSED  # Orquestrado pelo Airflow
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
      schema: ds_open_meteo
      silver_catalog: silver_prod
      pause_status: PAUSED  # Orquestrado pelo Airflow
      pipeline_development: false
```

### Parâmetros dos Notebooks

Todos os notebooks de ingestão recebem os seguintes parâmetros via `dbutils.widgets`:

| Parâmetro | Tipo | Descrição | Exemplo |
|-----------|------|-----------|---------|
| `catalog` | String | Catálogo de destino | `bronze_dev` ou `bronze_prod` |
| `schema` | String | Schema de destino | `ds_open_meteo` |

**Exemplo de uso no notebook**:
```python
dbutils.widgets.text("catalog", "bronze_prod")
used_catalog = dbutils.widgets.get("catalog")

dbutils.widgets.text("schema", "ds_open_meteo")
used_schema = dbutils.widgets.get("schema")

tabela_destino = f"{used_catalog}.{used_schema}.raw_historical_weather"
```

**Importante**: Os parâmetros são passados pelo Airflow via `notebook_params` no `DatabricksRunNowOperator`.

---

## Orquestração: Apache Airflow

### Por que Airflow?

Este bundle é orquestrado pelo **Apache Airflow** (não pelo Databricks) para demonstrar um cenário comum em empresas que:
* Já possuem Airflow para orquestração centralizada
* Precisam integrar Databricks com outras ferramentas
* Querem lógica condicional complexa em DAGs
* Necessitam visibilidade unificada de workflows

### Como Funciona a Integração

**1. Status PAUSED no Databricks**

No arquivo `databricks.yml`, o job está configurado como:
```yaml
pause_status: PAUSED
```

Isso garante que o Databricks **NÃO execute o job automaticamente**. A execução é controlada exclusivamente pelo Airflow.

**2. DAG do Airflow**

O Airflow possui um DAG que dispara o job via API REST do Databricks:

```python
from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'email': ['delacortearthur@gmail.com'],
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'open_meteo_ingestion',
    default_args=default_args,
    description='Ingestão de dados meteorológicos via Open-Meteo API',
    schedule_interval='0 6 * * *',  # Diariamente às 6h AM
    catchup=False,
    tags=['databricks', 'weather', 'open-meteo'],
)

run_job = DatabricksRunNowOperator(
    task_id='run_open_meteo_job',
    databricks_conn_id='databricks_default',  # Configurado no Airflow
    job_id=12345,  # ID do job no Databricks
    notebook_params={
        'catalog': 'bronze_prod',
        'schema': 'ds_open_meteo'
    },
    dag=dag,
)
```

**3. Fluxo de Execução**

```
1. Airflow Scheduler (6h AM)
        │
        ↓
2. DatabricksRunNowOperator
        │
        ↓
3. POST /api/2.1/jobs/run-now
   (com token de autenticação)
        │
        ↓
4. Databricks executa o job
   (todas as 5 tasks + DLT pipeline)
        │
        ↓
5. Airflow monitora via polling
   (verifica status a cada 30s)
        │
        ↓
6. Job finaliza com sucesso/erro
        │
        ↓
7. Airflow marca tarefa como success/failed
```

**4. Configuração do Airflow Connection**

No Airflow UI → Admin → Connections:

```
Connection ID: databricks_default
Connection Type: Databricks
Host: https://dbc-414ad402-adeb.cloud.databricks.com
Token: dapi******************************** (oculto)
```

---

## Job e Orquestração

### Job: open_meteo_job

```yaml
Nome: open_meteo_dev (dev) / open_meteo_prod (prod)
Agendamento: PAUSED (orquestrado externamente pelo Airflow)
Execução: Via API REST do Databricks
```

### Diagrama de Dependências do Job

```
ingest_tabela_auxiliar_localidades
                │
                └──> ingest_geocoding_localidades
                            │
                            ├──> ingest_historical_weather
                            │
                            ├──> ingest_air_quality
                            │
                            └──> ingest_daily_forecast
                                        │
    ┌───────────────────────────────────┴───────┐
    │                                           │
    │  (Aguarda todas as 5 tasks de ingestão)  │
    │                                           │
    └──────────> refresh_dlt_pipeline <─────────┘
```

### Parâmetros do Job

| Parâmetro | Descrição | Padrão (dev) | Produção |
|-----------|-----------|--------------|----------|
| `environment` | Ambiente de execução | `dev` | `prod` |
| `catalog` | Catálogo Bronze de destino | `bronze_dev` | `bronze_prod` |
| `schema` | Schema de destino | `ds_open_meteo` | `ds_open_meteo` |

---

## Ambientes (Targets)

### Ambiente de Desenvolvimento (dev)
```yaml
Catálogo Bronze: bronze_dev
Catálogo Silver: silver_dev
Schema: ds_open_meteo (em ambos)
Agendamento: PAUSED (orquestrado pelo Airflow DEV)
```

### Ambiente de Produção (prod)
```yaml
Catálogo Bronze: bronze_prod
Catálogo Silver: silver_prod
Schema: ds_open_meteo (em ambos)
Agendamento: PAUSED (orquestrado pelo Airflow PROD)
```

---

## Como Usar

### Deploy do Bundle

```bash
# Deploy em desenvolvimento
cd open_meteo
databricks bundle deploy -t dev

# Deploy em produção
databricks bundle deploy -t prod
```

**Nota**: O deploy apenas cria/atualiza o job no Databricks. A execução é controlada pelo Airflow.

### Executar Job Manualmente (via Databricks)

```bash
# Executar em dev (sem Airflow)
databricks bundle run open_meteo_job -t dev

# Executar em prod (sem Airflow)
databricks bundle run open_meteo_job -t prod
```

### Executar via Airflow (Recomendado)

No Airflow UI:
1. Navegue até DAGs → `open_meteo_ingestion`
2. Clique em **Trigger DAG** (botão ▶️)
3. Acompanhe a execução no Graph View
4. Verifique logs no Databricks após disparo

### Validar Configuração

```bash
databricks bundle validate
```

---

## Tabelas Geradas

### Camada Bronze (bronze_dev.ds_open_meteo)

| Tabela | Descrição | Registros (aprox.) |
|--------|-----------|---------------------|
| `auxiliar_localidades` | 27 capitais + UF + região | 27 |
| `tabela_controle` | Controle de ingestão incremental | 1 |
| `raw_geocoding_localidades` | Coordenadas geográficas | 27 |
| `raw_historical_weather` | Dados históricos por capital/data | ~10.000 por capital |
| `raw_air_quality` | Qualidade do ar por capital/data | ~5.000 por capital |
| `raw_daily_forecast` | Previsão de 7 dias por capital | 27 × 7 = 189 |

### Camada Silver (silver_dev.ds_open_meteo)

| Tabela | Descrição | Transformações |
|--------|-----------|----------------|
| `cleaned_localidades` | Localidades normalizadas | Trim, uppercase, validações |
| `cleaned_geocoding_localidades` | Coordenadas validadas | Validação de lat/long |
| `cleaned_historical_weather` | Dados históricos tratados | Remoção de nulls, padronização |
| `cleaned_air_quality` | Qualidade do ar limpa | Conversão de unidades |
| `cleaned_daily_forecast` | Previsão normalizada | Datas padronizadas |

---

## Métricas de Volume e Performance

### Volume de Dados Estimado

| Camada | Tabelas | Registros Totais | Tamanho Aprox. |
|--------|---------|------------------|----------------|
| Bronze | 6 tabelas | ~405.000 registros | ~120 MB |
| Silver | 5 tabelas | ~405.000 registros | ~95 MB (otimizado) |

### Detalhamento por Tabela (Bronze)

| Tabela | Registros | Descrição |
|--------|-----------|-----------|
| `auxiliar_localidades` | 27 | Capitais brasileiras |
| `raw_geocoding_localidades` | 27 | Coordenadas geográficas |
| `raw_historical_weather` | ~270.000 | ~10.000 registros/capital (aprox. 27 anos) |
| `raw_air_quality` | ~135.000 | ~5.000 registros/capital |
| `raw_daily_forecast` | 189 | 7 dias × 27 capitais |
| `tabela_controle` | 1 | Controle incremental |

### Performance de Execução

| Task | Tempo Médio | Registros Processados |
|------|-------------|------------------------|
| `ingest_tabela_auxiliar_localidades` | ~5 segundos | 27 (hardcoded) |
| `ingest_geocoding_localidades` | ~10 segundos | 27 |
| `ingest_historical_weather` | ~20-30 minutos | ~27.000 (1 ano por capital) |
| `ingest_air_quality` | ~15-20 minutos | ~13.500 (1 ano por capital) |
| `ingest_daily_forecast` | ~10 segundos | 189 (7 dias) |
| **Pipeline Silver** | ~10 minutos | ~405.000 registros |
| **Total (primeira execução)** | ~1-1.5 horas | ~27 anos de dados históricos |
| **Total (incremental diário)** | ~5-10 minutos | Apenas últimas 24h |

**Nota**: Primeira execução ingere muitos anos de dados históricos. Execuções incrementais são muito mais rápidas.

---

## Exemplos de Queries SQL

### 1. Temperaturas Médias por Região do Brasil

```sql
SELECT 
    l.regiao,
    COUNT(DISTINCT l.capital) AS qtd_capitais,
    ROUND(AVG(hw.temperature_2m), 2) AS temp_media,
    ROUND(MIN(hw.temperature_2m), 2) AS temp_minima,
    ROUND(MAX(hw.temperature_2m), 2) AS temp_maxima,
    DATE_FORMAT(hw.data, 'yyyy-MM') AS mes_ano
FROM silver_prod.ds_open_meteo.cleaned_historical_weather hw
JOIN silver_prod.ds_open_meteo.cleaned_localidades l 
    ON hw.localidade_id = l.id
WHERE hw.data >= DATE_SUB(CURRENT_DATE(), 365)  -- Último ano
GROUP BY l.regiao, DATE_FORMAT(hw.data, 'yyyy-MM')
ORDER BY mes_ano DESC, regiao;
```

### 2. Ranking de Capitais com Melhor Qualidade do Ar

```sql
WITH media_anual AS (
    SELECT 
        l.capital,
        l.uf,
        l.regiao,
        ROUND(AVG(aq.pm2_5), 2) AS pm25_media,
        ROUND(AVG(aq.pm10), 2) AS pm10_media,
        ROUND(AVG(aq.no2), 2) AS no2_media,
        ROUND(AVG(aq.o3), 2) AS o3_media
    FROM silver_prod.ds_open_meteo.cleaned_air_quality aq
    JOIN silver_prod.ds_open_meteo.cleaned_localidades l 
        ON aq.localidade_id = l.id
    WHERE aq.data >= DATE_SUB(CURRENT_DATE(), 365)
    GROUP BY l.capital, l.uf, l.regiao
),
score_qualidade AS (
    SELECT 
        *,
        -- Score: menor é melhor (normalizado 0-100)
        ROUND(
            (pm25_media / 50 * 25) +  -- PM2.5 (limite OMS: 15)
            (pm10_media / 100 * 25) +  -- PM10 (limite OMS: 45)
            (no2_media / 100 * 25) +   -- NO2
            (o3_media / 150 * 25),     -- O3
            2
        ) AS score_poluicao
    FROM media_anual
)
SELECT 
    capital,
    uf,
    regiao,
    pm25_media,
    pm10_media,
    score_poluicao,
    CASE 
        WHEN score_poluicao <= 25 THEN 'Excelente'
        WHEN score_poluicao <= 50 THEN 'Boa'
        WHEN score_poluicao <= 75 THEN 'Regular'
        ELSE 'Ruim'
    END AS classificacao
FROM score_qualidade
ORDER BY score_poluicao ASC
LIMIT 10;
```

### 3. Comparação: Clima das Regiões Sul vs Norte

```sql
WITH dados_regionais AS (
    SELECT 
        l.regiao,
        YEAR(hw.data) AS ano,
        ROUND(AVG(hw.temperature_2m), 2) AS temp_media,
        ROUND(AVG(hw.precipitation), 2) AS precip_media,
        ROUND(AVG(hw.relative_humidity_2m), 2) AS umidade_media
    FROM silver_prod.ds_open_meteo.cleaned_historical_weather hw
    JOIN silver_prod.ds_open_meteo.cleaned_localidades l 
        ON hw.localidade_id = l.id
    WHERE l.regiao IN ('Sul', 'Norte')
      AND hw.data >= DATE_SUB(CURRENT_DATE(), 1825)  -- Últimos 5 anos
    GROUP BY l.regiao, YEAR(hw.data)
)
SELECT 
    ano,
    MAX(CASE WHEN regiao = 'Sul' THEN temp_media END) AS temp_sul,
    MAX(CASE WHEN regiao = 'Norte' THEN temp_media END) AS temp_norte,
    ROUND(
        MAX(CASE WHEN regiao = 'Norte' THEN temp_media END) - 
        MAX(CASE WHEN regiao = 'Sul' THEN temp_media END),
        2
    ) AS diferenca_temperatura,
    MAX(CASE WHEN regiao = 'Sul' THEN precip_media END) AS precip_sul,
    MAX(CASE WHEN regiao = 'Norte' THEN precip_media END) AS precip_norte
FROM dados_regionais
GROUP BY ano
ORDER BY ano DESC;
```

### 4. Acurácia das Previsões Meteorológicas

```sql
WITH forecast_vs_real AS (
    SELECT 
        f.capital,
        f.data_previsao,
        f.data_alvo,
        f.temperature_2m_max AS temp_prevista,
        hw.temperature_2m AS temp_real,
        ABS(f.temperature_2m_max - hw.temperature_2m) AS erro_absoluto
    FROM (
        SELECT 
            l.capital,
            df.data AS data_previsao,
            df.forecast_date AS data_alvo,
            df.temperature_2m_max,
            df.localidade_id
        FROM silver_prod.ds_open_meteo.cleaned_daily_forecast df
        JOIN silver_prod.ds_open_meteo.cleaned_localidades l 
            ON df.localidade_id = l.id
        WHERE df.data >= DATE_SUB(CURRENT_DATE(), 30)  -- Último mês
    ) f
    JOIN silver_prod.ds_open_meteo.cleaned_historical_weather hw 
        ON f.localidade_id = hw.localidade_id 
        AND f.data_alvo = hw.data
)
SELECT 
    capital,
    COUNT(*) AS qtd_previsoes,
    ROUND(AVG(erro_absoluto), 2) AS erro_medio,
    ROUND(MIN(erro_absoluto), 2) AS erro_minimo,
    ROUND(MAX(erro_absoluto), 2) AS erro_maximo,
    CASE 
        WHEN AVG(erro_absoluto) <= 2 THEN 'Alta Acurácia'
        WHEN AVG(erro_absoluto) <= 5 THEN 'Boa Acurácia'
        ELSE 'Baixa Acurácia'
    END AS classificacao_acuracia
FROM forecast_vs_real
GROUP BY capital
ORDER BY erro_medio ASC;
```

### 5. Identificar Ondas de Calor e Frio Extremo

```sql
WITH temperaturas_extremas AS (
    SELECT 
        l.capital,
        l.regiao,
        hw.data,
        hw.temperature_2m,
        AVG(hw.temperature_2m) OVER (
            PARTITION BY l.capital 
            ORDER BY hw.data 
            ROWS BETWEEN 30 PRECEDING AND CURRENT ROW
        ) AS temp_media_30d,
        STDDEV(hw.temperature_2m) OVER (
            PARTITION BY l.capital 
            ORDER BY hw.data 
            ROWS BETWEEN 30 PRECEDING AND CURRENT ROW
        ) AS temp_stddev_30d
    FROM silver_prod.ds_open_meteo.cleaned_historical_weather hw
    JOIN silver_prod.ds_open_meteo.cleaned_localidades l 
        ON hw.localidade_id = l.id
    WHERE hw.data >= DATE_SUB(CURRENT_DATE(), 365)
),
eventos_extremos AS (
    SELECT 
        capital,
        regiao,
        data,
        temperature_2m,
        temp_media_30d,
        temp_stddev_30d,
        CASE 
            WHEN temperature_2m > (temp_media_30d + (2 * temp_stddev_30d)) THEN 'Onda de Calor'
            WHEN temperature_2m < (temp_media_30d - (2 * temp_stddev_30d)) THEN 'Onda de Frio'
            ELSE 'Normal'
        END AS tipo_evento
    FROM temperaturas_extremas
    WHERE temp_stddev_30d IS NOT NULL  -- Garante 30 dias de histórico
)
SELECT 
    capital,
    regiao,
    tipo_evento,
    COUNT(*) AS qtd_eventos,
    ROUND(AVG(temperature_2m), 2) AS temp_media_evento,
    ROUND(MIN(temperature_2m), 2) AS temp_minima_evento,
    ROUND(MAX(temperature_2m), 2) AS temp_maxima_evento
FROM eventos_extremos
WHERE tipo_evento != 'Normal'
GROUP BY capital, regiao, tipo_evento
ORDER BY qtd_eventos DESC, capital;
```

---

## Controle Incremental

O bundle utiliza a tabela `tabela_controle` para evitar reprocessamento:

```sql
CREATE TABLE bronze_prod.ds_open_meteo.tabela_controle (
    id INT,
    ultima_data_historical DATE,
    ultima_data_air_quality DATE,
    ultima_data_forecast DATE
);
```

**Como funciona**:
1. Antes da ingestão: consulta a última data processada
2. Durante a ingestão: filtra apenas dados novos (data > última_processada)
3. Após a ingestão: atualiza o controle com a nova data

---

## Casos de Uso e Análises Possíveis

### Análises Climáticas
* Comparação de temperaturas entre regiões do Brasil
* Identificação de capitais mais quentes/frias
* Análise de sazonalidade (verão vs inverno)
* Estudo de precipitação por região

### Qualidade do Ar
* Ranking de capitais com melhor/pior qualidade do ar
* Correlação entre poluição e temperatura
* Identificação de padrões temporais (dias da semana, meses)
* Alertas de qualidade do ar crítica

### Previsões Meteorológicas
* Análise de acurácia das previsões
* Identificação de tendências climáticas
* Planejamento de eventos baseado no clima
* Alertas de condições extremas

### Análises Regionais
* Comparação Norte vs Sul vs Centro-Oeste
* Impacto de fenômenos climáticos (El Niño, La Niña)
* Estudo de climas tropicais, subtropicais e equatoriais

---

## Notificações

Em caso de falha em qualquer task, um email é enviado para:
* **delacortearthur@gmail.com**

Notificações também são configuradas no Airflow para alertas de:
* Falha de execução
* Timeout (job travado)
* Retry esgotado

---

## Tecnologias Utilizadas

### Plataforma e Infraestrutura
* **Databricks**: Plataforma de lakehouse unificada
* **Apache Airflow**: Orquestração externa de workflows
* **Delta Lake**: Armazenamento ACID sobre Data Lake
* **Unity Catalog**: Governança centralizada de dados

### Processamento e Transformação
* **Delta Live Tables (DLT)**: Pipelines gerenciados de transformação
* **PySpark**: Processamento distribuído
* **Spark SQL**: Queries e transformações

### APIs e Integrações
* **Open-Meteo API**: Dados meteorológicos gratuitos
* **Databricks REST API**: Integração Airflow ↔ Databricks
* **DatabricksRunNowOperator**: Operador do Airflow para Databricks

---

## Boas Práticas Implementadas

### Governança de Dados
* Separação de ambientes (dev/prod)
* Catálogos isolados por camada e ambiente
* Unity Catalog para controle de acesso
* Auditoria via Delta Lake (time travel)

### Qualidade de Dados
* Ingestão incremental com controle de estado
* Validação de coordenadas geográficas
* Deduplicação de registros
* Limpeza de valores nulos
* Padronização de tipos

### Integração e Orquestração
* **Job PAUSED no Databricks** (orquestração externa)
* **Airflow como ponto central de controle**
* **Separação de responsabilidades** (Databricks = processamento, Airflow = orquestração)
* **Autenticação via token** (seguro)
* **Retry automático** no Airflow e no Databricks

### Performance
* Delta Lake para otimização automática
* Serverless para escalabilidade
* Particionamento por data
* Z-ordering para queries otimizadas

---

## Troubleshooting

### Problema: Job não executa automaticamente

**Causa**: O job está com `pause_status: PAUSED` por design.

**Solução**: Verificar se o DAG do Airflow está ativo e rodando no horário correto.

---

### Problema: Airflow não consegue disparar o job

**Causa**: Token inválido ou expirado no Airflow Connection.

**Solução**:
1. Acesse Airflow UI → Admin → Connections
2. Edite `databricks_default`
3. Gere um novo token no Databricks (User Settings → Access Tokens)
4. Atualize o token no Airflow Connection

---

### Problema: Job dispara mas falha imediatamente

**Causa**: Parâmetros incorretos passados pelo Airflow.

**Solução**: Verificar `notebook_params` no DatabricksRunNowOperator:
```python
notebook_params={
    'catalog': 'bronze_prod',  # Verificar se o catálogo existe
    'schema': 'ds_open_meteo'   # Verificar se o schema existe
}
```

---

### Problema: API Open-Meteo retorna erro 429 (Too Many Requests)

**Causa**: Limite de requisições excedido.

**Solução**: Adicionar delay entre requisições (já implementado com `time.sleep(0.2)`).

---

## Diferenças vs Outros Bundles

### Por que este bundle é diferente?

| Característica | Outros Bundles | Open Meteo |
|----------------|----------------|------------|
| **Orquestração** | Databricks Workflows (nativo) | Apache Airflow (externo) |
| **Schedule** | Configurado no databricks.yml | Configurado no Airflow DAG |
| **Pause Status** | UNPAUSED (prod) | PAUSED (sempre) |
| **Execução** | Automática pelo Databricks | Disparada via API REST |
| **Monitoramento** | Databricks UI | Airflow UI + Databricks UI |

### Quando usar cada abordagem?

**Databricks Workflows (carros, pokemon, ibge, camara_deputados)**:
* Pipeline simples e linear
* Sem dependências externas ao Databricks
* Execução isolada e independente
* Time pequeno focado apenas em Databricks

**Apache Airflow (open_meteo)**:
* Workflows complexos com lógica condicional
* Integração com múltiplas ferramentas (não só Databricks)
* Orquestração centralizada de toda a empresa
* Necessidade de visibilidade unificada
* Reutilização de DAGs existentes

---

## Referências

### Documentação Open-Meteo
* [Open-Meteo API Docs](https://open-meteo.com/en/docs)
* [Historical Weather API](https://open-meteo.com/en/docs/historical-weather-api)
* [Air Quality API](https://open-meteo.com/en/docs/air-quality-api)
* [Forecast API](https://open-meteo.com/en/docs)
* [Geocoding API](https://open-meteo.com/en/docs/geocoding-api)

### Documentação Databricks
* [Databricks Asset Bundles](https://docs.databricks.com/dev-tools/bundles/)
* [Delta Live Tables](https://docs.databricks.com/delta-live-tables/)
* [Databricks REST API](https://docs.databricks.com/api/workspace/jobs/runnow)

### Documentação Airflow
* [Apache Airflow](https://airflow.apache.org/docs/)
* [DatabricksRunNowOperator](https://airflow.apache.org/docs/apache-airflow-providers-databricks/stable/operators/run_now.html)
* [Databricks Provider](https://airflow.apache.org/docs/apache-airflow-providers-databricks/stable/index.html)

---

**Última atualização**: 25 de Abril de 2026  
**Versão**: 3.0 - Documentação expandida com variáveis, queries e métricas  
**Mantido por**: delacortearthur@gmail.com  
**Orquestração**: Apache Airflow (externo)

