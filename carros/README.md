# Asset Bundle: Carros (Tabela FIPE)

## Visão Geral

Este asset bundle implementa um pipeline completo de ingestão e transformação de dados da **Tabela FIPE** (Fundação Instituto de Pesquisas Econômicas), que fornece preços médios de veículos no mercado brasileiro.

O projeto segue a **Arquitetura Medalhão** (Medallion Architecture) com três camadas:
* **Bronze**: Dados brutos ingeridos da API FIPE
* **Silver**: Dados limpos e padronizados
* **Gold**: Dados refinados e modelados dimensionalmente

---

## Arquitetura do Projeto

### Estrutura de Diretórios

```
carros/
├── databricks.yml              # Configuração principal do bundle
├── resources/
│   ├── carros_job.job.yml     # Definição do workflow/job
│   └── carros_etl.pipeline.yml # Definição dos pipelines DLT
├── src/
│   ├── ingest_marcas.py       # Ingestão de marcas
│   ├── ingest_modelos.py      # Ingestão de modelos
│   ├── ingest_anos.py         # Ingestão de anos
│   ├── ingest_referencias.py  # Ingestão de referências (meses)
│   ├── ingest_fipe.py         # Ingestão de preços FIPE
│   ├── dlt_carros.sql         # Transformações Bronze → Silver
│   └── dlt_carros_gold.sql    # Transformações Silver → Gold
├── tests/                      # Testes unitários
└── fixtures/                   # Dados de teste
```

---

## Fluxo de Dados Completo

### Camada Bronze: Ingestão de Dados

O processo de ingestão segue uma **hierarquia de dependências**, pois cada etapa precisa dos dados da anterior:

```
┌─────────────────┐
│ ingest_marcas   │ ← Busca todas as marcas de veículos
└────────┬────────┘
         │
         ↓
┌─────────────────┐
│ ingest_modelos  │ ← Para cada marca, busca todos os modelos
└────────┬────────┘
         │
         ↓
┌─────────────────┐
│ ingest_anos     │ ← Para cada modelo, busca os anos disponíveis
└────────┬────────┘
         │
         ↓
┌─────────────────┐
│ingest_referencias│ ← Busca meses de referência dos preços
└────────┬────────┘
         │
         ↓
┌─────────────────┐
│  ingest_fipe    │ ← Busca os preços finais para cada combinação
└─────────────────┘
```

#### Detalhamento das Tasks de Ingestão

| Task | Descrição | Fonte de Dados |
|------|-----------|----------------|
| **ingest_marcas** | Extrai lista de marcas de veículos (ex: Fiat, Ford, VW) | API FIPE `/marcas` |
| **ingest_modelos** | Extrai modelos para cada marca (ex: Uno, Gol, Corsa) | API FIPE `/modelos` |
| **ingest_anos** | Extrai anos/combustível disponíveis por modelo | API FIPE `/anos` |
| **ingest_referencias** | Extrai meses de referência da tabela FIPE | API FIPE `/referencias` |
| **ingest_fipe** | Extrai preços finais combinando todas as dimensões | API FIPE `/valor` |

**Nota**: Todas as tasks salvam dados no catálogo **bronze_dev** (dev) ou **bronze_prod** (prod) no schema `ds_carros`.

---

## API FIPE: Detalhes Técnicos

### Endpoints Utilizados

**Base URL**: `https://veiculos.fipe.org.br/api/veiculos/`

| Endpoint | Método | Parâmetros | Retorno |
|----------|--------|------------|---------|
| `/ConsultarMarcas` | POST | `codigoTabelaReferencia`, `codigoTipoVeiculo` | Lista de marcas |
| `/ConsultarModelos` | POST | `codigoMarca`, `codigoTabelaReferencia`, `codigoTipoVeiculo` | Lista de modelos |
| `/ConsultarAnoModelo` | POST | `codigoMarca`, `codigoModelo`, `codigoTabelaReferencia`, `codigoTipoVeiculo` | Anos disponíveis |
| `/ConsultarTabelaDeReferencia` | POST | - | Meses de referência |
| `/ConsultarValorComTodosParametros` | POST | Todos os parâmetros combinados | Preço e detalhes do veículo |

### Tipos de Veículos

| Código | Tipo |
|--------|------|
| `1` | Carros e utilitários pequenos |
| `2` | Motos |
| `3` | Caminhões e ônibus |

### Rate Limiting e Boas Práticas

* **Delay entre requisições**: 0.1 segundo
* **Timeout**: 10 segundos por requisição
* **Retry**: Automático em caso de erro 5xx
* **Headers**: `Content-Type: application/json`

---

## Controle de Ingestão Incremental

Este bundle utiliza **duas estratégias de controle** para evitar reprocessamento desnecessário:

### 1. Tabela de Controle de Anos

**Localização**: `{catalog}.ds_carros.controle_anos`

**Como funciona**:
1. **Antes da ingestão**: Consulta quais combinações marca/modelo/ano já foram processadas
2. **Durante a ingestão**: Filtra apenas combinações novas
3. **Após a ingestão**: Insere as novas combinações processadas

### 2. Tabela de Controle Geral

**Localização**: `{catalog}.ds_carros.tabela_controle`

**Como funciona**:
* Armazena o código da última referência (mês FIPE) processada
* Garante que apenas dados novos sejam ingeridos
* Permite retomada após falhas

---

### Camada Silver: Limpeza e Padronização

Após a ingestão, o **pipeline DLT Silver** (`carros_etl_silver`) transforma os dados:

```
Pipeline: carros_pipeline_silver
├── Catálogo: silver_dev / silver_prod
├── Schema: ds_carros
└── Notebook: dlt_carros.sql
```

**Transformações realizadas:**
* Limpeza de dados nulos e inconsistentes
* Padronização de tipos de dados
* Deduplicação de registros
* Normalização de strings (trim, uppercase/lowercase)
* Validação de integridade referencial

---

### Camada Gold: Modelagem Dimensional

O **pipeline DLT Gold** (`carros_etl_gold`) cria o modelo dimensional final:

```
Pipeline: carros_pipeline_gold
├── Catálogo: gold_dev / gold_prod
├── Schema: dm_carros
└── Notebook: dlt_carros_gold.sql
```

**Características:**
* **Modelo dimensional** (Star Schema ou Snowflake)
* Tabelas de **Fatos** e **Dimensões**
* Dados otimizados para **análise e BI**
* Performance otimizada com **Delta Lake**

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
    default: ds_carros

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
      schema: ds_carros
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
      schema: ds_carros
      silver_catalog: silver_prod
      pause_status: UNPAUSED
      pipeline_development: false
```

### Parâmetros dos Notebooks

Todos os notebooks de ingestão recebem os seguintes parâmetros via `dbutils.widgets`:

| Parâmetro | Tipo | Descrição | Exemplo |
|-----------|------|-----------|---------|
| `catalog` | String | Catálogo de destino | `bronze_dev` ou `bronze_prod` |
| `schema` | String | Schema de destino | `ds_carros` |

**Exemplo de uso no notebook**:
```python
dbutils.widgets.text("catalog", "bronze_prod")
used_catalog = dbutils.widgets.get("catalog")

dbutils.widgets.text("schema", "ds_carros")
used_schema = dbutils.widgets.get("schema")

tabela_destino = f"{used_catalog}.{used_schema}.raw_marcas"
```

---

## Job e Orquestração

### Job: carros_job

O job orquestra todo o fluxo de ponta a ponta:

```yaml
Nome: carros_dev (dev) / carros_prod (prod)
Agendamento: Diário às 01:30 AM (America/Sao_Paulo)
Status: PAUSED (dev) / UNPAUSED (prod)
```

### Diagrama de Dependências do Job

```
ingest_marcas
      │
      └──> ingest_modelos
                │
                └──> ingest_anos
                        │
                        └──> ingest_referencias
                                │
                                └──> ingest_fipe
                                        │
    ┌───────────────────────────────────┴─────┬─────────────┐
    │                                         │             │
    │                                         │             │
    │                                         │             │
    └──────────> refresh_pipeline_silver <────┴─────────────┘
                        │
                        └──> refresh_pipeline_gold
```

---

## Ambientes (Targets)

### Ambiente de Desenvolvimento (dev)
```yaml
Catálogo Bronze: bronze_dev
Catálogo Silver: silver_dev
Catálogo Gold: gold_dev
Schema: ds_carros (em todos)
Agendamento: PAUSED (execução manual)
```

### Ambiente de Produção (prod)
```yaml
Catálogo Bronze: bronze_prod
Catálogo Silver: silver_prod
Catálogo Gold: gold_prod
Schema: ds_carros (em todos)
Agendamento: UNPAUSED (automático às 01:30 AM)
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
databricks bundle run carros_job -t dev

# Executar em prod
databricks bundle run carros_job -t prod
```

### Validar Configuração

```bash
databricks bundle validate
```

---

## Tabelas Geradas

### Camada Bronze (bronze_dev.ds_carros)

| Tabela | Descrição | Registros (aprox.) |
|--------|-----------|---------------------|
| `raw_marcas` | Marcas de veículos | ~200 |
| `raw_modelos` | Modelos por marca | ~10.000 |
| `raw_anos` | Anos disponíveis por modelo | ~50.000 |
| `raw_referencias` | Meses de referência | ~300 |
| `raw_fipe` | Preços FIPE completos | ~500.000 |
| `controle_anos` | Controle incremental | ~50.000 |
| `tabela_controle` | Controle geral | 1 |

### Camada Silver (silver_dev.ds_carros)

| Tabela | Descrição | Transformações |
|--------|-----------|----------------|
| `tb_marcas` | Marcas limpas e padronizadas | Trim, uppercase, dedup |
| `tb_modelos` | Modelos normalizados | Limpeza de caracteres especiais |
| `tb_anos` | Anos validados | Validação de formato |
| `tb_referencias` | Referências estruturadas | Conversão de datas |
| `tb_fipe` | Preços FIPE tratados | Conversão de moeda, nulls |

### Camada Gold (gold_dev.dm_carros)

| Tabela | Tipo | Descrição |
|--------|------|-----------|
| `dim_marca` | Dimensão | Marcas com hierarquia |
| `dim_modelo` | Dimensão | Modelos com atributos |
| `dim_ano` | Dimensão | Anos e combustível |
| `dim_tempo` | Dimensão | Referências temporais |
| `fato_preco_fipe` | Fato | Preços históricos |

---

## Métricas de Volume e Performance

### Volume de Dados Estimado

| Camada | Tabelas | Registros Totais | Tamanho Aprox. |
|--------|---------|------------------|----------------|
| Bronze | 7 tabelas | ~560.000 registros | ~150 MB |
| Silver | 5 tabelas | ~560.000 registros | ~120 MB (otimizado) |
| Gold | 5 tabelas | ~500.000 registros | ~100 MB (agregado) |

### Performance de Execução

| Task | Tempo Médio | Registros Processados |
|------|-------------|------------------------|
| `ingest_marcas` | ~30 segundos | ~200 marcas |
| `ingest_modelos` | ~15 minutos | ~10.000 modelos |
| `ingest_anos` | ~45 minutos | ~50.000 combinações |
| `ingest_referencias` | ~10 segundos | ~300 referências |
| `ingest_fipe` | ~3-4 horas | ~500.000 preços |
| **Pipeline Silver** | ~10 minutos | ~560.000 registros |
| **Pipeline Gold** | ~5 minutos | ~500.000 registros |
| **Total (primeira execução)** | ~4-5 horas | - |
| **Total (incremental)** | ~30-60 minutos | Apenas novos dados |

**Nota**: Tempos variam conforme disponibilidade da API FIPE e volume de dados novos.

---

## Exemplos de Queries SQL

### 1. Top 10 Marcas Mais Caras (Preço Médio)

```sql
SELECT 
    m.nome AS marca,
    COUNT(DISTINCT f.codigo_modelo) AS qtd_modelos,
    ROUND(AVG(f.valor), 2) AS preco_medio,
    ROUND(MIN(f.valor), 2) AS preco_minimo,
    ROUND(MAX(f.valor), 2) AS preco_maximo
FROM bronze_prod.ds_carros.raw_fipe f
JOIN bronze_prod.ds_carros.raw_marcas m 
    ON f.codigo_marca = m.codigo
WHERE f.valor > 0
GROUP BY m.nome
ORDER BY preco_medio DESC
LIMIT 10;
```

### 2. Evolução de Preço de um Veículo Específico

```sql
SELECT 
    ref.mes AS mes_referencia,
    f.valor AS preco_fipe,
    LAG(f.valor) OVER (ORDER BY ref.codigo) AS preco_mes_anterior,
    ROUND(
        ((f.valor - LAG(f.valor) OVER (ORDER BY ref.codigo)) / 
         LAG(f.valor) OVER (ORDER BY ref.codigo)) * 100, 
        2
    ) AS variacao_percentual
FROM bronze_prod.ds_carros.raw_fipe f
JOIN bronze_prod.ds_carros.raw_referencias ref 
    ON f.codigo_referencia = ref.codigo
WHERE f.codigo_marca = 59  -- Exemplo: VW
  AND f.codigo_modelo = 5585  -- Exemplo: Gol
  AND f.codigo_ano = '2020-1'  -- Ano 2020, Gasolina
ORDER BY ref.codigo DESC
LIMIT 12;  -- Últimos 12 meses
```

### 3. Comparação de Depreciação por Ano de Fabricação

```sql
WITH precos_por_ano AS (
    SELECT 
        CAST(SUBSTRING(f.codigo_ano, 1, 4) AS INT) AS ano_fabricacao,
        AVG(f.valor) AS preco_medio
    FROM bronze_prod.ds_carros.raw_fipe f
    WHERE f.codigo_marca = 59  -- VW
      AND f.codigo_modelo = 5585  -- Gol
      AND f.valor > 0
    GROUP BY CAST(SUBSTRING(f.codigo_ano, 1, 4) AS INT)
)
SELECT 
    ano_fabricacao,
    preco_medio,
    LAG(preco_medio) OVER (ORDER BY ano_fabricacao DESC) - preco_medio AS depreciacao_absoluta,
    ROUND(
        ((LAG(preco_medio) OVER (ORDER BY ano_fabricacao DESC) - preco_medio) / 
         LAG(preco_medio) OVER (ORDER BY ano_fabricacao DESC)) * 100,
        2
    ) AS depreciacao_percentual
FROM precos_por_ano
ORDER BY ano_fabricacao DESC;
```

### 4. Análise de Tipos de Combustível Mais Caros

```sql
SELECT 
    CASE 
        WHEN codigo_ano LIKE '%-1' THEN 'Gasolina'
        WHEN codigo_ano LIKE '%-2' THEN 'Álcool'
        WHEN codigo_ano LIKE '%-3' THEN 'Diesel'
        ELSE 'Outros'
    END AS tipo_combustivel,
    COUNT(*) AS qtd_veiculos,
    ROUND(AVG(valor), 2) AS preco_medio,
    ROUND(MIN(valor), 2) AS preco_minimo,
    ROUND(MAX(valor), 2) AS preco_maximo
FROM bronze_prod.ds_carros.raw_fipe
WHERE valor > 0
GROUP BY CASE 
    WHEN codigo_ano LIKE '%-1' THEN 'Gasolina'
    WHEN codigo_ano LIKE '%-2' THEN 'Álcool'
    WHEN codigo_ano LIKE '%-3' THEN 'Diesel'
    ELSE 'Outros'
END
ORDER BY preco_medio DESC;
```

### 5. Modelos com Maior Valorização nos Últimos 6 Meses

```sql
WITH precos_recentes AS (
    SELECT 
        m.nome AS marca,
        md.nome AS modelo,
        f.valor AS preco_atual,
        ref.mes AS mes_referencia,
        ref.codigo AS codigo_ref,
        ROW_NUMBER() OVER (
            PARTITION BY f.codigo_marca, f.codigo_modelo, f.codigo_ano 
            ORDER BY ref.codigo DESC
        ) AS rn
    FROM bronze_prod.ds_carros.raw_fipe f
    JOIN bronze_prod.ds_carros.raw_marcas m ON f.codigo_marca = m.codigo
    JOIN bronze_prod.ds_carros.raw_modelos md ON f.codigo_modelo = md.codigo
    JOIN bronze_prod.ds_carros.raw_referencias ref ON f.codigo_referencia = ref.codigo
    WHERE f.valor > 0
),
comparacao AS (
    SELECT 
        marca,
        modelo,
        MAX(CASE WHEN rn = 1 THEN preco_atual END) AS preco_atual,
        MAX(CASE WHEN rn = 6 THEN preco_atual END) AS preco_6_meses_atras
    FROM precos_recentes
    WHERE rn <= 6
    GROUP BY marca, modelo
)
SELECT 
    marca,
    modelo,
    preco_atual,
    preco_6_meses_atras,
    ROUND(preco_atual - preco_6_meses_atras, 2) AS valorizacao_absoluta,
    ROUND(
        ((preco_atual - preco_6_meses_atras) / preco_6_meses_atras) * 100,
        2
    ) AS valorizacao_percentual
FROM comparacao
WHERE preco_6_meses_atras IS NOT NULL
  AND preco_atual > preco_6_meses_atras
ORDER BY valorizacao_percentual DESC
LIMIT 20;
```

---

## Casos de Uso e Análises Possíveis

### 1. Análise de Mercado Automotivo

**Objetivo**: Identificar tendências de preços e demanda

**Análises**:
* Evolução de preços por segmento (popular, médio, premium)
* Marcas com maior valorização/desvalorização
* Impacto de lançamentos em modelos antigos
* Análise de sazonalidade (vendas de fim de ano, férias)

**Exemplo de insight**:
> "Carros elétricos tiveram valorização média de 15% nos últimos 6 meses, enquanto carros a combustão desvalorizaram 8%."

---

### 2. Consultoria para Compra/Venda

**Objetivo**: Recomendar melhor momento para comprar ou vender

**Análises**:
* Identificar veículos com preço abaixo da média
* Prever valorização com base em histórico
* Comparar preços entre modelos similares
* Identificar oportunidades de revenda

---

### 3. Análise Competitiva entre Marcas

**Objetivo**: Comparar posicionamento de marcas no mercado

**Análises**:
* Distribuição de modelos por faixa de preço
* Market share por marca
* Preço médio por marca vs qualidade percebida
* Gap de preços entre concorrentes diretos

---

### 4. Análise de Depreciação

**Objetivo**: Calcular taxa de depreciação por ano

**Análises**:
* Curva de depreciação por marca/modelo
* Identificar veículos que mantém valor
* Comparar depreciação de diferentes segmentos
* Prever valor futuro de veículos

**Exemplo de insight**:
> "Pickup trucks depreciam apenas 35% nos primeiros 3 anos, enquanto sedãs depreciam 55% no mesmo período."

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
* **Databricks Workflows**: Orquestração de jobs

---

## Boas Práticas Implementadas

* **Separação de ambientes** (dev/prod)
* **Arquitetura Medalhão** (Bronze/Silver/Gold)
* **Ingestão incremental** com controle de dependências
* **Testes automatizados** (pasta tests/)
* **Retry automático** (max_retries: 1)
* **Notificações de erro** por email
* **Parametrização** via variáveis
* **Serverless** para escalabilidade automática
* **Rate limiting** nas chamadas de API
* **Controle de reprocessamento** com tabelas de controle

---

## Troubleshooting

### Problema: Ingestão de modelos muito lenta

**Causa**: Grande volume de modelos para processar (10.000+)

**Solução**: 
* Ajustar paralelização no notebook
* Aumentar timeout de execução
* Processar em lotes menores

---

### Problema: Erro 500 na API FIPE

**Causa**: Instabilidade temporária da API

**Solução**:
* O retry automático tentará novamente
* Verificar status da API FIPE em https://veiculos.fipe.org.br/

---

## Referências

### Documentação Databricks
* [Databricks Asset Bundles Documentation](https://docs.databricks.com/dev-tools/bundles/)
* [Delta Live Tables Documentation](https://docs.databricks.com/delta-live-tables/)
* [Unity Catalog](https://docs.databricks.com/data-governance/unity-catalog/)

### API FIPE
* [Tabela FIPE Oficial](https://veiculos.fipe.org.br/)
* [Documentação da API](https://deividfortuna.github.io/fipe/)

---

**Última atualização**: 25 de Abril de 2026  
**Versão**: 3.0 - Documentação expandida com variáveis, queries e métricas  
**Mantido por**: delacortearthur@gmail.com
