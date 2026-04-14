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

 Task | Descrição | Fonte de Dados |
------|-----------|----------------|
 **ingest_marcas** | Extrai lista de marcas de veículos (ex: Fiat, Ford, VW) | API FIPE `/marcas` |
 **ingest_modelos** | Extrai modelos para cada marca (ex: Uno, Gol, Corsa) | API FIPE `/modelos` |
 **ingest_anos** | Extrai anos/combustível disponíveis por modelo | API FIPE `/anos` |
 **ingest_referencias** | Extrai meses de referência da tabela FIPE | API FIPE `/referencias` |
 **ingest_fipe** | Extrai preços finais combinando todas as dimensões | API FIPE `/valor` |

**Nota**: Todas as tasks salvam dados no catálogo **bronze_dev** (dev) ou **bronze_prod** (prod) no schema `ds_carros`.

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

### Parâmetros do Job

 Parâmetro | Descrição | Padrão (dev) | Produção |
-----------|-----------|--------------|----------|
 `environment` | Ambiente de execução | `dev` | `prod` |
 `catalog` | Catálogo Bronze de destino | `bronze_dev` | `bronze_prod` |
 `schema` | Schema de destino | `ds_carros` | `ds_carros` |

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
* `tb_marcas_bronze` - Marcas de veículos
* `tb_modelos_bronze` - Modelos por marca
* `tb_anos_bronze` - Anos disponíveis por modelo
* `tb_referencias_bronze` - Meses de referência
* `tb_fipe_bronze` - Preços FIPE completos

### Camada Silver (silver_dev.ds_carros)
* `tb_marcas` - Marcas limpas e padronizadas
* `tb_modelos` - Modelos normalizados
* `tb_anos` - Anos validados
* `tb_referencias` - Referências estruturadas
* `tb_fipe` - Preços FIPE tratados

### Camada Gold (gold_dev.dm_carros)
* `dim_marca` - Dimensão de marcas
* `dim_modelo` - Dimensão de modelos
* `dim_ano` - Dimensão de anos
* `dim_tempo` - Dimensão temporal (referências)
* `fato_preco_fipe` - Fato de preços FIPE

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

---

## Referências

* [Databricks Asset Bundles Documentation](https://docs.databricks.com/dev-tools/bundles/)
* [Delta Live Tables Documentation](https://docs.databricks.com/delta-live-tables/)
* [Tabela FIPE](https://veiculos.fipe.org.br/)

---

**Última atualização**: $(date +"%Y-%m-%d")  
**Mantido por**: delacortearthur@gmail.com
