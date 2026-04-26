# Asset Bundle: Câmara dos Deputados

## Visão Geral

Este asset bundle implementa um pipeline completo de ingestão e transformação de dados da **API de Dados Abertos da Câmara dos Deputados do Brasil** (https://dadosabertos.camara.leg.br/), fornecendo informações oficiais sobre deputados federais, partidos políticos, despesas parlamentares e votações.

O projeto coleta múltiplas fontes de dados da Câmara dos Deputados, seguindo a **Arquitetura Medalhão** (Bronze → Silver → Gold).

---

## Arquitetura do Projeto

### Estrutura de Diretórios

```
camara_deputados/
├── databricks.yml                      # Configuração principal do bundle
├── resources/
│   ├── camara_deputados_job.job.yml     # Definição do workflow/job
│   └── camara_deputados_etl.pipeline.yml # Definição do pipeline DLT
├── src/
│   ├── ingest_deputados.py              # Ingestão de deputados
│   ├── ingest_partidos.py               # Ingestão de partidos
│   ├── ingest_despesas.py               # Ingestão de despesas (cota parlamentar)
│   ├── ingest_votacoes.py               # Ingestão de votações
│   └── dlt_camara_deputados.sql         # Transformações Bronze → Silver
├── tests/                               # Testes unitários
├── fixtures/                            # Dados de teste
└── README.md                            # Este arquivo
```

### Camadas de Dados

#### Bronze (Raw Data)
Dados brutos da API, sem transformações:
* `bronze_dev.ds_camara_deputados.raw_deputados`
* `bronze_dev.ds_camara_deputados.raw_partidos`
* `bronze_dev.ds_camara_deputados.raw_despesas`
* `bronze_dev.ds_camara_deputados.raw_votacoes`
* `bronze_dev.ds_camara_deputados.controle_ingestao`

#### Silver (Cleaned Data)
Dados limpos e padronizados:
* `silver_dev.ds_camara_deputados.tb_deputados`
* `silver_dev.ds_camara_deputados.tb_partidos`
* `silver_dev.ds_camara_deputados.tb_despesas`
* `silver_dev.ds_camara_deputados.tb_votacoes`

#### Gold (Business Data) - Futuro
Agregações e análises de negócio:
* Rankings de despesas
* Análises de votações por partido
* Indicadores de produtividade legislativa

---

## Fluxo de Dados

```
API Câmara dos Deputados
    │
    ├─── /deputados ────────────→ ingest_deputados.py ────→ raw_deputados
    │
    ├─── /partidos ─────────────→ ingest_partidos.py ─────→ raw_partidos
    │
    ├─── /deputados/{id}/despesas → ingest_despesas.py ──→ raw_despesas
    │
    └─── /votacoes ─────────────→ ingest_votacoes.py ─────→ raw_votacoes
                                       │
                                       ↓
                              DLT Pipeline (dlt_camara_deputados.sql)
                                       │
                                       ↓
                         ┌─────────────┴──────────────┐
                         │                            │
                    tb_deputados                 tb_partidos
                    tb_despesas                  tb_votacoes
```

---

## Fontes de Dados

### 1. Deputados
**Endpoint**: `/deputados`  
**Tipo**: Ingestão incremental por ID  
**Frequência**: Diária  
**Campos Capturados**: id, nome, sigla_partido, sigla_uf, id_legislatura, email, url_foto

### 2. Partidos
**Endpoint**: `/partidos`  
**Tipo**: Ingestão completa (dimensão)  
**Frequência**: Diária  
**Campos Capturados**: id, sigla, nome, uri, status  
**Volume**: ~21 partidos

### 3. Despesas (Cota Parlamentar)
**Endpoint**: `/deputados/{id}/despesas`  
**Tipo**: Ingestão incremental por mês  
**Frequência**: Mensal  
**Campos Capturados**: deputado_id, data, tipo_despesa, fornecedor, valor_documento, valor_liquido  
**Categorias**: Combustível, Passagens, Consultorias, Alimentação, Hospedagem, etc.

### 4. Votações
**Endpoint**: `/votacoes`  
**Tipo**: Ingestão incremental por data  
**Frequência**: Diária  
**Campos Capturados**: id, data, proposicao, orgao, resultado, aprovacao  
**Informações**: Votações do plenário e comissões

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
    default: ds_camara_deputados

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
      schema: ds_camara_deputados
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
      schema: ds_camara_deputados
      silver_catalog: silver_prod
      pause_status: UNPAUSED  # Execução automática
      pipeline_development: false
```

### Parâmetros dos Notebooks

Todos os notebooks de ingestão recebem os seguintes parâmetros via `dbutils.widgets`:

| Parâmetro | Tipo | Descrição | Exemplo |
|-----------|------|-----------|---------|
| `catalog` | String | Catálogo de destino | `bronze_dev` ou `bronze_prod` |
| `schema` | String | Schema de destino | `ds_camara_deputados` |

**Exemplo de uso no notebook**:
```python
dbutils.widgets.text("catalog", "bronze_prod")
used_catalog = dbutils.widgets.get("catalog")

dbutils.widgets.text("schema", "ds_camara_deputados")
used_schema = dbutils.widgets.get("schema")

tabela_destino = f"{used_catalog}.{used_schema}.raw_deputados"
```

---

## Job de Ingestão

O job `camara_deputados_job` orquestra todas as ingestões com as seguintes tasks:

### Tasks e Dependências

```
┌─────────────────┐
│ ingest_deputados│
└────────┬────────┘
         │
         ├──────────────────┐
         │                  │
         ↓                  ↓
┌─────────────────┐  ┌─────────────────┐
│ ingest_partidos │  │ ingest_despesas │
└────────┬────────┘  └────────┬────────┘
         │                    │
         │           ┌─────────────────┐
         │           │ ingest_votacoes │
         │           └────────┬────────┘
         │                    │
         └────────┬───────────┘
                  ↓
         ┌─────────────────┐
         │ refresh_pipeline│
         └─────────────────┘
```

### Configuração do Schedule

* **Horário**: 06:00 AM (America/Sao_Paulo)
* **Frequência**: Diária
* **Status**: Configurável por ambiente (dev: PAUSED, prod: UNPAUSED)

---

## Ambientes

### Desenvolvimento (dev)
* Catalog: `bronze_dev` / `silver_dev`
* Schema: `ds_camara_deputados`
* Status: PAUSED (execução manual)

### Produção (prod)
* Catalog: `bronze_prod` / `silver_prod`
* Schema: `ds_camara_deputados`
* Status: UNPAUSED (execução automática)

---

## Como Usar

### 1. Validar Configuração

```bash
cd project_lake_house/camara_deputados
databricks bundle validate
```

### 2. Deploy em Dev

```bash
databricks bundle deploy -t dev
```

### 3. Executar Job Manualmente

```bash
databricks bundle run camara_deputados_job -t dev
```

### 4. Deploy em Produção

```bash
databricks bundle deploy -t prod
```

---

## Tabelas e Schemas

### Tabela de Controle

```sql
CREATE TABLE bronze_dev.ds_camara_deputados.controle_ingestao (
  id INT,                    -- ID fixo = 1
  raw_deputados INT,         -- Último ID de deputado processado
  raw_partidos INT,          -- Último ID de partido processado
  raw_despesas STRING,       -- Último período (YYYY-MM) de despesas processado
  raw_votacoes STRING        -- Última data (YYYY-MM-DD) de votações processada
)
```

### Tabelas Bronze

#### raw_deputados
Dados brutos de deputados federais.

#### raw_partidos
Dados brutos de partidos políticos (dimensão).

#### raw_despesas
Despesas da cota parlamentar por deputado.

#### raw_votacoes
Votações do plenário e comissões.

### Tabelas Silver

#### tb_deputados
Deputados com dados limpos e padronizados.

#### tb_partidos
Partidos políticos (dimensão) com dados padronizados.

#### tb_despesas
Despesas limpas com valores tipados corretamente.

#### tb_votacoes
Votações com datas e timestamps padronizados.

---

## Métricas de Volume e Performance

### Volume de Dados Estimado

| Camada | Tabelas | Registros Totais | Tamanho Aprox. |
|--------|---------|------------------|----------------|
| Bronze | 5 tabelas | ~2.500.000 registros | ~800 MB |
| Silver | 4 tabelas | ~2.500.000 registros | ~650 MB (otimizado) |

### Detalhamento por Tabela (Bronze)

| Tabela | Registros Aprox. | Descrição |
|--------|------------------|-----------|
| `controle_ingestao` | 1 | Controle incremental |
| `raw_deputados` | ~600 | Deputados ativos + histórico |
| `raw_partidos` | ~30 | Partidos ativos + extintos |
| `raw_despesas` | ~2.000.000 | Despesas mensais (vários anos) |
| `raw_votacoes` | ~500.000 | Votações plenário + comissões |

### Performance de Execução

| Task | Tempo Médio | Registros Processados |
|------|-------------|------------------------|
| `ingest_deputados` | ~2 minutos | ~600 deputados |
| `ingest_partidos` | ~30 segundos | ~30 partidos |
| `ingest_despesas` | ~30-45 minutos | ~100.000 despesas/mês |
| `ingest_votacoes` | ~15-20 minutos | ~2.000 votações/mês |
| **Pipeline Silver** | ~20 minutos | ~2.500.000 registros |
| **Total (primeira execução)** | ~3-4 horas | Vários anos de histórico |
| **Total (incremental diário)** | ~15-30 minutos | Apenas últimas 24h |

**Nota**: Primeira execução ingere anos de histórico. Execuções incrementais processam apenas novos dados.

---

## Exemplos de Queries SQL

### 1. Ranking de Deputados por Total de Gastos (Ano Atual)

```sql
SELECT 
    d.nome AS deputado,
    d.sigla_partido AS partido,
    d.sigla_uf AS uf,
    COUNT(desp.id) AS qtd_despesas,
    ROUND(SUM(desp.valor_liquido), 2) AS total_gasto,
    ROUND(AVG(desp.valor_liquido), 2) AS valor_medio_despesa
FROM silver_prod.ds_camara_deputados.tb_deputados d
JOIN silver_prod.ds_camara_deputados.tb_despesas desp 
    ON d.id = desp.deputado_id
WHERE YEAR(desp.data) = YEAR(CURRENT_DATE())
GROUP BY d.id, d.nome, d.sigla_partido, d.sigla_uf
ORDER BY total_gasto DESC
LIMIT 20;
```

### 2. Análise de Despesas por Categoria e Partido

```sql
WITH despesas_agregadas AS (
    SELECT 
        p.sigla AS partido,
        desp.tipo_despesa,
        COUNT(*) AS qtd_despesas,
        ROUND(SUM(desp.valor_liquido), 2) AS total_gasto
    FROM silver_prod.ds_camara_deputados.tb_despesas desp
    JOIN silver_prod.ds_camara_deputados.tb_deputados d 
        ON desp.deputado_id = d.id
    JOIN silver_prod.ds_camara_deputados.tb_partidos p 
        ON d.sigla_partido = p.sigla
    WHERE YEAR(desp.data) = YEAR(CURRENT_DATE())
    GROUP BY p.sigla, desp.tipo_despesa
)
SELECT 
    partido,
    tipo_despesa,
    qtd_despesas,
    total_gasto,
    ROUND(total_gasto * 100.0 / SUM(total_gasto) OVER (PARTITION BY partido), 2) AS percentual_partido
FROM despesas_agregadas
ORDER BY partido, total_gasto DESC;
```

### 3. Fornecedores Mais Frequentes nas Despesas Parlamentares

```sql
SELECT 
    desp.fornecedor,
    COUNT(DISTINCT desp.deputado_id) AS qtd_deputados,
    COUNT(*) AS qtd_despesas,
    ROUND(SUM(desp.valor_liquido), 2) AS total_recebido,
    ROUND(AVG(desp.valor_liquido), 2) AS valor_medio
FROM silver_prod.ds_camara_deputados.tb_despesas desp
WHERE YEAR(desp.data) >= YEAR(CURRENT_DATE()) - 1  -- Últimos 2 anos
  AND desp.fornecedor IS NOT NULL
  AND TRIM(desp.fornecedor) != ''
GROUP BY desp.fornecedor
HAVING COUNT(*) >= 50  -- Pelo menos 50 transações
ORDER BY total_recebido DESC
LIMIT 30;
```

### 4. Taxa de Aprovação de Votações por Partido

```sql
WITH votacoes_partido AS (
    SELECT 
        p.sigla AS partido,
        v.resultado,
        COUNT(*) AS qtd_votacoes,
        -- Assumindo que deputados votam alinhados ao partido na maioria das vezes
        CASE 
            WHEN v.resultado = 'Aprovado' THEN 1
            ELSE 0
        END AS aprovado_flag
    FROM silver_prod.ds_camara_deputados.tb_votacoes v
    JOIN silver_prod.ds_camara_deputados.tb_deputados d 
        ON v.proposicao LIKE CONCAT('%', d.nome, '%')  -- Simplificação
    JOIN silver_prod.ds_camara_deputados.tb_partidos p 
        ON d.sigla_partido = p.sigla
    WHERE YEAR(v.data) = YEAR(CURRENT_DATE())
    GROUP BY p.sigla, v.resultado, aprovado_flag
)
SELECT 
    partido,
    SUM(qtd_votacoes) AS total_votacoes,
    SUM(CASE WHEN resultado = 'Aprovado' THEN qtd_votacoes ELSE 0 END) AS votacoes_aprovadas,
    ROUND(
        SUM(CASE WHEN resultado = 'Aprovado' THEN qtd_votacoes ELSE 0 END) * 100.0 / 
        NULLIF(SUM(qtd_votacoes), 0),
        2
    ) AS taxa_aprovacao_pct
FROM votacoes_partido
GROUP BY partido
ORDER BY taxa_aprovacao_pct DESC;
```

### 5. Comparação de Gastos: Estados x Regiões

```sql
WITH gastos_uf AS (
    SELECT 
        d.sigla_uf AS uf,
        CASE 
            WHEN d.sigla_uf IN ('AC', 'AP', 'AM', 'PA', 'RO', 'RR', 'TO') THEN 'Norte'
            WHEN d.sigla_uf IN ('AL', 'BA', 'CE', 'MA', 'PB', 'PE', 'PI', 'RN', 'SE') THEN 'Nordeste'
            WHEN d.sigla_uf IN ('DF', 'GO', 'MT', 'MS') THEN 'Centro-Oeste'
            WHEN d.sigla_uf IN ('ES', 'MG', 'RJ', 'SP') THEN 'Sudeste'
            WHEN d.sigla_uf IN ('PR', 'RS', 'SC') THEN 'Sul'
        END AS regiao,
        COUNT(DISTINCT d.id) AS qtd_deputados,
        ROUND(SUM(desp.valor_liquido), 2) AS total_gasto,
        ROUND(AVG(desp.valor_liquido), 2) AS gasto_medio_despesa
    FROM silver_prod.ds_camara_deputados.tb_deputados d
    JOIN silver_prod.ds_camara_deputados.tb_despesas desp 
        ON d.id = desp.deputado_id
    WHERE YEAR(desp.data) = YEAR(CURRENT_DATE())
    GROUP BY d.sigla_uf, regiao
)
SELECT 
    regiao,
    uf,
    qtd_deputados,
    total_gasto,
    ROUND(total_gasto / qtd_deputados, 2) AS gasto_per_capita_deputado,
    gasto_medio_despesa
FROM gastos_uf
ORDER BY regiao, total_gasto DESC;
```

### 6. Evolução Temporal de Despesas Parlamentares

```sql
SELECT 
    DATE_FORMAT(desp.data, 'yyyy-MM') AS mes_ano,
    COUNT(DISTINCT desp.deputado_id) AS qtd_deputados_ativos,
    COUNT(*) AS qtd_despesas,
    ROUND(SUM(desp.valor_liquido), 2) AS total_gasto,
    ROUND(AVG(desp.valor_liquido), 2) AS valor_medio,
    ROUND(SUM(desp.valor_liquido) / COUNT(DISTINCT desp.deputado_id), 2) AS gasto_medio_por_deputado
FROM silver_prod.ds_camara_deputados.tb_despesas desp
WHERE desp.data >= DATE_SUB(CURRENT_DATE(), 730)  -- Últimos 2 anos
GROUP BY DATE_FORMAT(desp.data, 'yyyy-MM')
ORDER BY mes_ano DESC;
```

---

## Controle Incremental

### Deputados
* Controle por `ID` (último ID processado)
* Filtra apenas deputados com ID maior que o último processado

### Partidos
* Controle por `ID` (último ID processado)
* Ingestão completa de todos os partidos novos

### Despesas
* Controle por `ANO-MÊS` (formato: YYYY-MM)
* Processa um mês por execução
* Itera por todos os deputados ativos

### Votações
* Controle por `DATA` (formato: YYYY-MM-DD)
* Processa votações a partir da última data processada
* Paginação automática da API

---

## Casos de Uso e Análises Possíveis

### 1. Análise de Transparência

**Objetivo**: Identificar padrões de gastos parlamentares

**Análises**:
* Ranking de gastos por deputado
* Identificação de fornecedores mais frequentes
* Análise temporal de despesas
* Comparação entre estados e partidos

**Exemplo de insight**:
> "Deputados da região Sudeste gastam em média 15% a mais que a média nacional, principalmente em passagens aéreas."

### 2. Análise Política e Partidária

**Objetivo**: Estudar comportamento e alinhamento partidário

**Análises**:
* Alinhamento de votos por partido
* Identificação de clusters de deputados
* Taxa de aprovação de proposições
* Disciplina partidária em votações

### 3. Business Intelligence Governamental

**Objetivo**: Dashboards de despesas públicas e atividade legislativa

**Aplicações**:
* Dashboards de despesas parlamentares
* Indicadores de produtividade legislativa
* Análise de representatividade por estado/partido
* Alertas de gastos atípicos

### 4. Fiscalização e Auditoria

**Objetivo**: Detectar anomalias e padrões suspeitos

**Análises**:
* Gastos acima da média em categorias específicas
* Fornecedores com concentração de recursos
* Despesas em períodos atípicos (recesso parlamentar)
* Comparação com legislaturas anteriores

---

## Tecnologias Utilizadas

* **Databricks Asset Bundles**: Orquestração e deploy
* **Delta Live Tables (DLT)**: Pipeline de transformação
* **Python**: Ingestão de dados
* **SQL**: Transformações de dados
* **REST API**: Comunicação com API pública

---

## Boas Práticas Implementadas

1. **Controle Incremental**: Evita reprocessamento de dados
2. **Idempotência**: Execuções múltiplas não duplicam dados
3. **Separação de Ambientes**: Dev e Prod isolados
4. **Retry Logic**: Reprocessamento automático em falhas
5. **Logging**: Mensagens claras de progresso e erros
6. **Rate Limiting**: Respeito aos limites da API (0.1-0.2s entre requisições)
7. **Schema Evolution**: Tabelas Delta suportam evolução de schema
8. **Documentation**: Código documentado e README completo

---

## Expansões Futuras

A API oferece outros endpoints interessantes:

* **Proposições**: Projetos de lei, MPs, PDCs, etc.
* **Eventos**: Sessões, audiências públicas, agenda
* **Órgãos**: Comissões permanentes e temporárias
* **Frentes Parlamentares**: Agrupamentos temáticos
* **Blocos Partidários**: Alianças entre partidos

---

## Referências

* **API Docs**: https://dadosabertos.camara.leg.br/swagger/api.html
* **Portal de Dados Abertos**: https://dadosabertos.camara.leg.br/
* **Databricks Asset Bundles**: https://docs.databricks.com/dev-tools/bundles/
* **Delta Live Tables**: https://docs.databricks.com/delta-live-tables/

---

## Notificações

Em caso de falha em qualquer task, um email é enviado para:
* **delacortearthur@gmail.com**

---

## Troubleshooting

### Problema: Ingestão de despesas muito lenta

**Causa**: Grande volume de dados (anos de histórico)

**Solução**: 
* É esperado na primeira execução (~30-45 minutos)
* Execuções incrementais são mais rápidas (~5-10 minutos)
* Considere processar apenas últimos 2-3 anos inicialmente

---

### Problema: Erro 429 na API da Câmara

**Causa**: Limite de requisições excedido

**Solução**:
* O delay de 0.1-0.2s entre requisições já está implementado
* Se persistir, aumentar delay para 0.3s
* Executar fora de horários de pico

---

**Última atualização**: 25 de Abril de 2026  
**Versão**: 3.0 - Documentação expandida com variáveis, queries e métricas  
**Mantido por**: delacortearthur@gmail.com
