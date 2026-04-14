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
- `bronze_dev.ds_camara_deputados.raw_deputados`
- `bronze_dev.ds_camara_deputados.raw_partidos`
- `bronze_dev.ds_camara_deputados.raw_despesas`
- `bronze_dev.ds_camara_deputados.raw_votacoes`
- `bronze_dev.ds_camara_deputados.controle_ingestao`

#### Silver (Cleaned Data)
Dados limpos e padronizados:
- `silver_dev.ds_camara_deputados.tb_deputados`
- `silver_dev.ds_camara_deputados.tb_partidos`
- `silver_dev.ds_camara_deputados.tb_despesas`
- `silver_dev.ds_camara_deputados.tb_votacoes`

#### Gold (Business Data) - Futuro
Agregações e análises de negócio:
- Rankings de despesas
- Análises de votações por partido
- Indicadores de produtividade legislativa

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

- **Horário**: 06:00 AM (America/Sao_Paulo)
- **Frequência**: Diária
- **Status**: Configurável por ambiente (dev: PAUSED, prod: UNPAUSED)

---

## Ambientes

### Desenvolvimento (dev)
- Catalog: `bronze_dev` / `silver_dev`
- Schema: `ds_camara_deputados`
- Status: PAUSED (execução manual)

### Produção (prod)
- Catalog: `bronze_prod` / `silver_prod`
- Schema: `ds_camara_deputados`
- Status: UNPAUSED (execução automática)

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

## Controle Incremental

### Deputados
- Controle por `ID` (último ID processado)
- Filtra apenas deputados com ID maior que o último processado

### Partidos
- Controle por `ID` (último ID processado)
- Ingestão completa de todos os partidos novos

### Despesas
- Controle por `ANO-MÊS` (formato: YYYY-MM)
- Processa um mês por execução
- Itera por todos os deputados ativos

### Votações
- Controle por `DATA` (formato: YYYY-MM-DD)
- Processa votações a partir da última data processada
- Paginação automática da API

---

## Tecnologias Utilizadas

- **Databricks Asset Bundles**: Orquestração e deploy
- **Delta Live Tables (DLT)**: Pipeline de transformação
- **Python**: Ingestão de dados
- **SQL**: Transformações de dados
- **REST API**: Comunicação com API pública

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

## Casos de Uso

### Análise de Transparência
- Ranking de gastos por deputado
- Identificação de fornecedores mais frequentes
- Análise temporal de despesas

### Análise Política
- Alinhamento de votos por partido
- Identificação de clusters de deputados
- Taxa de aprovação de proposições

### Business Intelligence
- Dashboards de despesas públicas
- Indicadores de produtividade legislativa
- Análise de representatividade por estado/partido

---

## Expansões Futuras

A API oferece outros endpoints interessantes:

- **Proposições**: Projetos de lei, MPs, PDCs, etc.
- **Eventos**: Sessões, audiências públicas, agenda
- **Órgãos**: Comissões permanentes e temporárias
- **Frentes Parlamentares**: Agrupamentos temáticos
- **Blocos Partidários**: Alianças entre partidos

---

## Referências

- **API Docs**: https://dadosabertos.camara.leg.br/swagger/api.html
- **Portal de Dados Abertos**: https://dadosabertos.camara.leg.br/
- **Databricks Asset Bundles**: https://docs.databricks.com/dev-tools/bundles/
- **Delta Live Tables**: https://docs.databricks.com/delta-live-tables/

---

## Contato

**Mantenedor**: Arthur Delacorte  
**Email**: delacortearthur@gmail.com  
**Ambiente**: Databricks na AWS

---

**Última Atualização**: Abril 2026  
**Versão**: 2.0 (Expandido com Partidos, Despesas e Votações)
