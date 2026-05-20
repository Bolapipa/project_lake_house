-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Pipeline DLT - Câmara dos Deputados
-- MAGIC
-- MAGIC Transformações Bronze -> Silver para dados da Câmara dos Deputados:
-- MAGIC - Deputados
-- MAGIC - Partidos
-- MAGIC - Despesas
-- MAGIC - Votações

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Tabela Silver: Deputados

-- COMMAND ----------

-- DBTITLE 1,tb_deputados
CREATE OR REFRESH STREAMING LIVE TABLE cleaned_deputados
COMMENT "Tabela silver de deputados - dados limpos e padronizados"
AS
SELECT
  id,
  uri,
  TRIM(nome) as nome,
  UPPER(TRIM(sigla_partido)) as sigla_partido,
  uri_partido,
  UPPER(TRIM(sigla_uf)) as sigla_uf,
  id_legislatura,
  url_foto,
  LOWER(TRIM(email)) as email,
  current_timestamp() as data_processamento
FROM STREAM(${source_catalog}.${source_schema}.raw_deputados)
WHERE id IS NOT NULL
  AND nome IS NOT NULL

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Tabela Silver: Partidos

-- COMMAND ----------

-- DBTITLE 1,tb_partidos
CREATE OR REFRESH STREAMING LIVE TABLE cleaned_partidos
COMMENT "Tabela silver de partidos políticos - dimensão"
AS
SELECT
  id,
  UPPER(TRIM(sigla)) as sigla,
  TRIM(nome) as nome,
  uri,
  status,
  current_timestamp() as data_processamento
FROM STREAM(${source_catalog}.${source_schema}.raw_partidos)
WHERE id IS NOT NULL
  AND sigla IS NOT NULL

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Tabela Silver: Despesas

-- COMMAND ----------

-- DBTITLE 1,tb_despesas
CREATE OR REFRESH STREAMING LIVE TABLE cleaned_despesas
COMMENT "Tabela silver de despesas dos deputados (cota parlamentar)"
AS
SELECT
  id_deputado,
  ano,
  mes,
  TRIM(tipo_despesa) as tipo_despesa,
  cod_documento,
  TRIM(tipo_documento) as tipo_documento,
  cod_tipo_documento,
  CAST(data_documento AS DATE) as data_documento,
  TRIM(num_documento) as num_documento,
  CAST(valor_documento AS DECIMAL(15,2)) as valor_documento,
  url_documento,
  TRIM(nome_fornecedor) as nome_fornecedor,
  TRIM(cnpj_cpf_fornecedor) as cnpj_cpf_fornecedor,
  CAST(valor_liquido AS DECIMAL(15,2)) as valor_liquido,
  CAST(valor_glosa AS DECIMAL(15,2)) as valor_glosa,
  num_ressarcimento,
  cod_lote,
  parcela,
  current_timestamp() as data_processamento
FROM STREAM(${source_catalog}.${source_schema}.raw_despesas)
WHERE id_deputado IS NOT NULL
  AND ano IS NOT NULL
  AND mes IS NOT NULL
  AND valor_documento > 0

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Tabela Silver: Votações

-- COMMAND ----------

-- DBTITLE 1,tb_votacoes
CREATE OR REFRESH STREAMING LIVE TABLE cleaned_votacoes
COMMENT "Tabela silver de votações do plenário e comissões"
AS
SELECT
  TRIM(id) as id,
  CAST(data AS DATE) as data,
  CAST(REPLACE(data_hora_registro, 'T', ' ') AS TIMESTAMP) as data_hora_registro,
  UPPER(TRIM(sigla_orgao)) as sigla_orgao,
  id_orgao,
  uri_orgao,
  uri_evento,
  proposicao_cod_tipo,
  proposicao_numero,
  proposicao_ano,
  uri_proposicao_votada,
  CAST(aprovacao AS INT) as aprovacao,
  TRIM(objeto_votacao) as objeto_votacao,
  current_timestamp() as data_processamento
FROM STREAM(${source_catalog}.${source_schema}.raw_votacoes)
WHERE id IS NOT NULL
  AND data IS NOT NULL