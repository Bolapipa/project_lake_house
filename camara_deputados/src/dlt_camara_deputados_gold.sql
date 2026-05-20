-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Pipeline DLT Gold - Câmara dos Deputados
-- MAGIC
-- MAGIC **Modelagem Dimensional Star Schema**
-- MAGIC
-- MAGIC Transformações Silver → Gold para análise de dados da Câmara dos Deputados.
-- MAGIC
-- MAGIC ## Dimensões
-- MAGIC * dim_deputado - Deputados federais
-- MAGIC * dim_partido - Partidos políticos
-- MAGIC * dim_tempo - Calendário temporal
-- MAGIC * dim_tipo_despesa - Tipos de despesas parlamentares
-- MAGIC * dim_fornecedor - Fornecedores de produtos/serviços
-- MAGIC * dim_orgao - Órgãos legislativos (Plenário e Comissões)
-- MAGIC
-- MAGIC ## Fatos
-- MAGIC * fact_despesas - Despesas da cota parlamentar
-- MAGIC * fact_votacoes - Votações em Plenário e Comissões

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Dimensão: Deputados

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Dimensão: Deputados

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW dim_deputado
(
  sk_deputado BIGINT COMMENT 'Chave substituta do deputado, gerada a partir do ID natural.',
  id BIGINT COMMENT 'Identificador único do deputado na API da Câmara.',
  nome STRING COMMENT 'Nome completo do deputado.',
  sigla_partido STRING COMMENT 'Sigla do partido político do deputado.',
  sigla_uf STRING COMMENT 'Sigla da Unidade Federativa (estado) que o deputado representa.',
  id_legislatura BIGINT COMMENT 'Identificador da legislatura.',
  url_foto STRING COMMENT 'URL da foto oficial do deputado.',
  email STRING COMMENT 'E-mail de contato do deputado.',
  CONSTRAINT not_null_sk_deputado EXPECT (sk_deputado IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT 'Dimensão de deputados federais, contendo informações cadastrais e de mandato.'
TBLPROPERTIES (
  "quality" = "gold",
  pipelines.autoOptimize.managed = true,
  delta.autoOptimize.optimizeWrite = true,
  delta.autoOptimize.autoCompact = true
)
CLUSTER BY AUTO
AS
SELECT DISTINCT
  xxhash64(CAST(cd.id AS STRING)) AS sk_deputado,
  cd.id,
  cd.nome,
  cd.sigla_partido,
  cd.sigla_uf,
  cd.id_legislatura,
  cd.url_foto,
  cd.email
FROM ${silver_catalog}.ds_camara_deputados.cleaned_deputados cd
WHERE cd.id IS NOT NULL;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Dimensão: Partidos

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW dim_partido
(
  id BIGINT COMMENT 'Identificador único do partido político.',
  sigla STRING COMMENT 'Sigla oficial do partido (ex: PT, PSDB, MDB).',
  nome STRING COMMENT 'Nome completo do partido político.',
  uri STRING COMMENT 'URI da API para consulta detalhada do partido.',
  status STRING COMMENT 'Status atual do partido (ativo, extinto, etc.).',
  CONSTRAINT not_null_id_partido EXPECT (id IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT 'Dimensão de partidos políticos brasileiros, utilizada para análises de bancadas e coligações.'
TBLPROPERTIES (
  "quality" = "gold",
  pipelines.autoOptimize.managed = true,
  delta.autoOptimize.optimizeWrite = true,
  delta.autoOptimize.autoCompact = true
)
CLUSTER BY AUTO
AS
SELECT DISTINCT
  cp.id,
  cp.sigla,
  cp.nome,
  cp.uri,
  cp.status
FROM ${silver_catalog}.ds_camara_deputados.cleaned_partidos cp
WHERE cp.id IS NOT NULL;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Dimensão: Tempo

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW dim_tempo
(
  sk_data INT COMMENT 'Chave substituta da data no formato YYYYMMDD.',
  data DATE COMMENT 'Data completa.',
  ano INT COMMENT 'Ano (ex: 2026).',
  mes INT COMMENT 'Mês (1-12).',
  trimestre INT COMMENT 'Trimestre (1-4).',
  semestre INT COMMENT 'Semestre (1-2).',
  nome_mes STRING COMMENT 'Nome do mês por extenso (ex: Janeiro, Fevereiro).',
  dia_semana INT COMMENT 'Dia da semana (1=Domingo, 7=Sábado).',
  nome_dia_semana STRING COMMENT 'Nome do dia da semana (ex: Segunda-feira, Terça-feira).',
  eh_fim_semana BOOLEAN COMMENT 'Indicador se é fim de semana (Sábado ou Domingo).',
  CONSTRAINT not_null_sk_data EXPECT (sk_data IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT 'Dimensão de calendário temporal para análises de séries temporais e sazonalidade.'
TBLPROPERTIES (
  "quality" = "gold",
  pipelines.autoOptimize.managed = true,
  delta.autoOptimize.optimizeWrite = true,
  delta.autoOptimize.autoCompact = true
)
CLUSTER BY AUTO
AS
WITH datas_unicas AS (
  SELECT DISTINCT data_documento AS data
  FROM ${silver_catalog}.ds_camara_deputados.cleaned_despesas
  WHERE data_documento IS NOT NULL
  
  UNION
  
  SELECT DISTINCT data
  FROM ${silver_catalog}.ds_camara_deputados.cleaned_votacoes
  WHERE data IS NOT NULL
)
SELECT
  CAST(DATE_FORMAT(data, 'yyyyMMdd') AS INT) AS sk_data,
  data,
  YEAR(data) AS ano,
  MONTH(data) AS mes,
  QUARTER(data) AS trimestre,
  CASE WHEN MONTH(data) <= 6 THEN 1 ELSE 2 END AS semestre,
  CASE MONTH(data)
    WHEN 1 THEN 'Janeiro'
    WHEN 2 THEN 'Fevereiro'
    WHEN 3 THEN 'Março'
    WHEN 4 THEN 'Abril'
    WHEN 5 THEN 'Maio'
    WHEN 6 THEN 'Junho'
    WHEN 7 THEN 'Julho'
    WHEN 8 THEN 'Agosto'
    WHEN 9 THEN 'Setembro'
    WHEN 10 THEN 'Outubro'
    WHEN 11 THEN 'Novembro'
    WHEN 12 THEN 'Dezembro'
  END AS nome_mes,
  DAYOFWEEK(data) AS dia_semana,
  CASE DAYOFWEEK(data)
    WHEN 1 THEN 'Domingo'
    WHEN 2 THEN 'Segunda-feira'
    WHEN 3 THEN 'Terça-feira'
    WHEN 4 THEN 'Quarta-feira'
    WHEN 5 THEN 'Quinta-feira'
    WHEN 6 THEN 'Sexta-feira'
    WHEN 7 THEN 'Sábado'
  END AS nome_dia_semana,
  CASE WHEN DAYOFWEEK(data) IN (1, 7) THEN true ELSE false END AS eh_fim_semana
FROM datas_unicas
WHERE data IS NOT NULL;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Dimensão: Tipos de Despesa
-- MAGIC

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW dim_tipo_despesa
(
  sk_tipo_despesa BIGINT COMMENT 'Chave substituta do tipo de despesa.',
  tipo_despesa STRING COMMENT 'Descrição do tipo de despesa (ex: Combustíveis, Passagens Aéreas, Telefonia).',
  cod_tipo_documento BIGINT COMMENT 'Código do tipo de documento.',
  tipo_documento STRING COMMENT 'Descrição do tipo de documento fiscal.',
  CONSTRAINT not_null_sk_tipo_despesa EXPECT (sk_tipo_despesa IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT 'Dimensão de tipos de despesas parlamentares da cota para custeio de atividades.'
TBLPROPERTIES (
  "quality" = "gold",
  pipelines.autoOptimize.managed = true,
  delta.autoOptimize.optimizeWrite = true,
  delta.autoOptimize.autoCompact = true
)
CLUSTER BY AUTO
AS
SELECT DISTINCT
  xxhash64(
    COALESCE(CAST(cd.tipo_despesa AS STRING), ''),
    COALESCE(CAST(cd.cod_tipo_documento AS STRING), '')
  ) AS sk_tipo_despesa,
  cd.tipo_despesa,
  cd.cod_tipo_documento,
  cd.tipo_documento
FROM ${silver_catalog}.ds_camara_deputados.cleaned_despesas cd
WHERE cd.tipo_despesa IS NOT NULL;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Dimensão: Fornecedores
-- MAGIC

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW dim_fornecedor
(
  sk_fornecedor BIGINT COMMENT 'Chave substituta do fornecedor.',
  cnpj_cpf STRING COMMENT 'CNPJ ou CPF do fornecedor.',
  nome_fornecedor STRING COMMENT 'Razão social ou nome do fornecedor.',
  CONSTRAINT not_null_sk_fornecedor EXPECT (sk_fornecedor IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT 'Dimensão de fornecedores que prestam serviços ou vendem produtos aos deputados.'
TBLPROPERTIES (
  "quality" = "gold",
  pipelines.autoOptimize.managed = true,
  delta.autoOptimize.optimizeWrite = true,
  delta.autoOptimize.autoCompact = true
)
CLUSTER BY AUTO
AS
SELECT DISTINCT
  xxhash64(COALESCE(cd.cnpj_cpf_fornecedor, 'SEM_CNPJ')) AS sk_fornecedor,
  cd.cnpj_cpf_fornecedor AS cnpj_cpf,
  cd.nome_fornecedor
FROM ${silver_catalog}.ds_camara_deputados.cleaned_despesas cd
WHERE cd.nome_fornecedor IS NOT NULL;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Dimensão: Órgãos Legislativos
-- MAGIC

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW dim_orgao
(
  sk_orgao BIGINT COMMENT 'Chave substituta do órgão legislativo.',
  id_orgao STRING COMMENT 'Identificador do órgão (Plenário ou Comissão).',
  sigla_orgao STRING COMMENT 'Sigla do órgão legislativo.',
  uri_orgao STRING COMMENT 'URI da API para consulta detalhada do órgão.',
  CONSTRAINT not_null_sk_orgao EXPECT (sk_orgao IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT 'Dimensão de órgãos legislativos onde ocorrem votações (Plenário, Comissões temáticas, etc.).'
TBLPROPERTIES (
  "quality" = "gold",
  pipelines.autoOptimize.managed = true,
  delta.autoOptimize.optimizeWrite = true,
  delta.autoOptimize.autoCompact = true
)
CLUSTER BY AUTO
AS
SELECT DISTINCT
  xxhash64(COALESCE(cv.id_orgao, 'DESCONHECIDO')) AS sk_orgao,
  cv.id_orgao,
  cv.sigla_orgao,
  cv.uri_orgao
FROM ${silver_catalog}.ds_camara_deputados.cleaned_votacoes cv
WHERE cv.id_orgao IS NOT NULL;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Fato: Despesas Parlamentares

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW fact_despesas
(
  sk_deputado BIGINT COMMENT 'Chave estrangeira para dim_deputado.',
  sk_data INT COMMENT 'Chave estrangeira para dim_tempo.',
  sk_tipo_despesa BIGINT COMMENT 'Chave estrangeira para dim_tipo_despesa.',
  sk_fornecedor BIGINT COMMENT 'Chave estrangeira para dim_fornecedor.',
  cod_documento STRING COMMENT 'Código único do documento fiscal (chave natural).',
  num_documento STRING COMMENT 'Número do documento fiscal.',
  valor_documento DECIMAL(15,2) COMMENT 'Valor bruto do documento em reais.',
  valor_liquido DECIMAL(15,2) COMMENT 'Valor líquido pago ao fornecedor em reais.',
  valor_glosa DECIMAL(15,2) COMMENT 'Valor glosado (não reembolsado) em reais.',
  num_ressarcimento STRING COMMENT 'Número do ressarcimento.',
  url_documento STRING COMMENT 'URL para acesso ao documento fiscal digitalizado.',
  cod_lote BIGINT COMMENT 'Código do lote de processamento.',
  parcela BIGINT COMMENT 'Número da parcela (para despesas parceladas).',
  CONSTRAINT not_null_cod_documento EXPECT (cod_documento IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT 'Tabela fato de despesas da cota parlamentar, contendo os gastos dos deputados federais.'
TBLPROPERTIES (
  "quality" = "gold",
  pipelines.autoOptimize.managed = true,
  delta.autoOptimize.optimizeWrite = true,
  delta.autoOptimize.autoCompact = true
)
CLUSTER BY AUTO
AS
SELECT
  xxhash64(CAST(cd.id_deputado AS STRING)) AS sk_deputado,
  CAST(DATE_FORMAT(cd.data_documento, 'yyyyMMdd') AS INT) AS sk_data,
  xxhash64(
    COALESCE(CAST(cd.tipo_despesa AS STRING), ''),
    COALESCE(CAST(cd.cod_tipo_documento AS STRING), '')
  ) AS sk_tipo_despesa,
  xxhash64(COALESCE(cd.cnpj_cpf_fornecedor, 'SEM_CNPJ')) AS sk_fornecedor,
  cd.cod_documento,
  cd.num_documento,
  cd.valor_documento,
  cd.valor_liquido,
  cd.valor_glosa,
  cd.num_ressarcimento,
  cd.url_documento,
  cd.cod_lote,
  cd.parcela
FROM ${silver_catalog}.ds_camara_deputados.cleaned_despesas cd
WHERE cd.cod_documento IS NOT NULL
  AND cd.id_deputado IS NOT NULL
  AND cd.data_documento IS NOT NULL;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Fato: Votações

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW fact_votacoes
(
  sk_votacao BIGINT COMMENT 'Chave substituta da votação (hash do ID original).',
  sk_orgao BIGINT COMMENT 'Chave estrangeira para dim_orgao.',
  sk_data INT COMMENT 'Chave estrangeira para dim_tempo.',
  data_hora_registro TIMESTAMP COMMENT 'Data e hora completa do registro da votação.',
  proposicao_cod_tipo STRING COMMENT 'Código do tipo da proposição votada.',
  proposicao_numero STRING COMMENT 'Número da proposição votada.',
  proposicao_ano STRING COMMENT 'Ano da proposição votada.',
  uri_proposicao STRING COMMENT 'URI da API para consulta da proposição.',
  aprovacao INT COMMENT 'Resultado da votação (1=Aprovado, 0=Rejeitado).',
  objeto_votacao STRING COMMENT 'Descrição do objeto/ementa da votação.',
  CONSTRAINT not_null_sk_votacao EXPECT (sk_votacao IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT 'Tabela fato de votações realizadas em Plenário e Comissões da Câmara dos Deputados.'
TBLPROPERTIES (
  "quality" = "gold",
  pipelines.autoOptimize.managed = true,
  delta.autoOptimize.optimizeWrite = true,
  delta.autoOptimize.autoCompact = true
)
CLUSTER BY AUTO
AS
SELECT
  xxhash64(CAST(cv.id AS STRING)) AS sk_votacao,
  xxhash64(COALESCE(cv.id_orgao, 'DESCONHECIDO')) AS sk_orgao,
  CAST(DATE_FORMAT(cv.data, 'yyyyMMdd') AS INT) AS sk_data,
  cv.data_hora_registro,
  cv.proposicao_cod_tipo,
  cv.proposicao_numero,
  cv.proposicao_ano,
  cv.uri_proposicao_votada AS uri_proposicao,
  cv.aprovacao,
  cv.objeto_votacao
FROM ${silver_catalog}.ds_camara_deputados.cleaned_votacoes cv
WHERE cv.id IS NOT NULL
  AND cv.data IS NOT NULL;
