# Databricks notebook source
# MAGIC %md
# MAGIC # Pipeline DLT Gold - Câmara dos Deputados
# MAGIC
# MAGIC **Modelagem Dimensional Star Schema**
# MAGIC
# MAGIC Transformações Silver → Gold para análise de dados da Câmara dos Deputados.
# MAGIC
# MAGIC ## Dimensões
# MAGIC * dim_deputado - Deputados federais
# MAGIC * dim_partido - Partidos políticos
# MAGIC * dim_tempo - Calendário temporal
# MAGIC * dim_tipo_despesa - Tipos de despesas parlamentares
# MAGIC * dim_fornecedor - Fornecedores de produtos/serviços
# MAGIC * dim_orgao - Órgãos legislativos (Plenário e Comissões)
# MAGIC
# MAGIC ## Fatos
# MAGIC * fact_despesas - Despesas da cota parlamentar
# MAGIC * fact_votacoes - Votações em Plenário e Comissões

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dimensão: Deputados

# COMMAND ----------

# DBTITLE 1,dim_deputado
# MAGIC %sql
# MAGIC CREATE OR REFRESH MATERIALIZED VIEW dim_deputado
# MAGIC (
# MAGIC   sk_deputado BIGINT COMMENT 'Chave substituta do deputado, gerada a partir do ID natural.',
# MAGIC   id BIGINT COMMENT 'Identificador único do deputado na API da Câmara.',
# MAGIC   nome STRING COMMENT 'Nome completo do deputado.',
# MAGIC   sigla_partido STRING COMMENT 'Sigla do partido político do deputado.',
# MAGIC   sigla_uf STRING COMMENT 'Sigla da Unidade Federativa (estado) que o deputado representa.',
# MAGIC   id_legislatura BIGINT COMMENT 'Identificador da legislatura.',
# MAGIC   url_foto STRING COMMENT 'URL da foto oficial do deputado.',
# MAGIC   email STRING COMMENT 'E-mail de contato do deputado.',
# MAGIC   CONSTRAINT not_null_sk_deputado EXPECT (sk_deputado IS NOT NULL) ON VIOLATION DROP ROW
# MAGIC )
# MAGIC COMMENT 'Dimensão de deputados federais, contendo informações cadastrais e de mandato.'
# MAGIC TBLPROPERTIES (
# MAGIC   "quality" = "gold",
# MAGIC   pipelines.autoOptimize.managed = true,
# MAGIC   delta.autoOptimize.optimizeWrite = true,
# MAGIC   delta.autoOptimize.autoCompact = true
# MAGIC )
# MAGIC CLUSTER BY AUTO
# MAGIC AS
# MAGIC SELECT DISTINCT
# MAGIC   xxhash64(CAST(cd.id AS STRING)) AS sk_deputado,
# MAGIC   cd.id,
# MAGIC   cd.nome,
# MAGIC   cd.sigla_partido,
# MAGIC   cd.sigla_uf,
# MAGIC   cd.id_legislatura,
# MAGIC   cd.url_foto,
# MAGIC   cd.email
# MAGIC FROM ${silver_catalog}.ds_camara_deputados.cleaned_deputados cd
# MAGIC WHERE cd.id IS NOT NULL;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dimensão: Partidos

# COMMAND ----------

# DBTITLE 1,dim_partido
# MAGIC %sql
# MAGIC CREATE OR REFRESH MATERIALIZED VIEW dim_partido
# MAGIC (
# MAGIC   id BIGINT COMMENT 'Identificador único do partido político.',
# MAGIC   sigla STRING COMMENT 'Sigla oficial do partido (ex: PT, PSDB, MDB).',
# MAGIC   nome STRING COMMENT 'Nome completo do partido político.',
# MAGIC   uri STRING COMMENT 'URI da API para consulta detalhada do partido.',
# MAGIC   status STRING COMMENT 'Status atual do partido (ativo, extinto, etc.).',
# MAGIC   CONSTRAINT not_null_id_partido EXPECT (id IS NOT NULL) ON VIOLATION DROP ROW
# MAGIC )
# MAGIC COMMENT 'Dimensão de partidos políticos brasileiros, utilizada para análises de bancadas e coligações.'
# MAGIC TBLPROPERTIES (
# MAGIC   "quality" = "gold",
# MAGIC   pipelines.autoOptimize.managed = true,
# MAGIC   delta.autoOptimize.optimizeWrite = true,
# MAGIC   delta.autoOptimize.autoCompact = true
# MAGIC )
# MAGIC CLUSTER BY AUTO
# MAGIC AS
# MAGIC SELECT DISTINCT
# MAGIC   cp.id,
# MAGIC   cp.sigla,
# MAGIC   cp.nome,
# MAGIC   cp.uri,
# MAGIC   cp.status
# MAGIC FROM ${silver_catalog}.ds_camara_deputados.cleaned_partidos cp
# MAGIC WHERE cp.id IS NOT NULL;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dimensão: Tempo

# COMMAND ----------

# DBTITLE 1,dim_tempo
# MAGIC %sql
# MAGIC CREATE OR REFRESH MATERIALIZED VIEW dim_tempo
# MAGIC (
# MAGIC   sk_data INT COMMENT 'Chave substituta da data no formato YYYYMMDD.',
# MAGIC   data DATE COMMENT 'Data completa.',
# MAGIC   ano INT COMMENT 'Ano (ex: 2026).',
# MAGIC   mes INT COMMENT 'Mês (1-12).',
# MAGIC   trimestre INT COMMENT 'Trimestre (1-4).',
# MAGIC   semestre INT COMMENT 'Semestre (1-2).',
# MAGIC   nome_mes STRING COMMENT 'Nome do mês por extenso (ex: Janeiro, Fevereiro).',
# MAGIC   dia_semana INT COMMENT 'Dia da semana (1=Domingo, 7=Sábado).',
# MAGIC   nome_dia_semana STRING COMMENT 'Nome do dia da semana (ex: Segunda-feira, Terça-feira).',
# MAGIC   eh_fim_semana BOOLEAN COMMENT 'Indicador se é fim de semana (Sábado ou Domingo).',
# MAGIC   CONSTRAINT not_null_sk_data EXPECT (sk_data IS NOT NULL) ON VIOLATION DROP ROW
# MAGIC )
# MAGIC COMMENT 'Dimensão de calendário temporal para análises de séries temporais e sazonalidade.'
# MAGIC TBLPROPERTIES (
# MAGIC   "quality" = "gold",
# MAGIC   pipelines.autoOptimize.managed = true,
# MAGIC   delta.autoOptimize.optimizeWrite = true,
# MAGIC   delta.autoOptimize.autoCompact = true
# MAGIC )
# MAGIC CLUSTER BY AUTO
# MAGIC AS
# MAGIC WITH datas_unicas AS (
# MAGIC   SELECT DISTINCT data_documento AS data
# MAGIC   FROM ${silver_catalog}.ds_camara_deputados.cleaned_despesas
# MAGIC   WHERE data_documento IS NOT NULL
# MAGIC   
# MAGIC   UNION
# MAGIC   
# MAGIC   SELECT DISTINCT data
# MAGIC   FROM ${silver_catalog}.ds_camara_deputados.cleaned_votacoes
# MAGIC   WHERE data IS NOT NULL
# MAGIC )
# MAGIC SELECT
# MAGIC   CAST(DATE_FORMAT(data, 'yyyyMMdd') AS INT) AS sk_data,
# MAGIC   data,
# MAGIC   YEAR(data) AS ano,
# MAGIC   MONTH(data) AS mes,
# MAGIC   QUARTER(data) AS trimestre,
# MAGIC   CASE WHEN MONTH(data) <= 6 THEN 1 ELSE 2 END AS semestre,
# MAGIC   CASE MONTH(data)
# MAGIC     WHEN 1 THEN 'Janeiro'
# MAGIC     WHEN 2 THEN 'Fevereiro'
# MAGIC     WHEN 3 THEN 'Março'
# MAGIC     WHEN 4 THEN 'Abril'
# MAGIC     WHEN 5 THEN 'Maio'
# MAGIC     WHEN 6 THEN 'Junho'
# MAGIC     WHEN 7 THEN 'Julho'
# MAGIC     WHEN 8 THEN 'Agosto'
# MAGIC     WHEN 9 THEN 'Setembro'
# MAGIC     WHEN 10 THEN 'Outubro'
# MAGIC     WHEN 11 THEN 'Novembro'
# MAGIC     WHEN 12 THEN 'Dezembro'
# MAGIC   END AS nome_mes,
# MAGIC   DAYOFWEEK(data) AS dia_semana,
# MAGIC   CASE DAYOFWEEK(data)
# MAGIC     WHEN 1 THEN 'Domingo'
# MAGIC     WHEN 2 THEN 'Segunda-feira'
# MAGIC     WHEN 3 THEN 'Terça-feira'
# MAGIC     WHEN 4 THEN 'Quarta-feira'
# MAGIC     WHEN 5 THEN 'Quinta-feira'
# MAGIC     WHEN 6 THEN 'Sexta-feira'
# MAGIC     WHEN 7 THEN 'Sábado'
# MAGIC   END AS nome_dia_semana,
# MAGIC   CASE WHEN DAYOFWEEK(data) IN (1, 7) THEN true ELSE false END AS eh_fim_semana
# MAGIC FROM datas_unicas
# MAGIC WHERE data IS NOT NULL;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dimensão: Tipos de Despesa

# COMMAND ----------

# DBTITLE 1,dim_tipo_despesa
# MAGIC %sql
# MAGIC CREATE OR REFRESH MATERIALIZED VIEW dim_tipo_despesa
# MAGIC (
# MAGIC   sk_tipo_despesa BIGINT COMMENT 'Chave substituta do tipo de despesa.',
# MAGIC   tipo_despesa STRING COMMENT 'Descrição do tipo de despesa (ex: Combustíveis, Passagens Aéreas, Telefonia).',
# MAGIC   cod_tipo_documento BIGINT COMMENT 'Código do tipo de documento.',
# MAGIC   tipo_documento STRING COMMENT 'Descrição do tipo de documento fiscal.',
# MAGIC   CONSTRAINT not_null_sk_tipo_despesa EXPECT (sk_tipo_despesa IS NOT NULL) ON VIOLATION DROP ROW
# MAGIC )
# MAGIC COMMENT 'Dimensão de tipos de despesas parlamentares da cota para custeio de atividades.'
# MAGIC TBLPROPERTIES (
# MAGIC   "quality" = "gold",
# MAGIC   pipelines.autoOptimize.managed = true,
# MAGIC   delta.autoOptimize.optimizeWrite = true,
# MAGIC   delta.autoOptimize.autoCompact = true
# MAGIC )
# MAGIC CLUSTER BY AUTO
# MAGIC AS
# MAGIC SELECT DISTINCT
# MAGIC   xxhash64(
# MAGIC     COALESCE(CAST(cd.tipo_despesa AS STRING), ''),
# MAGIC     COALESCE(CAST(cd.cod_tipo_documento AS STRING), '')
# MAGIC   ) AS sk_tipo_despesa,
# MAGIC   cd.tipo_despesa,
# MAGIC   cd.cod_tipo_documento,
# MAGIC   cd.tipo_documento
# MAGIC FROM ${silver_catalog}.ds_camara_deputados.cleaned_despesas cd
# MAGIC WHERE cd.tipo_despesa IS NOT NULL;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dimensão: Fornecedores

# COMMAND ----------

# DBTITLE 1,dim_fornecedor
# MAGIC %sql
# MAGIC CREATE OR REFRESH MATERIALIZED VIEW dim_fornecedor
# MAGIC (
# MAGIC   sk_fornecedor BIGINT COMMENT 'Chave substituta do fornecedor.',
# MAGIC   cnpj_cpf STRING COMMENT 'CNPJ ou CPF do fornecedor.',
# MAGIC   nome_fornecedor STRING COMMENT 'Razão social ou nome do fornecedor.',
# MAGIC   CONSTRAINT not_null_sk_fornecedor EXPECT (sk_fornecedor IS NOT NULL) ON VIOLATION DROP ROW
# MAGIC )
# MAGIC COMMENT 'Dimensão de fornecedores que prestam serviços ou vendem produtos aos deputados.'
# MAGIC TBLPROPERTIES (
# MAGIC   "quality" = "gold",
# MAGIC   pipelines.autoOptimize.managed = true,
# MAGIC   delta.autoOptimize.optimizeWrite = true,
# MAGIC   delta.autoOptimize.autoCompact = true
# MAGIC )
# MAGIC CLUSTER BY AUTO
# MAGIC AS
# MAGIC SELECT DISTINCT
# MAGIC   xxhash64(COALESCE(cd.cnpj_cpf_fornecedor, 'SEM_CNPJ')) AS sk_fornecedor,
# MAGIC   cd.cnpj_cpf_fornecedor AS cnpj_cpf,
# MAGIC   cd.nome_fornecedor
# MAGIC FROM ${silver_catalog}.ds_camara_deputados.cleaned_despesas cd
# MAGIC WHERE cd.nome_fornecedor IS NOT NULL;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dimensão: Órgãos Legislativos

# COMMAND ----------

# DBTITLE 1,dim_orgao
# MAGIC %sql
# MAGIC CREATE OR REFRESH MATERIALIZED VIEW dim_orgao
# MAGIC (
# MAGIC   sk_orgao BIGINT COMMENT 'Chave substituta do órgão legislativo.',
# MAGIC   id_orgao STRING COMMENT 'Identificador do órgão (Plenário ou Comissão).',
# MAGIC   sigla_orgao STRING COMMENT 'Sigla do órgão legislativo.',
# MAGIC   uri_orgao STRING COMMENT 'URI da API para consulta detalhada do órgão.',
# MAGIC   CONSTRAINT not_null_sk_orgao EXPECT (sk_orgao IS NOT NULL) ON VIOLATION DROP ROW
# MAGIC )
# MAGIC COMMENT 'Dimensão de órgãos legislativos onde ocorrem votações (Plenário, Comissões temáticas, etc.).'
# MAGIC TBLPROPERTIES (
# MAGIC   "quality" = "gold",
# MAGIC   pipelines.autoOptimize.managed = true,
# MAGIC   delta.autoOptimize.optimizeWrite = true,
# MAGIC   delta.autoOptimize.autoCompact = true
# MAGIC )
# MAGIC CLUSTER BY AUTO
# MAGIC AS
# MAGIC SELECT DISTINCT
# MAGIC   xxhash64(COALESCE(cv.id_orgao, 'DESCONHECIDO')) AS sk_orgao,
# MAGIC   cv.id_orgao,
# MAGIC   cv.sigla_orgao,
# MAGIC   cv.uri_orgao
# MAGIC FROM ${silver_catalog}.ds_camara_deputados.cleaned_votacoes cv
# MAGIC WHERE cv.id_orgao IS NOT NULL;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fato: Despesas Parlamentares

# COMMAND ----------

# DBTITLE 1,fact_despesas
# MAGIC %sql
# MAGIC CREATE OR REFRESH MATERIALIZED VIEW fact_despesas
# MAGIC (
# MAGIC   sk_deputado BIGINT COMMENT 'Chave estrangeira para dim_deputado.',
# MAGIC   sk_data INT COMMENT 'Chave estrangeira para dim_tempo.',
# MAGIC   sk_tipo_despesa BIGINT COMMENT 'Chave estrangeira para dim_tipo_despesa.',
# MAGIC   sk_fornecedor BIGINT COMMENT 'Chave estrangeira para dim_fornecedor.',
# MAGIC   cod_documento STRING COMMENT 'Código único do documento fiscal (chave natural).',
# MAGIC   num_documento STRING COMMENT 'Número do documento fiscal.',
# MAGIC   valor_documento DECIMAL(15,2) COMMENT 'Valor bruto do documento em reais.',
# MAGIC   valor_liquido DECIMAL(15,2) COMMENT 'Valor líquido pago ao fornecedor em reais.',
# MAGIC   valor_glosa DECIMAL(15,2) COMMENT 'Valor glosado (não reembolsado) em reais.',
# MAGIC   num_ressarcimento STRING COMMENT 'Número do ressarcimento.',
# MAGIC   url_documento STRING COMMENT 'URL para acesso ao documento fiscal digitalizado.',
# MAGIC   cod_lote BIGINT COMMENT 'Código do lote de processamento.',
# MAGIC   parcela BIGINT COMMENT 'Número da parcela (para despesas parceladas).',
# MAGIC   CONSTRAINT not_null_cod_documento EXPECT (cod_documento IS NOT NULL) ON VIOLATION DROP ROW
# MAGIC )
# MAGIC COMMENT 'Tabela fato de despesas da cota parlamentar, contendo os gastos dos deputados federais.'
# MAGIC TBLPROPERTIES (
# MAGIC   "quality" = "gold",
# MAGIC   pipelines.autoOptimize.managed = true,
# MAGIC   delta.autoOptimize.optimizeWrite = true,
# MAGIC   delta.autoOptimize.autoCompact = true
# MAGIC )
# MAGIC CLUSTER BY AUTO
# MAGIC AS
# MAGIC SELECT
# MAGIC   xxhash64(CAST(cd.id_deputado AS STRING)) AS sk_deputado,
# MAGIC   CAST(DATE_FORMAT(cd.data_documento, 'yyyyMMdd') AS INT) AS sk_data,
# MAGIC   xxhash64(
# MAGIC     COALESCE(CAST(cd.tipo_despesa AS STRING), ''),
# MAGIC     COALESCE(CAST(cd.cod_tipo_documento AS STRING), '')
# MAGIC   ) AS sk_tipo_despesa,
# MAGIC   xxhash64(COALESCE(cd.cnpj_cpf_fornecedor, 'SEM_CNPJ')) AS sk_fornecedor,
# MAGIC   cd.cod_documento,
# MAGIC   cd.num_documento,
# MAGIC   cd.valor_documento,
# MAGIC   cd.valor_liquido,
# MAGIC   cd.valor_glosa,
# MAGIC   cd.num_ressarcimento,
# MAGIC   cd.url_documento,
# MAGIC   cd.cod_lote,
# MAGIC   cd.parcela
# MAGIC FROM ${silver_catalog}.ds_camara_deputados.cleaned_despesas cd
# MAGIC WHERE cd.cod_documento IS NOT NULL
# MAGIC   AND cd.id_deputado IS NOT NULL
# MAGIC   AND cd.data_documento IS NOT NULL;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fato: Votações

# COMMAND ----------

# DBTITLE 1,fact_votacoes
# MAGIC %sql
# MAGIC CREATE OR REFRESH MATERIALIZED VIEW fact_votacoes
# MAGIC (
# MAGIC   sk_votacao BIGINT COMMENT 'Chave substituta da votação (hash do ID original).',
# MAGIC   sk_orgao BIGINT COMMENT 'Chave estrangeira para dim_orgao.',
# MAGIC   sk_data INT COMMENT 'Chave estrangeira para dim_tempo.',
# MAGIC   data_hora_registro TIMESTAMP COMMENT 'Data e hora completa do registro da votação.',
# MAGIC   proposicao_cod_tipo STRING COMMENT 'Código do tipo da proposição votada.',
# MAGIC   proposicao_numero STRING COMMENT 'Número da proposição votada.',
# MAGIC   proposicao_ano STRING COMMENT 'Ano da proposição votada.',
# MAGIC   uri_proposicao STRING COMMENT 'URI da API para consulta da proposição.',
# MAGIC   aprovacao INT COMMENT 'Resultado da votação (1=Aprovado, 0=Rejeitado).',
# MAGIC   objeto_votacao STRING COMMENT 'Descrição do objeto/ementa da votação.',
# MAGIC   CONSTRAINT not_null_sk_votacao EXPECT (sk_votacao IS NOT NULL) ON VIOLATION DROP ROW
# MAGIC )
# MAGIC COMMENT 'Tabela fato de votações realizadas em Plenário e Comissões da Câmara dos Deputados.'
# MAGIC TBLPROPERTIES (
# MAGIC   "quality" = "gold",
# MAGIC   pipelines.autoOptimize.managed = true,
# MAGIC   delta.autoOptimize.optimizeWrite = true,
# MAGIC   delta.autoOptimize.autoCompact = true
# MAGIC )
# MAGIC CLUSTER BY AUTO
# MAGIC AS
# MAGIC SELECT
# MAGIC   xxhash64(CAST(cv.id AS STRING)) AS sk_votacao,
# MAGIC   xxhash64(COALESCE(cv.id_orgao, 'DESCONHECIDO')) AS sk_orgao,
# MAGIC   CAST(DATE_FORMAT(cv.data, 'yyyyMMdd') AS INT) AS sk_data,
# MAGIC   cv.data_hora_registro,
# MAGIC   cv.proposicao_cod_tipo,
# MAGIC   cv.proposicao_numero,
# MAGIC   cv.proposicao_ano,
# MAGIC   cv.uri_proposicao_votada AS uri_proposicao,
# MAGIC   cv.aprovacao,
# MAGIC   cv.objeto_votacao
# MAGIC FROM ${silver_catalog}.ds_camara_deputados.cleaned_votacoes cv
# MAGIC WHERE cv.id IS NOT NULL
# MAGIC   AND cv.data IS NOT NULL;