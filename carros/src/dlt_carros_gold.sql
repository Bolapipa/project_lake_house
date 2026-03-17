-- Databricks notebook source
-- MAGIC %md
-- MAGIC ABAIXO CÓDIGOS DE TRANSFORMAÇÃO: SILVER -> GOLD

-- COMMAND ----------

-- DBTITLE 1,dim_referencia
CREATE OR REFRESH MATERIALIZED VIEW gold_dev.ds_carros.dim_referencia
(
  id_referencia STRING COMMENT 'Identificador incremental da referência FIPE.',
  ano_mes_referencia STRING COMMENT 'Ano e mês da referência no formato ano_mes, ex: 2026_03.',
  mes_referencia INT COMMENT 'Número do mês da referência FIPE.',
  ano_referencia INT COMMENT 'Ano da referência FIPE.',
  CONSTRAINT not_null_id_referencia EXPECT (id_referencia IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT 'Dimensão de referências FIPE, utilizada para organizar e analisar os preços dos veículos ao longo do tempo.'
TBLPROPERTIES (
  "quality" = "gold",
  pipelines.autoOptimize.managed = true,
  delta.autoOptimize.optimizeWrite = true,
  delta.autoOptimize.autoCompact = true
)
CLUSTER BY AUTO
AS
SELECT
  cr.id_referencia,
  cr.ano_mes_referencia,
  cr.mes_referencia,
  cr.ano_referencia
FROM silver_dev.ds_carros.cleaned_referencias cr;

-- COMMAND ----------

-- DBTITLE 1,dim_marca
CREATE OR REFRESH MATERIALIZED VIEW gold_dev.ds_carros.dim_marca
(
  id_marca STRING COMMENT 'Identificador da marca do veículo.',
  nome_marca STRING COMMENT 'Nome da marca do veículo, como Fiat, Volkswagen, Chevrolet e outras.',
  CONSTRAINT not_null_id_marca EXPECT (id_marca IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT 'Dimensão de marcas de veículos, utilizada para padronizar e organizar os fabricantes presentes na base FIPE.'
TBLPROPERTIES (
  "quality" = "gold",
  pipelines.autoOptimize.managed = true,
  delta.autoOptimize.optimizeWrite = true,
  delta.autoOptimize.autoCompact = true
)
CLUSTER BY AUTO
AS
SELECT DISTINCT
  cm.id_marca,
  cm.nome_marca
FROM silver_dev.ds_carros.cleaned_marcas cm;

-- COMMAND ----------

-- DBTITLE 1,dim_modelo
CREATE OR REFRESH MATERIALIZED VIEW gold_dev.ds_carros.dim_modelo
(
  id_marca STRING COMMENT 'Identificador da marca à qual o modelo pertence.',
  id_modelo STRING COMMENT 'Identificador do modelo do veículo.',
  nome_modelo STRING COMMENT 'Nome do modelo do veículo, como Gol, Palio, Civic e outros.',
  CONSTRAINT not_null_id_modelo EXPECT (id_modelo IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT 'Dimensão de modelos de veículos por marca, utilizada para organizar os modelos disponíveis na base FIPE.'
TBLPROPERTIES (
  "quality" = "gold",
  pipelines.autoOptimize.managed = true,
  delta.autoOptimize.optimizeWrite = true,
  delta.autoOptimize.autoCompact = true
)
CLUSTER BY AUTO
AS
SELECT DISTINCT
  cmo.id_marca,
  cmo.id_modelo,
  cmo.nome_modelo
FROM silver_dev.ds_carros.cleaned_modelos cmo;

-- COMMAND ----------

-- DBTITLE 1,dim_veiculo
CREATE OR REFRESH MATERIALIZED VIEW gold_dev.ds_carros.dim_veiculo
(
  sk_veiculo BIGINT COMMENT 'Chave substituta do veículo, gerada a partir da combinação de marca, modelo, ano, combustível e código FIPE.',
  id_marca STRING COMMENT 'Identificador da marca do veículo.',
  nome_marca STRING COMMENT 'Nome da marca do veículo.',
  id_modelo STRING COMMENT 'Identificador do modelo do veículo.',
  nome_modelo STRING COMMENT 'Nome do modelo do veículo.',
  modelo_fipe STRING COMMENT 'Descrição completa do modelo conforme retornado pela FIPE.',
  ano_modelo INT COMMENT 'Ano do modelo do veículo.',
  combustivel STRING COMMENT 'Tipo de combustível do veículo, como gasolina, álcool, diesel, flex ou elétrico.',
  codigo_fipe STRING COMMENT 'Código FIPE do veículo, utilizado para identificar de forma padronizada uma versão específica.',
  id_ano STRING COMMENT 'Identificador do ano retornado na estrutura da API.',
  CONSTRAINT not_null_sk_veiculo EXPECT (sk_veiculo IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT 'Dimensão de veículos FIPE, contendo a identificação analítica de cada veículo por marca, modelo, ano, combustível e código FIPE.'
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
    CAST(cf.id_marca AS STRING),
    CAST(cf.id_modelo AS STRING),
    CAST(cf.ano_modelo AS STRING),
    COALESCE(CAST(cf.combustivel AS STRING), ''),
    COALESCE(CAST(cf.codigo_fipe AS STRING), '')
  ) AS sk_veiculo,
  cf.id_marca,
  cm.nome_marca,
  cf.id_modelo,
  cf.nome_modelo,
  cf.modelo_fipe,
  cf.ano_modelo,
  cf.combustivel,
  cf.codigo_fipe,
  cf.id_ano
FROM silver_dev.ds_carros.cleaned_fipe cf
LEFT JOIN silver_dev.ds_carros.cleaned_marcas cm
  ON cf.id_marca = cm.id_marca;

-- COMMAND ----------

-- DBTITLE 1,fact_preco_fipe
CREATE OR REFRESH MATERIALIZED VIEW gold_dev.ds_carros.fact_preco_fipe
(
  sk_veiculo BIGINT COMMENT 'Chave substituta do veículo, utilizada para relacionar a fato com a dimensão de veículos.',
  id_referencia STRING COMMENT 'Identificador da referência FIPE utilizada na cotação do veículo.',
  valor_fipe DECIMAL(12,2) COMMENT 'Valor do veículo na tabela FIPE para a referência informada.',
  CONSTRAINT not_null_id_referencia EXPECT (id_referencia IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT 'Tabela fato de preços FIPE, contendo o valor de cada veículo em cada referência temporal da FIPE.'
TBLPROPERTIES (
  "quality" = "gold",
  pipelines.autoOptimize.managed = true,
  delta.autoOptimize.optimizeWrite = true,
  delta.autoOptimize.autoCompact = true
)
CLUSTER BY AUTO
AS
SELECT
  xxhash64(
    CAST(cf.id_marca AS STRING),
    CAST(cf.id_modelo AS STRING),
    CAST(cf.ano_modelo AS STRING),
    COALESCE(CAST(cf.combustivel AS STRING), ''),
    COALESCE(CAST(cf.codigo_fipe AS STRING), '')
  ) AS sk_veiculo,
  cf.id_referencia,
  CAST(
    REPLACE(
      REPLACE(
        REGEXP_REPLACE(cf.valor, 'R\\$\\s*', ''),
        '.',
        ''
      ),
      ',',
      '.'
    ) AS DECIMAL(12,2)
  ) AS valor_fipe
FROM silver_dev.ds_carros.cleaned_fipe cf
WHERE cf.valor IS NOT NULL;
