-- Databricks notebook source
-- DBTITLE 1,Bronze para Silver: marcas e modelos
CREATE OR REFRESH MATERIALIZED VIEW silver_dev.ds_carros.cleaned_marcas
(
  id_marca STRING COMMENT 'Identificador único da marca de veículo.',
  nome_marca STRING COMMENT 'Nome limpo e padronizado da marca.'
)
COMMENT "Tabela padronizada de marcas de veículos, pronta para análises e relatórios."
TBLPROPERTIES(
  delta.autoOptimize.optimizeWrite = true,
  delta.autoOptimize.autoCompact = true,
  pipeline.autoOptimize.managed = true,
  delta.enableRowTracking = true,
  quality = 'silver'
)
CLUSTER BY AUTO
AS
SELECT
  CAST(id_marca AS STRING) AS id_marca,
  trim(nome_marca) AS nome_marca
FROM bronze_dev.ds_carros.raw_marcas;

-- COMMAND ----------

-- DBTITLE 1,Bronze para Silver: modelos
CREATE OR REFRESH MATERIALIZED VIEW silver_dev.ds_carros.cleaned_modelos
(
  id_marca STRING COMMENT 'Identificador único da marca relacionada ao modelo.',
  nome_marca STRING COMMENT 'Nome limpo e padronizado da marca.',
  id_modelo STRING COMMENT 'Identificador único do modelo de veículo.',
  nome_modelo STRING COMMENT 'Nome limpo e padronizado do modelo.'
)
COMMENT "Tabela padronizada de modelos de veículos, pronta para análises e relatórios."
TBLPROPERTIES(
  delta.autoOptimize.optimizeWrite = true,
  delta.autoOptimize.autoCompact = true,
  pipeline.autoOptimize.managed = true,
  delta.enableRowTracking = true,
  quality = 'silver'
)
CLUSTER BY AUTO
AS
SELECT
  CAST(id_marca AS STRING) AS id_marca,
  trim(nome_marca) AS nome_marca,
  CAST(id_modelo AS STRING) AS id_modelo,
  trim(nome_modelo) AS nome_modelo
FROM bronze_dev.ds_carros.raw_modelos;

-- COMMAND ----------

-- DBTITLE 1,Bronze para Silver: anos de veículos
CREATE OR REFRESH MATERIALIZED VIEW silver_dev.ds_carros.cleaned_anos
(
  id_marca STRING COMMENT 'Identificador único da marca de veículo.',
  nome_marca STRING COMMENT 'Nome limpo e padronizado da marca.',
  id_modelo STRING COMMENT 'Identificador único do modelo de veículo.',
  nome_modelo STRING COMMENT 'Nome limpo e padronizado do modelo.',
  ano_carro INT COMMENT 'Ano do carro (extraído do nome_ano).',
  combustivel STRING COMMENT 'Tipo de combustível do carro (extraído do nome_ano).'
)
COMMENT "Tabela padronizada de anos de veículos, separando ano e combustível para análises e relatórios."
TBLPROPERTIES(
  delta.autoOptimize.optimizeWrite = true,
  delta.autoOptimize.autoCompact = true,
  pipeline.autoOptimize.managed = true,
  delta.enableRowTracking = true,
  quality = 'silver'
)
CLUSTER BY AUTO
AS
SELECT
  CAST(id_marca AS STRING) AS id_marca,
  trim(nome_marca) AS nome_marca,
  CAST(id_modelo AS STRING) AS id_modelo,
  trim(nome_modelo) AS nome_modelo,
  CAST(split(trim(nome_ano), ' ')[0] AS INT) AS ano_carro,
  trim(substring(trim(nome_ano), length(split(trim(nome_ano), ' ')[0]) + 2)) AS combustivel
FROM bronze_dev.ds_carros.raw_anos;

-- COMMAND ----------

-- DBTITLE 1,Bronze para Silver: referencias
CREATE OR REFRESH MATERIALIZED VIEW silver_dev.ds_carros.cleaned_referencias
(
  id_referencia STRING COMMENT 'Identificador incremental da referência FIPE.',
  ano_mes_referencia STRING COMMENT 'Ano e mês de referência no formato ano_mes, ex: 2026_03.',
  mes_referencia INT COMMENT 'Número do mês referente à consulta.',
  ano_referencia INT COMMENT 'Ano referente à consulta.'
)
COMMENT 'Tabela padronizada de referências FIPE, com extrações e normalizações para análises.'
TBLPROPERTIES(
  delta.autoOptimize.optimizeWrite = true,
  delta.autoOptimize.autoCompact = true,
  pipeline.autoOptimize.managed = true,
  delta.enableRowTracking = true,
  quality = 'silver'
)
CLUSTER BY AUTO
AS
SELECT
  id_referencia,

  -- agora gera no padrão 2026_03
  concat(
    split(mes_referencia, '/')[1],
    '_',
    lpad(
      CAST(
        CASE lower(split(mes_referencia, '/')[0])
          WHEN 'janeiro' THEN 1
          WHEN 'fevereiro' THEN 2
          WHEN 'março' THEN 3
          WHEN 'marco' THEN 3
          WHEN 'abril' THEN 4
          WHEN 'maio' THEN 5
          WHEN 'junho' THEN 6
          WHEN 'julho' THEN 7
          WHEN 'agosto' THEN 8
          WHEN 'setembro' THEN 9
          WHEN 'outubro' THEN 10
          WHEN 'novembro' THEN 11
          WHEN 'dezembro' THEN 12
        END AS STRING
      ),
      2,
      '0'
    )
  ) AS ano_mes_referencia,

  -- converte o nome do mês em número
  CASE lower(split(mes_referencia, '/')[0])
    WHEN 'janeiro' THEN 1
    WHEN 'fevereiro' THEN 2
    WHEN 'março' THEN 3
    WHEN 'marco' THEN 3
    WHEN 'abril' THEN 4
    WHEN 'maio' THEN 5
    WHEN 'junho' THEN 6
    WHEN 'julho' THEN 7
    WHEN 'agosto' THEN 8
    WHEN 'setembro' THEN 9
    WHEN 'outubro' THEN 10
    WHEN 'novembro' THEN 11
    WHEN 'dezembro' THEN 12
  END AS mes_referencia,

  -- ano agora vem da posição correta
  CAST(split(mes_referencia, '/')[1] AS INT) AS ano_referencia

FROM bronze_dev.ds_carros.raw_referencias;

-- COMMAND ----------

-- DBTITLE 1,etl fipe
CREATE OR REPLACE MATERIALIZED VIEW silver_dev.ds_carros.cleaned_fipe
(
  id_referencia STRING COMMENT 'Identificador da referência FIPE utilizada na consulta.',
  id_marca STRING COMMENT 'Identificador da marca do veículo.',
  id_modelo STRING COMMENT 'Identificador do modelo do veículo.',
  nome_modelo STRING COMMENT 'Nome do modelo do veículo.',
  id_ano STRING COMMENT 'Identificador do ano/combustível retornado pela origem, ex: 2020-1.',
  valor STRING COMMENT 'Valor FIPE bruto em texto, ex: R$ 62.540,00.',
  modelo_fipe STRING COMMENT 'Nome padronizado do modelo retornado pela API FIPE.',
  ano_modelo INT COMMENT 'Ano do modelo retornado pela API.',
  combustivel STRING COMMENT 'Tipo de combustível do veículo.',
  codigo_fipe STRING COMMENT 'Código FIPE do veículo.'
)
COMMENT 'Tabela silver da FIPE com padronização e conversão do valor para uso analítico.'
TBLPROPERTIES(
  delta.autoOptimize.optimizeWrite = true,
  delta.autoOptimize.autoCompact = true,
  pipeline.autoOptimize.managed = true,
  delta.enableRowTracking = true,
  quality = 'silver'
)
CLUSTER BY AUTO
AS
SELECT
  CAST(id_referencia AS STRING) AS id_referencia,
  CAST(id_marca AS STRING) AS id_marca,
  CAST(id_modelo AS STRING) AS id_modelo,
  trim(nome_modelo) AS nome_modelo,
  id_ano,
  valor,
  modelo_fipe,
  TRY_CAST(ano_modelo AS INT) AS ano_modelo,
  combustivel,
  codigo_fipe
FROM bronze_dev.ds_carros.raw_fipe;
