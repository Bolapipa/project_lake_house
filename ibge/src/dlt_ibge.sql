-- Databricks notebook source
-- DBTITLE 1,Bronze para Silver: regiões
CREATE OR REFRESH MATERIALIZED VIEW cleaned_regioes
(
  id_regiao BIGINT COMMENT 'Identificador único da região brasileira.',
  nome_regiao STRING COMMENT 'Nome completo da região.',
  sigla_regiao STRING COMMENT 'Sigla oficial da região (N, NE, SE, S, CO).'
)
COMMENT "Tabela padronizada de regiões do Brasil (5 regiões), pronta para análises e relatórios."
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
  CAST(id AS BIGINT) AS id_regiao,
  trim(nome) AS nome_regiao,
  trim(sigla) AS sigla_regiao
FROM bronze_dev.ds_ibge.raw_regioes;

-- COMMAND ----------

-- DBTITLE 1,Bronze para Silver: estados
CREATE OR REFRESH MATERIALIZED VIEW cleaned_estados
(
  id_estado BIGINT COMMENT 'Identificador único da UF (Unidade Federativa).',
  nome_estado STRING COMMENT 'Nome completo do estado.',
  sigla_estado STRING COMMENT 'Sigla oficial da UF (ex: SP, RJ, MG).',
  id_regiao BIGINT COMMENT 'Identificador da região à qual o estado pertence.',
  nome_regiao STRING COMMENT 'Nome da região.',
  sigla_regiao STRING COMMENT 'Sigla da região.'
)
COMMENT "Tabela padronizada de estados brasileiros (27 UFs), pronta para análises e relatórios."
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
  CAST(id AS BIGINT) AS id_estado,
  trim(nome) AS nome_estado,
  trim(sigla) AS sigla_estado,
  CAST(regiao['id'] AS BIGINT) AS id_regiao,
  trim(regiao['nome']) AS nome_regiao,
  trim(regiao['sigla']) AS sigla_regiao
FROM bronze_dev.ds_ibge.raw_ufs;

-- COMMAND ----------

-- DBTITLE 1,Bronze para Silver: mesorregiões
CREATE OR REFRESH MATERIALIZED VIEW cleaned_mesorregioes
(
  id_mesorregiao BIGINT COMMENT 'Identificador único da mesorregião.',
  nome_mesorregiao STRING COMMENT 'Nome completo da mesorregião.',
  id_estado BIGINT COMMENT 'Identificador do estado ao qual pertence.',
  nome_estado STRING COMMENT 'Nome do estado.',
  sigla_estado STRING COMMENT 'Sigla da UF.'
)
COMMENT "Tabela padronizada de mesorregiões do Brasil, pronta para análises e relatórios."
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
  CAST(id AS BIGINT) AS id_mesorregiao,
  trim(nome) AS nome_mesorregiao,
  CAST(from_json(UF, 'id BIGINT').id AS BIGINT) AS id_estado,
  trim(from_json(UF, 'nome STRING').nome) AS nome_estado,
  trim(from_json(UF, 'sigla STRING').sigla) AS sigla_estado
FROM bronze_dev.ds_ibge.raw_mesorregioes;

-- COMMAND ----------

-- DBTITLE 1,Bronze para Silver: microrregiões
CREATE OR REFRESH MATERIALIZED VIEW cleaned_microrregioes
(
  id_microrregiao BIGINT COMMENT 'Identificador único da microrregião.',
  nome_microrregiao STRING COMMENT 'Nome completo da microrregião.',
  id_mesorregiao BIGINT COMMENT 'Identificador da mesorregião à qual pertence.',
  nome_mesorregiao STRING COMMENT 'Nome da mesorregião.',
  id_estado BIGINT COMMENT 'Identificador do estado.',
  nome_estado STRING COMMENT 'Nome do estado.',
  sigla_estado STRING COMMENT 'Sigla da UF.'
)
COMMENT "Tabela padronizada de microrregiões do Brasil, pronta para análises e relatórios."
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
  CAST(id AS BIGINT) AS id_microrregiao,
  trim(nome) AS nome_microrregiao,
  CAST(from_json(mesorregiao, 'id BIGINT').id AS BIGINT) AS id_mesorregiao,
  trim(from_json(mesorregiao, 'nome STRING').nome) AS nome_mesorregiao,
  CAST(from_json(mesorregiao, 'UF STRUCT<id:BIGINT>').UF.id AS BIGINT) AS id_estado,
  trim(from_json(mesorregiao, 'UF STRUCT<nome:STRING>').UF.nome) AS nome_estado,
  trim(from_json(mesorregiao, 'UF STRUCT<sigla:STRING>').UF.sigla) AS sigla_estado
FROM bronze_dev.ds_ibge.raw_microrregioes;

-- COMMAND ----------

-- DBTITLE 1,Bronze para Silver: regiões intermediárias
CREATE OR REFRESH MATERIALIZED VIEW cleaned_regioes_intermediarias
(
  id_regiao_intermediaria BIGINT COMMENT 'Identificador único da região intermediária.',
  nome_regiao_intermediaria STRING COMMENT 'Nome completo da região intermediária.',
  id_estado BIGINT COMMENT 'Identificador do estado ao qual pertence.',
  nome_estado STRING COMMENT 'Nome do estado.',
  sigla_estado STRING COMMENT 'Sigla da UF.'
)
COMMENT "Tabela padronizada de regiões intermediárias do Brasil (divisão geográfica de 2017), pronta para análises e relatórios."
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
  CAST(id AS BIGINT) AS id_regiao_intermediaria,
  trim(nome) AS nome_regiao_intermediaria,
  CAST(from_json(UF, 'id BIGINT').id AS BIGINT) AS id_estado,
  trim(from_json(UF, 'nome STRING').nome) AS nome_estado,
  trim(from_json(UF, 'sigla STRING').sigla) AS sigla_estado
FROM bronze_dev.ds_ibge.raw_regioes_intermediarias;

-- COMMAND ----------

-- DBTITLE 1,Bronze para Silver: regiões imediatas
CREATE OR REFRESH MATERIALIZED VIEW cleaned_regioes_imediatas
(
  id_regiao_imediata BIGINT COMMENT 'Identificador único da região imediata.',
  nome_regiao_imediata STRING COMMENT 'Nome completo da região imediata.',
  id_regiao_intermediaria BIGINT COMMENT 'Identificador da região intermediária à qual pertence.',
  nome_regiao_intermediaria STRING COMMENT 'Nome da região intermediária.',
  id_estado BIGINT COMMENT 'Identificador do estado.',
  nome_estado STRING COMMENT 'Nome do estado.',
  sigla_estado STRING COMMENT 'Sigla da UF.'
)
COMMENT "Tabela padronizada de regiões imediatas do Brasil (divisão geográfica de 2017), pronta para análises e relatórios."
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
  CAST(id AS BIGINT) AS id_regiao_imediata,
  trim(nome) AS nome_regiao_imediata,
  CAST(from_json(regiao_intermediaria, 'id BIGINT').id AS BIGINT) AS id_regiao_intermediaria,
  trim(from_json(regiao_intermediaria, 'nome STRING').nome) AS nome_regiao_intermediaria,
  CAST(from_json(regiao_intermediaria, 'UF STRUCT<id:BIGINT>').UF.id AS BIGINT) AS id_estado,
  trim(from_json(regiao_intermediaria, 'UF STRUCT<nome:STRING>').UF.nome) AS nome_estado,
  trim(from_json(regiao_intermediaria, 'UF STRUCT<sigla:STRING>').UF.sigla) AS sigla_estado
FROM bronze_dev.ds_ibge.raw_regioes_imediatas;

-- COMMAND ----------

-- DBTITLE 1,Bronze para Silver: municípios
CREATE OR REFRESH MATERIALIZED VIEW cleaned_municipios
(
  id_municipio BIGINT COMMENT 'Identificador único do município (código IBGE de 7 dígitos).',
  nome_municipio STRING COMMENT 'Nome completo do município.',
  id_microrregiao BIGINT COMMENT 'Identificador da microrregião à qual pertence.',
  nome_microrregiao STRING COMMENT 'Nome da microrregião.',
  id_mesorregiao BIGINT COMMENT 'Identificador da mesorregião.',
  nome_mesorregiao STRING COMMENT 'Nome da mesorregião.',
  id_regiao_imediata BIGINT COMMENT 'Identificador da região imediata.',
  nome_regiao_imediata STRING COMMENT 'Nome da região imediata.',
  id_regiao_intermediaria BIGINT COMMENT 'Identificador da região intermediária.',
  nome_regiao_intermediaria STRING COMMENT 'Nome da região intermediária.',
  id_estado BIGINT COMMENT 'Identificador do estado (UF).',
  nome_estado STRING COMMENT 'Nome do estado.',
  sigla_estado STRING COMMENT 'Sigla da UF.',
  id_regiao BIGINT COMMENT 'Identificador da região brasileira.',
  nome_regiao STRING COMMENT 'Nome da região.',
  sigla_regiao STRING COMMENT 'Sigla da região.'
)
COMMENT "Tabela padronizada de municípios brasileiros (5.570 municípios), pronta para análises e relatórios."
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
  CAST(id AS BIGINT) AS id_municipio,
  trim(nome) AS nome_municipio,
  CAST(from_json(microrregiao, 'id BIGINT').id AS BIGINT) AS id_microrregiao,
  trim(from_json(microrregiao, 'nome STRING').nome) AS nome_microrregiao,
  CAST(from_json(microrregiao, 'mesorregiao STRUCT<id:BIGINT>').mesorregiao.id AS BIGINT) AS id_mesorregiao,
  trim(from_json(microrregiao, 'mesorregiao STRUCT<nome:STRING>').mesorregiao.nome) AS nome_mesorregiao,
  CAST(from_json(regiao_imediata, 'id BIGINT').id AS BIGINT) AS id_regiao_imediata,
  trim(from_json(regiao_imediata, 'nome STRING').nome) AS nome_regiao_imediata,
  CAST(from_json(regiao_imediata, '`regiao-intermediaria` STRUCT<id:BIGINT>')['regiao-intermediaria'].id AS BIGINT) AS id_regiao_intermediaria,
  trim(from_json(regiao_imediata, '`regiao-intermediaria` STRUCT<nome:STRING>')['regiao-intermediaria'].nome) AS nome_regiao_intermediaria,
  CAST(from_json(microrregiao, 'mesorregiao STRUCT<UF:STRUCT<id:BIGINT>>').mesorregiao.UF.id AS BIGINT) AS id_estado,
  trim(from_json(microrregiao, 'mesorregiao STRUCT<UF:STRUCT<nome:STRING>>').mesorregiao.UF.nome) AS nome_estado,
  trim(from_json(microrregiao, 'mesorregiao STRUCT<UF:STRUCT<sigla:STRING>>').mesorregiao.UF.sigla) AS sigla_estado,
  CAST(from_json(microrregiao, 'mesorregiao STRUCT<UF:STRUCT<regiao:STRUCT<id:BIGINT>>>').mesorregiao.UF.regiao.id AS BIGINT) AS id_regiao,
  trim(from_json(microrregiao, 'mesorregiao STRUCT<UF:STRUCT<regiao:STRUCT<nome:STRING>>>').mesorregiao.UF.regiao.nome) AS nome_regiao,
  trim(from_json(microrregiao, 'mesorregiao STRUCT<UF:STRUCT<regiao:STRUCT<sigla:STRING>>>').mesorregiao.UF.regiao.sigla) AS sigla_regiao
FROM bronze_dev.ds_ibge.raw_municipios;

-- COMMAND ----------

-- DBTITLE 1,Bronze para Silver: distritos
CREATE OR REFRESH MATERIALIZED VIEW cleaned_distritos
(
  id_distrito BIGINT COMMENT 'Identificador único do distrito.',
  nome_distrito STRING COMMENT 'Nome completo do distrito.',
  id_municipio BIGINT COMMENT 'Identificador do município ao qual pertence.',
  nome_municipio STRING COMMENT 'Nome do município.',
  id_microrregiao BIGINT COMMENT 'Identificador da microrregião.',
  nome_microrregiao STRING COMMENT 'Nome da microrregião.',
  id_mesorregiao BIGINT COMMENT 'Identificador da mesorregião.',
  nome_mesorregiao STRING COMMENT 'Nome da mesorregião.',
  id_regiao_imediata BIGINT COMMENT 'Identificador da região imediata.',
  nome_regiao_imediata STRING COMMENT 'Nome da região imediata.',
  id_regiao_intermediaria BIGINT COMMENT 'Identificador da região intermediária.',
  nome_regiao_intermediaria STRING COMMENT 'Nome da região intermediária.',
  id_estado BIGINT COMMENT 'Identificador do estado.',
  nome_estado STRING COMMENT 'Nome do estado.',
  sigla_estado STRING COMMENT 'Sigla da UF.',
  id_regiao BIGINT COMMENT 'Identificador da região brasileira.',
  nome_regiao STRING COMMENT 'Nome da região.',
  sigla_regiao STRING COMMENT 'Sigla da região.'
)
COMMENT "Tabela padronizada de distritos brasileiros, pronta para análises e relatórios."
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
  CAST(id AS BIGINT) AS id_distrito,
  trim(nome) AS nome_distrito,
  CAST(from_json(municipio, 'id BIGINT').id AS BIGINT) AS id_municipio,
  trim(from_json(municipio, 'nome STRING').nome) AS nome_municipio,
  CAST(from_json(municipio, 'microrregiao STRUCT<id:BIGINT>').microrregiao.id AS BIGINT) AS id_microrregiao,
  trim(from_json(municipio, 'microrregiao STRUCT<nome:STRING>').microrregiao.nome) AS nome_microrregiao,
  CAST(from_json(municipio, 'microrregiao STRUCT<mesorregiao:STRUCT<id:BIGINT>>').microrregiao.mesorregiao.id AS BIGINT) AS id_mesorregiao,
  trim(from_json(municipio, 'microrregiao STRUCT<mesorregiao:STRUCT<nome:STRING>>').microrregiao.mesorregiao.nome) AS nome_mesorregiao,
  CAST(from_json(municipio, '`regiao-imediata` STRUCT<id:BIGINT>')['regiao-imediata'].id AS BIGINT) AS id_regiao_imediata,
  trim(from_json(municipio, '`regiao-imediata` STRUCT<nome:STRING>')['regiao-imediata'].nome) AS nome_regiao_imediata,
  CAST(from_json(municipio, '`regiao-imediata` STRUCT<`regiao-intermediaria`:STRUCT<id:BIGINT>>')['regiao-imediata']['regiao-intermediaria'].id AS BIGINT) AS id_regiao_intermediaria,
  trim(from_json(municipio, '`regiao-imediata` STRUCT<`regiao-intermediaria`:STRUCT<nome:STRING>>')['regiao-imediata']['regiao-intermediaria'].nome) AS nome_regiao_intermediaria,
  CAST(from_json(municipio, 'microrregiao STRUCT<mesorregiao:STRUCT<UF:STRUCT<id:BIGINT>>>').microrregiao.mesorregiao.UF.id AS BIGINT) AS id_estado,
  trim(from_json(municipio, 'microrregiao STRUCT<mesorregiao:STRUCT<UF:STRUCT<nome:STRING>>>').microrregiao.mesorregiao.UF.nome) AS nome_estado,
  trim(from_json(municipio, 'microrregiao STRUCT<mesorregiao:STRUCT<UF:STRUCT<sigla:STRING>>>').microrregiao.mesorregiao.UF.sigla) AS sigla_estado,
  CAST(from_json(municipio, 'microrregiao STRUCT<mesorregiao:STRUCT<UF:STRUCT<regiao:STRUCT<id:BIGINT>>>>').microrregiao.mesorregiao.UF.regiao.id AS BIGINT) AS id_regiao,
  trim(from_json(municipio, 'microrregiao STRUCT<mesorregiao:STRUCT<UF:STRUCT<regiao:STRUCT<nome:STRING>>>>').microrregiao.mesorregiao.UF.regiao.nome) AS nome_regiao,
  trim(from_json(municipio, 'microrregiao STRUCT<mesorregiao:STRUCT<UF:STRUCT<regiao:STRUCT<sigla:STRING>>>>').microrregiao.mesorregiao.UF.regiao.sigla) AS sigla_regiao
FROM bronze_dev.ds_ibge.raw_distritos;

-- COMMAND ----------

-- DBTITLE 1,Bronze para Silver: subdistritos
CREATE OR REFRESH MATERIALIZED VIEW cleaned_subdistritos
(
  id_subdistrito BIGINT COMMENT 'Identificador único do subdistrito.',
  nome_subdistrito STRING COMMENT 'Nome completo do subdistrito.',
  id_distrito BIGINT COMMENT 'Identificador do distrito ao qual pertence.',
  nome_distrito STRING COMMENT 'Nome do distrito.',
  id_municipio BIGINT COMMENT 'Identificador do município.',
  nome_municipio STRING COMMENT 'Nome do município.',
  id_microrregiao BIGINT COMMENT 'Identificador da microrregião.',
  nome_microrregiao STRING COMMENT 'Nome da microrregião.',
  id_mesorregiao BIGINT COMMENT 'Identificador da mesorregião.',
  nome_mesorregiao STRING COMMENT 'Nome da mesorregião.',
  id_regiao_imediata BIGINT COMMENT 'Identificador da região imediata.',
  nome_regiao_imediata STRING COMMENT 'Nome da região imediata.',
  id_regiao_intermediaria BIGINT COMMENT 'Identificador da região intermediária.',
  nome_regiao_intermediaria STRING COMMENT 'Nome da região intermediária.',
  id_estado BIGINT COMMENT 'Identificador do estado.',
  nome_estado STRING COMMENT 'Nome do estado.',
  sigla_estado STRING COMMENT 'Sigla da UF.',
  id_regiao BIGINT COMMENT 'Identificador da região brasileira.',
  nome_regiao STRING COMMENT 'Nome da região.',
  sigla_regiao STRING COMMENT 'Sigla da região.'
)
COMMENT "Tabela padronizada de subdistritos brasileiros, pronta para análises e relatórios."
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
  CAST(id AS BIGINT) AS id_subdistrito,
  trim(nome) AS nome_subdistrito,
  CAST(from_json(distrito, 'id BIGINT').id AS BIGINT) AS id_distrito,
  trim(from_json(distrito, 'nome STRING').nome) AS nome_distrito,
  CAST(from_json(distrito, 'municipio STRUCT<id:BIGINT>').municipio.id AS BIGINT) AS id_municipio,
  trim(from_json(distrito, 'municipio STRUCT<nome:STRING>').municipio.nome) AS nome_municipio,
  CAST(from_json(distrito, 'municipio STRUCT<microrregiao:STRUCT<id:BIGINT>>').municipio.microrregiao.id AS BIGINT) AS id_microrregiao,
  trim(from_json(distrito, 'municipio STRUCT<microrregiao:STRUCT<nome:STRING>>').municipio.microrregiao.nome) AS nome_microrregiao,
  CAST(from_json(distrito, 'municipio STRUCT<microrregiao:STRUCT<mesorregiao:STRUCT<id:BIGINT>>>').municipio.microrregiao.mesorregiao.id AS BIGINT) AS id_mesorregiao,
  trim(from_json(distrito, 'municipio STRUCT<microrregiao:STRUCT<mesorregiao:STRUCT<nome:STRING>>>').municipio.microrregiao.mesorregiao.nome) AS nome_mesorregiao,
  CAST(from_json(distrito, 'municipio STRUCT<`regiao-imediata`:STRUCT<id:BIGINT>>').municipio['regiao-imediata'].id AS BIGINT) AS id_regiao_imediata,
  trim(from_json(distrito, 'municipio STRUCT<`regiao-imediata`:STRUCT<nome:STRING>>').municipio['regiao-imediata'].nome) AS nome_regiao_imediata,
  CAST(from_json(distrito, 'municipio STRUCT<`regiao-imediata`:STRUCT<`regiao-intermediaria`:STRUCT<id:BIGINT>>>').municipio['regiao-imediata']['regiao-intermediaria'].id AS BIGINT) AS id_regiao_intermediaria,
  trim(from_json(distrito, 'municipio STRUCT<`regiao-imediata`:STRUCT<`regiao-intermediaria`:STRUCT<nome:STRING>>>').municipio['regiao-imediata']['regiao-intermediaria'].nome) AS nome_regiao_intermediaria,
  CAST(from_json(distrito, 'municipio STRUCT<microrregiao:STRUCT<mesorregiao:STRUCT<UF:STRUCT<id:BIGINT>>>>').municipio.microrregiao.mesorregiao.UF.id AS BIGINT) AS id_estado,
  trim(from_json(distrito, 'municipio STRUCT<microrregiao:STRUCT<mesorregiao:STRUCT<UF:STRUCT<nome:STRING>>>>').municipio.microrregiao.mesorregiao.UF.nome) AS nome_estado,
  trim(from_json(distrito, 'municipio STRUCT<microrregiao:STRUCT<mesorregiao:STRUCT<UF:STRUCT<sigla:STRING>>>>').municipio.microrregiao.mesorregiao.UF.sigla) AS sigla_estado,
  CAST(from_json(distrito, 'municipio STRUCT<microrregiao:STRUCT<mesorregiao:STRUCT<UF:STRUCT<regiao:STRUCT<id:BIGINT>>>>>').municipio.microrregiao.mesorregiao.UF.regiao.id AS BIGINT) AS id_regiao,
  trim(from_json(distrito, 'municipio STRUCT<microrregiao:STRUCT<mesorregiao:STRUCT<UF:STRUCT<regiao:STRUCT<nome:STRING>>>>>').municipio.microrregiao.mesorregiao.UF.regiao.nome) AS nome_regiao,
  trim(from_json(distrito, 'municipio STRUCT<microrregiao:STRUCT<mesorregiao:STRUCT<UF:STRUCT<regiao:STRUCT<sigla:STRING>>>>>').municipio.microrregiao.mesorregiao.UF.regiao.sigla) AS sigla_regiao
FROM bronze_dev.ds_ibge.raw_subdistritos;

-- COMMAND ----------

-- DBTITLE 1,Bronze para Silver: regiões metropolitanas
CREATE OR REFRESH MATERIALIZED VIEW cleaned_regioes_metropolitanas
(
  id_regiao_metropolitana BIGINT COMMENT 'Identificador único da região metropolitana.',
  nome_regiao_metropolitana STRING COMMENT 'Nome completo da região metropolitana.',
  id_estado BIGINT COMMENT 'Identificador do estado principal.',
  nome_estado STRING COMMENT 'Nome do estado.',
  sigla_estado STRING COMMENT 'Sigla da UF.',
  id_regiao BIGINT COMMENT 'Identificador da região brasileira.',
  nome_regiao STRING COMMENT 'Nome da região.',
  sigla_regiao STRING COMMENT 'Sigla da região.'
)
COMMENT "Tabela padronizada de regiões metropolitanas, aglomerações urbanas e RIDEs do Brasil, pronta para análises e relatórios."
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
  CAST(id AS BIGINT) AS id_regiao_metropolitana,
  trim(nome) AS nome_regiao_metropolitana,
  CAST(from_json(UF, 'id BIGINT').id AS BIGINT) AS id_estado,
  trim(from_json(UF, 'nome STRING').nome) AS nome_estado,
  trim(from_json(UF, 'sigla STRING').sigla) AS sigla_estado,
  CAST(from_json(UF, 'regiao STRUCT<id:BIGINT>').regiao.id AS BIGINT) AS id_regiao,
  trim(from_json(UF, 'regiao STRUCT<nome:STRING>').regiao.nome) AS nome_regiao,
  trim(from_json(UF, 'regiao STRUCT<sigla:STRING>').regiao.sigla) AS sigla_regiao
FROM bronze_dev.ds_ibge.raw_regioes_metropolitanas;

-- COMMAND ----------

-- DBTITLE 1,Bronze para Silver: aglomerações urbanas
CREATE OR REFRESH MATERIALIZED VIEW cleaned_aglomeracoes_urbanas
(
  id_aglomeracao_urbana BIGINT COMMENT 'Identificador único da aglomeração urbana.',
  nome_aglomeracao_urbana STRING COMMENT 'Nome completo da aglomeração urbana.',
  id_estado BIGINT COMMENT 'Identificador do estado principal.',
  nome_estado STRING COMMENT 'Nome do estado.',
  sigla_estado STRING COMMENT 'Sigla da UF.',
  id_regiao BIGINT COMMENT 'Identificador da região brasileira.',
  nome_regiao STRING COMMENT 'Nome da região.',
  sigla_regiao STRING COMMENT 'Sigla da região.'
)
COMMENT "Tabela padronizada de aglomerações urbanas brasileiras, pronta para análises e relatórios."
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
  CAST(id AS BIGINT) AS id_aglomeracao_urbana,
  trim(nome) AS nome_aglomeracao_urbana,
  CAST(from_json(municipios, 'ARRAY<STRUCT<UF:STRUCT<id:BIGINT>>>')[0].UF.id AS BIGINT) AS id_estado,
  trim(from_json(municipios, 'ARRAY<STRUCT<UF:STRUCT<nome:STRING>>>')[0].UF.nome) AS nome_estado,
  trim(from_json(municipios, 'ARRAY<STRUCT<UF:STRUCT<sigla:STRING>>>')[0].UF.sigla) AS sigla_estado,
  CAST(from_json(municipios, 'ARRAY<STRUCT<UF:STRUCT<regiao:STRUCT<id:BIGINT>>>>')[0].UF.regiao.id AS BIGINT) AS id_regiao,
  trim(from_json(municipios, 'ARRAY<STRUCT<UF:STRUCT<regiao:STRUCT<nome:STRING>>>>')[0].UF.regiao.nome) AS nome_regiao,
  trim(from_json(municipios, 'ARRAY<STRUCT<UF:STRUCT<regiao:STRUCT<sigla:STRING>>>>')[0].UF.regiao.sigla) AS sigla_regiao
FROM bronze_dev.ds_ibge.raw_aglomeracoes_urbanas;

-- COMMAND ----------

-- DBTITLE 1,Bronze para Silver: regiões integradas de desenvolvimento
CREATE OR REFRESH MATERIALIZED VIEW cleaned_regioes_integradas_desenvolvimento
(
  id_ride STRING COMMENT 'Identificador único da RIDE (Região Integrada de Desenvolvimento).',
  nome_ride STRING COMMENT 'Nome completo da RIDE.'
)
COMMENT "Tabela padronizada de RIDEs (Regiões Integradas de Desenvolvimento Econômico), pronta para análises e relatórios."
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
  id AS id_ride,
  trim(nome) AS nome_ride
FROM bronze_dev.ds_ibge.raw_regioes_integradas_desenvolvimento;

-- COMMAND ----------

-- DBTITLE 1,Bronze para Silver: RIDE Municípios (relacionamento)
CREATE OR REFRESH MATERIALIZED VIEW ride_municipios
(
  id_ride STRING COMMENT 'Identificador da RIDE.',
  id_municipio BIGINT COMMENT 'Identificador do município.',
  nome_municipio STRING COMMENT 'Nome do município.',
  id_estado BIGINT COMMENT 'Identificador do estado do município.',
  nome_estado STRING COMMENT 'Nome do estado.',
  sigla_estado STRING COMMENT 'Sigla da UF.',
  id_regiao BIGINT COMMENT 'Identificador da região.',
  nome_regiao STRING COMMENT 'Nome da região.',
  sigla_regiao STRING COMMENT 'Sigla da região.'
)
COMMENT "Tabela de relacionamento entre RIDEs e seus municípios (expandida)."
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
  id AS id_ride,
  CAST(municipio_exploded.id AS BIGINT) AS id_municipio,
  trim(municipio_exploded.nome) AS nome_municipio,
  CAST(municipio_exploded.UF.id AS BIGINT) AS id_estado,
  trim(municipio_exploded.UF.nome) AS nome_estado,
  trim(municipio_exploded.UF.sigla) AS sigla_estado,
  CAST(municipio_exploded.UF.regiao.id AS BIGINT) AS id_regiao,
  trim(municipio_exploded.UF.regiao.nome) AS nome_regiao,
  trim(municipio_exploded.UF.regiao.sigla) AS sigla_regiao
FROM bronze_dev.ds_ibge.raw_regioes_integradas_desenvolvimento
LATERAL VIEW explode(from_json(municipios, 'ARRAY<STRUCT<id:BIGINT,nome:STRING,UF:STRUCT<id:BIGINT,nome:STRING,sigla:STRING,regiao:STRUCT<id:BIGINT,nome:STRING,sigla:STRING>>>>')) AS municipio_exploded;

-- COMMAND ----------

-- DBTITLE 1,Bronze para Silver: Região Metropolitana Municípios (relacionamento)
CREATE OR REFRESH MATERIALIZED VIEW regiao_metropolitana_municipios
(
  id_regiao_metropolitana BIGINT COMMENT 'Identificador da região metropolitana.',
  id_municipio BIGINT COMMENT 'Identificador do município.',
  nome_municipio STRING COMMENT 'Nome do município.'
)
COMMENT "Tabela de relacionamento entre regiões metropolitanas e seus municípios (expandida)."
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
  CAST(id AS BIGINT) AS id_regiao_metropolitana,
  CAST(municipio_exploded.id AS BIGINT) AS id_municipio,
  trim(municipio_exploded.nome) AS nome_municipio
FROM bronze_dev.ds_ibge.raw_regioes_metropolitanas
LATERAL VIEW explode(from_json(municipios, 'ARRAY<STRUCT<id:BIGINT,nome:STRING>>')) AS municipio_exploded;

-- COMMAND ----------

-- DBTITLE 1,Bronze para Silver: Aglomeração Urbana Municípios (relacionamento)
CREATE OR REFRESH MATERIALIZED VIEW aglomeracao_urbana_municipios
(
  id_aglomeracao_urbana BIGINT COMMENT 'Identificador da aglomeração urbana.',
  id_municipio BIGINT COMMENT 'Identificador do município.',
  nome_municipio STRING COMMENT 'Nome do município.',
  id_estado BIGINT COMMENT 'Identificador do estado do município.',
  nome_estado STRING COMMENT 'Nome do estado.',
  sigla_estado STRING COMMENT 'Sigla da UF.',
  id_regiao BIGINT COMMENT 'Identificador da região.',
  nome_regiao STRING COMMENT 'Nome da região.',
  sigla_regiao STRING COMMENT 'Sigla da região.'
)
COMMENT "Tabela de relacionamento entre aglomerações urbanas e seus municípios (expandida)."
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
  CAST(id AS BIGINT) AS id_aglomeracao_urbana,
  CAST(municipio_exploded.id AS BIGINT) AS id_municipio,
  trim(municipio_exploded.nome) AS nome_municipio,
  CAST(municipio_exploded.UF.id AS BIGINT) AS id_estado,
  trim(municipio_exploded.UF.nome) AS nome_estado,
  trim(municipio_exploded.UF.sigla) AS sigla_estado,
  CAST(municipio_exploded.UF.regiao.id AS BIGINT) AS id_regiao,
  trim(municipio_exploded.UF.regiao.nome) AS nome_regiao,
  trim(municipio_exploded.UF.regiao.sigla) AS sigla_regiao
FROM bronze_dev.ds_ibge.raw_aglomeracoes_urbanas
LATERAL VIEW explode(from_json(municipios, 'ARRAY<STRUCT<id:BIGINT,nome:STRING,UF:STRUCT<id:BIGINT,nome:STRING,sigla:STRING,regiao:STRUCT<id:BIGINT,nome:STRING,sigla:STRING>>>>')) AS municipio_exploded;
