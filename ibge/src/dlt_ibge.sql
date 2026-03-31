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
  CAST(regiao.id AS BIGINT) AS id_regiao,
  trim(regiao.nome) AS nome_regiao,
  trim(regiao.sigla) AS sigla_regiao
FROM bronze_dev.ds_ibge.raw_estados;

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
  CAST(UF.id AS BIGINT) AS id_estado,
  trim(UF.nome) AS nome_estado,
  trim(UF.sigla) AS sigla_estado
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
  CAST(mesorregiao.id AS BIGINT) AS id_mesorregiao,
  trim(mesorregiao.nome) AS nome_mesorregiao,
  CAST(mesorregiao.UF.id AS BIGINT) AS id_estado,
  trim(mesorregiao.UF.nome) AS nome_estado,
  trim(mesorregiao.UF.sigla) AS sigla_estado
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
  CAST(UF.id AS BIGINT) AS id_estado,
  trim(UF.nome) AS nome_estado,
  trim(UF.sigla) AS sigla_estado
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
  CAST(`regiao-intermediaria`.id AS BIGINT) AS id_regiao_intermediaria,
  trim(`regiao-intermediaria`.nome) AS nome_regiao_intermediaria,
  CAST(`regiao-intermediaria`.UF.id AS BIGINT) AS id_estado,
  trim(`regiao-intermediaria`.UF.nome) AS nome_estado,
  trim(`regiao-intermediaria`.UF.sigla) AS sigla_estado
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
  CAST(microrregiao.id AS BIGINT) AS id_microrregiao,
  trim(microrregiao.nome) AS nome_microrregiao,
  CAST(microrregiao.mesorregiao.id AS BIGINT) AS id_mesorregiao,
  trim(microrregiao.mesorregiao.nome) AS nome_mesorregiao,
  CAST(`regiao-imediata`.id AS BIGINT) AS id_regiao_imediata,
  trim(`regiao-imediata`.nome) AS nome_regiao_imediata,
  CAST(`regiao-imediata`.`regiao-intermediaria`.id AS BIGINT) AS id_regiao_intermediaria,
  trim(`regiao-imediata`.`regiao-intermediaria`.nome) AS nome_regiao_intermediaria,
  CAST(microrregiao.mesorregiao.UF.id AS BIGINT) AS id_estado,
  trim(microrregiao.mesorregiao.UF.nome) AS nome_estado,
  trim(microrregiao.mesorregiao.UF.sigla) AS sigla_estado,
  CAST(microrregiao.mesorregiao.UF.regiao.id AS BIGINT) AS id_regiao,
  trim(microrregiao.mesorregiao.UF.regiao.nome) AS nome_regiao,
  trim(microrregiao.mesorregiao.UF.regiao.sigla) AS sigla_regiao
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
  CAST(municipio.id AS BIGINT) AS id_municipio,
  trim(municipio.nome) AS nome_municipio,
  CAST(municipio.microrregiao.id AS BIGINT) AS id_microrregiao,
  trim(municipio.microrregiao.nome) AS nome_microrregiao,
  CAST(municipio.microrregiao.mesorregiao.id AS BIGINT) AS id_mesorregiao,
  trim(municipio.microrregiao.mesorregiao.nome) AS nome_mesorregiao,
  CAST(municipio.`regiao-imediata`.id AS BIGINT) AS id_regiao_imediata,
  trim(municipio.`regiao-imediata`.nome) AS nome_regiao_imediata,
  CAST(municipio.`regiao-imediata`.`regiao-intermediaria`.id AS BIGINT) AS id_regiao_intermediaria,
  trim(municipio.`regiao-imediata`.`regiao-intermediaria`.nome) AS nome_regiao_intermediaria,
  CAST(municipio.microrregiao.mesorregiao.UF.id AS BIGINT) AS id_estado,
  trim(municipio.microrregiao.mesorregiao.UF.nome) AS nome_estado,
  trim(municipio.microrregiao.mesorregiao.UF.sigla) AS sigla_estado,
  CAST(municipio.microrregiao.mesorregiao.UF.regiao.id AS BIGINT) AS id_regiao,
  trim(municipio.microrregiao.mesorregiao.UF.regiao.nome) AS nome_regiao,
  trim(municipio.microrregiao.mesorregiao.UF.regiao.sigla) AS sigla_regiao
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
  CAST(distrito.id AS BIGINT) AS id_distrito,
  trim(distrito.nome) AS nome_distrito,
  CAST(distrito.municipio.id AS BIGINT) AS id_municipio,
  trim(distrito.municipio.nome) AS nome_municipio,
  CAST(distrito.municipio.microrregiao.id AS BIGINT) AS id_microrregiao,
  trim(distrito.municipio.microrregiao.nome) AS nome_microrregiao,
  CAST(distrito.municipio.microrregiao.mesorregiao.id AS BIGINT) AS id_mesorregiao,
  trim(distrito.municipio.microrregiao.mesorregiao.nome) AS nome_mesorregiao,
  CAST(distrito.municipio.`regiao-imediata`.id AS BIGINT) AS id_regiao_imediata,
  trim(distrito.municipio.`regiao-imediata`.nome) AS nome_regiao_imediata,
  CAST(distrito.municipio.`regiao-imediata`.`regiao-intermediaria`.id AS BIGINT) AS id_regiao_intermediaria,
  trim(distrito.municipio.`regiao-imediata`.`regiao-intermediaria`.nome) AS nome_regiao_intermediaria,
  CAST(distrito.municipio.microrregiao.mesorregiao.UF.id AS BIGINT) AS id_estado,
  trim(distrito.municipio.microrregiao.mesorregiao.UF.nome) AS nome_estado,
  trim(distrito.municipio.microrregiao.mesorregiao.UF.sigla) AS sigla_estado,
  CAST(distrito.municipio.microrregiao.mesorregiao.UF.regiao.id AS BIGINT) AS id_regiao,
  trim(distrito.municipio.microrregiao.mesorregiao.UF.regiao.nome) AS nome_regiao,
  trim(distrito.municipio.microrregiao.mesorregiao.UF.regiao.sigla) AS sigla_regiao
FROM bronze_dev.ds_ibge.raw_subdistritos;

-- COMMAND ----------

-- DBTITLE 1,Bronze para Silver: regiões metropolitanas
CREATE OR REFRESH MATERIALIZED VIEW cleaned_regioes_metropolitanas
(
  id_regiao_metropolitana BIGINT COMMENT 'Identificador único da região metropolitana.',
  nome_regiao_metropolitana STRING COMMENT 'Nome completo da região metropolitana.',
  tipo_regiao_metropolitana STRING COMMENT 'Tipo de região (Região Metropolitana, RIDE, etc).',
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
  trim(tipo) AS tipo_regiao_metropolitana,
  CAST(UF.id AS BIGINT) AS id_estado,
  trim(UF.nome) AS nome_estado,
  trim(UF.sigla) AS sigla_estado,
  CAST(UF.regiao.id AS BIGINT) AS id_regiao,
  trim(UF.regiao.nome) AS nome_regiao,
  trim(UF.regiao.sigla) AS sigla_regiao
FROM bronze_dev.ds_ibge.raw_regioes_metropolitanas;

-- COMMAND ----------

-- DBTITLE 1,Bronze para Silver: aglomerações urbanas
CREATE OR REFRESH MATERIALIZED VIEW cleaned_aglomeracoes_urbanas
(
  id_aglomeracao_urbana BIGINT COMMENT 'Identificador único da aglomeração urbana.',
  nome_aglomeracao_urbana STRING COMMENT 'Nome completo da aglomeração urbana.',
  tipo_aglomeracao_urbana STRING COMMENT 'Tipo de aglomeração urbana.',
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
  trim(tipo) AS tipo_aglomeracao_urbana,
  CAST(UF.id AS BIGINT) AS id_estado,
  trim(UF.nome) AS nome_estado,
  trim(UF.sigla) AS sigla_estado,
  CAST(UF.regiao.id AS BIGINT) AS id_regiao,
  trim(UF.regiao.nome) AS nome_regiao,
  trim(UF.regiao.sigla) AS sigla_regiao
FROM bronze_dev.ds_ibge.raw_aglomeracoes_urbanas;

-- COMMAND ----------

-- DBTITLE 1,Bronze para Silver: regiões integradas de desenvolvimento
CREATE OR REFRESH MATERIALIZED VIEW cleaned_regioes_integradas_desenvolvimento
(
  id_ride BIGINT COMMENT 'Identificador único da RIDE (Região Integrada de Desenvolvimento).',
  nome_ride STRING COMMENT 'Nome completo da RIDE.',
  sigla_ride STRING COMMENT 'Sigla da RIDE.',
  tipo_ride STRING COMMENT 'Tipo da região integrada.'
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
  CAST(id AS BIGINT) AS id_ride,
  trim(nome) AS nome_ride,
  trim(sigla) AS sigla_ride,
  trim(tipo) AS tipo_ride
FROM bronze_dev.ds_ibge.raw_regioes_integradas_desenvolvimento;
