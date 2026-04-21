-- Databricks notebook source
-- DBTITLE 1,cleaned_item_attributes
CREATE OR REFRESH MATERIALIZED VIEW cleaned_item_attributes
(
  id_item_attribute INT COMMENT 'Identificador único para cada item de Pokémon.',
  item_name STRING COMMENT 'Nome do item.'
)
COMMENT "Tabela de atributos de items limpos, padronizada para análises e relatórios."
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
  CAST(id_item_attribute AS INT) AS id_item_attribute,
  name AS item_name
FROM ${source_catalog}.${source_schema}.raw_item_attributes;

-- COMMAND ----------

-- DBTITLE 1,cleaned_items
CREATE OR REFRESH MATERIALIZED VIEW cleaned_items
(
  item_id INT COMMENT 'Identificador único para cada item de Pokémon.',
  item_name STRING COMMENT 'Nome do item.'
)
COMMENT "Tabela de items gerias limpas, padronizada para análises e relatórios."
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
  CAST(id_item AS INT) AS item_id,
  name AS item_name
FROM ${source_catalog}.${source_schema}.raw_items;

-- COMMAND ----------

-- DBTITLE 1,cleaned_locations
CREATE OR REFRESH MATERIALIZED VIEW cleaned_locations
(
  id_location INT COMMENT 'Identificador único para cada local de Pokémon.',
  location_name STRING COMMENT 'Nome do local.'
)
COMMENT "Tabela de locais limpos, padronizada para análises e relatórios."
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
  CAST(id_location AS INT) AS id_location,
  REPLACE(name, '-', '_') AS location_name
FROM ${source_catalog}.${source_schema}.raw_locations;

-- COMMAND ----------

-- DBTITLE 1,cleaned_pokemon_abilities
CREATE OR REFRESH LIVE TABLE cleaned_pokemon_abilities
(
  ability_id INT COMMENT 'Identificador único para cada habilidade de Pokémon.',
  ability_name STRING COMMENT 'Nome da habilidade.'
)
COMMENT "Tabela de habilidades limpas dos Pokémon, padronizada para análises e relatórios."
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
  CAST(id_ability AS INT) AS ability_id,
  CAST(name AS STRING) AS ability_name
FROM ${source_catalog}.${source_schema}.raw_pokemon_abilities;

-- COMMAND ----------

-- DBTITLE 1,cleaned_pokemon_egg_groups
CREATE OR REFRESH LIVE TABLE cleaned_pokemon_egg_groups
(
  id_egg_group INT COMMENT 'Identificador único para cada grupo de ovo de Pokémon.',
  egg_group_name STRING COMMENT 'Nome do grupo de ovo.'
)
COMMENT "Tabela de grupos de ovo limpos dos Pokémon, padronizada para análises e relatórios."
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
  CAST(id_egg_group AS INT) AS id_egg_group,
  CAST(name AS STRING) AS egg_group_name
FROM ${source_catalog}.${source_schema}.raw_pokemon_egg_groups;

-- COMMAND ----------

-- DBTITLE 1,cleaned_pokemon_number_dex
CREATE OR REFRESH LIVE TABLE cleaned_pokemon_number_dex
(
  num_pokedex INT COMMENT 'Número do Pokémon na Pokédex.',
  pokemon_name STRING COMMENT 'Nome do Pokémon.'
)
COMMENT "Tabela de números e nomes limpos dos Pokémon, padronizada para análises e relatórios."
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
  CAST(num_pokedex AS INT) AS num_pokedex,
  CAST(name AS STRING) AS pokemon_name
FROM ${source_catalog}.${source_schema}.raw_pokemon_number_dex;

-- COMMAND ----------

-- DBTITLE 1,cleaned_pokemon_name
CREATE OR REFRESH LIVE TABLE cleaned_pokemon_name
(
  id INT COMMENT 'Identificador único para cada Pokémon.',
  pokemon_name STRING COMMENT 'Nome do Pokémon.'
)
COMMENT "Tabela de nomes limpos dos Pokémon, padronizada para análises e relatórios."
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
  CAST(id AS INT) AS id,
  CAST(name AS STRING) AS pokemon_name
FROM ${source_catalog}.${source_schema}.raw_pokemon_name;

-- COMMAND ----------

-- DBTITLE 1,cleaned_pokemon_genders
CREATE OR REFRESH LIVE TABLE cleaned_pokemon_genders
(
  id_gender INT COMMENT 'Identificador único para cada gênero de Pokémon.',
  gender_name STRING COMMENT 'Nome do gênero.'
)
COMMENT "Tabela de gêneros limpos dos Pokémon, padronizada para análises e relatórios."
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
  CAST(id_gender AS INT) AS id_gender,
  CAST(name AS STRING) AS gender_name
FROM ${source_catalog}.${source_schema}.raw_pokemon_genders;

-- COMMAND ----------

-- DBTITLE 1,cleaned_pokemon_growth_rates
CREATE OR REFRESH LIVE TABLE cleaned_pokemon_growth_rates
(
  id_growth_rate INT COMMENT 'Identificador único para cada taxa de crescimento de Pokémon.',
  growth_rate_name STRING COMMENT 'Nome da taxa de crescimento.'
)
COMMENT "Tabela de taxas de crescimento limpas dos Pokémon, padronizada para análises e relatórios."
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
  CAST(id_growth_rate AS INT) AS id_growth_rate,
  CAST(name AS STRING) AS growth_rate_name
FROM ${source_catalog}.${source_schema}.raw_pokemon_growth_rates;

-- COMMAND ----------

-- DBTITLE 1,cleaned_pokemon_location_areas
CREATE OR REFRESH LIVE TABLE cleaned_pokemon_location_areas
(
  id_location_area INT COMMENT 'Identificador único para cada área de localização de Pokémon.',
  location_area_name STRING COMMENT 'Nome da área de localização.'
)
COMMENT "Tabela de áreas de localização limpas dos Pokémon, padronizada para análises e relatórios."
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
  CAST(id_location_area AS INT) AS id_location_area,
  CAST(name AS STRING) AS location_area_name
FROM ${source_catalog}.${source_schema}.raw_pokemon_location_areas;

-- COMMAND ----------

-- DBTITLE 1,cleaned_pokemon_natures
CREATE OR REFRESH LIVE TABLE cleaned_pokemon_natures
(
  id_nature INT COMMENT 'Identificador único para cada natureza de Pokémon.',
  nature_name STRING COMMENT 'Nome da natureza.'
)
COMMENT "Tabela de naturezas limpas dos Pokémon, padronizada para análises e relatórios."
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
  CAST(id_nature AS INT) AS id_nature,
  CAST(name AS STRING) AS nature_name
FROM ${source_catalog}.${source_schema}.raw_pokemon_natures;

-- COMMAND ----------

-- DBTITLE 1,cleaned_pokemon_pokeathlon_stats
CREATE OR REFRESH LIVE TABLE cleaned_pokemon_pokeathlon_stats
(
  id_pokeathlon_stat INT COMMENT 'Identificador único para cada estatística de Pokeathlon.',
  pokeathlon_stat_name STRING COMMENT 'Nome da estatística de Pokeathlon.'
)
COMMENT "Tabela de estatísticas de Pokeathlon limpas dos Pokémon, padronizada para análises e relatórios."
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
  CAST(id_pokeathlon_stat AS INT) AS id_pokeathlon_stat,
  CAST(name AS STRING) AS pokeathlon_stat_name
FROM ${source_catalog}.${source_schema}.raw_pokemon_pokeathlon_stats;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Tabela Silver: Versions

-- COMMAND ----------

-- DBTITLE 1,cleaned_pokemon_versions
CREATE OR REFRESH LIVE TABLE cleaned_pokemon_versions
(
  id_version INT COMMENT 'Identificador único para cada versão de jogo Pokémon.',
  version_name STRING COMMENT 'Nome da versão do jogo (ex: red, blue, sword, scarlet).',
  version_group STRING COMMENT 'Grupo da versão (ex: red-blue, sword-shield, scarlet-violet).'
)
COMMENT "Tabela de versões de jogos Pokémon limpas, padronizada para análises e relatórios."
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
  CAST(id AS INT) AS id_version,
  CAST(name AS STRING) AS version_name,
  CAST(version_group AS STRING) AS version_group
FROM ${source_catalog}.${source_schema}.raw_pokemon_versions;
