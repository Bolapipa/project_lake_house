-- Databricks notebook source
-- DBTITLE 1,Bronze para Silver: Qualidade do Ar
CREATE OR REFRESH MATERIALIZED VIEW cleaned_air_quality
(
  capital STRING COMMENT 'Nome da capital brasileira.',
  uf STRING COMMENT 'Sigla da unidade federativa (estado).',
  latitude DOUBLE COMMENT 'Latitude da localização.',
  longitude DOUBLE COMMENT 'Longitude da localização.',
  elevation DOUBLE COMMENT 'Elevação em metros acima do nível do mar.',
  time TIMESTAMP COMMENT 'Data e hora da medição.',
  timezone STRING COMMENT 'Fuso horário da localização.',
  carbon_monoxide DOUBLE COMMENT 'Concentração de monóxido de carbono (CO) em µg/m³.',
  nitrogen_dioxide DOUBLE COMMENT 'Concentração de dióxido de nitrogênio (NO₂) em µg/m³.',
  ozone DOUBLE COMMENT 'Concentração de ozônio (O₃) em µg/m³.',
  pm2_5 DOUBLE COMMENT 'Concentração de material particulado PM2.5 em µg/m³.',
  pm10 DOUBLE COMMENT 'Concentração de material particulado PM10 em µg/m³.',
  sulphur_dioxide DOUBLE COMMENT 'Concentração de dióxido de enxofre (SO₂) em µg/m³.',
  european_aqi INT COMMENT 'Índice de qualidade do ar europeu (1-5).',
  us_aqi INT COMMENT 'Índice de qualidade do ar dos EUA (0-500).',
  uv_index DOUBLE COMMENT 'Índice de radiação ultravioleta.'
)
COMMENT 'Tabela padronizada de qualidade do ar para capitais brasileiras, pronta para análises e relatórios.'
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
  trim(capital) AS capital,
  upper(trim(uf)) AS uf,
  CAST(latitude AS DOUBLE) AS latitude,
  CAST(longitude AS DOUBLE) AS longitude,
  CAST(elevation AS DOUBLE) AS elevation,
  CAST(time AS TIMESTAMP) AS time,
  trim(timezone) AS timezone,
  CAST(carbon_monoxide AS DOUBLE) AS carbon_monoxide,
  CAST(nitrogen_dioxide AS DOUBLE) AS nitrogen_dioxide,
  CAST(ozone AS DOUBLE) AS ozone,
  CAST(pm2_5 AS DOUBLE) AS pm2_5,
  CAST(pm10 AS DOUBLE) AS pm10,
  CAST(sulphur_dioxide AS DOUBLE) AS sulphur_dioxide,
  CAST(european_aqi AS INT) AS european_aqi,
  CAST(us_aqi AS INT) AS us_aqi,
  CAST(uv_index AS DOUBLE) AS uv_index
FROM bronze_dev.ds_open_meteo.raw_air_quality;

-- COMMAND ----------

-- DBTITLE 1,Bronze para Silver: Clima Histórico
CREATE OR REFRESH MATERIALIZED VIEW cleaned_historical_weather
(
  capital STRING COMMENT 'Nome da capital brasileira.',
  uf STRING COMMENT 'Sigla da unidade federativa (estado).',
  latitude DOUBLE COMMENT 'Latitude da localização.',
  longitude DOUBLE COMMENT 'Longitude da localização.',
  elevation DOUBLE COMMENT 'Elevação em metros acima do nível do mar.',
  time DATE COMMENT 'Data da medição histórica.',
  timezone STRING COMMENT 'Fuso horário da localização.',
  temperature_2m_max DOUBLE COMMENT 'Temperatura máxima diária a 2m do solo (°C).',
  temperature_2m_min DOUBLE COMMENT 'Temperatura mínima diária a 2m do solo (°C).',
  temperature_2m_mean DOUBLE COMMENT 'Temperatura média diária a 2m do solo (°C).',
  precipitation_sum DOUBLE COMMENT 'Precipitação total diária (mm).',
  wind_speed_10m_max DOUBLE COMMENT 'Velocidade máxima do vento a 10m (km/h).',
  weather_code INT COMMENT 'Código WMO do tipo de clima (0=céu limpo, 1-3=parcial nublado, etc).'
)
COMMENT 'Tabela padronizada de dados históricos de clima para capitais brasileiras, pronta para análises e relatórios.'
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
  trim(capital) AS capital,
  upper(trim(uf)) AS uf,
  CAST(latitude AS DOUBLE) AS latitude,
  CAST(longitude AS DOUBLE) AS longitude,
  CAST(elevation AS DOUBLE) AS elevation,
  CAST(time AS DATE) AS time,
  trim(timezone) AS timezone,
  CAST(temperature_2m_max AS DOUBLE) AS temperature_2m_max,
  CAST(temperature_2m_min AS DOUBLE) AS temperature_2m_min,
  CAST(temperature_2m_mean AS DOUBLE) AS temperature_2m_mean,
  CAST(precipitation_sum AS DOUBLE) AS precipitation_sum,
  CAST(wind_speed_10m_max AS DOUBLE) AS wind_speed_10m_max,
  CAST(weather_code AS INT) AS weather_code
FROM bronze_dev.ds_open_meteo.raw_historical_weather;

-- COMMAND ----------

-- DBTITLE 1,Bronze para Silver: Previsão Diária
CREATE OR REFRESH MATERIALIZED VIEW cleaned_daily_forecast
(
  latitude DOUBLE COMMENT 'Latitude da localização.',
  longitude DOUBLE COMMENT 'Longitude da localização.',
  elevation DOUBLE COMMENT 'Elevação em metros acima do nível do mar.',
  timezone STRING COMMENT 'Fuso horário da localização.',
  timezone_abbreviation STRING COMMENT 'Abreviação do fuso horário.',
  utc_offset_seconds INT COMMENT 'Diferença em segundos do UTC.',
  time DATE COMMENT 'Data da previsão.',
  temperature_2m_max DOUBLE COMMENT 'Temperatura máxima prevista a 2m do solo (°C).',
  temperature_2m_min DOUBLE COMMENT 'Temperatura mínima prevista a 2m do solo (°C).',
  temperature_2m_mean DOUBLE COMMENT 'Temperatura média prevista a 2m do solo (°C).',
  precipitation_sum DOUBLE COMMENT 'Precipitação total prevista (mm).',
  wind_speed_10m_max DOUBLE COMMENT 'Velocidade máxima do vento prevista a 10m (km/h).',
  weather_code INT COMMENT 'Código WMO do tipo de clima previsto.',
  sunrise TIMESTAMP COMMENT 'Horário do nascer do sol.',
  sunset TIMESTAMP COMMENT 'Horário do pôr do sol.',
  generationtime_ms DOUBLE COMMENT 'Tempo de geração da previsão em milissegundos.'
)
COMMENT 'Tabela padronizada de previsão diária do tempo, pronta para análises e relatórios.'
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
  CAST(latitude AS DOUBLE) AS latitude,
  CAST(longitude AS DOUBLE) AS longitude,
  CAST(elevation AS DOUBLE) AS elevation,
  trim(timezone) AS timezone,
  trim(timezone_abbreviation) AS timezone_abbreviation,
  CAST(utc_offset_seconds AS INT) AS utc_offset_seconds,
  CAST(time AS DATE) AS time,
  CAST(temperature_2m_max AS DOUBLE) AS temperature_2m_max,
  CAST(temperature_2m_min AS DOUBLE) AS temperature_2m_min,
  CAST(temperature_2m_mean AS DOUBLE) AS temperature_2m_mean,
  CAST(precipitation_sum AS DOUBLE) AS precipitation_sum,
  CAST(wind_speed_10m_max AS DOUBLE) AS wind_speed_10m_max,
  CAST(weather_code AS INT) AS weather_code,
  CAST(sunrise AS TIMESTAMP) AS sunrise,
  CAST(sunset AS TIMESTAMP) AS sunset,
  CAST(generationtime_ms AS DOUBLE) AS generationtime_ms
FROM bronze_dev.ds_open_meteo.raw_daily_forecast;

-- COMMAND ----------

-- DBTITLE 1,Bronze para Silver: Localidades
CREATE OR REFRESH MATERIALIZED VIEW cleaned_localidades
(
  capital STRING COMMENT 'Nome da capital brasileira.',
  uf STRING COMMENT 'Sigla da unidade federativa (estado).',
  latitude DOUBLE COMMENT 'Latitude da localização.',
  longitude DOUBLE COMMENT 'Longitude da localização.'
)
COMMENT 'Tabela padronizada de localidades (capitais brasileiras) com suas coordenadas geográficas.'
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
  trim(capital) AS capital,
  upper(trim(uf)) AS uf,
  CAST(latitude AS DOUBLE) AS latitude,
  CAST(longitude AS DOUBLE) AS longitude
FROM bronze_dev.ds_open_meteo.auxiliar_localidades;
