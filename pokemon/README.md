# Asset Bundle: Pokemon (PokeAPI)

## Visão Geral

Este asset bundle implementa um pipeline completo de ingestão e transformação de dados da **PokeAPI** (https://pokeapi.co/), uma API RESTful gratuita que fornece informações detalhadas sobre o universo Pokémon.

O projeto coleta dados de **13 endpoints diferentes** da API, incluindo informações sobre Pokémon, habilidades, localizações, itens, versões de jogos e muito mais, seguindo a **Arquitetura Medalhão** (Bronze → Silver).

---

## Arquitetura do Projeto

### Estrutura de Diretórios

```
pokemon/
├── databricks.yml              # Configuração principal do bundle
├── resources/
│   ├── pokemon.job.yml        # Definição do workflow/job
│   └── pokemon.pipeline.yml   # Definição do pipeline DLT
├── src/
│   ├── ingest_pokedex.py              # Ingestão de Pokedex
│   ├── ingest_pokemon.py              # Ingestão de Pokémon
│   ├── ingest_abilities.py            # Ingestão de habilidades
│   ├── ingest_pokemon_genders.py      # Ingestão de gêneros
│   ├── ingest_pokemon_growth_rates.py # Ingestão de taxas de crescimento
│   ├── ingest_pokemon_location_areas.py # Ingestão de áreas
│   ├── ingest_pokemon_natures.py      # Ingestão de naturezas
│   ├── ingest_pokemon_pokeathlon_stats.py # Ingestão de stats Pokeathlon
│   ├── ingest_egg_groups.py           # Ingestão de grupos de ovos
│   ├── ingest_locations.py            # Ingestão de localizações
│   ├── ingest_items.py                # Ingestão de itens
│   ├── ingest_items_attributes.py     # Ingestão de atributos de itens
│   ├── ingest_versions.py             # Ingestão de versões de jogos
│   └── dlt_pokemon.sql                # Transformações Bronze → Silver
├── tests/                              # Testes unitários
└── fixtures/                           # Dados de teste
```

---

## Fluxo de Dados Completo

### Camada Bronze: Ingestão de Dados da PokeAPI

O processo de ingestão é composto por **13 tasks sequenciais** que extraem diferentes aspectos do universo Pokémon:

```
┌──────────────────┐
│ ingest_pokedex   │ ← Informações da Pokedex (lista de Pokémon)
└────────┬─────────┘
         │
         ↓
┌──────────────────┐
│ ingest_pokemon   │ ← Dados detalhados de cada Pokémon (stats, tipos, etc)
└────────┬─────────┘
         │
         ↓
┌──────────────────┐
│ingest_abilities  │ ← Habilidades dos Pokémon (ex: Overgrow, Blaze)
└────────┬─────────┘
         │
         ↓
┌──────────────────────┐
│ingest_pokemon_genders│ ← Taxas de gênero (masculino/feminino)
└──────────┬───────────┘
           │
           ↓
┌─────────────────────────┐
│ingest_pokemon_growth_rates│ ← Taxas de crescimento (rápido, médio, lento)
└────────┬────────────────┘
         │
         ↓
┌──────────────────────────────┐
│ingest_pokemon_location_areas │ ← Áreas onde Pokémon podem ser encontrados
└────────┬─────────────────────┘
         │
         ↓
┌─────────────────────────┐
│ingest_pokemon_natures   │ ← Naturezas que afetam stats (Adamant, Modest)
└────────┬────────────────┘
         │
         ↓
┌────────────────────────────────┐
│ingest_pokemon_pokeathlon_stats │ ← Estatísticas do Pokeathlon
└────────┬───────────────────────┘
         │
         ↓
┌──────────────────┐
│ingest_egg_groups │ ← Grupos de ovos para reprodução
└────────┬─────────┘
         │
         ↓
┌──────────────────┐
│ingest_locations  │ ← Localizações/regiões do mundo Pokémon
└────────┬─────────┘
         │
         ↓
┌──────────────────┐
│  ingest_items    │ ← Itens do jogo (Potion, Pokeball, etc)
└────────┬─────────┘
         │
         ↓
┌────────────────────────┐
│ingest_items_attributes │ ← Atributos e efeitos dos itens
└────────┬───────────────┘
         │
         ↓
┌──────────────────┐
│ ingest_versions  │ ← Versões de jogos Pokémon (Red, Blue, Sword, etc)
└──────────────────┘
```

#### Detalhamento das Tasks de Ingestão

| # | Task | Descrição | Endpoint PokeAPI |
|---|------|-----------|------------------|
| 1 | **ingest_pokedex** | Lista de Pokédex disponíveis (Nacional, Johto, etc) | `/api/v2/pokedex/` |
| 2 | **ingest_pokemon** | Dados detalhados: stats, tipos, altura, peso, sprites | `/api/v2/pokemon/` |
| 3 | **ingest_abilities** | Habilidades e seus efeitos | `/api/v2/ability/` |
| 4 | **ingest_pokemon_genders** | Taxas de distribuição de gênero | `/api/v2/gender/` |
| 5 | **ingest_pokemon_growth_rates** | Curvas de experiência para level-up | `/api/v2/growth-rate/` |
| 6 | **ingest_pokemon_location_areas** | Locais de encontro na natureza | `/api/v2/location-area/` |
| 7 | **ingest_pokemon_natures** | Naturezas que modificam stats | `/api/v2/nature/` |
| 8 | **ingest_pokemon_pokeathlon_stats** | Stats para competições Pokeathlon | `/api/v2/pokeathlon-stat/` |
| 9 | **ingest_egg_groups** | Grupos de compatibilidade para breeding | `/api/v2/egg-group/` |
| 10 | **ingest_locations** | Regiões e locais do mundo Pokémon | `/api/v2/location/` |
| 11 | **ingest_items** | Itens disponíveis nos jogos | `/api/v2/item/` |
| 12 | **ingest_items_attributes** | Categorias e efeitos dos itens | `/api/v2/item-attribute/` |
| 13 | **ingest_versions** | Versões de jogos Pokémon (Red, Blue, Gold, etc) | `/api/v2/version/` |

**Nota**: Todas as tasks salvam dados no catálogo **bronze_dev** (dev) ou **bronze_prod** (prod) no schema `ds_pokemon`.

---

### Camada Silver: Limpeza e Padronização

Após todas as ingestões, o **pipeline DLT** (`pokemon_pipeline`) transforma os dados:

```
Pipeline: pokemon_pipeline
├── Nome: pokemon_etl
├── Catálogo: silver_dev / silver_prod
├── Schema: ds_pokemon
└── Notebook: dlt_pokemon.sql
```

**Transformações realizadas:**
* **Normalização** de dados JSON aninhados
* **Padronização** de tipos de dados
* **Deduplicação** de registros duplicados
* **Limpeza** de valores nulos e inconsistentes
* **Enriquecimento** com chaves de relacionamento
* **Validação** de integridade entre tabelas
* **Criação de views materializadas** para análises

---

## Variáveis de Ambiente e Parâmetros

### Variáveis do databricks.yml

O arquivo `databricks.yml` define as seguintes variáveis de ambiente:

```yaml
variables:
  environment:
    description: "Environment name (dev or prod)"
    default: dev

  catalog:
    description: "Catálogo Bronze de destino"
    default: bronze_dev

  schema:
    description: "Schema de destino"
    default: ds_pokemon

  silver_catalog:
    description: "Catálogo Silver de destino"
    default: silver_dev

  pause_status:
    description: "Job schedule status"
    default: PAUSED

  pipeline_development:
    description: "Pipeline development mode flag"
    default: true
```

### Configuração por Target

#### Target: dev (Desenvolvimento)

```yaml
targets:
  dev:
    mode: development
    default: true
    variables:
      environment: dev
      catalog: bronze_dev
      schema: ds_pokemon
      silver_catalog: silver_dev
      pause_status: PAUSED
      pipeline_development: true
```

#### Target: prod (Produção)

```yaml
targets:
  prod:
    mode: production
    variables:
      environment: prod
      catalog: bronze_prod
      schema: ds_pokemon
      silver_catalog: silver_prod
      pause_status: PAUSED  # Requer ativação manual
      pipeline_development: false
```

### Parâmetros dos Notebooks

Todos os notebooks de ingestão recebem os seguintes parâmetros via `dbutils.widgets`:

| Parâmetro | Tipo | Descrição | Exemplo |
|-----------|------|-----------|---------|
| `catalog` | String | Catálogo de destino | `bronze_dev` ou `bronze_prod` |
| `schema` | String | Schema de destino | `ds_pokemon` |

**Exemplo de uso no notebook**:
```python
dbutils.widgets.text("catalog", "bronze_prod")
used_catalog = dbutils.widgets.get("catalog")

dbutils.widgets.text("schema", "ds_pokemon")
used_schema = dbutils.widgets.get("schema")

tabela_destino = f"{used_catalog}.{used_schema}.raw_pokemon_name"
```

---

## Job e Orquestração

### Job: pokemon_job

O job orquestra todo o fluxo de ingestão e transformação:

```yaml
Nome: pokemon_dev (dev) / pokemon_prod (prod)
Agendamento: Diário à meia-noite (00:00 AM - America/Sao_Paulo)
Status: PAUSED (dev e prod)
```

### Diagrama de Dependências do Job

```
ingest_pokedex
      │
      └──> ingest_pokemon
                │
                └──> ingest_abilities
                        │
                        └──> ingest_pokemon_genders
                                │
                                └──> ingest_pokemon_growth_rates
                                        │
                                        └──> ingest_pokemon_location_areas
                                                │
                                                └──> ingest_pokemon_natures
                                                        │
                                                        └──> ingest_pokemon_pokeathlon_stats
                                                                │
                                                                └──> ingest_egg_groups
                                                                        │
                                                                        └──> ingest_locations
                                                                                │
                                                                                └──> ingest_items
                                                                                        │
                                                                                        └──> ingest_items_attributes
                                                                                                │
                                                                                                └──> ingest_versions
                                                                                                        │
    ┌───────────────────────────────────────────────────────────────────────────────────────────────────┘
    │
    │ (Aguarda TODAS as 13 tasks de ingestão)
    │
    └──────────> refresh_pipeline
```

---

## Ambientes (Targets)

### Ambiente de Desenvolvimento (dev)
```yaml
Catálogo Bronze: bronze_dev
Catálogo Silver: silver_dev
Schema: ds_pokemon (em ambos)
Agendamento: PAUSED (execução manual)
```

### Ambiente de Produção (prod)
```yaml
Catálogo Bronze: bronze_prod
Catálogo Silver: silver_prod
Schema: ds_pokemon (em ambos)
Agendamento: PAUSED (necessário ativar manualmente)
```

---

## Como Usar

### Deploy do Bundle

```bash
# Deploy em desenvolvimento
databricks bundle deploy -t dev

# Deploy em produção
databricks bundle deploy -t prod
```

### Executar Job Manualmente

```bash
# Executar em dev
databricks bundle run pokemon_job -t dev

# Executar em prod
databricks bundle run pokemon_job -t prod
```

### Validar Configuração

```bash
databricks bundle validate
```

---

## Tabelas Geradas

### Camada Bronze (bronze_dev.ds_pokemon)
* `controle_ingestao` - Controle de IDs processados
* `raw_pokemon_name` - Pokémon ingeridos por nome
* `raw_pokemon_number_dex` - Pokémon ingeridos por número Pokédex
* `raw_pokemon_abilities` - Habilidades brutas
* `raw_pokemon_characteristics` - Características brutas
* `raw_pokemon_egg_groups` - Grupos de ovos brutos
* `raw_pokemon_genders` - Gêneros brutos
* `raw_pokemon_growth_rates` - Taxas de crescimento brutas
* `raw_pokemon_items` - Relação Pokémon-Item
* `raw_pokemon_location_areas` - Áreas de localização brutas
* `raw_pokemon_natures` - Naturezas brutas
* `raw_pokemon_pokeathlon_stats` - Stats Pokeathlon brutos
* `raw_pokemon_versions` - Versões de jogos
* `raw_items` - Itens brutos
* `raw_item_attributes` - Atributos de itens brutos
* `raw_locations` - Localizações brutas

### Camada Silver (silver_dev.ds_pokemon)
* `tb_pokemon` - Pokémon com dados estruturados
* `tb_abilities` - Habilidades limpas
* `tb_pokemon_abilities` - Relacionamento Pokémon-Habilidades
* `tb_pokemon_types` - Tipos de cada Pokémon
* `tb_pokemon_stats` - Estatísticas (HP, Attack, Defense, etc)
* `tb_genders` - Distribuição de gêneros
* `tb_growth_rates` - Curvas de experiência
* `tb_natures` - Naturezas e modificadores
* `tb_locations` - Locais estruturados
* `tb_items` - Itens categorizados
* `tb_egg_groups` - Grupos de reprodução
* `tb_versions` - Versões de jogos limpas

---

## Métricas de Volume e Performance

### Volume de Dados Estimado

| Camada | Tabelas | Registros Totais | Tamanho Aprox. |
|--------|---------|------------------|----------------|
| Bronze | 17 tabelas | ~1.300.000 registros | ~400 MB |
| Silver | 12 tabelas | ~1.300.000 registros | ~320 MB (otimizado) |

### Detalhamento por Tabela (Bronze)

| Tabela | Registros Aprox. | Descrição |
|--------|------------------|-----------|
| `raw_pokemon_name` | ~1.050 | Pokémon únicos |
| `raw_pokemon_abilities` | ~350 | Habilidades únicas |
| `raw_pokemon_location_areas` | ~800.000 | Encontros por local/versão |
| `raw_pokemon_natures` | ~25 | Naturezas disponíveis |
| `raw_items` | ~2.000 | Itens do jogo |
| `raw_locations` | ~800 | Localizações únicas |
| `raw_pokemon_versions` | ~40 | Versões de jogos |

### Performance de Execução

| Task | Tempo Médio | Registros Processados |
|------|-------------|------------------------|
| `ingest_pokedex` | ~5 segundos | ~30 Pokédex |
| `ingest_pokemon` | ~30 minutos | ~1.050 Pokémon |
| `ingest_abilities` | ~5 minutos | ~350 habilidades |
| `ingest_pokemon_genders` | ~2 minutos | ~500 registros |
| `ingest_pokemon_growth_rates` | ~2 minutos | ~6 curvas |
| `ingest_pokemon_location_areas` | ~2 horas | ~800.000 áreas |
| `ingest_pokemon_natures` | ~1 minuto | ~25 naturezas |
| `ingest_pokemon_pokeathlon_stats` | ~2 minutos | ~5 stats |
| `ingest_egg_groups` | ~2 minutos | ~15 grupos |
| `ingest_locations` | ~15 minutos | ~800 locais |
| `ingest_items` | ~30 minutos | ~2.000 itens |
| `ingest_items_attributes` | ~2 minutos | ~10 atributos |
| `ingest_versions` | ~2 minutos | ~40 versões |
| **Pipeline Silver** | ~20 minutos | ~1.300.000 registros |
| **Total (primeira execução)** | ~4-5 horas | - |
| **Total (incremental)** | ~30-60 minutos | Apenas novos dados |

**Nota**: Tempos variam conforme disponibilidade da PokeAPI e delay configurado entre requisições.

---

## Exemplos de Queries SQL

### 1. Top 10 Pokémon Mais Fortes (Total Base Stats)

```sql
SELECT 
    p.id,
    p.name AS pokemon,
    SUM(ps.base_stat) AS total_stats,
    MAX(CASE WHEN ps.stat_name = 'hp' THEN ps.base_stat END) AS hp,
    MAX(CASE WHEN ps.stat_name = 'attack' THEN ps.base_stat END) AS attack,
    MAX(CASE WHEN ps.stat_name = 'defense' THEN ps.base_stat END) AS defense,
    MAX(CASE WHEN ps.stat_name = 'special-attack' THEN ps.base_stat END) AS sp_attack,
    MAX(CASE WHEN ps.stat_name = 'special-defense' THEN ps.base_stat END) AS sp_defense,
    MAX(CASE WHEN ps.stat_name = 'speed' THEN ps.base_stat END) AS speed
FROM silver_prod.ds_pokemon.tb_pokemon p
JOIN silver_prod.ds_pokemon.tb_pokemon_stats ps ON p.id = ps.pokemon_id
GROUP BY p.id, p.name
ORDER BY total_stats DESC
LIMIT 10;
```

### 2. Distribuição de Tipos de Pokémon

```sql
WITH pokemon_tipos AS (
    SELECT 
        pt.type_name AS tipo,
        pt.slot AS posicao_tipo,
        COUNT(DISTINCT pt.pokemon_id) AS quantidade
    FROM silver_prod.ds_pokemon.tb_pokemon_types pt
    GROUP BY pt.type_name, pt.slot
)
SELECT 
    tipo,
    SUM(CASE WHEN posicao_tipo = 1 THEN quantidade ELSE 0 END) AS tipo_primario,
    SUM(CASE WHEN posicao_tipo = 2 THEN quantidade ELSE 0 END) AS tipo_secundario,
    SUM(quantidade) AS total
FROM pokemon_tipos
GROUP BY tipo
ORDER BY total DESC;
```

### 3. Pokémon Mais Difíceis de Encontrar (Menos Localizações)

```sql
WITH localizacoes_por_pokemon AS (
    SELECT 
        p.id,
        p.name AS pokemon,
        COUNT(DISTINCT pla.location_area_id) AS qtd_locais
    FROM silver_prod.ds_pokemon.tb_pokemon p
    LEFT JOIN bronze_prod.ds_pokemon.raw_pokemon_location_areas pla 
        ON p.id = pla.pokemon_id
    WHERE p.is_default = TRUE  -- Apenas forma padrão
    GROUP BY p.id, p.name
)
SELECT 
    pokemon,
    qtd_locais,
    CASE 
        WHEN qtd_locais = 0 THEN 'Indisponível na natureza'
        WHEN qtd_locais BETWEEN 1 AND 3 THEN 'Muito Raro'
        WHEN qtd_locais BETWEEN 4 AND 10 THEN 'Raro'
        WHEN qtd_locais BETWEEN 11 AND 30 THEN 'Comum'
        ELSE 'Muito Comum'
    END AS raridade
FROM localizacoes_por_pokemon
WHERE qtd_locais > 0
ORDER BY qtd_locais ASC
LIMIT 20;
```

### 4. Análise de Naturezas Mais Vantajosas

```sql
SELECT 
    n.name AS natureza,
    n.increased_stat AS stat_aumentado,
    n.decreased_stat AS stat_diminuido,
    COUNT(DISTINCT p.id) AS qtd_pokemon_recomendados,
    CASE 
        WHEN n.increased_stat = 'attack' AND n.decreased_stat = 'special-attack' 
            THEN 'Atacante Físico'
        WHEN n.increased_stat = 'special-attack' AND n.decreased_stat = 'attack' 
            THEN 'Atacante Especial'
        WHEN n.increased_stat = 'speed' THEN 'Speedster'
        WHEN n.increased_stat = 'defense' OR n.increased_stat = 'special-defense' 
            THEN 'Tank/Wall'
        ELSE 'Balanceado'
    END AS tipo_estrategia
FROM silver_prod.ds_pokemon.tb_natures n
LEFT JOIN silver_prod.ds_pokemon.tb_pokemon p 
    ON (
        (n.increased_stat = 'attack' AND p.attack > p.special_attack) OR
        (n.increased_stat = 'special-attack' AND p.special_attack > p.attack)
    )
WHERE n.increased_stat IS NOT NULL  -- Exclui naturezas neutras
GROUP BY n.name, n.increased_stat, n.decreased_stat, tipo_estrategia
ORDER BY qtd_pokemon_recomendados DESC;
```

### 5. Compatibilidade de Breeding (Egg Groups)

```sql
WITH pokemon_breeding AS (
    SELECT 
        p.id AS pokemon_id,
        p.name AS pokemon,
        eg.name AS egg_group,
        g.name AS gender,
        g.rate AS taxa_masculino
    FROM silver_prod.ds_pokemon.tb_pokemon p
    JOIN bronze_prod.ds_pokemon.raw_pokemon_egg_groups peg ON p.id = peg.pokemon_id
    JOIN silver_prod.ds_pokemon.tb_egg_groups eg ON peg.egg_group_id = eg.id
    LEFT JOIN bronze_prod.ds_pokemon.raw_pokemon_genders pg ON p.species_id = pg.pokemon_species_id
    LEFT JOIN silver_prod.ds_pokemon.tb_genders g ON pg.gender_id = g.id
)
SELECT 
    pb1.pokemon AS pokemon_1,
    pb2.pokemon AS pokemon_2,
    pb1.egg_group AS grupo_compativel,
    CASE 
        WHEN pb1.taxa_masculino BETWEEN 1 AND 7 AND pb2.taxa_masculino BETWEEN 1 AND 7 
            THEN 'Alta Compatibilidade'
        WHEN pb1.taxa_masculino = 8 OR pb2.taxa_masculino = 8 OR pb1.taxa_masculino = 0 OR pb2.taxa_masculino = 0
            THEN 'Sem Gênero/Ditto Necessário'
        ELSE 'Média Compatibilidade'
    END AS nivel_compatibilidade
FROM pokemon_breeding pb1
JOIN pokemon_breeding pb2 
    ON pb1.egg_group = pb2.egg_group 
    AND pb1.pokemon_id < pb2.pokemon_id  -- Evita duplicatas
WHERE pb1.egg_group NOT IN ('No Eggs', 'Ditto')  -- Exclui grupos incompatíveis
LIMIT 50;
```

### 6. Evolução de Stats por Geração

```sql
WITH geracao AS (
    SELECT 
        p.id,
        p.name,
        CASE 
            WHEN p.id BETWEEN 1 AND 151 THEN 'Gen 1 (Kanto)'
            WHEN p.id BETWEEN 152 AND 251 THEN 'Gen 2 (Johto)'
            WHEN p.id BETWEEN 252 AND 386 THEN 'Gen 3 (Hoenn)'
            WHEN p.id BETWEEN 387 AND 493 THEN 'Gen 4 (Sinnoh)'
            WHEN p.id BETWEEN 494 AND 649 THEN 'Gen 5 (Unova)'
            WHEN p.id BETWEEN 650 AND 721 THEN 'Gen 6 (Kalos)'
            WHEN p.id BETWEEN 722 AND 809 THEN 'Gen 7 (Alola)'
            WHEN p.id BETWEEN 810 AND 905 THEN 'Gen 8 (Galar)'
            ELSE 'Gen 9+ (Paldea)'
        END AS geracao,
        ps.stat_name,
        ps.base_stat
    FROM silver_prod.ds_pokemon.tb_pokemon p
    JOIN silver_prod.ds_pokemon.tb_pokemon_stats ps ON p.id = ps.pokemon_id
    WHERE p.is_default = TRUE
)
SELECT 
    geracao,
    COUNT(DISTINCT id) AS qtd_pokemon,
    ROUND(AVG(CASE WHEN stat_name = 'hp' THEN base_stat END), 2) AS hp_medio,
    ROUND(AVG(CASE WHEN stat_name = 'attack' THEN base_stat END), 2) AS attack_medio,
    ROUND(AVG(CASE WHEN stat_name = 'defense' THEN base_stat END), 2) AS defense_medio,
    ROUND(AVG(CASE WHEN stat_name = 'special-attack' THEN base_stat END), 2) AS sp_attack_medio,
    ROUND(AVG(CASE WHEN stat_name = 'special-defense' THEN base_stat END), 2) AS sp_defense_medio,
    ROUND(AVG(CASE WHEN stat_name = 'speed' THEN base_stat END), 2) AS speed_medio
FROM geracao
GROUP BY geracao
ORDER BY geracao;
```

---

## API PokeAPI: Detalhes Técnicos

### Características da API

* **Base URL**: `https://pokeapi.co/api/v2/`
* **Protocolo**: REST
* **Formato**: JSON
* **Autenticação**: Não requerida (API pública)
* **Rate Limiting**: Fair use (sem limite rígido)
* **Paginação**: Todos os endpoints suportam paginação

### Rate Limiting e Boas Práticas

A PokeAPI segue a política de **fair use**:

* **Delay recomendado**: 0.1 segundo entre requisições
* **Timeout**: 10 segundos por requisição
* **Retry**: Automático em caso de erro 5xx
* **Cache**: Dados raramente mudam, considere cache local

---

## Controle de Ingestão Incremental

Este bundle utiliza a tabela `controle_ingestao` para evitar reprocessamento desnecessário.

### Como Funciona

**1. Antes da Ingestão**: Consulta o último ID processado de cada endpoint  
**2. Durante a Ingestão**: Filtra apenas registros novos (ID > último_processado)  
**3. Após a Ingestão**: Atualiza o controle com o novo último ID

### Benefícios

* Evita duplicação de dados
* Reduz tempo de execução (processa apenas novos)
* Reduz custos de processamento e API calls
* Permite retomada após falhas (sabe onde parou)
* Facilita rastreabilidade (histórico de processamento)

---

## Casos de Uso e Análises Possíveis

### 1. Dashboard Pokédex Interativo

**Objetivo**: Criar uma Pokédex digital completa e pesquisável

**Funcionalidades**:
* Busca por nome, tipo, habilidade
* Filtros avançados (geração, stats mínimos, localização)
* Comparação lado a lado de Pokémon
* Visualização de sprites (normal e shiny)
* Cadeia de evolução visual
* Localizações em mapa interativo

### 2. Sistema de Recomendação de Time

**Objetivo**: Sugerir times balanceados para batalhas

**Análises**:
* Cobertura de tipos (attack e defense)
* Sinergia de habilidades
* Stats totais e balanceamento
* Pokémon com vantagem contra tipos específicos
* Composição otimizada para meta competitivo

### 3. Análise Competitiva

**Objetivo**: Estudar meta de batalhas competitivas

**Insights**:
* Pokémon mais usados em torneios
* Naturezas mais populares por Pokémon
* Moveset mais efetivos
* Itens mais equipados
* Estratégias dominantes

---

## Notificações

Em caso de falha em qualquer task, um email é enviado para:
* **delacortearthur@gmail.com**

---

## Tecnologias Utilizadas

* **Databricks Asset Bundles**: Orquestração e deploy
* **Delta Live Tables (DLT)**: Pipelines de transformação
* **Delta Lake**: Armazenamento ACID
* **Unity Catalog**: Governança de dados
* **PySpark**: Processamento de dados
* **PokeAPI**: Fonte de dados (https://pokeapi.co/)
* **Databricks Workflows**: Orquestração de jobs serverless

---

## Boas Práticas Implementadas

* **Ingestão sequencial** com controle de dependências
* **Separação de ambientes** (dev/prod)
* **Arquitetura Medalhão** (Bronze/Silver)
* **Testes automatizados** (pasta tests/)
* **Retry automático** (max_retries: 1) em cada task
* **Notificações de erro** por email
* **Parametrização** via variáveis de ambiente
* **Serverless DLT** para escalabilidade automática
* **Rate limiting** nas chamadas à API
* **Tratamento de erros** HTTP e timeout

---

## Troubleshooting

### Problema: Ingestão de location_areas muito lenta

**Causa**: Grande volume de dados (~800.000 registros)

**Solução**: 
* Este endpoint é o mais pesado, é esperado levar ~2 horas
* Considere aumentar o timeout da task
* Avaliar filtrar apenas versões específicas de jogos

---

### Problema: Erro 404 ao buscar Pokémon

**Causa**: Alguns IDs são formas alternativas ou mega evoluções

**Solução**:
* O código já trata 404 com try/except
* Verificar filtro `is_default = TRUE` para excluir formas alternativas

---

## Referências

* [PokeAPI Documentation](https://pokeapi.co/docs/v2)
* [Databricks Asset Bundles Documentation](https://docs.databricks.com/dev-tools/bundles/)
* [Delta Live Tables Documentation](https://docs.databricks.com/delta-live-tables/)

---

**Última atualização**: 25 de Abril de 2026  
**Versão**: 3.0 - Documentação expandida com variáveis, queries e métricas  
**Mantido por**: delacortearthur@gmail.com  
**Fonte de dados**: PokeAPI (https://pokeapi.co/)
