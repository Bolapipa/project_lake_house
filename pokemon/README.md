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

 # | Task | Descrição | Endpoint PokeAPI |
---|------|-----------|------------------|
 1 | **ingest_pokedex** | Lista de Pokédex disponíveis (Nacional, Johto, etc) | `/api/v2/pokedex/` |
 2 | **ingest_pokemon** | Dados detalhados: stats, tipos, altura, peso, sprites | `/api/v2/pokemon/` |
 3 | **ingest_abilities** | Habilidades e seus efeitos | `/api/v2/ability/` |
 4 | **ingest_pokemon_genders** | Taxas de distribuição de gênero | `/api/v2/gender/` |
 5 | **ingest_pokemon_growth_rates** | Curvas de experiência para level-up | `/api/v2/growth-rate/` |
 6 | **ingest_pokemon_location_areas** | Locais de encontro na natureza | `/api/v2/location-area/` |
 7 | **ingest_pokemon_natures** | Naturezas que modificam stats | `/api/v2/nature/` |
 8 | **ingest_pokemon_pokeathlon_stats** | Stats para competições Pokeathlon | `/api/v2/pokeathlon-stat/` |
 9 | **ingest_egg_groups** | Grupos de compatibilidade para breeding | `/api/v2/egg-group/` |
 10 | **ingest_locations** | Regiões e locais do mundo Pokémon | `/api/v2/location/` |
 11 | **ingest_items** | Itens disponíveis nos jogos | `/api/v2/item/` |
 12 | **ingest_items_attributes** | Categorias e efeitos dos itens | `/api/v2/item-attribute/` |
 13 | **ingest_versions** | Versões de jogos Pokémon (Red, Blue, Gold, etc) | `/api/v2/version/` |

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

### Parâmetros do Job

 Parâmetro | Descrição | Padrão (dev) | Produção |
-----------|-----------|--------------|----------|
 `environment` | Ambiente de execução | `dev` | `prod` |
 `catalog` | Catálogo Bronze de destino | `bronze_dev` | `bronze_prod` |
 `schema` | Schema de destino | `ds_pokemon` | `ds_pokemon` |

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
* `tb_pokedex_bronze` - Listas de Pokédex
* `tb_pokemon_bronze` - Dados brutos de Pokémon
* `tb_abilities_bronze` - Habilidades brutas
* `tb_genders_bronze` - Gêneros brutos
* `tb_growth_rates_bronze` - Taxas de crescimento brutas
* `tb_location_areas_bronze` - Áreas de localização brutas
* `tb_natures_bronze` - Naturezas brutas
* `tb_pokeathlon_stats_bronze` - Stats Pokeathlon brutos
* `tb_egg_groups_bronze` - Grupos de ovos brutos
* `tb_locations_bronze` - Localizações brutas
* `tb_items_bronze` - Itens brutos
* `tb_items_attributes_bronze` - Atributos de itens brutos
* `raw_pokemon_versions` - Versões de jogos Pokémon brutas

### Camada Silver (silver_dev.ds_pokemon)
* `tb_pokedex` - Pokédex normalizadas
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
* `tb_versions` - Versões de jogos limpas e categorizadas

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

## Casos de Uso

Este dataset pode ser usado para:

* **Análises estatísticas** de Pokémon (distribuição de tipos, stats médios por geração)
* **Machine Learning** (predição de tipos, classificação de força)
* **Visualizações** interativas (mapas de localização, gráficos de evolução)
* **Simulações** de batalhas e estratégias competitivas
* **Aplicações** de Pokédex personalizadas
* **Busca e filtragem** avançada de Pokémon
* **Análise histórica** por versão de jogo

---

## Referências

* [PokeAPI Documentation](https://pokeapi.co/docs/v2)
* [Databricks Asset Bundles Documentation](https://docs.databricks.com/dev-tools/bundles/)
* [Delta Live Tables Documentation](https://docs.databricks.com/delta-live-tables/)

---

**Última atualização**: $(date +"%Y-%m-%d")  
**Mantido por**: delacortearthur@gmail.com  
**Fonte de dados**: PokeAPI (https://pokeapi.co/)
