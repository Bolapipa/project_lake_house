# Documentação: Genie Spaces

## O que é o Genie Space?

O **Genie Space** é uma funcionalidade inteligente do Databricks que permite criar assistentes de IA especializados em conjuntos específicos de dados. Ele funciona como um analista de dados personalizado que conhece profundamente suas tabelas e pode responder perguntas em linguagem natural, gerar análises e criar visualizações automaticamente.

### Para que serve?

O Genie Space serve para:

* **Democratizar o acesso aos dados**: Usuários sem conhecimento técnico em SQL podem fazer perguntas complexas em linguagem natural
* **Acelerar análises**: Reduz o tempo necessário para explorar dados e gerar insights
* **Garantir consistência**: Todos os usuários trabalham com as mesmas definições de métricas e dimensões
* **Facilitar a governança**: Cada Space tem acesso apenas aos dados específicos configurados, respeitando permissões do Unity Catalog

### Como funciona?

1. **Configuração**: Você seleciona tabelas do Unity Catalog relacionadas a um domínio específico de negócio
2. **IA Contextualizada**: O Genie aprende a estrutura, relacionamentos e significado dos seus dados
3. **Interação Natural**: Usuários fazem perguntas em português e recebem respostas com dados, gráficos e insights
4. **Aprendizado Contínuo**: O Genie melhora suas respostas com base no feedback e uso

---

## Arquitetura dos Nossos Genie Spaces

Nossa implementação segue uma arquitetura de dados moderna em camadas (Lakehouse):

```
┌─────────────────────────────────────────────────────┐
│                  GENIE SPACES                        │
│          (Camada de Análise Conversacional)          │
├─────────────────────────────────────────────────────┤
│                                                      │
│   Pokémon   Câmara   Carros   Open Meteo   IBGE    │
│                                                      │
└─────────────────────────────────────────────────────┘
                         ↓
┌─────────────────────────────────────────────────────┐
│              CAMADA GOLD (gold_prod)                 │
│         Dados Modelados para Negócio                 │
│    Modelo Dimensional (Star Schema / Snowflake)      │
└─────────────────────────────────────────────────────┘
                         ↓
┌─────────────────────────────────────────────────────┐
│           CAMADA SILVER (silver_prod)                │
│         Dados Limpos e Padronizados                  │
│   Transformações de Qualidade e Conformidade         │
└─────────────────────────────────────────────────────┘
                         ↓
┌─────────────────────────────────────────────────────┐
│            CAMADA RAW (raw_prod)                     │
│      Dados Brutos das APIs e Fontes Externas         │
└─────────────────────────────────────────────────────┘
```

### Por que configuramos assim?

#### Separação por Domínio de Negócio
Cada Genie Space é especializado em um domínio específico:
* **Foco**: Permite que o Genie entenda profundamente o contexto de cada área
* **Performance**: Consultas mais rápidas ao trabalhar apenas com dados relevantes
* **Segurança**: Acesso granular por domínio de negócio

#### Uso de Dados Gold e Silver
Configuramos os Spaces para utilizar dados das camadas Silver e Gold:
* **Qualidade**: Dados já validados, limpos e sem inconsistências
* **Modelo Dimensional**: Estruturas otimizadas para análise (fatos e dimensões)
* **Documentação Rica**: Colunas com comentários explicativos que ajudam o Genie a entender o significado

---

## 1. Genie Space Pokémon

### Objetivo
Análise de dados do universo Pokémon, incluindo características, habilidades, tipos, itens e taxas de crescimento.

### Fonte de Dados
**Catálogo**: `gold_prod.dm_pokemon`  
**API Origem**: PokeAPI (https://pokeapi.co/)

### Tabelas Configuradas

#### Dimensões

**`dim_pokemon`**
* **Propósito**: Cadastro principal de todos os Pokémon
* **Colunas**:
  - `id_pokemon`: Número da Pokédex
  - `nome_pokemon`: Nome do Pokémon

**`dim_ability`**
* **Propósito**: Habilidades que os Pokémon podem possuir
* **Colunas**:
  - `id_ability`: Identificador único da habilidade
  - `nome_ability`: Nome da habilidade

**`dim_nature`**
* **Propósito**: Naturezas que afetam os atributos dos Pokémon
* **Colunas**:
  - `id_nature`: Identificador único da natureza
  - `nome_nature`: Nome da natureza

**`dim_location`**
* **Propósito**: Localizações e regiões do jogo
* **Colunas**:
  - `id_location`: Identificador único da localização
  - `nome_location`: Nome da localização/região

**`dim_item`**
* **Propósito**: Itens disponíveis no universo Pokémon
* **Colunas**:
  - `id_item`: Identificador único do item
  - `nome_item`: Nome do item

**`dim_growth_rate`**
* **Propósito**: Taxas de crescimento e evolução dos Pokémon
* **Colunas**:
  - Informações sobre velocidade de evolução

### Exemplos de Perguntas
* "Quais são os Pokémon do tipo Fogo?"
* "Liste as habilidades mais comuns entre Pokémon lendários"
* "Mostre a distribuição de Pokémon por taxa de crescimento"

---

## 2. Genie Space Câmara dos Deputados

### Objetivo
Análise de dados da Câmara dos Deputados do Brasil, incluindo despesas parlamentares, votações, deputados e fornecedores.

### Fonte de Dados
**Catálogo**: `gold_prod.dm_camara_deputados`  
**API Origem**: Dados Abertos da Câmara dos Deputados

### Tabelas Configuradas

#### Dimensões

**`dim_deputado`**
* **Propósito**: Cadastro de deputados federais
* **Colunas**:
  - `sk_deputado`: Chave substituta
  - `id`: ID oficial do deputado
  - `nome`: Nome completo
  - `sigla_partido`: Partido político
  - `sigla_uf`: Estado que representa
  - `id_legislatura`: Legislatura atual
  - `url_foto`: Foto oficial
  - `email`: Contato

**`dim_partido`**
* **Propósito**: Partidos políticos brasileiros
* **Colunas**:
  - `id`: Identificador do partido
  - `sigla`: Sigla (PT, PSDB, etc.)
  - `nome`: Nome completo do partido
  - `status`: Situação atual

**`dim_tipo_despesa`**
* **Propósito**: Categorias de gastos parlamentares
* **Colunas**:
  - `sk_tipo_despesa`: Chave substituta
  - `tipo_despesa`: Descrição (ex: Combustíveis, Passagens Aéreas)
  - `cod_tipo_documento`: Código do tipo de documento
  - `tipo_documento`: Descrição do documento fiscal

**`dim_fornecedor`**
* **Propósito**: Empresas e prestadores de serviço
* **Colunas**:
  - Dados dos fornecedores que recebem pagamentos

**`dim_orgao`**
* **Propósito**: Órgãos e comissões da Câmara
* **Colunas**:
  - Informações sobre comissões e órgãos internos

**`dim_tempo`**
* **Propósito**: Dimensão temporal para análises cronológicas
* **Colunas**:
  - Data, ano, mês, trimestre, etc.

#### Fatos

**`fact_despesas`**
* **Propósito**: Registro detalhado de todas as despesas parlamentares
* **Colunas**:
  - `sk_deputado`: Deputado que realizou a despesa
  - `sk_data`: Data da despesa
  - `sk_tipo_despesa`: Tipo de gasto
  - `sk_fornecedor`: Quem recebeu o pagamento
  - `cod_documento`: Código único do documento
  - `valor_documento`: Valor bruto em reais
  - `valor_liquido`: Valor pago ao fornecedor
  - `valor_glosa`: Valor não reembolsado
  - `url_documento`: Link para o documento fiscal

**`fact_votacoes`**
* **Propósito**: Registro de votações no plenário
* **Colunas**:
  - `sk_votacao`: Identificador da votação
  - `sk_orgao`: Órgão responsável
  - `sk_data`: Data da votação
  - `proposicao_cod_tipo`: Tipo de proposição
  - `proposicao_numero`: Número da proposição
  - `aprovacao`: Resultado (aprovado/rejeitado)
  - `objeto_votacao`: Descrição do que foi votado

### Exemplos de Perguntas
* "Qual deputado teve maior gasto com passagens aéreas em 2025?"
* "Mostre a evolução das despesas por partido nos últimos 12 meses"
* "Quais foram as votações mais importantes de abril?"
* "Liste os fornecedores que mais receberam pagamentos"

---

## 3. Genie Space Carros FIPE

### Objetivo
Análise de preços e características de veículos segundo a Tabela FIPE (Fundação Instituto de Pesquisas Econômicas).

### Fonte de Dados
**Catálogo**: `gold_prod.dm_carros`  
**API Origem**: FIPE (https://veiculos.fipe.org.br/)

### Tabelas Configuradas

#### Dimensões

**`dim_marca`**
* **Propósito**: Fabricantes de veículos
* **Colunas**:
  - `id_marca`: Identificador da marca
  - `nome_marca`: Nome (Fiat, Volkswagen, Chevrolet, etc.)

**`dim_modelo`**
* **Propósito**: Modelos de veículos por marca
* **Colunas**:
  - `id_marca`: Marca do veículo
  - `id_modelo`: Identificador do modelo
  - `nome_modelo`: Nome (Gol, Palio, Civic, etc.)

**`dim_veiculo`**
* **Propósito**: Versões específicas de veículos
* **Colunas**:
  - `sk_veiculo`: Chave substituta única
  - `id_marca` / `nome_marca`: Marca
  - `id_modelo` / `nome_modelo`: Modelo
  - `modelo_fipe`: Descrição completa da FIPE
  - `ano_modelo`: Ano do modelo
  - `combustivel`: Tipo (gasolina, álcool, diesel, flex, elétrico)
  - `codigo_fipe`: Código padronizado FIPE
  - `id_ano`: Identificador do ano

**`dim_referencia`**
* **Propósito**: Períodos de referência da tabela FIPE
* **Colunas**:
  - `id_referencia`: Identificador incremental
  - `ano_mes_referencia`: Formato ano_mes (ex: 2026_05)
  - `mes_referencia`: Mês
  - `ano_referencia`: Ano

#### Fatos

**`fact_preco_fipe`**
* **Propósito**: Histórico de preços de veículos
* **Colunas**:
  - `sk_veiculo`: Veículo específico
  - `id_referencia`: Período da consulta
  - `valor_fipe`: Preço em reais na tabela FIPE

### Exemplos de Perguntas
* "Qual é o preço médio de um Gol 2020 flex?"
* "Mostre a evolução do preço dos carros da marca Volkswagen"
* "Quais são os carros elétricos disponíveis e seus preços?"
* "Compare o preço médio entre carros a gasolina e flex em 2025"

---

## 4. Genie Space Open Meteo

### Objetivo
Análise de dados meteorológicos, incluindo clima histórico, previsões do tempo e qualidade do ar.

### Fonte de Dados
**Catálogo**: `gold_dev.dm_open_meteo`  
**API Origem**: Open-Meteo (https://open-meteo.com/)

### Tabelas Configuradas

#### Dimensões

**`dim_localidade`**
* **Propósito**: Localidades monitoradas
* **Colunas**:
  - Código e nome das cidades/regiões
  - Coordenadas geográficas

**`dim_tempo_clima`**
* **Propósito**: Dimensão temporal para dados climáticos
* **Colunas**:
  - Data, hora, dia da semana, estação do ano

#### Fatos

**`fact_clima_historico`**
* **Propósito**: Dados históricos de clima
* **Colunas**:
  - Temperatura, umidade, precipitação
  - Velocidade do vento, pressão atmosférica
  - Dados históricos por localidade e período

**`fact_previsao_tempo`**
* **Propósito**: Previsões meteorológicas
* **Colunas**:
  - Temperatura prevista (mínima e máxima)
  - Probabilidade de chuva
  - Condições climáticas esperadas

**`fact_qualidade_ar`**
* **Propósito**: Índices de qualidade do ar
* **Colunas**:
  - Índice de qualidade do ar (AQI)
  - Concentração de poluentes (PM2.5, PM10, O3, NO2, etc.)
  - Classificação da qualidade

### Exemplos de Perguntas
* "Qual foi a temperatura média em São Paulo nos últimos 30 dias?"
* "Mostre a previsão do tempo para os próximos 7 dias"
* "Qual cidade teve a pior qualidade do ar esta semana?"
* "Compare a precipitação de 2025 com o histórico de 2024"

---

## 5. Genie Space IBGE

### Objetivo
Análise de dados geográficos do Brasil segundo o IBGE (Instituto Brasileiro de Geografia e Estatística).

### Fonte de Dados
**Catálogo**: `silver_prod.ds_ibge`  
**API Origem**: API de Localidades do IBGE

### Tabelas Configuradas

#### Divisões Administrativas

**`cleaned_regioes`**
* **Propósito**: 5 regiões do Brasil (Norte, Nordeste, Centro-Oeste, Sudeste, Sul)

**`cleaned_estados`**
* **Propósito**: 27 unidades federativas (26 estados + DF)

**`cleaned_municipios`**
* **Propósito**: Todos os 5.570 municípios brasileiros
* **Uso Principal**: Base para análises regionalizadas

**`cleaned_distritos`**
* **Propósito**: Subdivisões dos municípios

**`cleaned_subdistritos`**
* **Propósito**: Subdivisões dos distritos

#### Divisões Regionais

**`cleaned_mesorregioes`**
* **Propósito**: Divisões regionais intermediárias (ex: Sul de Minas)

**`cleaned_microrregioes`**
* **Propósito**: Divisões regionais menores com características homogêneas

**`cleaned_regioes_intermediarias`**
* **Propósito**: Nova divisão regional do IBGE (2017)
* **Característica**: Polos de influência regional

**`cleaned_regioes_imediatas`**
* **Propósito**: Nova divisão regional do IBGE (2017)
* **Característica**: Áreas de influência direta

#### Arranjos Urbanos

**`cleaned_regioes_metropolitanas`**
* **Propósito**: Grandes aglomerações urbanas oficiais
* **Exemplos**: Grande São Paulo, Grande Rio, Grande BH

**`regiao_metropolitana_municipios`**
* **Propósito**: Relacionamento entre regiões metropolitanas e seus municípios

**`cleaned_aglomeracoes_urbanas`**
* **Propósito**: Concentrações urbanas menores que regiões metropolitanas

**`aglomeracao_urbana_municipios`**
* **Propósito**: Relacionamento entre aglomerações urbanas e municípios

**`cleaned_regioes_integradas_desenvolvimento`**
* **Propósito**: RIDEs - arranjos de municípios em estados diferentes
* **Exemplos**: RIDE-DF (Distrito Federal e entorno)

**`ride_municipios`**
* **Propósito**: Relacionamento entre RIDEs e seus municípios

### Por que usamos dados Silver aqui?

Para o IBGE, utilizamos dados da camada **Silver** porque:
* **Estrutura Adequada**: Os dados já estão limpos e padronizados
* **Completude**: Todas as divisões geográficas estão disponíveis
* **Relacionamentos**: As tabelas de relacionamento já estão prontas
* **Estabilidade**: Dados geográficos mudam raramente, não requerem modelagem dimensional

### Exemplos de Perguntas
* "Quantos municípios existem no estado de São Paulo?"
* "Quais cidades fazem parte da Região Metropolitana de Belo Horizonte?"
* "Liste todos os estados da região Nordeste"
* "Quantas microrregiões existem em Minas Gerais?"

---

## Resumo Comparativo

| Genie Space | Camada de Dados | Tipo de Modelo | Tabelas | Foco Principal |
|-------------|----------------|----------------|---------|----------------|
| **Pokémon** | Gold (dm_pokemon) | Dimensional | 6 dimensões | Análise de características |
| **Câmara dos Deputados** | Gold (dm_camara_deputados) | Star Schema | 6 dims + 2 fatos | Transparência e gastos |
| **Carros FIPE** | Gold (dm_carros) | Star Schema | 4 dims + 1 fato | Análise de preços |
| **Open Meteo** | Gold (dm_open_meteo) | Star Schema | 2 dims + 3 fatos | Clima e qualidade do ar |
| **IBGE** | Silver (ds_ibge) | Tabelas Limpas | 15 tabelas | Geografia do Brasil |

---

## Benefícios Alcançados

### 1. Acessibilidade
Qualquer pessoa pode explorar dados complexos usando linguagem natural, sem precisar conhecer SQL ou a estrutura das tabelas.

### 2. Consistência
Todos os usuários trabalham com as mesmas definições de negócio, métricas e dimensões, garantindo análises alinhadas.

### 3. Governança
* Acesso controlado por domínio de negócio
* Respeito às permissões do Unity Catalog
* Auditoria de uso e perguntas realizadas

### 4. Performance
* Consultas otimizadas pelo modelo dimensional
* Dados pré-agregados quando necessário
* Cache inteligente de respostas frequentes

### 5. Escalabilidade
Arquitetura permite adicionar novos Spaces facilmente, cada um especializado em seu domínio.

---

## Próximos Passos

### Expansão dos Spaces Existentes
* **Câmara dos Deputados**: Adicionar dados de proposições e discursos
* **Open Meteo**: Incluir dados de eventos climáticos extremos
* **IBGE**: Adicionar dados de população e censo

### Novos Genie Spaces Potenciais
* **Vendas e Receita**: Análise de dados comerciais
* **Recursos Humanos**: Análise de colaboradores e folha
* **Marketing**: Campanhas e performance de canais

### Melhorias Técnicas
* Implementar refresh automático dos dados
* Criar alertas para anomalias nos dados
* Desenvolver dashboards executivos integrados

---

## Conclusão

Os **Genie Spaces** representam uma evolução significativa na forma como democratizamos o acesso a dados na organização. Ao combinar:

* **Arquitetura moderna de dados** (Lakehouse com camadas Raw → Silver → Gold)
* **Modelagem dimensional** (Star Schema otimizado para análise)
* **Inteligência Artificial contextualizada** (Genie com conhecimento profundo dos dados)
* **Governança robusta** (Unity Catalog com controle de acesso)

Criamos um ambiente onde **qualquer pessoa pode se tornar um analista de dados**, fazendo perguntas em linguagem natural e obtendo insights acionáveis de forma rápida e confiável.

Cada Genie Space atua como um especialista em seu domínio, pronto para responder perguntas, gerar visualizações e apoiar decisões baseadas em dados.

---

**Documentação criada em**: Maio de 2026  
**Última atualização**: Maio de 2026  
**Versão**: 1.0
