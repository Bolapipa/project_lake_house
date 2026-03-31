# Project Lake House | Data Engineering Portfolio

Data Lake pessoal desenvolvido no **Databricks** com foco em **estudo prático, evolução técnica e construção de portfólio em Engenharia de Dados**.

Este repositório foi criado para consolidar conhecimentos em arquitetura de dados, ingestão, transformação, modelagem analítica, automação e boas práticas de organização de ambientes em nuvem.

---

## Sobre o projeto

O **Project Lake House** é um projeto autoral voltado para a prática de conceitos fundamentais de Engenharia de Dados em um ambiente moderno baseado em **Databricks**.

A proposta é construir, de forma progressiva e bem documentada, um Data Lake com organização por camadas, processos de ingestão, tratamento, modelagem e automação de deploy, simulando boas práticas adotadas em projetos reais.

Este projeto tem como foco principal:

- aprendizado prático;
- experimentação controlada;
- documentação técnica;
- construção de portfólio público no GitHub.

---

## Objetivos

Este projeto foi criado para praticar e demonstrar conhecimento em:

- arquitetura **Medallion**;
- organização de dados em camadas **Bronze, Silver e Gold**;
- ingestão **full** e **incremental**;
- uso de **tabelas de controle**;
- modelagem analítica com **fatos e dimensões**;
- aplicação de **Star Schema**;
- orquestração com **Jobs e Pipelines** no Databricks;
- automação com **GitHub Actions**;
- versionamento com **GitHub**;
- organização de ambientes **dev** e **prod**;
- boas práticas de **segurança da informação** e alinhamento com **LGPD**.

---

## Destaques para recrutadores

Este projeto demonstra, na prática:

- capacidade de estruturar um **Data Lake no Databricks**;
- entendimento de **arquitetura por camadas**;
- aplicação de **cargas incrementais** com controle de processamento;
- organização de ambientes **dev** e **prod**;
- uso de **CI/CD** para validação e deploy;
- preocupação com **documentação, segurança e governança**;
- evolução de um projeto técnico com foco em **portfólio público**.

---

## Arquitetura da solução

O projeto foi estruturado com base na arquitetura **Medallion**, separando claramente os dados por responsabilidade e estágio de maturidade.

### Bronze
Camada de entrada dos dados.

Responsável por armazenar os dados brutos, com o mínimo possível de transformação, preservando rastreabilidade e permitindo reprocessamento futuro.

**Exemplos de uso:**
- ingestão de APIs;
- carga inicial de arquivos;
- armazenamento de dados brutos.

### Silver
Camada de tratamento e padronização.

Responsável por limpar, transformar, normalizar e validar os dados vindos da Bronze.

**Exemplos de uso:**
- casting de tipos;
- remoção de duplicidades;
- tratamento de nulos;
- padronização de colunas;
- enriquecimento dos dados.

### Gold
Camada analítica.

Responsável por disponibilizar os dados refinados para análise, consumo e modelagem analítica.

**Exemplos de uso:**
- tabelas analíticas;
- visões consolidadas;
- fatos e dimensões;
- estrutura para dashboards e análises.

---

## Modelagem analítica

Na camada Gold, o projeto adota conceitos de modelagem analítica para facilitar consumo e interpretação dos dados.

### Tabela fato
Representa eventos, medições, transações ou ocorrências do negócio.

**Exemplos:**
- preços por período;
- medições;
- movimentações;
- eventos registrados.

### Tabelas dimensão
Representam o contexto descritivo da fato.

**Exemplos:**
- tempo;
- categoria;
- modelo;
- localização;
- produto.

### Star Schema
Quando aplicável, a organização da camada Gold segue o padrão **Star Schema**, com:

- uma tabela fato central;
- várias dimensões ao redor.

Essa abordagem melhora:
- entendimento do modelo;
- legibilidade das consultas;
- organização analítica;
- preparação para ferramentas de BI.

---

## Estratégias de ingestão

O projeto utiliza abordagens de ingestão adequadas ao tipo de dado e ao objetivo da carga.

### Carga full
Utilizada quando todos os registros precisam ser processados novamente.

**Cenários comuns:**
- primeira carga;
- tabelas pequenas;
- dados de referência;
- estruturas simples.

### Carga incremental
Utilizada quando apenas os dados novos precisam ser processados.

**Cenários comuns:**
- APIs paginadas;
- tabelas com ids crescentes;
- rotinas recorrentes;
- cargas com necessidade de otimização.

### Tabelas de controle
Para suportar incrementalidade, o projeto utiliza tabelas de controle para armazenar informações como:

- último id processado;
- última data processada;
- último marcador de referência.

**Benefícios:**
- evita reprocessamento desnecessário;
- reduz duplicidade;
- melhora performance;
- facilita retomada após falhas.

---

## Organização por ambiente

O projeto foi estruturado com separação entre ambientes para manter previsibilidade e controle de publicação.

### Ambientes
- **dev**: desenvolvimento e validação;
- **prod**: publicação e execução principal.

### Exemplo de padrão adotado
- `bronze_dev`, `silver_dev`, `gold_dev`
- `bronze_prod`, `silver_prod`, `gold_prod`

### Exemplo de nomenclatura
`bronze_dev.ds_pokemon.raw_pokemon_name`

---

## Orquestração no Databricks

O ambiente utiliza recursos nativos do Databricks para organizar a execução dos processos.

### Jobs
Utilizados para:
- encadear notebooks;
- controlar ordem das tarefas;
- automatizar execuções;
- integrar diferentes etapas do fluxo.

### Pipelines
Utilizadas para:
- estruturar transformações;
- padronizar fluxos;
- organizar melhor as etapas de processamento.

---

## CI/CD

O projeto utiliza **GitHub Actions** para automatizar validação e deploy.

### Fluxo adotado

#### Branch `dev`
Ao fazer commit em `dev`, o pipeline executa:
- validação do bundle.

#### Branch `prod`
Ao fazer commit em `prod`, o pipeline executa:
- validação do bundle;
- deploy do bundle;
- execução do job principal.

### Benefícios desse fluxo
- automação por branch;
- menor risco de erro manual;
- separação clara entre validação e publicação;
- melhor controle do ciclo de entrega.

---

## Databricks Asset Bundles

Os **Databricks Asset Bundles** são utilizados para padronizar e versionar os recursos do ambiente, como:

- jobs;
- pipelines;
- variáveis;
- targets;
- permissões;
- parâmetros de catálogo e schema.

Essa abordagem ajuda a garantir:
- consistência entre ambientes;
- organização do deploy;
- versionamento da estrutura junto com o código.

---

## Estrutura do repositório

```
.
├── .github/
│   └── workflows/
├── carros/
├── dm_carros/
├── dm_pokemon/
├── pokemon/
├── README.md
└── LICENSE
```

### Papel das principais pastas
- `.github/workflows/` → automações de CI/CD
- `carros/` → projeto e ingestões relacionadas ao domínio de carros
- `pokemon/` → projeto e ingestões relacionadas ao domínio Pokémon
- `dm_carros/` → estruturas analíticas e modelagem do domínio carros
- `dm_pokemon/` → estruturas analíticas e modelagem do domínio Pokémon

---

## Boas práticas adotadas

Este projeto procura seguir boas práticas técnicas e organizacionais, como:

- separação clara entre dado bruto, tratado e analítico;
- código simples, legível e fácil de manter;
- regras importantes explícitas no código;
- uso de incrementalidade quando aplicável;
- versionamento de notebooks, workflows e bundles;
- nomenclatura consistente entre arquivos, jobs, pipelines e tabelas;
- documentação contínua da arquitetura e decisões técnicas.

---

## Segurança da informação e LGPD

Este projeto foi pensado para exposição pública no GitHub. Por isso, segue princípios importantes de segurança da informação.

### Princípios adotados
- não publicar credenciais, tokens ou segredos;
- não utilizar dados corporativos internos;
- não expor dados pessoais reais;
- priorizar dados públicos, fictícios ou anonimizados;
- revisar conteúdos antes de publicação.

### Alinhamento com LGPD
Mesmo sendo um projeto pessoal, o repositório busca seguir princípios compatíveis com a LGPD, como:

- minimização de dados;
- uso responsável da informação;
- cuidado com exposição indevida;
- priorização de exemplos não sensíveis.

---

## Tecnologias e conceitos praticados

### Ferramentas e tecnologias
- Databricks
- Delta Lake
- Python
- SQL
- GitHub
- GitHub Actions

### Conceitos
- Data Lake
- Lakehouse
- Arquitetura Medallion
- Ingestão full e incremental
- Tabelas de controle
- Modelagem analítica
- Star Schema
- Orquestração de pipelines
- CI/CD
- Governança básica de dados

---

## Documentação

A documentação do projeto está sendo organizada para cobrir temas como:

- visão geral da arquitetura;
- arquitetura Medallion;
- modelagem analítica;
- ingestão incremental;
- tabelas de controle;
- jobs e pipelines;
- CI/CD;
- segurança da informação e LGPD.

---

## Próximos passos

Evoluções planejadas para o projeto:

- expansão de novas fontes públicas de dados;
- enriquecimento das transformações na Silver;
- evolução da camada Gold com novas tabelas analíticas;
- criação de dashboards demonstrativos;
- ampliação da documentação técnica;
- melhoria de observabilidade e testes.

---

## Valor deste projeto como portfólio

Este repositório demonstra, na prática:

- capacidade de estruturar um Data Lake no Databricks;
- entendimento de arquitetura por camadas;
- aplicação de cargas incrementais;
- organização de ambientes dev e prod;
- uso de automação com CI/CD;
- preocupação com documentação, segurança e governança.

---

## Autor

**Arthur Delacorte**  
Projeto pessoal de estudo, prática e portfólio em **Engenharia de Dados**.
