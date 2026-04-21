# 🎯 Instruções de Correção - Todos os Bundles DAB

## ✅ Status das Correções

### Notebooks DLT - ✅ CONCLUÍDO AUTOMATICAMENTE
Todos os notebooks DLT foram corrigidos automaticamente:
* ✅ **open_meteo** → dlt_open_meteo.sql
* ✅ **pokemon** → dlt_pokemon.sql
* ✅ **camara_deputados** → dlt_camara_deputados.sql
* ✅ **carros** → dlt_carros.sql
* ✅ **ibge** → dlt_ibge.sql

**Mudança aplicada:** `bronze_dev.SCHEMA` → `${source_catalog}.${source_schema}`

### Arquivos YAML - ⚠️ CORREÇÃO MANUAL NECESSÁRIA
Os arquivos YAML precisam ser corrigidos manualmente para cada bundle:

---

## 📋 Correções Necessárias por Bundle

### 1️⃣ Bundle: POKEMON

#### Arquivo: `pokemon/databricks.yml`
Adicionar após a variável `schema`:

```yaml
  silver_catalog:
    description: "Catalog de destino da silver"
    default: silver_dev

  pipeline_development:
    description: "Pipeline development mode flag"
    default: true
```

No target `dev`, adicionar:
```yaml
      silver_catalog: silver_dev
      pipeline_development: true
```

No target `prod`, adicionar:
```yaml
      silver_catalog: silver_prod
      pipeline_development: false
```

#### Arquivo: `pokemon/resources/pokemon.pipeline.yml`
Substituir TODO o conteúdo por:

```yaml
resources:
  pipelines:
    pokemon_pipeline:
      name: 'Pokemon ETL Pipeline'
      catalog: ${var.silver_catalog}
      target: ${var.schema}
      
      libraries:
        - notebook:
            path: ../src/dlt_pokemon.sql
      
      configuration:
        source_catalog: ${var.catalog}
        source_schema: ${var.schema}
      
      serverless: true
      continuous: false
      development: ${var.pipeline_development}
      photon: true
      channel: current
      
      permissions:
        - level: CAN_VIEW
          group_name: 'users'
```

---

### 2️⃣ Bundle: CAMARA_DEPUTADOS

#### Arquivo: `camara_deputados/databricks.yml`
Adicionar as mesmas variáveis:

```yaml
  silver_catalog:
    description: "Catalog de destino da silver"
    default: silver_dev

  pipeline_development:
    description: "Pipeline development mode flag"
    default: true
```

Nos targets `dev` e `prod`, adicionar:
```yaml
# dev:
      silver_catalog: silver_dev
      pipeline_development: true

# prod:
      silver_catalog: silver_prod
      pipeline_development: false
```

#### Arquivo: `camara_deputados/resources/camara_deputados_etl.pipeline.yml`
```yaml
resources:
  pipelines:
    camara_deputados_pipeline:
      name: 'Camara Deputados ETL Pipeline'
      catalog: ${var.silver_catalog}
      target: ${var.schema}
      
      libraries:
        - notebook:
            path: ../src/dlt_camara_deputados.sql
      
      configuration:
        source_catalog: ${var.catalog}
        source_schema: ${var.schema}
      
      serverless: true
      continuous: false
      development: ${var.pipeline_development}
      photon: true
      channel: current
      
      permissions:
        - level: CAN_VIEW
          group_name: 'users'
```

---

### 3️⃣ Bundle: CARROS

Seguir o mesmo padrão dos anteriores:

#### `carros/databricks.yml`
* Adicionar variáveis `silver_catalog` e `pipeline_development`
* Atualizar targets dev/prod

#### `carros/resources/carros_etl.pipeline.yml`
* Adicionar `catalog: ${var.silver_catalog}`
* Adicionar `configuration` com source_catalog/source_schema
* Adicionar `development: ${var.pipeline_development}`

---

### 4️⃣ Bundle: IBGE

#### `ibge/databricks.yml`
* Adicionar variáveis `silver_catalog` e `pipeline_development`
* Atualizar targets dev/prod

#### `ibge/resources/ibge_etl.pipeline.yml`
* Adicionar `catalog: ${var.silver_catalog}`
* Adicionar `configuration` com source_catalog/source_schema
* Adicionar `development: ${var.pipeline_development}`

---

## 🔍 Validação

Após fazer as mudanças, valide cada bundle:

```bash
cd ~/project_lake_house/pokemon
databricks bundle validate --strict --target dev
databricks bundle validate --strict --target prod

cd ~/project_lake_house/camara_deputados
databricks bundle validate --strict --target dev
databricks bundle validate --strict --target prod

cd ~/project_lake_house/carros
databricks bundle validate --strict --target dev
databricks bundle validate --strict --target prod

cd ~/project_lake_house/ibge
databricks bundle validate --strict --target dev
databricks bundle validate --strict --target prod
```

---

## 🎉 Resultado Final

Após todas as correções, seus bundles estarão 100% portáveis entre ambientes:

* ✅ **Dev**: Lê de `bronze_dev`, escreve em `silver_dev`
* ✅ **Prod**: Lê de `bronze_prod`, escreve em `silver_prod`
* ✅ **Pipeline mode**: `development: true` em dev, `development: false` em prod

