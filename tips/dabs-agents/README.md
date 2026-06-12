# DABs + AI Agents — laboratorio practico

Lab del meetup **De YAML a produccion: Deploy de AI Agents con Declarative Automation Bundles** (Databricks Meetup Uruguay, Qubika).

Vas a deployar un agente RAG completo usando un solo `databricks.yml`: modelo MLflow, serving endpoint con AI Gateway, job de refresh, y app de chat.
Requiere un workspace con **Unity Catalog** y **Model Serving** habilitado.

## Requisitos

- [Databricks CLI](https://docs.databricks.com/aws/en/dev-tools/cli/install) (v0.240+)
- Workspace con Unity Catalog y Model Serving (no disponible en Free Edition)
- Acceso a Foundation Model APIs (Meta Llama 3.3 70B o similar)

## Estructura

```
dabs-agents/
├── databricks.yml              # Bundle completo — todo el deploy
├── src/
│   ├── 00_setup_data.py        # Crea tabla de documentos de soporte
│   ├── 01_agent.py             # Agente RAG + registro en MLflow/UC
│   └── 02_refresh_index.py     # Job de refresh de documentos
└── app/
    ├── app.py                  # Chat UI (Streamlit) para Databricks Apps
    └── requirements.txt
```

## Como usarlo

### Opcion A: con Databricks CLI (recomendado)

```bash
# 1. Clonar el repo
git clone https://github.com/mauroloprete/spark-de-ideas-labs.git
cd spark-de-ideas-labs/tips/dabs-agents

# 2. Autenticarse
databricks auth login --host https://TU-WORKSPACE.databricks.com

# 3. Validar el bundle
databricks bundle validate

# 4. Ver que va a crear
databricks bundle plan

# 5. Deployar
databricks bundle deploy

# 6. Ejecutar el setup (crea datos + registra el modelo)
databricks bundle run setup_lab

# 7. Verificar recursos creados
databricks bundle summary
```

### Opcion B: importar notebooks manualmente

1. Importa los notebooks de `src/` en tu workspace
2. Ejecuta en orden: `00_setup_data.py` → `01_agent.py`
3. Crea los recursos (endpoint, job) manualmente desde la UI

## Que vas a practicar

| Concepto | Que aprendes |
|----------|-------------|
| **MLflow PythonModel** | Crear un agente RAG como modelo registrable |
| **Unity Catalog** | Registrar modelos con versionado y gobernanza |
| **Serving Endpoints** | Servir el agente con AI Gateway, rate limits, guardrails |
| **Auto Capture** | Logging automatico de inference en Delta Tables |
| **Variables + Targets** | Un YAML, multiples ambientes (dev/prod) |
| **Jobs** | Programar refresh de la base de conocimiento |
| **bundle plan** | Preview de cambios antes de deployar |

## Recursos definidos en el bundle

```
databricks.yml define:
├── experiments          → MLflow experiment para tracking
├── registered_models    → Modelo en Unity Catalog
├── model_serving_endpoints → Endpoint + AI Gateway + guardrails
└── jobs
    ├── setup_lab        → Pipeline: setup data → registrar agente
    └── refresh_index    → Cron job diario (6 AM UY)
```

## Personalizar

Edita las variables en `databricks.yml` segun tu workspace:

```yaml
variables:
  catalog:
    default: tu_catalog      # Cambiar por tu catalog
  schema:
    default: labs             # O el schema que prefieras
  vs_endpoint:
    default: tu-vs-endpoint   # Si tenes Vector Search
```

## Limpiar

```bash
# Destruir todos los recursos creados por el bundle
databricks bundle destroy --auto-approve

# Si creaste tablas manualmente:
# DROP TABLE IF EXISTS tu_catalog.labs.soporte_docs;
# DROP TABLE IF EXISTS tu_catalog.labs.soporte_bot_logs;
```

## Extras: Lakebase (memoria del agente)

Si tu workspace tiene Lakebase habilitado, agrega esto al `databricks.yml`:

```yaml
resources:
  postgres_projects:
    agent_memory:
      project_id: soporte-bot-memory
      display_name: "SoporteBot Memory Store"
      pg_version: 17

  postgres_branches:
    prod_branch:
      parent: ${resources.postgres_projects.agent_memory.id}
      branch_id: production
      no_expiry: true

  postgres_endpoints:
    prod_endpoint:
      parent: ${resources.postgres_branches.prod_branch.id}
      endpoint_id: primary
      endpoint_type: ENDPOINT_TYPE_READ_WRITE
      autoscaling_limit_min_cu: 0.5
      autoscaling_limit_max_cu: 2
```

Esto le da al agente memoria persistente: short-term (checkpoints de conversacion) y long-term (insights entre sesiones).

## Links

- [Slides del meetup](https://mauroloprete.github.io/mauroloprete/slides/databricks-meetup-dabs-agents/)
- [Blog — Spark de Ideas](https://mauroloprete.github.io/mauroloprete/)
- [Documentacion oficial de DABs](https://docs.databricks.com/aws/en/dev-tools/bundles)
- [Lakebase con DABs](https://learn.microsoft.com/en-us/azure/databricks/oltp/projects/manage-with-bundles)
- [Agent Bricks](https://www.databricks.com/company/newsroom/press-releases/databricks-launches-agent-bricks-new-approach-building-ai-agents)
- [Direct Deployment Engine](https://docs.databricks.com/aws/en/dev-tools/bundles/direct)
