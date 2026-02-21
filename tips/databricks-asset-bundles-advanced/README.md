# Databricks Asset Bundles — laboratorio práctico

Lab del blog post [Databricks Tips #1: Databricks Asset Bundles](https://mauroloprete.github.io/mauroloprete/blog/posts/databricks-asset-bundles-advanced/).

Acá vas a crear, validar, deployar y destruir un bundle usando el Databricks CLI.
Todo funciona en **Databricks Free Edition**.

## Requisitos

- [Databricks CLI](https://docs.databricks.com/aws/en/dev-tools/cli/install) (v0.200+)
- Cuenta en [Databricks Free Edition](https://www.databricks.com/try-databricks) (gratis, sin tarjeta)

## 1. Instalar el Databricks CLI

```bash
# macOS
brew tap databricks/tap
brew install databricks

# Linux / WSL
curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sh

# Verificar
databricks --version
```

## 2. Autenticarse

```bash
# Autenticación OAuth (recomendado)
databricks auth login --host https://community.cloud.databricks.com

# Se abre el browser, autorizás, listo.
# Verificar que funcione:
databricks auth env --host https://community.cloud.databricks.com
```

> Reemplazá la URL por la de tu workspace si es diferente.

## 3. Explorar el bundle

Este lab incluye un bundle funcional:

```
databricks-asset-bundles-advanced/
├── databricks.yml           # Configuración del bundle
└── src/
    └── hello_bundle.py      # Notebook que despliega el bundle
```

Mirá el `databricks.yml` — incluye:

- **Variables** simples y complejas (`type: complex`)
- **Targets**: `dev` (default) y `prod`
- **Presets** por target (prefijos, tags)
- **Sync patterns** (qué archivos suben al workspace)
- **Job** con notebook task en serverless compute

## 4. Validar el bundle

```bash
cd episodios/databricks-asset-bundles-advanced

databricks bundle validate
```

Si todo está bien, ves un resumen del bundle sin errores.

Para ver el plan completo de lo que se va a deployar:

```bash
databricks bundle validate --output json | python -m json.tool
```

## 5. Deployar

```bash
# Deploy al target dev (default)
databricks bundle deploy

# O ser explícito:
databricks bundle deploy --target dev
```

Esto crea en tu workspace:
- El notebook `hello_bundle.py` sincronizado
- Un job llamado `[dev <tu_usuario>] hello_dab_job`

Verificá en tu workspace que se haya creado el job.

## 6. Ejecutar el job

```bash
databricks bundle run hello_job
```

El CLI muestra el link al run. Podés ver el output en el workspace.

## 7. Ver el estado

```bash
# Ver qué recursos tiene el bundle deployado
databricks bundle summary
```

## 8. Experimentar

Probá modificar el bundle y re-deployar:

### Cambiar una variable

Editá `databricks.yml` y cambiá el valor de `greeting`:

```yaml
variables:
  greeting:
    default: "Hola desde DABs!"  # ← cambiá este valor
```

Re-deployá y ejecutá:

```bash
databricks bundle deploy && databricks bundle run hello_job
```

### Pasar variable por CLI

Sin tocar el YAML:

```bash
databricks bundle deploy --var="greeting=Probando variables desde CLI"
databricks bundle run hello_job
```

### Pasar variable por variable de entorno

```bash
export BUNDLE_VAR_greeting="Variable de entorno funciona!"
databricks bundle deploy
databricks bundle run hello_job
```

## 9. Limpiar

Cuando termines, destruí el bundle para eliminar todos los recursos:

```bash
databricks bundle destroy

# Confirmar con:
databricks bundle destroy --auto-approve
```

## Orden de precedencia de variables

Lo que viste en acción:

1. `--var` en CLI (mayor prioridad)
2. Variable de entorno `BUNDLE_VAR_*`
3. Valor en `targets.*.variables`
4. Valor `default` en la declaración

## Links

- [Blog post](https://mauroloprete.github.io/mauroloprete/blog/posts/databricks-asset-bundles-advanced/)
- [Documentación oficial de DABs](https://docs.databricks.com/aws/en/dev-tools/bundles)
- [Tutorial oficial: Develop a job](https://docs.databricks.com/aws/en/dev-tools/bundles/jobs-tutorial)
