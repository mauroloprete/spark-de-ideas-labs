# Databricks notebook source
# MAGIC %md
# MAGIC # Python: la base de todo
# MAGIC
# MAGIC Notebook del episodio **"Si arrancara de cero"** del podcast [Spark de Ideas](https://open.spotify.com/show/20WqZma0iLDYbg9wAEfv0w).
# MAGIC
# MAGIC Acá vemos los fundamentos de Python que más se usan en Data Engineering:
# MAGIC dataclasses, inmutabilidad, Pydantic y cuándo usar cada tipo de clase.
# MAGIC
# MAGIC > Ejecutable en Databricks Community Edition.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Dataclasses — la opción correcta en el 90% de los casos
# MAGIC
# MAGIC En data engineering modelás cosas todo el tiempo: configuraciones, conexiones, resultados de validaciones.
# MAGIC Si todo lo hacés con diccionarios, te la pasás haciendo `config["database"]["host"]` y rezando que la key exista.

# COMMAND ----------

from dataclasses import dataclass, field
from typing import List, Optional
import json

@dataclass
class DataProduct:
    name: str
    owner: str
    tables: List[str]
    sla_hours: Optional[int] = None

    def is_critical(self) -> bool:
        return self.sla_hours is not None and self.sla_hours <= 4

# Crear instancias
products = [
    DataProduct("transactions", "backend-team", ["raw.txn", "silver.txn"], sla_hours=2),
    DataProduct("clickstream", "analytics", ["raw.clicks"], sla_hours=24),
    DataProduct("reports", "bi-team", ["gold.daily_report"]),
]

critical = [p for p in products if p.is_critical()]
print(f"Productos críticos: {[p.name for p in critical]}")

# COMMAND ----------

# MAGIC %md
# MAGIC Con 5 líneas tenés `__init__`, `__repr__`, `__eq__` gratis.
# MAGIC Comparemos con la clase tradicional:

# COMMAND ----------

# Clase tradicional — mucho boilerplate
class PipelineTradicional:
    def __init__(self, name: str, source: str, target: str, schedule: str = "daily"):
        self.name = name
        self.source = source
        self.target = target
        self.schedule = schedule

    def __repr__(self):
        return f"Pipeline(name={self.name!r}, source={self.source!r})"

    def __eq__(self, other):
        return (self.name == other.name and self.source == other.source
                and self.target == other.target and self.schedule == other.schedule)


# Dataclass — lo mismo en 5 líneas
@dataclass
class Pipeline:
    name: str
    source: str
    target: str
    schedule: str = "daily"
    tags: list = field(default_factory=list)

    def full_target(self) -> str:
        return f"catalog.{self.target}"


p = Pipeline("ingest_sales", "raw.sales", "silver.sales")
print(p)
print(f"¿Son iguales? {p == Pipeline('ingest_sales', 'raw.sales', 'silver.sales')}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Inmutabilidad — frozen dataclasses
# MAGIC
# MAGIC Un objeto inmutable no se puede modificar después de crearlo. ¿Para qué?
# MAGIC Porque los objetos mutables generan bugs silenciosos cuando pasan por varias funciones.

# COMMAND ----------

# El PROBLEMA: objetos mutables
@dataclass
class ConfigMutable:
    host: str
    port: int
    timeout: int = 30

config = ConfigMutable("db.prod.internal", 5432)

def connect(cfg):
    return f"Connecting to {cfg.host}:{cfg.port}"

def add_debug_settings(cfg):
    cfg.host = "localhost"  # ← cambió la config de producción sin querer
    cfg.timeout = 999

print(connect(config))          # → db.prod.internal:5432 ✓
add_debug_settings(config)
print(connect(config))          # → localhost:5432 ← BUG silencioso

# COMMAND ----------

# La SOLUCIÓN: frozen=True
from dataclasses import replace

@dataclass(frozen=True)
class ConfigInmutable:
    host: str
    port: int
    timeout: int = 30

config = ConfigInmutable("db.prod.internal", 5432)

try:
    config.host = "localhost"  # ← FrozenInstanceError
except AttributeError as e:
    print(f"Error esperado: {e}")

# Si necesitás una versión modificada, creás una nueva:
debug_config = replace(config, host="localhost", timeout=999)

print(f"Original: {config.host}")       # → db.prod.internal
print(f"Debug:    {debug_config.host}")  # → localhost

# COMMAND ----------

# MAGIC %md
# MAGIC **Cuándo usar inmutabilidad:**
# MAGIC - Configuraciones, credenciales, resultados de validación
# MAGIC - Cualquier cosa que viaje entre funciones y no deba cambiar
# MAGIC
# MAGIC **La regla:** si dudás, hacelo inmutable.

# COMMAND ----------

# Ejemplos reales en data engineering
@dataclass(frozen=True)
class PipelineConfig:
    source_table: str
    target_table: str
    batch_size: int = 10000
    mode: str = "append"

@dataclass(frozen=True)
class QualityCheck:
    rule_name: str
    passed: bool
    value: float
    threshold: float

# Inmutables, seguros, hashables (se pueden usar en sets)
config1 = PipelineConfig("raw.sales", "silver.sales")
config2 = PipelineConfig("raw.sales", "silver.sales")
print(f"¿Son iguales? {config1 == config2}")
print(f"¿Hashable? {hash(config1)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. NamedTuple — cuando querés inmutabilidad + desempaquetado

# COMMAND ----------

from typing import NamedTuple

class ValidationResult(NamedTuple):
    passed: bool
    errors: list
    row_count: int

result = ValidationResult(False, ["nulls in amount"], 1500)

# Se desempaqueta como tupla:
passed, errors, count = result

# Pero accedés por nombre:
if not result.passed:
    print(f"Falló con {len(result.errors)} errores en {result.row_count} filas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Pydantic — cuando necesitás validación

# COMMAND ----------

# MAGIC %pip install pydantic -q

# COMMAND ----------

from pydantic import BaseModel, field_validator

class DataContract(BaseModel):
    name: str
    owner: str
    sla_hours: int
    columns: list

    @field_validator("sla_hours")
    @classmethod
    def sla_must_be_positive(cls, v):
        if v <= 0:
            raise ValueError("SLA debe ser mayor a 0")
        return v

# Valida automáticamente:
contract = DataContract(
    name="transactions",
    owner="backend-team",
    sla_hours=4,
    columns=["id", "amount", "date"]
)
print(contract)

# Esto falla con error claro:
try:
    bad = DataContract(name="x", owner="y", sla_hours=-1, columns=[])
except Exception as e:
    print(f"Validación: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resumen: ¿cuándo usar cada tipo?
# MAGIC
# MAGIC | Situación | Tipo |
# MAGIC |-----------|------|
# MAGIC | Modelo de datos simple (mayoría) | `@dataclass` |
# MAGIC | Config que no debe cambiar | `@dataclass(frozen=True)` |
# MAGIC | Resultado liviano, desempaquetable | `NamedTuple` |
# MAGIC | Parsing JSON/YAML con validación | Pydantic `BaseModel` |
# MAGIC | Lógica de negocio compleja | Clase tradicional |
# MAGIC | Dato descartable, se usa una vez | `dict` |
# MAGIC
# MAGIC **Regla general:** arrancá con `@dataclass`. Si necesitás inmutabilidad → `frozen=True`. Si necesitás validación → Pydantic.
