"""Benchmark client-side de Lakehouse//RT vs SQL Warehouse serverless.

Mide desde fuera vía Statement Execution API y guarda dos números por query:
client_ms (wall-clock del request, red incluida) y las métricas server-side
del Query History. Corre tres series intercaladas: RT, serverless con
parámetros aleatorios + nonce proyectado (el result cache matchea por plan,
así nunca pega) y serverless "(cache)" con las queries idénticas, donde el
cache sirve a partir de la segunda corrida. result_from_cache queda en el CSV
como auditoría: 0 en las dos primeras series, ~1 en la última.

El crudo (latencias.csv) se reescribe corrida a corrida; `Rscript bench/live.R`
levanta un panel que grafica mientras el bench corre.

Uso:
    python3 bench/bench.py --rt <warehouse_id> --serverless <warehouse_id> \
        [--niveles 1,4,16] [--corridas 24] [--warmup 5] \
        [--catalog dev_bronze] [--schema lakehouse_rt_tips18] \
        [--profile perfil] [--out bench/results]
"""
import argparse
import csv
import os
import random
import sys
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from databricks.sdk import WorkspaceClient
from databricks.sdk.core import Config
from databricks.sdk.service.sql import (
    QueryFilter,
    StatementParameterListItem,
    StatementState,
    TimeRange,
)

# Pools de parámetros: tienen que coincidir con las listas de src/01_setup.py.
CATEGORIAS = ["hogar", "electro", "deporte", "libreria", "juguetes", "jardin"]
PAISES = ["UY", "AR", "CL", "PY", "BR", "MX", "PE", "CO"]

COLUMNAS = [
    "warehouse", "tipo", "tamano", "query", "concurrencia", "client_ms",
    "total_time_ms", "execution_time_ms", "compilation_time_ms",
    "result_fetch_time_ms", "result_from_cache", "estado", "statement_id",
]


def queries(fq: str, con_cache: bool = False) -> dict:
    """Lookup puntual, panel 24 h y join por segmento.

    Con con_cache=True van lisas y con literales fijos, para que el cache pegue.
    """
    if con_cache:
        return {
            "q1_estado_cliente": {
                "sql": f"""
                    SELECT count(*) AS pedidos, round(sum(monto), 2) AS total,
                           max(ts) AS ultimo_pedido
                    FROM {fq}.pedidos
                    WHERE cliente_id = 123456
                """,
                "params": lambda _: None,
            },
            "q2_top_productos": {
                "sql": f"""
                    SELECT producto, count(*) AS n, round(sum(monto), 2) AS total
                    FROM {fq}.pedidos
                    WHERE categoria = 'electro'
                      AND ts >= timestampadd(HOUR, -24,
                                (SELECT max(ts) FROM {fq}.pedidos))
                    GROUP BY producto
                    ORDER BY total DESC
                    LIMIT 10
                """,
                "params": lambda _: None,
            },
            "q3_panel_segmento": {
                "sql": f"""
                    SELECT c.segmento, count(*) AS n, round(sum(p.monto), 2) AS total
                    FROM {fq}.pedidos p
                    JOIN {fq}.clientes c ON p.cliente_id = c.cliente_id
                    WHERE c.pais = 'UY'
                      AND p.ts >= timestampadd(DAY, -7,
                                  (SELECT max(ts) FROM {fq}.pedidos))
                    GROUP BY c.segmento
                    ORDER BY total DESC
                """,
                "params": lambda _: None,
            },
        }
    return {
        "q1_estado_cliente": {
            "sql": f"""
                SELECT count(*) AS pedidos, round(sum(monto), 2) AS total,
                       max(ts) AS ultimo_pedido, :nonce AS nonce
                FROM {fq}.pedidos
                WHERE cliente_id = :cliente_id
            """,
            "params": lambda n_cli: [
                StatementParameterListItem(
                    name="cliente_id",
                    value=str(random.randint(1, n_cli)),
                    type="INT",
                )
            ],
        },
        "q2_top_productos": {
            "sql": f"""
                SELECT producto, count(*) AS n, round(sum(monto), 2) AS total,
                       :nonce AS nonce
                FROM {fq}.pedidos
                WHERE categoria = :categoria
                  AND ts >= timestampadd(HOUR, -24,
                            (SELECT max(ts) FROM {fq}.pedidos))
                GROUP BY producto
                ORDER BY total DESC
                LIMIT 10
            """,
            "params": lambda _: [
                StatementParameterListItem(
                    name="categoria", value=random.choice(CATEGORIAS)
                )
            ],
        },
        "q3_panel_segmento": {
            "sql": f"""
                SELECT c.segmento, count(*) AS n, round(sum(p.monto), 2) AS total,
                       :nonce AS nonce
                FROM {fq}.pedidos p
                JOIN {fq}.clientes c ON p.cliente_id = c.cliente_id
                WHERE c.pais = :pais
                  AND p.ts >= timestampadd(DAY, -7,
                              (SELECT max(ts) FROM {fq}.pedidos))
                GROUP BY c.segmento
                ORDER BY total DESC
            """,
            "params": lambda _: [
                StatementParameterListItem(name="pais", value=random.choice(PAISES))
            ],
        },
    }


def asegurar_running(w: WorkspaceClient, warehouse_id: str) -> tuple[str, str, str]:
    wh = w.api_client.do("GET", f"/api/2.0/sql/warehouses/{warehouse_id}")
    nombre, tamano = wh["name"], wh.get("cluster_size", "?")
    # La API reporta PRO también para serverless; los distingue este flag.
    tipo = wh.get("warehouse_type", "?")
    if tipo != "REALTIME" and wh.get("enable_serverless_compute"):
        tipo = "SERVERLESS"
    if wh.get("state") != "RUNNING":
        print(f"  {nombre}: {wh.get('state')} → arrancando (puede demorar)...")
        w.warehouses.start(warehouse_id).result()
    print(f"  {nombre} ({tipo}, {tamano}): RUNNING")
    return nombre, tipo, tamano


def ejecutar(w, warehouse_id, sql, params):
    """Un request completo. Devuelve (client_ms, statement_id, estado)."""
    if ":nonce" in sql:
        params = (params or []) + [
            StatementParameterListItem(name="nonce", value=uuid.uuid4().hex)
        ]
    t0 = time.perf_counter()
    resp = w.statement_execution.execute_statement(
        warehouse_id=warehouse_id,
        statement=sql,
        parameters=params,
        wait_timeout="50s",
    )
    estado = resp.status.state if resp.status else None
    # Pasada la ventana sincrónica de 50 s se sigue por polling hasta terminal.
    while estado in (StatementState.PENDING, StatementState.RUNNING):
        time.sleep(3 + random.random() * 2)
        resp = w.statement_execution.get_statement(resp.statement_id)
        estado = resp.status.state if resp.status else None
    client_ms = (time.perf_counter() - t0) * 1000
    return client_ms, resp.statement_id, estado.value if estado else "?"


def medir_celda(w, warehouse_id, nombre_q, q, concurrencia, corridas, n_cli):
    """`corridas` requests de la query, con `concurrencia` workers en paralelo."""
    filas = []
    with ThreadPoolExecutor(max_workers=concurrencia) as pool:
        futs = [
            pool.submit(ejecutar, w, warehouse_id, q["sql"], q["params"](n_cli))
            for _ in range(corridas)
        ]
        for i, fut in enumerate(as_completed(futs), 1):
            try:
                client_ms, stmt_id, estado = fut.result()
            except Exception as e:  # red/API: se registra y sigue
                print(f"    run {i}: ERROR {type(e).__name__}: {e}")
                continue
            filas.append(
                {
                    "query": nombre_q,
                    "concurrencia": concurrencia,
                    "client_ms": round(client_ms, 1),
                    "statement_id": stmt_id,
                    "estado": estado,
                }
            )
    return filas


def escribir_csv(ruta, filas, lock):
    """Reescribe el crudo entero de forma atómica (tmp + rename), para que el
    panel en vivo nunca lea un archivo a medio escribir."""
    with lock:
        copia = [dict(f) for f in filas]
    tmp = ruta.with_suffix(".csv.tmp")
    with tmp.open("w", newline="") as fh:
        wcsv = csv.DictWriter(fh, fieldnames=COLUMNAS, extrasaction="ignore")
        wcsv.writeheader()
        wcsv.writerows(copia)
    os.replace(tmp, ruta)


def buscar_metricas(w, statement_ids, warehouse_ids, t_inicio_ms):
    """Una pasada por Query History; devuelve {statement_id: métricas}."""
    encontradas = {}
    for i in range(0, len(statement_ids), 100):
        filtro = QueryFilter(
            statement_ids=statement_ids[i:i + 100],
            warehouse_ids=warehouse_ids,
            query_start_time_range=TimeRange(
                start_time_ms=t_inicio_ms,
                end_time_ms=int(time.time() * 1000) + 60_000,
            ),
        )
        pagina = None
        try:
            while True:
                resp = w.query_history.list(
                    filter_by=filtro, include_metrics=True,
                    max_results=100, page_token=pagina,
                )
                for info in resp.res or []:
                    m = info.metrics
                    if m and m.total_time_ms:
                        encontradas[info.query_id] = {
                            "total_time_ms": m.total_time_ms,
                            "execution_time_ms": m.execution_time_ms,
                            "compilation_time_ms": m.compilation_time_ms,
                            "result_fetch_time_ms": m.result_fetch_time_ms,
                            "result_from_cache": int(bool(m.result_from_cache)),
                        }
                pagina = resp.next_page_token
                if not pagina:
                    break
        except Exception as e:
            print(f"  query history: {e}")
            break
    return encontradas


def hilo_metricas(w, filas, lock, ruta, warehouse_ids, t_inicio_ms, fin):
    """Completa en el CSV las métricas server-side a medida que el Query
    History las publica (demora ~15-30 s; drena hasta ~2 min tras el final)."""
    pasadas_post = 0
    while pasadas_post < 8:
        termino = fin.wait(15)
        with lock:
            pendientes = [f["statement_id"] for f in filas
                          if f.get("statement_id") and not f.get("total_time_ms")]
            total = len(filas)
        if pendientes:
            encontradas = buscar_metricas(w, pendientes, warehouse_ids, t_inicio_ms)
            if encontradas:
                with lock:
                    for f in filas:
                        if f["statement_id"] in encontradas:
                            f.update(encontradas[f["statement_id"]])
                escribir_csv(ruta, filas, lock)
                print(f"  métricas server-side: "
                      f"{total - len(pendientes) + len(encontradas)}/{total}")
        elif termino:
            break
        if termino:
            pasadas_post += 1


def pct(valores, p):
    if not valores:
        return float("nan")
    ordenados = sorted(valores)
    k = max(0, min(len(ordenados) - 1, round(p / 100 * len(ordenados)) - 1))
    return ordenados[k]


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--rt", required=True, help="warehouse_id del Lakehouse//RT")
    ap.add_argument("--serverless", "--pro", dest="serverless", required=True,
                    help="warehouse_id del SQL warehouse serverless a comparar")
    ap.add_argument("--niveles", default="1,4,16",
                    help="concurrencias; aguanta niveles altos, ej. 1,16,64,256,1000")
    ap.add_argument("--corridas", type=int, default=24,
                    help="requests por celda; se usa max(corridas, nivel)")
    ap.add_argument("--warmup", type=int, default=5)
    ap.add_argument("--catalog", default="dev_bronze")
    ap.add_argument("--schema", default="lakehouse_rt_tips18")
    ap.add_argument("--n-clientes", type=int, default=200_000)
    ap.add_argument("--profile", default="mauro_premium")
    ap.add_argument("--out", default="bench/results")
    args = ap.parse_args()

    niveles = [int(x) for x in args.niveles.split(",")]
    fq = f"{args.catalog}.{args.schema}"
    suite = queries(fq)
    w = WorkspaceClient(config=Config(
        profile=args.profile,
        max_connections_per_pool=max(20, max(niveles) + 10),
    ))
    t_inicio_ms = int(time.time() * 1000) - 60_000

    print("Warehouses:")
    warehouses = {}
    for wid in (args.rt, args.serverless):
        warehouses[wid] = asegurar_running(w, wid)

    # Tres brazos: RT, serverless, y el mismo serverless con las queries lisas.
    arms = [(wid, *warehouses[wid], suite) for wid in (args.rt, args.serverless)]
    nombre_sv, _, tamano_sv = warehouses[args.serverless]
    arms.append((args.serverless, f"{nombre_sv} (cache)", "SERVERLESS_CACHE",
                 tamano_sv, queries(fq, con_cache=True)))

    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    ruta = out / "latencias.csv"
    filas, lock, fin = [], threading.Lock(), threading.Event()
    hilo = threading.Thread(
        target=hilo_metricas,
        args=(w, filas, lock, ruta, list(warehouses), t_inicio_ms, fin),
        daemon=True,
    )
    hilo.start()

    def registrar(celda, nombre, tipo, tamano):
        with lock:
            filas.extend({**f, "warehouse": nombre, "tipo": tipo,
                          "tamano": tamano} for f in celda)
        escribir_csv(ruta, filas, lock)

    for wid, (nombre, tipo, tamano) in warehouses.items():
        print(f"\n[{nombre}] baseline SELECT 1 x10:")
        base = medir_celda(
            w, wid, "baseline_select_1",
            {"sql": "SELECT 1", "params": lambda _: None}, 1, 10, args.n_clientes,
        )
        print(f"  p50 = {pct([f['client_ms'] for f in base], 50):.0f} ms")
        registrar(base, nombre, tipo, tamano)

    # El warm-up además ceba el result cache de la serie "(cache)".
    for wid, nombre, tipo, tamano, s in arms:
        for nombre_q, q in s.items():
            medir_celda(w, wid, nombre_q, q, 1, args.warmup, args.n_clientes)
        extra = "; result cache cebado" if tipo == "SERVERLESS_CACHE" else ""
        print(f"[{nombre}] warm-up listo "
              f"({args.warmup} corridas/query descartadas{extra})")

    # Celdas intercaladas para que el panel se llene de a tríos.
    for nivel in niveles:
        corridas = max(args.corridas, nivel)
        for nombre_q in suite:
            for wid, nombre, tipo, tamano, s in arms:
                q = s[nombre_q]
                print(f"[{nombre}] {nombre_q} @ concurrencia {nivel} "
                      f"({corridas} corridas)...")
                celda = medir_celda(
                    w, wid, nombre_q, q, nivel, corridas, args.n_clientes
                )
                ok = [f for f in celda if f["estado"] == "SUCCEEDED"]
                print(f"  ok {len(ok)}/{len(celda)}  "
                      f"client p50 = {pct([f['client_ms'] for f in ok], 50):.0f} ms")
                registrar(celda, nombre, tipo, tamano)

    print("\nEsperando las métricas server-side que falten en Query History...")
    fin.set()
    hilo.join()
    with lock:
        con_srv = sum(1 for f in filas if f.get("total_time_ms"))
    print(f"métricas server-side: {con_srv}/{len(filas)} · crudo: {ruta}")

    print(f"\n{'warehouse':<28}{'query':<20}{'conc':>5}"
          f"{'cli p50':>9}{'cli p95':>9}{'cli p99':>9}{'srv p50':>9}{'srv p99':>9}"
          f"{'cache':>8}")
    for wid, nombre, _tipo, tamano, _s in arms:
        for nombre_q in ["baseline_select_1"] + list(suite):
            for nivel in ([1] if nombre_q == "baseline_select_1" else niveles):
                ok = [f for f in filas
                      if f["warehouse"] == nombre and f["query"] == nombre_q
                      and f["concurrencia"] == nivel and f["estado"] == "SUCCEEDED"]
                if not ok:
                    continue
                cli = [f["client_ms"] for f in ok]
                con_m = [f for f in ok if f.get("total_time_ms")]
                srv = [f["total_time_ms"] for f in con_m]
                hits = sum(1 for f in con_m if f.get("result_from_cache"))
                print(f"{nombre:<28}{nombre_q:<20}{nivel:>5}"
                      f"{pct(cli, 50):>9.0f}{pct(cli, 95):>9.0f}{pct(cli, 99):>9.0f}"
                      f"{pct(srv, 50):>9.0f}{pct(srv, 99):>9.0f}"
                      f"{f'{hits}/{len(con_m)}':>8}")

    sys.exit(0)


if __name__ == "__main__":
    main()
