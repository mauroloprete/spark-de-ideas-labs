-- Las queries del lab: qué se empuja a Postgres y qué se queda en el warehouse.
-- En cada plan, buscá la línea "External engine query": es el SQL literal que viaja a Neon.

-- A: filtro de fecha + agregado. Los dos se empujan:
--    Postgres agrega ~1,6M de filas y por el cable cruzan 3.
EXPLAIN FORMATTED
SELECT canal, count(*) AS ordenes
FROM pg_ventas.public.orders
WHERE fecha >= '2026-01-01'
GROUP BY canal;

-- B: levenshtein no tiene traducción SQL en Postgres (solo existe vía extensión).
--    El plan muestra un PhotonFilter local y la subquery remota viaja casi desnuda:
--    Postgres devuelve los 5M de filas por un único stream y el warehouse filtra después.
EXPLAIN FORMATTED
SELECT *
FROM pg_ventas.public.orders
WHERE levenshtein(cliente, 'cliente_42') <= 1;

-- C: el truco del AND. La parte empujable viaja aunque la otra no:
--    la subquery remota lleva el filtro de fecha; el PhotonFilter local, el levenshtein.
EXPLAIN FORMATTED
SELECT *
FROM pg_ventas.public.orders
WHERE fecha >= '2026-01-01'
  AND levenshtein(cliente, 'cliente_42') <= 1;

-- Bonus: el ejemplo "no empujable" de la doc, que hoy SÍ se empuja.
--    El motor reescribe ILIKE como LOWER("cliente") LIKE '%tech%' y lo manda a Postgres.
--    Verificá contra tu plan, no contra la doc.
EXPLAIN FORMATTED
SELECT *
FROM pg_ventas.public.orders
WHERE cliente ILIKE '%tech%';

-- La query A ejecutada de verdad (3 filas de tráfico):
SELECT canal, count(*) AS ordenes
FROM pg_ventas.public.orders
WHERE fecha >= '2026-01-01'
GROUP BY canal
ORDER BY ordenes DESC;
