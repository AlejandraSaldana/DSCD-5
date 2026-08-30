# ETL A2 — Estado de entrega consolidado de pedidos

Pipeline de 9 etapas (DEFINE → EXTRACT → STAGE → VALIDATE → TRANSFORM → INTEGRATE → QUALITY GATE → LOAD → AUDIT). Motor: pandas + SQLite. Consolida tres fuentes — `orders` en SQLite, `shipments` en CSV y `carriers` en JSON — en una tabla curada de un pedido por fila.

La casuística A2, resumida: un pedido puede tener varios envíos (relación 1:N), y quien consulte `orders_curated` — reporting, soporte, cualquiera que necesite saber en qué quedó el pedido X — no debería tener que resolver esa relación a mano cada vez. Todo el pipeline existe para eso, en el fondo: publicar un único estado de entrega por pedido.

## Cómo se corre

```bash
bash run_etl.sh
```

Un solo comando. Crea o reutiliza el entorno virtual, instala `requirements.txt`, siembra la base con `seed_database.py` y corre el pipeline tres veces seguidas, reseteando el watermark entre la 1 y la 2 para poder demostrar idempotencia y dejando una tercera corrida incremental vacía (ver [Evidencia](#evidencia)). Todo lo impreso queda además guardado en `evidencia_corridas.log` — incluso si el Quality Gate llega a fallar y el script aborta a la mitad, porque `run_demo | tee` va capturando la salida en streaming antes del corte.

## Estructura de archivos

```
DEFINE.md              contrato de datos de la casuística A2 (fuentes, grain, business key, refresh, contrato de salida)
config.json             rutas de datos, nombres de tabla y umbrales del Quality Gate
seed_database.py        siembra data/orders.db con los 5 pedidos (reutilizado del ejemplo de clase)
etl_a2.py               las 9 etapas del pipeline
run_etl.sh              orquesta la demo end-to-end (venv, seed, 3 corridas, log)
data/orders.db          SQLite: tabla orders (fuente) + orders_curated, shipments_quarantine, etl_runs (salida)
data/shipments.csv      envíos (fuente)
data/carriers.json      catálogo de transportistas: solo DHL y FDX (fuente)
data/watermark.json     último updated_at procesado con éxito (estado del refresh incremental)
evidencia_corridas.log  salida completa de las 3 corridas de bash run_etl.sh
evidencias/             capturas de terminal de cada prueba
```

## Las 9 etapas

1. **DEFINE** — el contrato vive en `DEFINE.md`: fuentes, grain (`order_id`), business key, refresh strategy y el esquema de `orders_curated`.
2. **EXTRACT** — lee `orders` (solo lo posterior al watermark), `shipments.csv` completo y `carriers.json` completo.
3. **STAGE** — estampa metadatos de procedencia en cada fuente por separado: `source_system` (`sqlite:orders`, `csv:shipments`, `json:carriers`), `batch_id` e `ingested_at`. Ninguna fila pierde de dónde vino ni en qué corrida entró.
4. **VALIDATE** — separa envíos válidos de inválidos. Los inválidos van a cuarentena con su motivo de rechazo.
5. **TRANSFORM** — por pedido se queda con un único envío (ver [Limitaciones](#limitaciones-conocidas)) y normaliza `carrier_code`.
6. **INTEGRATE** — merge de pedidos + envío elegido + carrier. Produce el reporte de reconciliación: matched, unmatched, duplicados, carrier desconocido.
7. **QUALITY GATE** — calcula `unknown_carrier_rate` e `invalid_date_rate` contra los umbrales de `config.json` y decide PASS/FAIL antes de tocar el LOAD.
8. **LOAD** — upsert en `orders_curated`, inserta (o ignora duplicados) en `shipments_quarantine`. El watermark avanza únicamente si el gate dio PASS.
9. **AUDIT** — registra la corrida en `etl_runs`, con sus métricas y su status, pase lo que pase (PASS, FAIL o SKIPPED_NO_DATA).

## Contrato de salida: `orders_curated`

Grain: un pedido por fila. Esto es lo que devuelve la corrida sobre la semilla actual:

| order_id | shipping_status | delivery_delay_days | carrier_name | shipment_id | carrier_code |
|---|---|---|---|---|---|
| 100 | delivered | 5 | DHL Express | S2 | DHL |
| 101 | delivered | 2 | FedEx | S3 | FDX |
| 102 | in_transit | NULL | FedEx | S4 | FDX |
| 103 | delivered | 2 | desconocido | S5 | XYZ |
| 104 | in_transit | NULL | desconocido | NULL | NULL |

`delivery_delay_days` guarda INTEGER real o NULL real —verificado con `typeof()` en SQLite sobre los pedidos 102 y 104—, no `0` ni `NaN`. Esos dos serían ambiguos con "sin retraso" o "dato corrupto" y ese fue justo uno de los puntos donde hubo que pensarlo dos veces antes de escribir el UPDATE.

Cuarentena (`shipments_quarantine`), 1 fila: `shipments / order_id 104 / S6 / rejection_reason = delivered_before_shipped`.

### Esquemas creados por LOAD/AUDIT

- **`orders_curated`**: `order_id` INTEGER PRIMARY KEY, `customer_id`, `order_date`, `status`, `shipping_status`, `delivery_delay_days`, `carrier_name`, `shipment_id`, `carrier_code`, `batch_id`, `ingested_at`. Se escribe con `INSERT ... ON CONFLICT(order_id) DO UPDATE`.
- **`shipments_quarantine`**: `id` autoincrement, `source`, `batch_id`, `order_id`, `shipment_id`, `rejection_reason`, con `UNIQUE(batch_id, shipment_id)` e `INSERT OR IGNORE`.
- **`etl_runs`**: `id`, `batch_id`, `started_at`, `finished_at`, `watermark_before`, `watermark_after`, `rows_extracted`, `rows_valid`, `rows_quarantined`, `rows_loaded`, `unknown_carrier_rate`, `invalid_date_rate`, `status`.

## Decisiones de diseño

Avanzar el watermark antes de validar la calidad sería un error grave. Si una corrida trae datos malos y el gate falla, esos pedidos nunca se cargan a `orders_curated` — pero si el watermark ya hubiera avanzado, la siguiente corrida los daría por "ya vistos" y jamás volvería a intentarlos. Se perderían en silencio, que es lo peor que le puede pasar a un pipeline. Por eso el watermark en `data/watermark.json` sólo avanza si el Quality Gate da PASS: es la pieza que conecta refresh incremental con confiabilidad de los datos.

UPSERT por `order_id`, no INSERT. Es lo que hace idempotente al LOAD: reprocesar la misma ventana no duplica el grain, solo actualiza la fila existente (corrida 2, ver [Evidencia](#evidencia)).

El `batch_id` es determinista y no un timestamp — esta fue probablemente la decisión que más costó justificar bien. Se deriva de la ventana de datos procesada: `ETL_<watermark_de_entrada>_<max_updated_at>`. `shipments_quarantine` es append-only y se deduplica con `UNIQUE(batch_id, shipment_id)`; si el `batch_id` fuera un timestamp, reprocesar la misma ventana generaría un `batch_id` distinto cada vez, volvería a insertar la misma fila S6 y la cuarentena crecería sin motivo real. Asi es como se evita ese crecimiento fantasma. En la corrida vacía (`SKIPPED_NO_DATA`) no hay ventana de datos que nombrar, entonces ahí sí se usa un `batch_id` por timestamp (`ETL_20260830_050747` en la corrida 3).

Tres semánticas distintas para "falta el dato". Es el núcleo de esta casuística y ningún caso se resuelve con un genérico "nulo = inválido":

- `delivered_at` vacío es un estado legítimo (el pedido sigue en camino) → `shipping_status = "in_transit"` con `delivery_delay_days = NULL` (pedido 102).
- `delivered_at` anterior a `shipped_at` es un dato imposible, un error real de origen → va a cuarentena (envío S6, pedido 104).
- `carrier_code` que no existe en `carriers.json` es un dato pobre pero no inválido → se publica `carrier_name = "desconocido"` y se cuenta en `unknown_carrier_rate`, sin descartar el pedido (pedido 103, código `XYZ`).

Si el gate da FAIL no se carga nada y el watermark se queda donde estaba. Aun así la corrida se audita, con `status = FAIL`, y el proceso termina con código de salida 1 — un problema de calidad tiene que notarse desde afuera y no solamente en el log.

El pedido 100 se consolida contra S2 y no contra S1 porque tiene dos paquetes — la relación 1:N de la que trata toda esta casuística —, y el estado consolidado se calcula contra el envío que determina cuándo quedó realmente resuelto el pedido. No contra el primero que salió.

## Quality Gate

Umbrales definidos en `config.json`:

```json
"quality_thresholds": {
  "unknown_carrier_rate_max": 0.25,
  "invalid_date_rate_max": 0.20
}
```

Cálculo real de la corrida:

| métrica | fórmula | valor | umbral | resultado |
|---|---|---|---|---|
| `unknown_carrier_rate` | 1 carrier desconocido / 5 pedidos curados | 0.2000 | ≤ 0.25 | PASS |
| `invalid_date_rate` | 1 envío en cuarentena / 6 envíos crudos | 0.1667 | ≤ 0.20 | PASS |

Veredicto global: **PASS**.

Reconciliación de INTEGRATE para esta corrida:

```
{'rows_before': 5, 'rows_after': 5, 'matched_with_shipment': 4, 'unmatched_shipment': 1, 'duplicated_order_id': 0, 'unknown_carrier': 1}
```

## Evidencia

`bash run_etl.sh` corre el pipeline tres veces y deja todo en `evidencia_corridas.log`. De ahí salen las 4 pruebas pedidas, y las capturas de cada una están en `evidencias/`.

Cuarentena con motivo: línea `Cuarentena:` de la corrida 1 (log línea ~27-29), el envío S6 del pedido 104 cae con `rejection_reason = delivered_before_shipped`.

Quality Gate con métricas y umbrales — línea `QUALITY_GATE:` de cualquiera de las dos corridas con datos: `unknown_carrier_rate=0.2000 (umbral<=0.2500) -> PASS | invalid_date_rate=0.1667 (umbral<=0.2000) -> PASS | veredicto global=PASS`.

Idempotencia del LOAD. Esta es la que más vale la pena mirar con calma: corrida 1 contra corrida 2, mismo watermark de entrada (`1900-01-01`), se vuelven a extraer y cargar 5 pedidos, y aun así `SELECT count(*) FROM orders_curated` da **5, no 10**. `shipments_quarantine` se queda en **1** en ambas corridas — nada mas cambia. El upsert por `order_id` y el `UNIQUE(batch_id, shipment_id)` de la cuarentena hacen su trabajo.

Y por último, la corrida incremental vacía. En la ultima corrida el watermark ya venía en `2026-08-20` (dejado ahí por la corrida 2): `EXTRACT: 0 pedidos nuevos/modificados`, status `SKIPPED_NO_DATA`, no se carga nada y los conteos siguen en 5 y 1.

`etl_runs` termina con 3 filas: PASS, PASS, SKIPPED_NO_DATA, con `rows_loaded` 5 / 5 / 0 respectivamente.

## Limitaciones conocidas

`transform()` selecciona el envío de cada pedido con `idxmax` sobre `shipped_at`: el enviado más recientemente. El contrato de `DEFINE.md` pide en cambio el envío más lento (el de mayor diferencia entre `shipped_at` y `delivered_at`). Con la semilla actual ambos criterios eligen el mismo envío para el pedido 100 (S2), así que el resultado publicado arriba es correcto. Pero la regla no generaliza: un pedido cuyo paquete más reciente llegue rápido y uno anterior haya llegado tarde se consolidaría contra el envío equivocado. Queda identificado y sin corregir en esta entrega.

## Qué se tomó de los ejemplos de clase y qué se resolvió distinto

De clase se reutilizó `seed_database.py` tal cual, y el esqueleto de nueve etapas del ejemplo 2: pandas + SQLite, cuarentena con motivo de rechazo, quality gate contra umbrales, auditoría por corrida. Lo que hubo que resolver distinto para la casuística A2 fue otra cosa. Primero, colapsar la relación 1:N pedido→envíos a un grain de un pedido por fila, cuando los ejemplos de clase trabajaban con joins 1:1. Después, distinguir tres semánticas distintas para un campo vacío —en tránsito legítimo, fecha imposible, catálogo incompleto— en vez de tratar todo nulo como inválido. Y por último, hacer el `batch_id` determinista en vez de un timestamp, para que la cuarentena resistiera el reproceso sin duplicarse.
