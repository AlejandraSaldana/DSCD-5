#!/usr/bin/env bash
#
# Demo completa del ETL A2. Un solo comando: bash run_etl.sh
#
# Corre el pipeline tres veces para dejar evidencia de las cuatro cosas que
# pide la entrega: el envio que cae en cuarentena, el quality gate contra sus
# umbrales, que reprocesar no duplique filas, y la corrida incremental que ya
# no encuentra nada nuevo.
#
# Lo del `if` de hasta abajo: etl_a2.py sale con codigo 1 si el gate falla, y
# con set -e eso mata el script antes de alcanzar a decir donde quedo el log.
# Metido como condicion de un if el fallo no dispara el set -e de afuera, y el
# codigo real se saca de PIPESTATUS. tee escribe en streaming, asi que el log
# se guarda igual aunque la corrida se corte a la mitad.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

VENV_DIR=".venv"
PY="$VENV_DIR/bin/python"
PIP="$VENV_DIR/bin/pip"
LOG_FILE="evidencia_corridas.log"

# Deja el watermark en 1900-01-01 para que la corrida siguiente vea todos los
# pedidos como nuevos. La ruta la lee de config.json, no va hardcodeada.
reset_watermark() {
    "$PY" -c "
import json
from pathlib import Path

with open('config.json', encoding='utf-8') as f:
    cfg = json.load(f)

watermark_path = Path(cfg['watermark_path'])
watermark_path.write_text(
    json.dumps({'last_processed_updated_at': '1900-01-01'}, indent=2),
    encoding='utf-8',
)
print(f'watermark reseteado -> {watermark_path} = last_processed_updated_at 1900-01-01')
"
}

# count(*) de las dos tablas de salida
print_counts() {
    "$PY" -c "
import json
import sqlite3

with open('config.json', encoding='utf-8') as f:
    cfg = json.load(f)

with sqlite3.connect(cfg['database_path']) as conn:
    for table in (cfg['output_table'], cfg['quarantine_table']):
        try:
            count = conn.execute(f'SELECT count(*) FROM {table}').fetchone()[0]
            print(f'SELECT count(*) FROM {table}  ->  {count}')
        except sqlite3.OperationalError as exc:
            print(f'SELECT count(*) FROM {table}  ->  (tabla aun no existe: {exc})')
"
}

# Vuelca completas orders_curated, shipments_quarantine y etl_runs.
dump_tables() {
    "$PY" -c "
import json
import sqlite3

import pandas as pd

with open('config.json', encoding='utf-8') as f:
    cfg = json.load(f)

with sqlite3.connect(cfg['database_path']) as conn:
    tablas = (
        ('orders_curated', cfg['output_table']),
        ('shipments_quarantine', cfg['quarantine_table']),
        ('etl_runs', cfg['audit_table']),
    )
    for etiqueta, tabla in tablas:
        print(f'--- {etiqueta} ({tabla}) ---')
        try:
            df = pd.read_sql(f'SELECT * FROM {tabla}', conn)
            if df.empty:
                print('(sin filas)')
            else:
                print(df.to_string(index=False))
        except Exception as exc:
            print(f'No se pudo leer {tabla}: {exc}')
        print()
"
}

run_demo() {
    echo "===== 0. PREPARACION DEL ENTORNO ====="
    if [ ! -d "$VENV_DIR" ]; then
        echo "Creando entorno virtual en $VENV_DIR ..."
        python3 -m venv "$VENV_DIR"
    else
        echo "Reutilizando entorno virtual existente en $VENV_DIR (no se recrea)."
    fi

    echo "Instalando dependencias de requirements.txt en $VENV_DIR ..."
    "$PIP" install -q -r requirements.txt
    echo "Entorno listo: $PY"

    echo
    echo "===== 1. SEMBRADO DE LA BASE (seed_database.py) ====="
    echo "Recrea data/orders.db desde cero con los 5 pedidos de la semilla,"
    echo "incluyendo el envio S6 (order_id 104) que dispara delivered_before_shipped."
    "$PY" seed_database.py

    echo
    echo "===== 2. RESET DE WATERMARK (arranque limpio de la demo) ====="
    reset_watermark

    echo
    echo "===== CORRIDA 1: carga inicial ====="
    echo "Evidencia esperada: los 5 pedidos se leen como nuevos, el envio S6"
    echo "(order_id 104, delivered_at < shipped_at) cae en cuarentena con el"
    echo "motivo exacto 'delivered_before_shipped', y al final el Quality Gate"
    echo "imprime sus metricas contra los umbrales de config.json con veredicto PASS/FAIL."
    "$PY" etl_a2.py

    echo
    echo "----- Conteo de filas tras la CORRIDA 1 -----"
    print_counts

    echo
    echo "===== RESET DE WATERMARK ANTES DE LA CORRIDA 2 ====="
    echo "Se resetea el watermark a 1900-01-01 otra vez, SIN volver a sembrar la"
    echo "base, para forzar que el pipeline vea de nuevo los mismos 5 pedidos ya"
    echo "cargados. Esto es lo que permite demostrar que el LOAD es idempotente:"
    echo "si orders_curated no duplica filas al reprocesar exactamente los mismos"
    echo "datos, la carga esta haciendo upsert/merge por clave y no un INSERT ciego."
    reset_watermark

    echo
    echo "===== CORRIDA 2: reproceso de los mismos pedidos (prueba de idempotencia) ====="
    "$PY" etl_a2.py

    echo
    echo "----- Conteo de filas tras la CORRIDA 2 (debe ser IGUAL al de la CORRIDA 1) -----"
    print_counts

    echo
    echo "===== CORRIDA 3: sin datos nuevos (watermark ya avanzado) ====="
    echo "No se toca el watermark: la CORRIDA 2 ya lo dejo en la fecha mas reciente"
    echo "procesada (2026-08-20 con la semilla actual), asi que el pipeline no debe"
    echo "encontrar pedidos con updated_at mayor y debe procesar 0 filas."
    "$PY" etl_a2.py

    echo
    echo "----- Conteo de filas tras la CORRIDA 3 (debe seguir IGUAL, no hubo carga nueva) -----"
    print_counts

    echo
    echo "===== TABLAS FINALES ====="
    dump_tables

    echo
    echo "===== FIN DE LA DEMO ====="
}

if run_demo 2>&1 | tee "$LOG_FILE"; then
    STATUS=0
else
    STATUS=${PIPESTATUS[0]}
fi

echo
if [ "$STATUS" -ne 0 ]; then
    echo "AVISO: la demo se interrumpio con un error real (codigo $STATUS)," \
         "posiblemente el Quality Gate en FAIL. Es intencional que aborte ahi;" \
         "toda la salida generada hasta ese punto ya quedo guardada en el log."
fi
echo "Log completo de la demo guardado en: $SCRIPT_DIR/$LOG_FILE"

exit "$STATUS"
