"""ETL de pedidos para la casuistica A2.

Implementa las etapas EXTRACT, STAGE y VALIDATE usando SQLite, CSV y JSON.
"""

import json
import logging
import sqlite3
import sys
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

PROJECT_DIR = Path(__file__).resolve().parent

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)


# DEFINE: contrato de configuracion del pipeline 
@dataclass(frozen=True)
class ETLConfig:
    database_path: Path
    shipments_path: Path
    carriers_path: Path
    watermark_path: Path
    output_table: str
    quarantine_table: str
    audit_table: str
    quality_thresholds: dict[str, float]


def load_config() -> ETLConfig:
    with (PROJECT_DIR / "config.json").open(encoding="utf-8") as file:
        raw = json.load(file)
    return ETLConfig(
        database_path=PROJECT_DIR / raw["database_path"],
        shipments_path=PROJECT_DIR / raw["shipments_path"],
        carriers_path=PROJECT_DIR / raw["carriers_path"],
        watermark_path=PROJECT_DIR / raw["watermark_path"],
        output_table=raw["output_table"],
        quarantine_table=raw["quarantine_table"],
        audit_table=raw["audit_table"],
        quality_thresholds=raw["quality_thresholds"],
    )


def read_watermark(path: Path) -> str:
    with path.open(encoding="utf-8") as file:
        return json.load(file)["last_processed_updated_at"]


def write_watermark(path: Path, value: str) -> None:
    path.write_text(
        json.dumps({"last_processed_updated_at": value}, indent=2),
        encoding="utf-8",
    )


# EXTRACT: cada fuente se lee de forma independiente
def extract(
    config: ETLConfig, watermark: str
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    with sqlite3.connect(config.database_path) as connection:
        orders = pd.read_sql(
            "SELECT * FROM orders WHERE updated_at > :watermark",
            connection,
            params={"watermark": watermark},
        )

    shipments = pd.read_csv(
        config.shipments_path,
        dtype={"shipment_id": "string", "carrier_code": "string"},
    )

    with config.carriers_path.open(encoding="utf-8") as file:
        carriers_json = json.load(file)
    carriers = (
        pd.DataFrame.from_dict(carriers_json, orient="index")
        .rename_axis("carrier_code")
        .reset_index()
    )

    logging.info(
        "EXTRACT: %s pedidos nuevos/modificados desde watermark=%s | "
        "%s envios | %s transportistas",
        len(orders),
        watermark,
        len(shipments),
        len(carriers),
    )
    return orders, shipments, carriers



# STAGE: se conserva evidencia de origen (source_system, batch_id e
def stage(
    orders: pd.DataFrame,
    shipments: pd.DataFrame,
    carriers: pd.DataFrame,
    batch_id: str,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    ingested_at = pd.Timestamp.now("UTC").isoformat()

    orders = orders.copy()
    orders["source_system"] = "sqlite:orders"
    orders["batch_id"] = batch_id
    orders["ingested_at"] = ingested_at

    shipments = shipments.copy()
    shipments["source_system"] = "csv:shipments"
    shipments["batch_id"] = batch_id
    shipments["ingested_at"] = ingested_at

    carriers = carriers.copy()
    carriers["source_system"] = "json:carriers"
    carriers["batch_id"] = batch_id
    carriers["ingested_at"] = ingested_at

    logging.info("STAGE: batch_id=%s asignado a las tres fuentes", batch_id)
    return orders, shipments, carriers



# VALIDATE: aplica el contrato de fechas de A2. Un delivered_at vacio es
def validate(
    staged_orders: pd.DataFrame,
    staged_shipments: pd.DataFrame,
    staged_carriers: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    required_columns = {
        "order_id",
        "shipment_id",
        "carrier_code",
        "shipped_at",
        "delivered_at",
        "source_system",
        "batch_id",
        "ingested_at",
    }
    missing_columns = required_columns.difference(staged_shipments.columns)
    if missing_columns:
        raise ValueError(
            "Faltan columnas requeridas en shipments: "
            + ", ".join(sorted(missing_columns))
        )

    shipments = staged_shipments.copy()
    raw_delivered_at = shipments["delivered_at"].copy()
    shipments["shipped_at"] = pd.to_datetime(
        shipments["shipped_at"], errors="coerce"
    )
    shipments["delivered_at"] = pd.to_datetime(
        shipments["delivered_at"], errors="coerce"
    )

    shipment_reasons = []
    for index, row in shipments.iterrows():
        reasons = []
        if pd.isna(row["shipped_at"]):
            reasons.append("shipped_at_invalid")
        if pd.notna(raw_delivered_at.loc[index]) and pd.isna(row["delivered_at"]):
            reasons.append("delivered_at_invalid")
        if (
            pd.notna(row["shipped_at"])
            and pd.notna(row["delivered_at"])
            and row["delivered_at"] < row["shipped_at"]
        ):
            reasons.append("delivered_before_shipped")
        shipment_reasons.append(";".join(reasons))

    shipments["rejection_reason"] = shipment_reasons
    quarantined_shipments = shipments[
        shipments["rejection_reason"] != ""
    ].copy()
    valid_shipments = shipments[shipments["rejection_reason"] == ""].copy()

    quarantine = quarantined_shipments.assign(source="shipments")[
        ["source", "batch_id", "order_id", "shipment_id", "rejection_reason"]
    ].reset_index(drop=True)

    logging.info(
        "VALIDATE: pedidos validos=%s | envios validos=%s quarantine=%s | "
        "transportistas=%s",
        len(staged_orders),
        len(valid_shipments),
        len(quarantine),
        len(staged_carriers),
    )
    if not quarantine.empty:
        logging.info("Cuarentena:\n%s", quarantine.to_string(index=False))

    return (
        staged_orders,
        valid_shipments.reset_index(drop=True),
        staged_carriers,
        quarantine,
    )

# TRANSFORM: Homologación de los datos para que coincidan en unidades, variables, etc.
def transform(
    valid_orders: pd.DataFrame,
    valid_shipments: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    orders = valid_orders.copy()

    shipments = valid_shipments.copy()
    shipments["carrier_code"] = shipments["carrier_code"].str.strip().str.upper()

    # Considerar (1:N)
    latest_idx = shipments.groupby("order_id")["shipped_at"].idxmax()
    latest_shipment = shipments.loc[latest_idx, ["order_id", "shipment_id", "carrier_code", "shipped_at", "delivered_at"]]

    logging.info(
        "TRANSFORM: %s pedidios transformados | %s envíos más recientes (de %s envíos)",
        len(orders),
        len(latest_shipment),
        len(shipments),
    )
    return orders, latest_shipment


# INTEGRATE: Combina la información de envíos + ordenes + transportista para validar cardinalidad
def integrate(
    orders: pd.DataFrame,
    latest_shipment: pd.DataFrame,
    carriers: pd.DataFrame,
) -> tuple[pd.DataFrame, dict]:
    rows_before = len(orders)
    merged = orders.merge(latest_shipment, on="order_id", how="left")

    if len(merged) != rows_before:
        raise ValueError(
            f"Cardinalidad inesperada en integrate: {rows_before} vehiculos -> {len(merged)} filas"
        )

    carrier_fields = list(next(iter(carriers.values()), {}).keys())
    default_carrier_info = dict.fromkeys(carrier_fields)
    carrier_info = merged["carrier_code"].map(
        lambda code: carriers.get(code, default_carrier_info)
    )

    for field in carrier_fields:
        merged[f"carrier_{field}"] = carrier_info.map(lambda info: info[field])

    unknown_carrier_mask = merged["carrier_code"].notna() & ~merged["carrier_code"].isin(carriers)

    reconciliation = {
            "rows_before": rows_before,
            "rows_after": len(merged),
            "matched_with_shipment": int(merged["shipment_id"].notna().sum()),
            "unmatched_shipment": int(merged["shipment_id"].isna().sum()),
            "duplicated_order_id": int(merged["order_id"].duplicated().sum()),
            "unknown_carrier": int(unknown_carrier_mask.sum()),
        }
    logging.info("INTEGRATE: %s", reconciliation)

    return merged, reconciliation


# BUILD_CURATED: de aqui sale la tabla que se publica
def build_curated(integrated: pd.DataFrame) -> pd.DataFrame:
    curated = pd.DataFrame()
    curated["order_id"] = integrated["order_id"]
    curated["customer_id"] = integrated.get("customer_id")
    curated["order_date"] = integrated.get("order_date")
    curated["status"] = integrated.get("status")

    delivered_at = integrated["delivered_at"]
    shipped_at = integrated["shipped_at"]

    curated["shipping_status"] = delivered_at.notna().map(
        {True: "delivered", False: "in_transit"}
    )

    delay = (delivered_at - shipped_at).dt.days
    delay = delay.where(delivered_at.notna())
    curated["delivery_delay_days"] = delay.astype("Int64")

    curated["carrier_name"] = integrated["carrier_carrier_name"].fillna("desconocido")

    curated["shipment_id"] = integrated.get("shipment_id")
    curated["carrier_code"] = integrated.get("carrier_code")
    curated["batch_id"] = integrated.get("batch_id")
    curated["ingested_at"] = integrated.get("ingested_at")

    delivered_count = int((curated["shipping_status"] == "delivered").sum())
    in_transit_count = int((curated["shipping_status"] == "in_transit").sum())
    unknown_carrier_count = int((curated["carrier_name"] == "desconocido").sum())

    logging.info(
        "BUILD_CURATED: %s pedidos consolidados | delivered=%s in_transit=%s | "
        "carrier desconocido=%s",
        len(curated),
        delivered_count,
        in_transit_count,
        unknown_carrier_count,
    )
    return curated


# QUALITY_GATE: Las dos metricas de la casuistica contra los umbrales de config.json.
# Si una se pasa, no se carga nada.
def quality_gate(
    curated: pd.DataFrame,
    reconciliation: dict,
    quarantine: pd.DataFrame,
    total_shipments: int,
    thresholds: dict[str, float],
) -> dict:
    total_curated = len(curated)
    unknown_carrier_rate = (
        reconciliation.get("unknown_carrier", 0) / total_curated
        if total_curated > 0
        else 0.0
    )
    invalid_date_rate = (
        len(quarantine) / total_shipments if total_shipments > 0 else 0.0
    )

    unknown_carrier_max = thresholds["unknown_carrier_rate_max"]
    invalid_date_max = thresholds["invalid_date_rate_max"]

    unknown_carrier_pass = unknown_carrier_rate <= unknown_carrier_max
    invalid_date_pass = invalid_date_rate <= invalid_date_max
    overall_status = "PASS" if (unknown_carrier_pass and invalid_date_pass) else "FAIL"

    result = {
        "unknown_carrier_rate": unknown_carrier_rate,
        "unknown_carrier_rate_max": unknown_carrier_max,
        "unknown_carrier_rate_pass": unknown_carrier_pass,
        "invalid_date_rate": invalid_date_rate,
        "invalid_date_rate_max": invalid_date_max,
        "invalid_date_rate_pass": invalid_date_pass,
        "status": overall_status,
    }

    logging.info(
        "QUALITY_GATE: unknown_carrier_rate=%.4f (umbral<=%.4f) -> %s | "
        "invalid_date_rate=%.4f (umbral<=%.4f) -> %s | veredicto global=%s",
        unknown_carrier_rate,
        unknown_carrier_max,
        "PASS" if unknown_carrier_pass else "FAIL",
        invalid_date_rate,
        invalid_date_max,
        "PASS" if invalid_date_pass else "FAIL",
        overall_status,
    )
    return result


# LOAD: upsert por order_id, para que correr esto dos veces no duplique el grain. La
# cuarentena va con OR IGNORE por la misma razon
def load(curated: pd.DataFrame, quarantine: pd.DataFrame, config: ETLConfig) -> int:
    with sqlite3.connect(config.database_path) as connection:
        connection.execute(
            f"""
            CREATE TABLE IF NOT EXISTS "{config.output_table}" (
                order_id INTEGER PRIMARY KEY,
                customer_id INTEGER,
                order_date TEXT,
                status TEXT,
                shipping_status TEXT,
                delivery_delay_days INTEGER,
                carrier_name TEXT,
                shipment_id TEXT,
                carrier_code TEXT,
                batch_id TEXT,
                ingested_at TEXT
            )
            """
        )
        connection.execute(
            f"""
            CREATE TABLE IF NOT EXISTS "{config.quarantine_table}" (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source TEXT,
                batch_id TEXT,
                order_id INTEGER,
                shipment_id TEXT,
                rejection_reason TEXT,
                UNIQUE(batch_id, shipment_id)
            )
            """
        )

        upsert_sql = f"""
            INSERT INTO "{config.output_table}" (
                order_id, customer_id, order_date, status, shipping_status,
                delivery_delay_days, carrier_name, shipment_id, carrier_code,
                batch_id, ingested_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(order_id) DO UPDATE SET
                customer_id = excluded.customer_id,
                order_date = excluded.order_date,
                status = excluded.status,
                shipping_status = excluded.shipping_status,
                delivery_delay_days = excluded.delivery_delay_days,
                carrier_name = excluded.carrier_name,
                shipment_id = excluded.shipment_id,
                carrier_code = excluded.carrier_code,
                batch_id = excluded.batch_id,
                ingested_at = excluded.ingested_at
        """
        curated_rows = [
            (
                int(row.order_id) if pd.notna(row.order_id) else None,
                int(row.customer_id) if pd.notna(row.customer_id) else None,
                row.order_date if pd.notna(row.order_date) else None,
                row.status if pd.notna(row.status) else None,
                row.shipping_status if pd.notna(row.shipping_status) else None,
                int(row.delivery_delay_days)
                if pd.notna(row.delivery_delay_days)
                else None,
                row.carrier_name if pd.notna(row.carrier_name) else None,
                row.shipment_id if pd.notna(row.shipment_id) else None,
                row.carrier_code if pd.notna(row.carrier_code) else None,
                row.batch_id if pd.notna(row.batch_id) else None,
                row.ingested_at if pd.notna(row.ingested_at) else None,
            )
            for row in curated.itertuples(index=False)
        ]
        connection.executemany(upsert_sql, curated_rows)

        quarantine_sql = f"""
            INSERT OR IGNORE INTO "{config.quarantine_table}" (
                source, batch_id, order_id, shipment_id, rejection_reason
            ) VALUES (?, ?, ?, ?, ?)
        """
        quarantine_rows = [
            (
                row.source if pd.notna(row.source) else None,
                row.batch_id if pd.notna(row.batch_id) else None,
                int(row.order_id) if pd.notna(row.order_id) else None,
                row.shipment_id if pd.notna(row.shipment_id) else None,
                row.rejection_reason if pd.notna(row.rejection_reason) else None,
            )
            for row in quarantine.itertuples(index=False)
        ]
        if quarantine_rows:
            connection.executemany(quarantine_sql, quarantine_rows)

        connection.commit()

    rows_loaded = len(curated_rows)
    logging.info(
        "LOAD: %s filas upsert en %s | %s filas insertadas/ignoradas en %s",
        rows_loaded,
        config.output_table,
        len(quarantine_rows),
        config.quarantine_table,
    )
    return rows_loaded


# AUDIT: una fila por corrida en etl_runs, aunque no haya habido datos
def audit(config: ETLConfig, registro: dict) -> None:
    with sqlite3.connect(config.database_path) as connection:
        connection.execute(
            f"""
            CREATE TABLE IF NOT EXISTS "{config.audit_table}" (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                batch_id TEXT,
                started_at TEXT,
                finished_at TEXT,
                watermark_before TEXT,
                watermark_after TEXT,
                rows_extracted INTEGER,
                rows_valid INTEGER,
                rows_quarantined INTEGER,
                rows_loaded INTEGER,
                unknown_carrier_rate REAL,
                invalid_date_rate REAL,
                status TEXT
            )
            """
        )
        connection.execute(
            f"""
            INSERT INTO "{config.audit_table}" (
                batch_id, started_at, finished_at, watermark_before, watermark_after,
                rows_extracted, rows_valid, rows_quarantined, rows_loaded,
                unknown_carrier_rate, invalid_date_rate, status
            ) VALUES (:batch_id, :started_at, :finished_at, :watermark_before,
                :watermark_after, :rows_extracted, :rows_valid, :rows_quarantined,
                :rows_loaded, :unknown_carrier_rate, :invalid_date_rate, :status)
            """,
            registro,
        )
        connection.commit()

    logging.info(
        "AUDIT: corrida batch_id=%s registrada con status=%s",
        registro.get("batch_id"),
        registro.get("status"),
    )


def main() -> None:
    config = load_config()
    started_at = pd.Timestamp.now("UTC").isoformat()
    watermark_before = read_watermark(config.watermark_path)

    logging.info("=== ETL A2 iniciado | watermark=%s ===", watermark_before)
    orders, shipments, carriers = extract(config, watermark_before)

    if orders.empty:
        # Aqui no hay ventana que nombrar, entonces timestamp y ya
        batch_id = f"ETL_{pd.Timestamp.now('UTC').strftime('%Y%m%d_%H%M%S')}"
        logging.info(
            "Sin pedidos nuevos desde el watermark: corrida incremental vacia "
            "(0 filas procesadas) | batch_id=%s",
            batch_id,
        )
        finished_at = pd.Timestamp.now("UTC").isoformat()
        audit(
            config,
            {
                "batch_id": batch_id,
                "started_at": started_at,
                "finished_at": finished_at,
                "watermark_before": watermark_before,
                "watermark_after": watermark_before,
                "rows_extracted": 0,
                "rows_valid": 0,
                "rows_quarantined": 0,
                "rows_loaded": 0,
                "unknown_carrier_rate": None,
                "invalid_date_rate": None,
                "status": "SKIPPED_NO_DATA",
            },
        )
        logging.info(
            "=== Resumen: rows_extracted=0 | status=SKIPPED_NO_DATA ==="
        )
        return

    # El batch_id sale de la ventana de datos (watermark de entrada + el
    # updated_at mas nuevo) y no de un timestamp. Con timestamp cada reproceso
    # generaria un id distinto y la cuarentena se duplicaria sola.
    batch_id = f"ETL_{watermark_before}_{orders['updated_at'].max()}"
    logging.info("batch_id asignado=%s", batch_id)

    staged_orders, staged_shipments, staged_carriers = stage(
        orders, shipments, carriers, batch_id
    )
    valid_orders, valid_shipments, valid_carriers, quarantine = validate(
        staged_orders, staged_shipments, staged_carriers
    )

    carrier_columns = [
        column
        for column in valid_carriers.columns
        if column not in {"carrier_code", "source_system", "batch_id", "ingested_at"}
    ]
    carriers_lookup = (
        valid_carriers.set_index("carrier_code")[carrier_columns].to_dict("index")
    )

    orders_t, latest_shipment = transform(valid_orders, valid_shipments)
    integrated, reconciliation = integrate(orders_t, latest_shipment, carriers_lookup)

    curated = build_curated(integrated)
    gate = quality_gate(
        curated,
        reconciliation,
        quarantine,
        total_shipments=len(shipments),
        thresholds=config.quality_thresholds,
    )

    rows_loaded = 0
    watermark_after = watermark_before
    status = gate["status"]

    if status == "FAIL":
        logging.error(
            "QUALITY_GATE en FAIL: no se carga, no se avanza el watermark"
        )
        finished_at = pd.Timestamp.now("UTC").isoformat()
        audit(
            config,
            {
                "batch_id": batch_id,
                "started_at": started_at,
                "finished_at": finished_at,
                "watermark_before": watermark_before,
                "watermark_after": watermark_after,
                "rows_extracted": len(orders),
                "rows_valid": len(valid_shipments),
                "rows_quarantined": len(quarantine),
                "rows_loaded": rows_loaded,
                "unknown_carrier_rate": gate["unknown_carrier_rate"],
                "invalid_date_rate": gate["invalid_date_rate"],
                "status": "FAIL",
            },
        )
        logging.info(
            "=== Resumen: pedidos=%s | envios validos=%s | cuarentena=%s | "
            "cargados=%s | status=FAIL ===",
            len(orders),
            len(valid_shipments),
            len(quarantine),
            rows_loaded,
        )
        sys.exit(1)

    rows_loaded = load(curated, quarantine, config)
    watermark_after = str(orders["updated_at"].max())
    write_watermark(config.watermark_path, watermark_after)

    finished_at = pd.Timestamp.now("UTC").isoformat()
    audit(
        config,
        {
            "batch_id": batch_id,
            "started_at": started_at,
            "finished_at": finished_at,
            "watermark_before": watermark_before,
            "watermark_after": watermark_after,
            "rows_extracted": len(orders),
            "rows_valid": len(valid_shipments),
            "rows_quarantined": len(quarantine),
            "rows_loaded": rows_loaded,
            "unknown_carrier_rate": gate["unknown_carrier_rate"],
            "invalid_date_rate": gate["invalid_date_rate"],
            "status": "PASS",
        },
    )

    logging.info(
        "=== Resumen: pedidos=%s | envios validos=%s | cuarentena=%s | "
        "cargados=%s | watermark %s -> %s | status=PASS ===",
        len(orders),
        len(valid_shipments),
        len(quarantine),
        rows_loaded,
        watermark_before,
        watermark_after,
    )


if __name__ == "__main__":
    main()
