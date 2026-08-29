"""ETL de pedidos para la casuistica A2.

Implementa las etapas EXTRACT, STAGE y VALIDATE usando SQLite, CSV y JSON.
"""

import json
import logging
import sqlite3
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

# QUALITY GATE: compara las métricas reales contra los umbrales definidos para A2
def quality_gate(
    config: ETLConfig,
    valid_shipments: pd.DataFrame,
    quarantine: pd.DataFrame,
    reconciliation: dict,
) -> tuple[dict, str]:
    total_shipments = len(valid_shipments) + len(quarantine)

    unknown_carrier_rate = (
        reconciliation["unknown_carrier"] / len(valid_shipments)
        if len(valid_shipments) > 0
        else 0.0
    )

    invalid_date_rate = (
        len(quarantine) / total_shipments
        if total_shipments > 0
        else 0.0
    )

    metrics = {
        "unknown_carrier_rate": unknown_carrier_rate,
        "invalid_date_rate": invalid_date_rate,
    }

    unknown_carrier_pass = (
        unknown_carrier_rate
        <= config.quality_thresholds["unknown_carrier_rate_max"]
    )

    invalid_date_pass = (
        invalid_date_rate
        <= config.quality_thresholds["invalid_date_rate_max"]
    )

    status = "PASS" if unknown_carrier_pass and invalid_date_pass else "FAIL"

    logging.info(
        "QUALITY GATE: "
        "unknown_carrier_rate=%.4f <= %.4f | "
        "invalid_date_rate=%.4f <= %.4f | "
        "status=%s",
        unknown_carrier_rate,
        config.quality_thresholds["unknown_carrier_rate_max"],
        invalid_date_rate,
        config.quality_thresholds["invalid_date_rate_max"],
        status,
    )

    return metrics, status

# LOAD: inserta o actualiza pedidos en orders_curated sin generar duplicados
def load(
    config: ETLConfig,
    integrated: pd.DataFrame,
    ) -> tuple[int, int]:
    rows = integrated.copy()

    def get_shipping_status(row):
        if pd.isna(row["shipment_id"]):
            return "no_valid_shipment"
        if pd.isna(row["delivered_at"]):
            return "in_transit"
        return "delivered"


    rows["shipping_status"] = rows.apply(get_shipping_status, axis=1)

    rows["delivery_delay_days"] = (
        rows["delivered_at"] - rows["shipped_at"]
    ).dt.days

    if "carrier_carrier_name" in rows.columns:
        rows["carrier_name"] = rows["carrier_carrier_name"]
    else:
        rows["carrier_name"] = None

    rows["carrier_name"] = rows["carrier_name"].fillna("Unknown")

    curated = rows[
        [
            "order_id",
            "shipping_status",
            "delivery_delay_days",
            "carrier_name",
        ]
    ].copy()

    inserted = 0
    updated = 0

    with sqlite3.connect(config.database_path) as connection:
        connection.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {config.output_table} (
                order_id INTEGER PRIMARY KEY,
                shipping_status TEXT,
                delivery_delay_days INTEGER,
                carrier_name TEXT
            )
            """
        )

        for _, row in curated.iterrows():
            exists = connection.execute(
                f"SELECT 1 FROM {config.output_table} WHERE order_id = ?",
                (int(row["order_id"]),),
            ).fetchone()

            delay = (
                None
                if pd.isna(row["delivery_delay_days"])
                else int(row["delivery_delay_days"])
            )

            connection.execute(
                f"""
                INSERT INTO {config.output_table}
                    (order_id, shipping_status, delivery_delay_days, carrier_name)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(order_id) DO UPDATE SET
                    shipping_status = excluded.shipping_status,
                    delivery_delay_days = excluded.delivery_delay_days,
                    carrier_name = excluded.carrier_name
                """,
                (
                    int(row["order_id"]),
                    row["shipping_status"],
                    delay,
                    row["carrier_name"],
                ),
            )

            if exists:
                updated += 1
            else:
                inserted += 1

        connection.commit()

    logging.info(
        "LOAD: %s insertados, %s actualizados (UPSERT) en %s",
        inserted,
        updated,
        config.output_table,
    )

    return inserted, updated

# QUARANTINE: guarda los registros inválidos junto con su motivo de rechazo
def persist_quarantine(
    config: ETLConfig,
    quarantine: pd.DataFrame,
    ) -> int:
    if quarantine.empty:
        return 0

    with sqlite3.connect(config.database_path) as connection:
        quarantine.to_sql(
            config.quarantine_table,
            connection,
            if_exists="append",
            index=False,
        )

    logging.info(
        "QUARANTINE: %s registros guardados en %s",
        len(quarantine),
        config.quarantine_table,
    )

    return len(quarantine)

# AUDIT: registra una fila por cada ejecución del pipeline
def audit(
    config: ETLConfig,
    batch_id: str,
    started_at: str,
    watermark_before: str,
    watermark_after: str,
    source_orders: int,
    valid_shipments: int,
    quarantined_shipments: int,
    inserted: int,
    updated: int,
    metrics: dict,
    quality_status: str,
    status: str,
    ) -> None:
    finished_at = pd.Timestamp.now("UTC").isoformat()

    with sqlite3.connect(config.database_path) as connection:
        connection.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {config.audit_table} (
                run_id TEXT PRIMARY KEY,
                started_at TEXT,
                finished_at TEXT,
                watermark_before TEXT,
                watermark_after TEXT,
                source_orders INTEGER,
                valid_shipments INTEGER,
                quarantined_shipments INTEGER,
                inserted INTEGER,
                updated INTEGER,
                unknown_carrier_rate REAL,
                invalid_date_rate REAL,
                quality_gate_status TEXT,
                status TEXT
            )
            """
        )

        connection.execute(
            f"""
            INSERT INTO {config.audit_table} (
                run_id,
                started_at,
                finished_at,
                watermark_before,
                watermark_after,
                source_orders,
                valid_shipments,
                quarantined_shipments,
                inserted,
                updated,
                unknown_carrier_rate,
                invalid_date_rate,
                quality_gate_status,
                status
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                batch_id,
                started_at,
                finished_at,
                watermark_before,
                watermark_after,
                source_orders,
                valid_shipments,
                quarantined_shipments,
                inserted,
                updated,
                metrics.get("unknown_carrier_rate"),
                metrics.get("invalid_date_rate"),
                quality_status,
                status,
            ),
        )

        connection.commit()

    logging.info(
        "AUDIT: run_id=%s status=%s registrado en %s",
        batch_id,
        status,
        config.audit_table,
    )

def main() -> None:
    config = load_config()
    started_at = pd.Timestamp.now("UTC").isoformat()
    batch_id = f"ETL_{pd.Timestamp.now('UTC').strftime('%Y%m%d_%H%M%S')}"
    watermark_before = read_watermark(config.watermark_path)

    logging.info(
        "=== ETL A2 iniciado | batch_id=%s ===", batch_id
    )
    orders, shipments, carriers = extract(config, watermark_before)

    if orders.empty:
        logging.info(
            "Sin pedidos nuevos desde el watermark: corrida incremental vacia"
        )

        audit(
            config=config,
            batch_id=batch_id,
            started_at=started_at,
            watermark_before=watermark_before,
            watermark_after=watermark_before,
            source_orders=0,
            valid_shipments=0,
            quarantined_shipments=0,
            inserted=0,
            updated=0,
            metrics={},
            quality_status="SKIPPED",
            status="SUCCESS",
        )

        return

    staged_orders, staged_shipments, staged_carriers = stage(
        orders, shipments, carriers, batch_id
    )
    valid_orders, valid_shipments, valid_carriers, quarantine = validate(
        staged_orders, staged_shipments, staged_carriers
    )

    persist_quarantine(
        config,
        quarantine,
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

    metrics, quality_status = quality_gate(
    config,
    valid_shipments,
    quarantine,
    reconciliation,
    )

    if quality_status == "FAIL":
        audit(
            config=config,
            batch_id=batch_id,
            started_at=started_at,
            watermark_before=watermark_before,
            watermark_after=watermark_before,
            source_orders=len(orders),
            valid_shipments=len(valid_shipments),
            quarantined_shipments=len(quarantine),
            inserted=0,
            updated=0,
            metrics=metrics,
            quality_status=quality_status,
            status="FAILED",
        )

        raise RuntimeError("QUALITY GATE falló. No se ejecutará LOAD.")

    inserted, updated = load(
        config,
        integrated,
    )

    watermark_after = (
        pd.to_datetime(valid_orders["updated_at"])
        .max()
        .isoformat()
    )

    write_watermark(
        config.watermark_path,
        watermark_after,
    )

    audit(
        config=config,
        batch_id=batch_id,
        started_at=started_at,
        watermark_before=watermark_before,
        watermark_after=watermark_after,
        source_orders=len(orders),
        valid_shipments=len(valid_shipments),
        quarantined_shipments=len(quarantine),
        inserted=inserted,
        updated=updated,
        metrics=metrics,
        quality_status=quality_status,
        status="SUCCESS",
    )


    logging.info(
        "=== Etapas terminadas: pedidos=%s | envios validos=%s | "
        "cuarentena=%s | transportistas=%s | integrados=%s | "
        "reconciliation=%s ===",
        len(valid_orders),
        len(valid_shipments),
        len(quarantine),
        len(valid_carriers),
        len(integrated),
        reconciliation,
    )


if __name__ == "__main__":
    main()
