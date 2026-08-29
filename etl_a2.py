import json
import logging
import sqlite3
import uuid
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

PROJECT_DIR = Path(__file__).resolve().parent

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)

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
    path.write_text(json.dumps({"last_processed_updated_at": value}, indent=2), encoding="utf-8")

