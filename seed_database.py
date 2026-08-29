"""Crea data/orders.db"""

import sqlite3
from pathlib import Path

DB_PATH = Path(__file__).resolve().parent / "data" / "orders.db"

orders_data = [
    (100, 1001, "2026-08-09", "shipped", "2026-08-16"),
    (101, 1002, "2026-08-10", "shipped", "2026-08-14"),
    (102, 1003, "2026-08-13", "shipped", "2026-08-15"),
    (103, 1004, "2026-08-15", "shipped", "2026-08-18"),
    (104, 1005, "2026-08-19", "shipped", "2026-08-20"),
]


def main() -> None:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    if DB_PATH.exists():
        DB_PATH.unlink()

    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            """
                CREATE TABLE IF NOT EXISTS orders (
                    order_id INTEGER PRIMARY KEY,
                    customer_id INTEGER,
                    order_date TEXT,
                    status TEXT,
                    updated_at TEXT
                )
                """
        )
        conn.executemany("INSERT OR REPLACE INTO orders VALUES (?, ?, ?, ?, ?)", orders_data)
        conn.commit()

    print(f"Sembrados {len(orders_data)} ordenes en {DB_PATH}")


if __name__ == "__main__":
    main()
