"""
------------------------------------------------------------------------------------
Module Name: bootstrap_metadata
------------------------------------------------------------------------------------
This script bootstraps the **control-plane metadata** for the Retail Sales ETL system.

It performs one-time registration of:
- Pipelines
- Warehouse tables
- Pipeline-to-table execution mappings

These metadata records describe the **static topology and configuration** of the
ETL system and are not part of routine pipeline execution.

------------------------------------------------------------------------------------
Scope & Responsibilities
------------------------------------------------------------------------------------
- Insert records into pipeline_md
- Insert records into table_md
- Insert records into pipeline_table_map
- Ensure metadata consistency before pipelines are executed

------------------------------------------------------------------------------------
Design Notes
------------------------------------------------------------------------------------
- This script is intended to be run **once per environment**
- Safe to re-run if INSERTs are guarded or tables are reset
- No ETL logic or data processing is performed here
- Uses explicit metadata definitions (no inference)
------------------------------------------------------------------------------------
"""

import sqlite3
from datetime import datetime, timezone
from pathlib import Path


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def bootstrap_metadata(connection: sqlite3.Connection) -> None:
    """
    Insert static metadata records for pipelines and warehouse tables.

    :param connection: Active SQLite connection to control database
    :return: None
    """
    now = _utc_now()

    # ------------------------------------------------------------------
    # Pipeline metadata
    # ------------------------------------------------------------------
    pipelines = [
        ("customers", "customers", "full", "manual", 1),
        ("products", "products", "full", "manual", 1),
        ("stores", "stores", "full", "manual", 1),
        ("sales", "sales", "full", "manual", 1),
    ]

    connection.executemany(
        """
        INSERT INTO pipeline_md
        (pipeline_name, source_name, load_strategy, schedule, is_active, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        [(p, s, ls, sch, ia, now, now) for p, s, ls, sch, ia in pipelines]
    )

    # ------------------------------------------------------------------
    # Table metadata
    # ------------------------------------------------------------------
    tables = [
        ("customers_dim", "load", "customers", "customer", "customer_id", "full", None),
        ("products_dim", "load", "products", "product", "product_id", "full", None),
        ("stores_dim", "load", "stores", "store", "store_id", "full", None),
        ("sales_fact", "load", "sales", "transaction", "sale_id", "full", None),
        ("date_dim", "load", "sales", "date", "date", "full", None),
    ]

    connection.executemany(
        """
        INSERT INTO table_md
        (
            table_name,
            layer,
            source_name,
            grain,
            primary_key,
            load_strategy,
            watermark_column,
            last_loaded_value,
            row_count,
            is_active,
            created_at,
            updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, NULL, 0, 1, ?, ?)
        """,
        [(t, l, s, g, pk, ls, wm, now, now) for t, l, s, g, pk, ls, wm in tables]
    )

    # ------------------------------------------------------------------
    # Pipeline-to-table mappings
    # ------------------------------------------------------------------
    pipeline_table_maps = [
        ("customers", "customers_dim", 1, "dimension"),
        ("products", "products_dim", 1, "dimension"),
        ("stores", "stores_dim", 1, "dimension"),
        ("sales", "date_dim", 1, "dimension"),
        ("sales", "sales_fact", 2, "fact"),
    ]

    connection.executemany(
        """
        INSERT INTO pipeline_table_map
        (pipeline_name, table_name, load_order, table_role)
        VALUES (?, ?, ?, ?)
        """,
        pipeline_table_maps
    )

    connection.commit()


if __name__ == "__main__":
    BASE_DIR = Path(__file__).resolve().parent
    PROJECT_ROOT = BASE_DIR.parent

    CONTROL_DB_PATH = PROJECT_ROOT / "db" / "etl_control.db"

    conn = sqlite3.connect(CONTROL_DB_PATH)
    conn.execute("PRAGMA foreign_keys = ON")

    try:
        bootstrap_metadata(conn)
        print("Metadata bootstrap completed successfully.")
    finally:
        conn.close()
