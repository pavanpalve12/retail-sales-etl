"""
------------------------------------------------------------------------------------
Module Name: init_warehouse_db
------------------------------------------------------------------------------------
This script initializes the **Retail Sales Analytics Warehouse database**.

It is responsible for:
- Resolving project-level paths
- Creating the warehouse SQLite database file (if missing)
- Enabling foreign key enforcement
- Executing warehouse DDL to create fact and dimension tables

This script is **idempotent** and safe to re-run.

------------------------------------------------------------------------------------
Schema Coverage
------------------------------------------------------------------------------------
Warehouse Tables:
- Dimension tables (customers, products, stores, date)
- Fact table (sales_fact)

------------------------------------------------------------------------------------
Design Notes
------------------------------------------------------------------------------------
- Designed for SQLite-based warehouse DB
- Foreign keys are enabled explicitly at runtime
- DDL execution is separated from data loading
- No data insertion occurs in this script
- Acts as the foundation for subsequent Load phase steps
------------------------------------------------------------------------------------
"""

from pathlib import Path
import sqlite3

# ====================================================================================
# Find db/ and sql/ directories
# ====================================================================================
BASE_DIR = Path(__file__).resolve().parent
PROJECT_PATH = BASE_DIR.parent

DB_FILE_NAME = "etl_retail_sales.db"
DDL_SCRIPT_NAME = "create_retail_sales_tables.sql"

DB_DIR_PATH = None
DDL_DIR_PATH = None
for path in PROJECT_PATH.iterdir():
    if path.name == "db":
        DB_DIR_PATH = path
    if path.name == "sql":
        DDL_DIR_PATH = path

# ====================================================================================
# Build db and ddl paths
# ====================================================================================
if DB_DIR_PATH is None or DDL_DIR_PATH is None:
    raise FileNotFoundError(
        f"Either DB or SQL directory not found. {DB_DIR_PATH}, {DDL_DIR_PATH}"
    )

DB_PATH = DB_DIR_PATH / DB_FILE_NAME
DDL_PATH = DDL_DIR_PATH / DDL_SCRIPT_NAME

# ====================================================================================
# Ensure db folder exists
# ====================================================================================
DB_PATH.parent.mkdir(exist_ok=True)

# ====================================================================================
# Connect to db file and execute DDL script
# ====================================================================================
connection = None
try:
    connection = sqlite3.connect(DB_PATH)
    connection.execute("PRAGMA foreign_keys = ON")

    with open(DDL_PATH) as ddl_file:
        connection.executescript(ddl_file.read())

    connection.commit()
    print("Warehouse DB initialized successfully")

except sqlite3.Error as err:
    if connection:
        connection.rollback()
    raise RuntimeError(f"SQLite error during warehouse DB init: {err}")

except FileNotFoundError as err:
    raise RuntimeError(f"DDL file not found: {err}")

finally:
    if connection:
        connection.close()
