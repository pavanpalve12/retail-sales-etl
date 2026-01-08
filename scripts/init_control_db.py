"""
------------------------------------------------------------------------------------
Module Name: init_control_db
------------------------------------------------------------------------------------
This module initializes the **ETL control database** used for logging
and metadata management in the Retail Sales ETL system.

It is responsible for:
- Locating the project-level `db/` and `sql/` directories
- Creating the SQLite control database file if it does not exist
- Executing the control-plane DDL script
- Enabling foreign key constraints for SQLite

This module is intended to be run **once during environment setup**
or safely re-run during development.

------------------------------------------------------------------------------------
Responsibilities
------------------------------------------------------------------------------------
- Resolve project-relative paths for database and DDL files
- Validate required directory and script presence
- Execute multi-statement SQL DDL safely
- Ensure transactional integrity during initialization

------------------------------------------------------------------------------------
Design Notes
------------------------------------------------------------------------------------
- Uses SQLite as a lightweight control-plane database
- Foreign key enforcement is explicitly enabled via PRAGMA
- No logging framework is used (bootstrap-safe)
- No ETL logic, data processing, or runtime orchestration exists here
- This module performs schema initialization only
------------------------------------------------------------------------------------
"""

import sqlite3
from pathlib import Path

# ====================================================================================
# Find db/ and sql/ directories
# ====================================================================================

BASE_DIR = Path(__file__).resolve().parent
PROJECT_PATH = BASE_DIR.parent

DB_FILE_NAME = "etl_control.db"
DDL_SCRIPT_NAME = "create_control_tables.sql"

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
    raise FileNotFoundError(f"Either DB or SQL directory not found. {DB_DIR_PATH}, {DDL_DIR_PATH}")

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
    if not DDL_PATH.exists():
        raise FileNotFoundError(f"DDL script missing at {DDL_PATH}")

    connection = sqlite3.connect(DB_PATH)
    connection.execute("PRAGMA foreign_keys = ON")

    with open(DDL_PATH) as ddl_file:
        connection.executescript(ddl_file.read())

    connection.commit()
    print("Control DB initialized")

except sqlite3.Error as err:
    if connection:
        connection.rollback()
    raise RuntimeError(f"SQLite error during control DB init: {err}")
except FileNotFoundError as err:
    raise RuntimeError(f"DDL file not found: {err}")
finally:
    if connection:
        connection.close()


