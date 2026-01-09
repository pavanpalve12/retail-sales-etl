"""
------------------------------------------------------------------------------------
Module Name: runner_config
------------------------------------------------------------------------------------
This module defines all **configuration and reference data** required by the ETL
pipeline runner.

It acts as the **single source of truth** for pipeline inputs, schema contracts,
and reference mappings used across Extract, Transform, and Load stages.

No executable logic is defined here.

------------------------------------------------------------------------------------
Configuration Coverage
------------------------------------------------------------------------------------
Pipeline Inputs:
- FILE_PATHS          : Source file locations per pipeline
- TABLE_NAMES         : Logical pipeline name → warehouse table mapping
- AS_OF_DATE          : Reference timestamp for deterministic time-based derivations

Schema Contracts:
- EXPECTED_COLUMNS    : Authoritative column list per warehouse table
- PRIMARY_KEYS        : Primary key definition per table
- DATA_TYPE_MAP       : Data type enforcement map (Clean phase only)
- DEFAULT_VALUE_MAP   : Default fill values for non-PK columns (Clean phase)

Reference Data:
- STATE_REGION_MAP    : State → region mapping for stores dimension

------------------------------------------------------------------------------------
Design Notes
------------------------------------------------------------------------------------
- This module contains **constants only**
- No imports from ETL logic modules
- Safe to import across runner, orchestration, and tests
- Changes here affect pipeline behavior without code changes elsewhere
------------------------------------------------------------------------------------
"""


from datetime import datetime, timezone
from pathlib import Path

# ==================================================================================================
# Arguments / parameters required by each stage
# ==================================================================================================

# expected_columns
EXPECTED_COLUMNS = {
    "customers_dim": [
        "customer_id",
        "first_name",
        "last_name",
        "email",
        "city",
        "signup_date"
    ],

    "products_dim": [
        "product_id",
        "product_name",
        "category",
        "price"
    ],

    "stores_dim": [
        "store_id",
        "store_name",
        "city",
        "state"
    ],

    "date_dim": [
        "date",
        "year",
        "month",
        "year_month",
        "day_of_week",
        "is_weekend",
        "quarter"
    ],

    "sales_fact": [
        "sale_id",
        "sale_date",
        "customer_id",
        "product_id",
        "store_id",
        "quantity",
        "unit_price",
        "discount_pct"
    ]
}

# primary_key
PRIMARY_KEYS = {
    "customers_dim": ["customer_id"],
    "products_dim": ["product_id"],
    "stores_dim": ["store_id"],
    "date_dim": ["date"],
    "sales_fact": ["sale_id"]
}

# default_value_map
DEFAULT_VALUE_MAP = {
    "customers_dim": {
        "first_name": "",
        "last_name": "",
        "email": "",
        "city": "UNKNOWN"
    },

    "products_dim": {
        "category": "UNKNOWN"
    },

    "stores_dim": {
        "city": "UNKNOWN",
        "state": "UNKNOWN"
    },

    "sales_fact": {
        "discount_pct": 0.0
    }
}

# dtype_map
DATA_TYPE_MAP = {
    "customers_dim": {
        "customer_id": "string",
        "first_name": "string",
        "last_name": "string",
        "email": "string",
        "city": "string",
        "signup_date": "datetime64[ns]"
    },

    "products_dim": {
        "product_id": "string",
        "product_name": "string",
        "category": "string",
        "price": "float64"
    },

    "stores_dim": {
        "store_id": "string",
        "store_name": "string",
        "city": "string",
        "state": "string"
    },

    "sales_fact": {
        "sale_id": "string",
        "sale_date": "datetime64[ns]",
        "customer_id": "string",
        "product_id": "string",
        "store_id": "string",
        "quantity": "int64",
        "unit_price": "float64",
        "discount_pct": "float64"
    }
}

# state_region_map
STATE_REGION_MAP = {
    # NORTH
    "JK": "NORTH",
    "HP": "NORTH",
    "PB": "NORTH",
    "HR": "NORTH",
    "DL": "NORTH",
    "UK": "NORTH",
    "UP": "NORTH",
    "CH": "NORTH",

    # SOUTH
    "KA": "SOUTH",
    "TN": "SOUTH",
    "KL": "SOUTH",
    "AP": "SOUTH",
    "TG": "SOUTH",
    "PY": "SOUTH",

    # EAST
    "WB": "EAST",
    "OD": "EAST",
    "BR": "EAST",
    "JH": "EAST",

    # WEST
    "MH": "WEST",
    "GJ": "WEST",
    "RJ": "WEST",
    "GA": "WEST",
    "DN": "WEST",
    "DD": "WEST",

    # CENTRAL
    "MP": "CENTRAL",
    "CG": "CENTRAL",

    # NORTH-EAST
    "AS": "NORTH_EAST",
    "AR": "NORTH_EAST",
    "ML": "NORTH_EAST",
    "MN": "NORTH_EAST",
    "MZ": "NORTH_EAST",
    "NL": "NORTH_EAST",
    "TR": "NORTH_EAST",
    "SK": "NORTH_EAST",

    # ISLANDS
    "AN": "ISLANDS",
    "LD": "ISLANDS",

    # OTHER UTs
    "LA": "NORTH"
}


# as_of_date
AS_OF_DATE = datetime.now(timezone.utc)

# table names
TABLE_NAMES = {
    "customers": "customers_dim",
    "products": "products_dim",
    "stores": "stores_dim",
    "sales": "sales_fact",
    "date": "date_dim"
}

# db paths
BASE_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = BASE_DIR.parent
CONTROL_DB_PATH = PROJECT_ROOT / "db" / "etl_control.db"
RETAIL_SALES_DB_PATH = PROJECT_ROOT/ "db" / "etl_retail_sales.db"

# file paths
FILE_PATHS = {
    "customers": PROJECT_ROOT / "data" / "customers" / "customers.csv",
    "products": PROJECT_ROOT / "data" / "products" / "products.csv",
    "stores": PROJECT_ROOT / "data" / "stores" / "stores.csv",
    "sales": PROJECT_ROOT / "data" / "sales" / "sales.csv"
}

# pipelines
PIPELINES = ["customers", "products", "stores", "sales"]


