"""
------------------------------------------------------------------------------------
Module Name: transform_model_t2
------------------------------------------------------------------------------------
This module implements **Transform Phase-2 (Modeling)** for the Retail Sales ETL
pipeline.

The Model phase is responsible for:
- Converting clean datasets into analytical fact and dimension tables
- Adding derived, business-meaningful attributes
- Enforcing table grain and schema contracts
- Preparing warehouse-ready outputs

No data cleaning, deduplication, or row filtering is performed in this phase.

------------------------------------------------------------------------------------
Scope & Responsibilities
------------------------------------------------------------------------------------
- Build fact and dimension tables from T1-clean data
- Generate deterministic derived columns
- Preserve row counts and grain
- Enforce schema, PK, and naming invariants
- Raise on any integrity violation

------------------------------------------------------------------------------------
Design Notes
------------------------------------------------------------------------------------
- Operates on single DataFrame inputs
- All functions are deterministic
- No database access
- No orchestration logic (handled externally)
------------------------------------------------------------------------------------
"""

import logging
import re
from typing import List, Dict

import pandas as pd

# ==================================================================================================
# Dimension Builders
# ==================================================================================================
def _build_customers_dim(
        source_name: str,
        data: pd.DataFrame,
        expected_columns: List[str],
        *,
        logger: logging.Logger,
        **kwargs
) -> (pd.DataFrame, List[str]):
    """
    Build customers dimension table.

    :param source_name: Source identifier
    :param data: Clean input DataFrame
    :param expected_columns: Final schema columns
    :param as_of_date: Reference date for tenure calculation
    :param logger: Shared ETL logger
    :return: customers_dim DataFrame
    """
    logger.info("Building customers_dim for source=%s", source_name)

    data = data[expected_columns].copy()

    data["customer_full_name"] = (
        data["first_name"].fillna("") + " " + data["last_name"].fillna("")
    ).str.strip()

    data["customer_tenure_days"] = (
            kwargs["as_of_date"] - pd.to_datetime(data["signup_date"], utc=True)
    ).dt.days
    data["customer_tenure_bucket"] = data["customer_tenure_days"].apply(
        _assign_customer_tenure_bucket
    )

    data["email_domain"] = data["email"].str.split("@").str[1]
    derived_columns = [column for column in data.columns if column not in expected_columns]

    return data, derived_columns


def _build_products_dim(
        source_name: str,
        data: pd.DataFrame,
        expected_columns: List[str],
        *,
        logger: logging.Logger,
        **kwargs
) -> (pd.DataFrame, List[str]):
    """
    Build products dimension table.

    :param source_name: Source identifier
    :param data: Clean input DataFrame
    :param expected_columns: Final schema columns
    :param logger: Shared ETL logger
    :return: products_dim DataFrame
    """
    logger.info("Building products_dim for source=%s", source_name)

    data = data[expected_columns].copy()

    data["price_band"] = data["price"].apply(_assign_band)
    data["is_premium_product"] = data["price"] > 2000
    data["category_normalized"] = data["category"].str.upper()

    derived_columns = [column for column in data.columns if column not in expected_columns]
    return data, derived_columns


def _build_stores_dim(
        source_name: str,
        data: pd.DataFrame,
        expected_columns: List[str],
        *,
        logger: logging.Logger,
        **kwargs
) -> (pd.DataFrame, List[str]):
    """
    Build stores dimension table.

    :param source_name: Source identifier
    :param data: Clean input DataFrame
    :param expected_columns: Final schema columns
    :param state_region_map: Mapping of state code to region
    :param logger: Shared ETL logger
    :return: stores_dim DataFrame
    """
    logger.info("Building stores_dim for source=%s", source_name)

    data = data[expected_columns].copy()

    data["city"] = data["city"].str.upper()
    data["state"] = data["state"].str.upper()

    metro_cities = {"MUMBAI", "DELHI", "BANGALORE", "CHENNAI"}
    data["is_metro_store"] = data["city"].isin(metro_cities)

    data["store_region"] = data["state"].map(kwargs["state_region_map"])

    if data["store_region"].isna().any():
        unmapped = data.loc[data["store_region"].isna(), "state"].unique()
        logger.error("Unmapped states found: %s", unmapped)
        raise ValueError("Unmapped states found while deriving store_region")

    derived_columns = [column for column in data.columns if column not in expected_columns]
    return data, derived_columns


# ==================================================================================================
# Fact Builder
# ==================================================================================================
def _build_sales_fact(
        source_name: str,
        data: pd.DataFrame,
        expected_columns: List[str],
        *,
        logger: logging.Logger,
        **kwargs
) -> (pd.DataFrame, List[str]):
    """
    Build sales fact table.

    :param source_name: Source identifier
    :param data: Clean input DataFrame
    :param expected_columns: Final schema columns
    :param logger: Shared ETL logger
    :return: sales_fact DataFrame
    """
    logger.info("Building sales_fact for source=%s", source_name)

    data = data[expected_columns].copy()

    data["gross_amount"] = data["quantity"] * data["unit_price"]
    data["discount_amount"] = data["gross_amount"] * (data["discount_pct"] / 100)
    data["net_amount"] = data["gross_amount"] - data["discount_amount"]
    data["is_discounted"] = data["discount_pct"] > 0

    data["sale_date"] = pd.to_datetime(data["sale_date"], utc=True)
    data["order_year"] = data["sale_date"].dt.year
    data["order_month"] = data["sale_date"].dt.strftime("%Y-%m")

    derived_columns = [column for column in data.columns if column not in expected_columns]
    return data, derived_columns


# ==================================================================================================
# Date Dimension Builder (called by orchestrator)
# ==================================================================================================
def build_date_dim(
    min_date: pd.Timestamp,
    max_date: pd.Timestamp,
    logger: logging.Logger
) -> pd.DataFrame:
    """
    Build date dimension for a given date range.

    :param min_date: Minimum date
    :param max_date: Maximum date
    :param logger: Shared ETL logger
    :return: date_dim DataFrame
    """
    logger.info("Building date_dim")

    dates = pd.date_range(start=min_date, end=max_date)
    date_dim = pd.DataFrame({"date": dates})

    date_dim["year"] = date_dim["date"].dt.year
    date_dim["month"] = date_dim["date"].dt.month
    date_dim["year_month"] = date_dim["date"].dt.strftime("%Y-%m")
    date_dim["day_of_week"] = date_dim["date"].dt.dayofweek
    date_dim["is_weekend"] = date_dim["day_of_week"].isin([5, 6])
    date_dim["quarter"] = date_dim["date"].dt.quarter

    return date_dim


# ==================================================================================================
# Execute Transform - Phase 2
# ==================================================================================================
def run_transform_data_modeling(
    source_name: str,
    data: pd.DataFrame,
    expected_columns: List[str],
    primary_key: List[str],
    logger: logging.Logger,
    **kwargs
) -> pd.DataFrame:
    """
    Execute Transform Phase-2 (Modeling).

    :param source_name: Source identifier
    :param data: Clean input DataFrame
    :param expected_columns: Final schema columns
    :param primary_key: Primary key columns
    :param logger: Shared ETL logger
    :param kwargs: Additional table-specific arguments
    :return: Modeled DataFrame
    """
    try:
        logger.info("Starting MODEL TRANSFORM (T2) for source=%s", source_name)

        row_count_before = len(data)

        builders = {
            "sales": _build_sales_fact,
            "products": _build_products_dim,
            "customers": _build_customers_dim,
            "stores": _build_stores_dim,
        }

        if source_name not in builders:
            raise ValueError(f"Invalid source name: {source_name}")

        data, derived_columns = builders[source_name](
            source_name,
            data,
            expected_columns,
            **kwargs,
            logger=logger
        )

        _validate_data_integrity(
            source_name=source_name,
            data=data,
            primary_key=primary_key,
            row_count_before=row_count_before,
            expected_columns=expected_columns,
            derived_columns=derived_columns,
            logger=logger
        )

        logger.info("MODEL TRANSFORM (T2) completed for source=%s", source_name)
        return data

    except Exception:
        logger.exception("MODEL TRANSFORM (T2) failed for source=%s", source_name)
        raise


# ==================================================================================================
# Common Integrity Validation
# ==================================================================================================
def _validate_data_integrity(
        source_name: str,
        data: pd.DataFrame,
        primary_key: List[str],
        row_count_before: int,
        expected_columns: List[str],
        derived_columns: List[str],
        logger: logging.Logger
) -> None:
    """
    Validate T2 integrity invariants.

    :param source_name: Table identifier
    :param data: Modeled DataFrame
    :param primary_key: Primary key columns
    :param row_count_before: Row count before modeling
    :param expected_columns: Expected final schema
    :param logger: Shared ETL logger
    :return: None
    """
    logger.info("Validating T2 data integrity for source=%s", source_name)
    logger.info("Primary key columns=%s", primary_key)
    logger.info("Row count before T2=%d, after T2=%d", row_count_before, len(data))

    # ------------------------------------------------------------------
    # NULL check on primary key
    # ------------------------------------------------------------------
    nulls_on_pk = data[primary_key].isna().any().any()
    logger.info("Null check on primary key passed=%s", not nulls_on_pk)

    if nulls_on_pk:
        logger.error(
            "NULL values detected on primary key columns=%s for source=%s",
            primary_key,
            source_name
        )
        raise ValueError(f"NULL values found in primary key for {source_name}")

    # ------------------------------------------------------------------
    # Duplicate check on primary key
    # ------------------------------------------------------------------
    has_duplicates = data.duplicated(subset=primary_key, keep=False).any()
    logger.info("Duplicate primary key check passed=%s", not has_duplicates)

    if has_duplicates:
        logger.error(
            "Duplicate primary keys detected for source=%s on columns=%s",
            source_name,
            primary_key
        )
        raise ValueError(f"Duplicate primary keys found for {source_name}")

    # ------------------------------------------------------------------
    # Row count reconciliation
    # ------------------------------------------------------------------
    if len(data) != row_count_before:
        logger.error(
            "Row count changed during T2 for source=%s: %d -> %d",
            source_name,
            row_count_before,
            len(data)
        )
        raise ValueError(
            f"Row count changed during T2 for {source_name}: "
            f"{row_count_before} -> {len(data)}"
        )

    logger.info("Row count reconciliation passed")

    # ------------------------------------------------------------------
    # Schema validation
    # ------------------------------------------------------------------
    read_columns = sorted(data.columns.tolist())
    expected_columns_sorted = sorted(expected_columns + derived_columns)

    logger.info("Expected columns=%s", expected_columns_sorted)
    logger.info("Read columns=%s", read_columns)

    if read_columns != expected_columns_sorted:
        logger.error(
            "Schema mismatch for source=%s. Expected=%s, Found=%s",
            source_name,
            expected_columns_sorted,
            read_columns
        )
        raise ValueError(f"Schema mismatch detected for {source_name}")

    # ------------------------------------------------------------------
    # Snake case validation
    # ------------------------------------------------------------------
    pattern = r"^[a-z0-9]+(?:_[a-z0-9]+)*$"
    for col in data.columns:
        if re.fullmatch(pattern, col) is None:
            logger.error(
                "Invalid column name detected for source=%s: %s",
                source_name,
                col
            )
            raise ValueError(f"Invalid column name detected: {col}")

    logger.info("T2 data integrity validation passed for source=%s", source_name)


# ==================================================================================================
# Helper Functions
# ==================================================================================================
def _assign_band(price: float) -> str:
    if price <= 500:
        return "LOW"
    elif price <= 2000:
        return "MEDIUM"
    return "HIGH"


def _assign_customer_tenure_bucket(days: int) -> str:
    if days <= 90:
        return "NEW"
    elif days <= 365:
        return "REGULAR"
    return "LOYAL"
