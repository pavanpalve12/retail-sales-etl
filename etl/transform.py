"""
------------------------------------------------------------------------------------
Module Name: transform_clean_t1
------------------------------------------------------------------------------------
This module implements the **Clean (Transform Phase-1)** layer of the Retail Sales
ETL pipeline.

The Clean phase is responsible for:
- Standardizing column names
- Handling missing values deterministically
- Removing duplicate records based on primary keys
- Converting columns to correct data types
- Enforcing basic primary key validity

No joins, aggregations, or business logic are applied in this phase.

------------------------------------------------------------------------------------
Scope & Responsibilities
------------------------------------------------------------------------------------
- Column name normalization (snake_case)
- Deterministic missing value handling
- Primary-key–based deduplication
- Explicit data type conversions
- Fail-fast behavior on data quality violations

------------------------------------------------------------------------------------
Design Notes
------------------------------------------------------------------------------------
- Operates on a single DataFrame
- Mutates data in-place
- All failures raise exceptions
- Text logging used for diagnostics
- No database writes performed here
------------------------------------------------------------------------------------
"""

import logging
import re
from typing import List, Dict, Any

import pandas as pd


# ==================================================================================================
# Transform Tasks
# ==================================================================================================
def _standardize_column_names(data: pd.DataFrame, logger: logging.Logger) -> None:
    """
    Normalize all column names to deterministic snake_case.

    :param data: Input DataFrame to be modified
    :param logger: Shared ETL logger instance
    :return: None
    """
    logger.info("Standardizing column names")

    normalized_columns = []
    for column in data.columns:
        new_name = _normalize_column_name(column)
        logger.info("Column rename: %s -> %s", column, new_name)
        normalized_columns.append(new_name)

    data.columns = normalized_columns


def _handle_missing_values(
    data: pd.DataFrame,
    primary_key: List[str],
    column_default_map: Dict[str, Any],
    logger: logging.Logger
) -> None:
    """
    Handle missing values deterministically.

    - Drop rows with missing primary key values
    - Fill non-PK columns using predefined defaults

    :param data: Input DataFrame to be modified
    :param primary_key: List of primary key columns
    :param column_default_map: Default values for non-PK columns
    :param logger: Shared ETL logger instance
    :return: None
    """
    logger.info("Handling missing values")

    before_count = len(data)
    data.dropna(subset=primary_key, inplace=True)

    dropped = before_count - len(data)
    if dropped > 0:
        logger.info("Dropped %d rows due to NULL primary keys", dropped)

    for col, default in column_default_map.items():
        if col in data.columns and data[col].isna().any():
            logger.info("Filling NULLs in column '%s' with default '%s'", col, default)

    data.fillna(value=column_default_map, inplace=True)


def _remove_duplicates(
    data: pd.DataFrame,
    primary_key: List[str],
    logger: logging.Logger
) -> None:
    """
    Remove duplicate records based on primary key.

    :param data: Input DataFrame to be modified
    :param primary_key: List of primary key columns
    :param logger: Shared ETL logger instance
    :return: None
    """
    logger.info("Removing duplicates using primary key: %s", primary_key)

    duplicates = data.duplicated(subset=primary_key, keep="first")
    if duplicates.any():
        dup_count = duplicates.sum()
        logger.error("Detected %d duplicate rows on primary key", dup_count)
        data.drop_duplicates(subset=primary_key, keep="first", inplace=True)


def _convert_data_types(
    data: pd.DataFrame,
    column_types_map: Dict[str, Any],
    logger: logging.Logger
) -> None:
    """
    Convert DataFrame columns to explicit target data types.

    :param data: Input DataFrame to be modified
    :param column_types_map: Mapping of column name to target dtype
    :param logger: Shared ETL logger instance
    :return: None
    """
    logger.info("Converting column data types")

    for col, dtype in column_types_map.items():
        logger.info("Converting column '%s' to type '%s'", col, dtype)

    data[:] = data.astype(dtype=column_types_map, errors="raise")


# ==================================================================================================
# Execute Transform
# ==================================================================================================
def run_transform(
    source_name: str,
    data: pd.DataFrame,
    primary_key: List[str],
    column_default_map: Dict[str, Any],
    column_types_map: Dict[str, Any],
    logger: logging.Logger
) -> pd.DataFrame:
    """
    Execute Clean Transform Phase (T1) on extracted data.

    :param source_name: Source entity name
    :param data: Extracted DataFrame
    :param primary_key: Primary key columns
    :param column_default_map: Default values for missing data
    :param column_types_map: Target data types for columns
    :param logger: Shared ETL logger instance
    :return: Cleaned DataFrame
    :raises: Exception on transform failure
    """
    try:
        logger.info("Starting CLEAN TRANSFORM (T1) for source=%s", source_name)

        _standardize_column_names(data, logger)
        _handle_missing_values(data, primary_key, column_default_map, logger)
        _remove_duplicates(data, primary_key, logger)
        _convert_data_types(data, column_types_map, logger)

        logger.info("CLEAN TRANSFORM (T1) completed for source=%s", source_name)
        return data

    except Exception:
        logger.exception("CLEAN TRANSFORM (T1) failed for source=%s", source_name)
        raise


# ==================================================================================================
# Transform Helpers
# ==================================================================================================
def _normalize_column_name(column: str) -> str:
    """
    Normalize a column name to deterministic snake_case.

    :param column: Raw column name
    :return: Normalized column name
    """
    column = column.strip().lower()
    column = re.sub(r"[^a-z0-9]+", "_", column)
    column = re.sub(r"_+", "_", column)
    return column.strip("_")
