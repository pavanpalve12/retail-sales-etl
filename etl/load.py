"""
------------------------------------------------------------------------------------
Module Name: load_warehouse_tables
------------------------------------------------------------------------------------
This module implements the **Load phase** for the Retail Sales ETL pipeline.

It is responsible for:
- Persisting modeled (T2) DataFrames into the warehouse database
- Performing idempotent loads using DELETE + INSERT
- Validating post-load integrity using SQL-based checks
- Logging all load activities and failures

Each invocation loads **exactly one DataFrame into one table** using a
single database connection managed by the orchestrator.

------------------------------------------------------------------------------------
Scope & Responsibilities
------------------------------------------------------------------------------------
- Bulk insert DataFrames into warehouse tables
- Enforce idempotent behavior (rerunnable loads)
- Validate row counts and primary key integrity
- Fail fast on any load or validation error

------------------------------------------------------------------------------------
Design Notes
------------------------------------------------------------------------------------
- Uses SQLite with foreign keys enabled by caller
- Uses explicit transactions
- No transformation logic
- No orchestration or ordering logic
------------------------------------------------------------------------------------
"""

from typing import List
import sqlite3
import logging

import pandas as pd


# ==================================================================================================
# Load Tasks
# ==================================================================================================
def _insert_data_in_db(
    data: pd.DataFrame,
    table_name: str,
    connection: sqlite3.Connection,
    logger: logging.Logger
) -> None:
    """
    Delete existing rows and bulk-insert DataFrame into a warehouse table.

    :param data: DataFrame to be loaded
    :param table_name: Target warehouse table
    :param connection: Active SQLite connection
    :param logger: Shared ETL logger
    :return: None
    """
    try:
        logger.info("Starting bulk load into table=%s", table_name)
        logger.info("Rows to be loaded: %d", len(data))

        connection.execute("BEGIN")

        connection.execute(f"DELETE FROM {table_name}")
        logger.info("Existing rows deleted from table=%s", table_name)

        if data.empty:
            logger.info("No rows to insert for table=%s", table_name)
            connection.commit()
            return

        columns = list(data.columns)
        column_list = ", ".join(columns)
        placeholders = ", ".join(["?"] * len(columns))

        insert_sql = f"""
            INSERT INTO {table_name} ({column_list})
            VALUES ({placeholders})
        """
        logger.info("Running insert sql query=%s", insert_sql)
        data = _normalize_sqlite_types(data)
        rows = [tuple(row) for row in data.itertuples(index=False, name=None)]

        connection.executemany(insert_sql, rows)

        connection.commit()
        logger.info(
            "Successfully loaded %d rows into table=%s",
            len(rows),
            table_name
        )

    except Exception:
        logger.exception("Failed to load data into table=%s", table_name)
        connection.rollback()
        raise


# ==================================================================================================
# Execute Load
# ==================================================================================================
def run_load(
    data: pd.DataFrame,
    table_name: str,
    primary_key: List[str],
    connection: sqlite3.Connection,
    logger: logging.Logger
) -> None:
    """
    Execute the Load phase for a single warehouse table.

    :param data: Modeled DataFrame to load
    :param table_name: Target warehouse table
    :param primary_key: Primary key column(s) for integrity validation
    :param connection: Active SQLite connection
    :param logger: Shared ETL logger
    :return: None
    """
    try:
        logger.info("Starting LOAD for table=%s", table_name)

        source_row_count = len(data)

        _insert_data_in_db(data, table_name, connection, logger)
        _validate_data_integrity(
            table_name=table_name,
            primary_key=primary_key,
            expected_row_count=source_row_count,
            connection=connection,
            logger=logger
        )

        logger.info("LOAD completed successfully for table=%s", table_name)

    except Exception:
        logger.exception("LOAD failed for table=%s", table_name)
        raise


# ==================================================================================================
# Load Helpers
# ==================================================================================================
def _validate_data_integrity(
    table_name: str,
    primary_key: List[str],
    expected_row_count: int,
    connection: sqlite3.Connection,
    logger: logging.Logger
) -> None:
    """
    Validate post-load integrity of a warehouse table.

    :param table_name: Target warehouse table
    :param primary_key: Primary key column(s)
    :param expected_row_count: Expected row count after load
    :param connection: Active SQLite connection
    :param logger: Shared ETL logger
    :return: None
    """
    logger.info("Validating post-load integrity for table=%s", table_name)

    # ------------------------------------------------------------------
    # Row count validation
    # ------------------------------------------------------------------
    cursor = connection.execute(f"SELECT COUNT(*) FROM {table_name}")
    actual_row_count = cursor.fetchone()[0]

    if actual_row_count != expected_row_count:
        raise ValueError(
            f"Row count mismatch after LOAD for {table_name}: "
            f"{expected_row_count} -> {actual_row_count}"
        )

    # ------------------------------------------------------------------
    # NULL check on primary key
    # ------------------------------------------------------------------
    null_pk_query = (
        f"SELECT COUNT(*) FROM {table_name} WHERE " +
        " OR ".join([f"{pk} IS NULL" for pk in primary_key])
    )
    cursor = connection.execute(null_pk_query)
    null_count = cursor.fetchone()[0]

    if null_count > 0:
        raise ValueError(
            f"NULL values found in primary key {primary_key} "
            f"after LOAD for table={table_name}"
        )

    # ------------------------------------------------------------------
    # Duplicate primary key check
    # ------------------------------------------------------------------
    dup_query = f"""
        SELECT COUNT(*) FROM (
            SELECT {", ".join(primary_key)}
            FROM {table_name}
            GROUP BY {", ".join(primary_key)}
            HAVING COUNT(*) > 1
        )
    """
    cursor = connection.execute(dup_query)
    dup_count = cursor.fetchone()[0]

    if dup_count > 0:
        raise ValueError(
            f"Duplicate primary keys detected after LOAD for table={table_name}"
        )

    logger.info("Post-load integrity validation passed for table=%s", table_name)


def _normalize_sqlite_types(data: pd.DataFrame) -> pd.DataFrame:
    """
    Function is coverting pandas timestamp columns to sqlite iso format
    :param data:
    :return:
    """
    data = data.copy()

    for col in data.columns:
        if data[col].apply(lambda x: isinstance(x, pd.Timestamp)).any():
            data[col] = data[col].apply(
                lambda x: x.isoformat() if isinstance(x, pd.Timestamp) else x
            )
    return data
