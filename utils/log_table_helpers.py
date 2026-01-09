"""
------------------------------------------------------------------------------------
Module Name: log_table_helpers
------------------------------------------------------------------------------------
This module provides **minimal database access helpers** for ETL logging tables.

It encapsulates all **read and write operations** for:
- ETL run-level logging
- ETL stage-level logging

These helpers are intentionally thin and contain **no business logic**.
They are designed to be used by higher-level logger and orchestrator modules.

------------------------------------------------------------------------------------
Schema Coverage
------------------------------------------------------------------------------------
Log Tables:
- etl_run_log   : One record per ETL pipeline execution
- etl_stage_log : One record per stage within an ETL run

------------------------------------------------------------------------------------
Design Notes
------------------------------------------------------------------------------------
- Designed for SQLite-based control DB
- No commits or rollbacks are performed here
- All timestamps are generated in UTC (ISO-8601)
- Errors are allowed to propagate to the caller
- This module does NOT perform logging itself
------------------------------------------------------------------------------------
"""

from typing import Any, Dict, List
from datetime import datetime, timezone


# ------------------------------------------------------------------
# Internal utilities
# ------------------------------------------------------------------
def _utc_now() -> str:
    """
    Return the current UTC timestamp in ISO-8601 format.

    :return: Current UTC timestamp as ISO-8601 string
    """
    return datetime.now(timezone.utc).isoformat()


# ------------------------------------------------------------------
# Run-level helpers
# ------------------------------------------------------------------
def insert_run(connection, run_data: Dict[str, Any]) -> None:
    """
    Insert a new ETL run record into etl_run_log.

    :param connection: Active SQLite connection
    :param run_data: Dictionary containing run metadata
    :return: None
    """
    sql_query = """
        INSERT INTO etl_run_log (
            run_id,
            pipeline_name,
            source_name,
            status,
            start_time,
            created_at,
            updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """
    query_params = (
        run_data["run_id"],
        run_data["pipeline_name"],
        run_data["source_name"],
        run_data["status"],
        run_data["start_time"],
        _utc_now(),
        _utc_now()
    )

    connection.execute(sql_query, query_params)
    connection.commit()


def update_run_status(connection, run_data: Dict[str, Any]) -> None:
    """
    Update the status and completion details of an ETL run.

    :param connection: Active SQLite connection
    :param run_data: Dictionary containing updated run fields
    :return: None
    """
    sql_query = """
        UPDATE etl_run_log
        SET
            status = ?,
            end_time = ?,
            error_message = ?,
            updated_at = ?
        WHERE run_id = ?
    """
    query_params = (
        run_data["status"],
        run_data.get("end_time"),
        run_data.get("error_message"),
        _utc_now(),
        run_data["run_id"]
    )

    connection.execute(sql_query, query_params)
    connection.commit()


def get_run(connection, run_id: str) -> Dict[str, Any] | None:
    """
    Fetch a single ETL run record by run_id.

    :param connection: Active SQLite connection
    :param run_id: Unique identifier of the ETL run
    :return: Run record as dict, or None if not found
    """
    sql_query = "SELECT * FROM etl_run_log WHERE run_id = ?"
    query_params = (run_id,)
    cursor = connection.execute(sql_query, query_params)

    run_row = cursor.fetchone()
    return dict(run_row) if run_row else None


# ------------------------------------------------------------------
# Stage-level helpers
# ------------------------------------------------------------------
def insert_stage(connection, stage_data: Dict[str, Any]) -> None:
    """
    Insert a new ETL stage record into etl_stage_log.

    :param connection: Active SQLite connection
    :param stage_data: Dictionary containing stage metadata
    :return: None
    """
    sql_query = """
        INSERT INTO etl_stage_log (
            run_id,
            stage_name,
            status,
            rows_in,
            start_time,
            created_at,
            updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """
    query_params = (
        stage_data["run_id"],
        stage_data["stage_name"],
        stage_data["status"],
        stage_data["rows_in"],
        stage_data["start_time"],
        _utc_now(),
        _utc_now()
    )

    connection.execute(sql_query, query_params)
    connection.commit()


def update_stage_status(connection, stage_data: Dict[str, Any]) -> None:
    """
    Update the status and completion details of an ETL stage.

    :param connection: Active SQLite connection
    :param stage_data: Dictionary containing updated stage fields
    :return: None
    """
    sql_query = """
        UPDATE etl_stage_log
        SET
            status = ?,
            rows_out = ?,
            end_time = ?,
            error_message = ?,
            updated_at = ?
        WHERE run_id = ? AND stage_name = ?
    """
    query_params = (
        stage_data["status"],
        stage_data.get("rows_out"),
        stage_data.get("end_time"),
        stage_data.get("error_message"),
        _utc_now(),
        stage_data["run_id"],
        stage_data["stage_name"]
    )

    connection.execute(sql_query, query_params)
    connection.commit()


def list_stages_for_run(connection, run_id: str) -> List[Dict[str, Any]]:
    """
    List all stage records associated with a given ETL run.

    :param connection: Active SQLite connection
    :param run_id: Unique identifier of the ETL run
    :return: List of stage records as dictionaries
    """
    sql_query = "SELECT * FROM etl_stage_log WHERE run_id = ?"
    query_params = (run_id,)
    cursor = connection.execute(sql_query, query_params)

    rows = cursor.fetchall()
    return [dict(row) for row in rows]
