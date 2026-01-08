"""
------------------------------------------------------------------------------------
Module Name: metadata_table_helpers
------------------------------------------------------------------------------------
This module defines **metadata access helper interfaces** for the Retail Sales ETL
control plane.

It provides a structured contract for interacting with:
- Pipeline-level metadata
- Table-level metadata
- Pipeline-to-table execution mappings

The functions declared here act as **placeholders / interfaces** and are intended
to be implemented with SQLite-backed logic in later iterations.

------------------------------------------------------------------------------------
Schema Coverage
------------------------------------------------------------------------------------
Metadata Tables:
- pipeline_md        : Pipeline configuration and activation metadata
- table_md           : Table-level load strategy and watermark tracking
- pipeline_table_map : Pipeline-to-table execution mapping and order

------------------------------------------------------------------------------------
Design Notes
------------------------------------------------------------------------------------
- This module contains **no implementation logic**
- Function signatures define the expected API surface
- All helpers are expected to be thin DB accessors
- Transaction control is handled by callers
- Timestamps are expected to be UTC (ISO-8601)
------------------------------------------------------------------------------------
"""

from typing import Any, Dict, List
from datetime import datetime, timezone

def _utc_now():
    return datetime.now(timezone.utc).isoformat()

# ------------------------------------------------------------------
# Pipeline metadata helpers
# ------------------------------------------------------------------
def get_pipeline(connection, pipeline_name: str) -> Dict[str, Any] | None:
    """
    Fetch pipeline metadata by pipeline name.

    :param connection: Active database connection
    :param pipeline_name: Unique pipeline identifier
    :return: Pipeline metadata record or None if not found
    """
    sql_query = "SELECT * FROM pipeline_md WHERE pipeline_name = ?"
    query_params = (pipeline_name, )

    cursor = connection.execute(sql_query, query_params)
    pipeline_row = cursor.fetchone()
    return dict(pipeline_row) if pipeline_row else None


def list_active_pipelines(connection) -> List[Dict[str, Any]]:
    """
    List all active pipelines.

    :param connection: Active database connection
    :return: List of active pipeline metadata records
    """
    sql_query = "SELECT * FROM pipeline_md WHERE is_active = 1"
    cursor = connection.execute(sql_query)
    active_pipelines = [dict(row) for row in cursor.fetchall()]
    return active_pipelines


def register_pipeline(connection, pipeline_data: Dict[str, Any]) -> None:
    """
    Register a new pipeline in metadata.

    :param connection: Active database connection
    :param pipeline_data: Dictionary containing pipeline metadata
    :return: None
    """
    sql_query = """
        INSERT INTO pipeline_md
        (
            pipeline_name,
            source_name,
            load_strategy,
            schedule,
            is_active,
            created_at,
            updated_at
        )
        VALUES(?, ?, ?, ?, ?, ?, ?)
    """
    query_params = (
        pipeline_data["pipeline_name"],
        pipeline_data["source_name"],
        pipeline_data["load_strategy"],
        pipeline_data["schedule"],
        pipeline_data.get("is_active", 1),
        _utc_now(),
        _utc_now()
    )

    connection.execute(sql_query, query_params)


def deactivate_pipeline(connection, pipeline_name: str) -> None:
    """
    Deactivate an existing pipeline.

    :param connection: Active database connection
    :param pipeline_name: Unique pipeline identifier
    :return: None
    """
    sql_query = """
        UPDATE pipeline_md
        SET 
            is_active = 0,
            updated_at = ?
        WHERE pipeline_name = ?
    """
    query_params = (
        _utc_now(),
        pipeline_name
    )

    connection.execute(sql_query, query_params)

# ------------------------------------------------------------------
# Table metadata helpers
# ------------------------------------------------------------------
def get_table(connection, table_name: str) -> Dict[str, Any] | None:
    """
    Fetch table metadata by table name.

    :param connection: Active database connection
    :param table_name: Target table name
    :return: Table metadata record or None if not found
    """
    sql_query = "SELECT * FROM table_md WHERE table_name = ?"
    query_params = (table_name, )

    cursor = connection.execute(sql_query, query_params)
    table_row = cursor.fetchone()
    return dict(table_row) if table_row else None


def list_active_tables_for_source(connection, source_name: str) -> List[Dict[str, Any]]:
    """
    List all active tables for a given source.

    :param connection: Active database connection
    :param source_name: Source system or entity name
    :return: List of active table metadata records
    """
    sql_query = """
        SELECT * 
        FROM table_md 
        WHERE source_name = ? AND is_active = 1
    """
    query_params = (source_name, )
    cursor = connection.execute(sql_query, query_params)
    table_rows = [dict(row) for row in cursor.fetchall()]
    return table_rows


def update_table_watermark(connection, table_data: Dict[str, Any]) -> None:
    """
    Update watermark and row count for a table.

    :param connection: Active database connection
    :param table_data: Target table data
    :return: None
    """
    sql_query = """
        UPDATE table_md
        SET 
            last_loaded_value = ?,
            row_count = ?,
            updated_at = ?
        WHERE table_name = ?
    """
    query_params = (
        table_data["last_loaded_value"],
        table_data["row_count"],
        _utc_now(),
        table_data["table_name"]
    )
    connection.execute(sql_query, query_params)

# ------------------------------------------------------------------
# Pipeline-to-table mapping helpers
# ------------------------------------------------------------------
def list_tables_for_pipeline(connection, pipeline_name: str) -> List[Dict[str, Any]]:
    """
    List all tables associated with a pipeline in execution order.

    :param connection: Active database connection
    :param pipeline_name: Unique pipeline identifier
    :return: Ordered list of table metadata records for the pipeline
    """
    sql_query = """
        SELECT tm.*, ptm.load_order, ptm.table_role
        FROM pipeline_table_map ptm
                 JOIN table_md tm
                      ON ptm.table_name = tm.table_name
        WHERE ptm.pipeline_name = ?
          AND tm.is_active = 1
        ORDER BY ptm.load_order
    """
    query_param = (pipeline_name, )
    cursor = connection.execute(sql_query, query_param)
    rows = [dict(row) for row in cursor.fetchall()]
    return rows
