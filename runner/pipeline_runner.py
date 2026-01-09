"""
------------------------------------------------------------------------------------
Module Name: pipeline_runner
------------------------------------------------------------------------------------
This module implements the **pipeline orchestration layer** for the Retail Sales
ETL system.

It is responsible for:
- Validating pipeline inputs
- Managing control-plane logging (run + stage logs)
- Executing Extract → Transform → Load stages in order
- Handling pipeline-specific behavior (e.g., sales date_dim)
- Managing database connections and failure handling

This module contains **no transformation logic** and acts purely as an orchestrator.

------------------------------------------------------------------------------------
Design Notes
------------------------------------------------------------------------------------
- One pipeline run = one run_id
- Each stage logs STARTED and terminal state (SUCCESS / FAILED)
- Stage modules raise exceptions; runner records failures
- Metadata tables are NOT modified here
------------------------------------------------------------------------------------
"""

import logging
import sqlite3
import uuid
from datetime import datetime, timezone
from typing import Dict, List, Any

from etl import (
    extract,
    transform_data_cleaning,
    transform_data_modeling,
    load
)
from runner import pipeline_config as config
from utils import log_table_helpers
from utils import text_logger
import sys

# ============================================================================================
# Main module to call pipelines
# ============================================================================================
def main():
    if len(sys.argv) != 2:
        print("Usage: python runner.py <pipeline_name>")
        sys.exit(1)

    pipeline_name = sys.argv[1]
    run_pipeline(pipeline_name)

# ============================================================================================
# Run pipeline
# ============================================================================================
def run_pipeline(pipeline_name: str) -> None:
    """
    Execute an end-to-end ETL pipeline for a given source.

    :param pipeline_name: Logical pipeline name (customers, products, stores, sales)
    :return: None
    """
    logger = text_logger.get_logger()
    file_path = None
    control_connection = None
    warehouse_connection = None
    stage_data = None

    try:
        logger.info("Checking if pipeline valid=%s", pipeline_name)
        _is_pipeline_valid(pipeline_name, config.PIPELINES, logger)

        logger.info("Running pipeline=%s", pipeline_name)

        # ------------------------------------------------------------------
        # Connect to databases
        # ------------------------------------------------------------------
        control_connection = _get_control_connection(
            config.CONTROL_DB_PATH, logger
        )
        warehouse_connection = _get_warehouse_connection(
            config.RETAIL_SALES_DB_PATH, logger
        )

        # ------------------------------------------------------------------
        # Create run entry
        # ------------------------------------------------------------------
        run_id = _get_run_id()
        logger.info("Creating pipeline run, run_id=%s", run_id)

        run_data = _insert_run(
            control_connection,
            run_id,
            pipeline_name,
            "STARTED"
        )

        # ------------------------------------------------------------------
        # Fetch pipeline config
        # ------------------------------------------------------------------
        file_path = config.FILE_PATHS[pipeline_name]
        table_name = config.TABLE_NAMES[pipeline_name]
        expected_columns = config.EXPECTED_COLUMNS[table_name]
        primary_key = config.PRIMARY_KEYS[table_name]
        column_defaults = config.DEFAULT_VALUE_MAP[table_name]
        column_types = config.DATA_TYPE_MAP[table_name]

        # ------------------------------------------------------------------
        # EXTRACT
        # ------------------------------------------------------------------
        stage_data = _insert_stage(
            control_connection,
            run_id,
            "EXTRACT",
            "STARTED",
            None
        )

        sourced = extract.run_extract(
            run_id,
            pipeline_name,
            file_path,
            expected_columns,
            logger
        )

        _update_stage(
            control_connection,
            stage_data,
            "SUCCESS",
            len(sourced),
            len(sourced),
            None
        )

        # ------------------------------------------------------------------
        # TRANSFORM CLEAN (T1)
        # ------------------------------------------------------------------
        stage_data = _insert_stage(
            control_connection,
            run_id,
            "TRANSFORM_P1",
            "STARTED",
            len(sourced)
        )

        cleaned = transform_data_cleaning.run_transform_data_cleaning(
            pipeline_name,
            sourced,
            primary_key,
            column_defaults,
            column_types,
            logger
        )

        _update_stage(
            control_connection,
            stage_data,
            "SUCCESS",
            len(sourced),
            len(cleaned),
            None
        )

        # ------------------------------------------------------------------
        # TRANSFORM MODEL (T2)
        # ------------------------------------------------------------------
        stage_data = _insert_stage(
            control_connection,
            run_id,
            "TRANSFORM_P2",
            "STARTED",
            len(cleaned)
        )

        modeled = transform_data_modeling.run_transform_data_modeling(
            pipeline_name,
            cleaned,
            expected_columns,
            primary_key,
            logger,
            config.STATE_REGION_MAP,
            config.AS_OF_DATE
        )

        _update_stage(
            control_connection,
            stage_data,
            "SUCCESS",
            len(cleaned),
            len(modeled),
            None
        )

        # ------------------------------------------------------------------
        # SALES-SPECIFIC DATE DIM
        # ------------------------------------------------------------------
        if pipeline_name == "sales":
            min_sale_date = modeled["sale_date"].min()
            max_sale_date = modeled["sale_date"].max()

            date_dim = transform_data_modeling.build_date_dim(
                min_sale_date,
                max_sale_date,
                logger
            )

            stage_data = _insert_stage(
                control_connection,
                run_id,
                "LOAD_DATE_DIM",
                "STARTED",
                len(date_dim)
            )

            load.run_load(
                date_dim,
                "date_dim",
                ["date"],
                warehouse_connection,
                logger
            )

            _update_stage(
                control_connection,
                stage_data,
                "SUCCESS",
                len(date_dim),
                len(date_dim),
                None
            )

        # ------------------------------------------------------------------
        # LOAD
        # ------------------------------------------------------------------
        stage_data = _insert_stage(
            control_connection,
            run_id,
            "LOAD",
            "STARTED",
            len(modeled)
        )

        load.run_load(
            modeled,
            table_name,
            primary_key,
            warehouse_connection,
            logger
        )

        _update_stage(
            control_connection,
            stage_data,
            "SUCCESS",
            len(modeled),
            len(modeled),
            None
        )

        _update_run(control_connection, run_data, "SUCCESS", None)

    except Exception as err:
        logger.exception("PIPELINE failed for source=%s", pipeline_name)

        if stage_data:
            _update_stage(
                control_connection,
                stage_data,
                "FAILED",
                None,
                None,
                str(err)
            )

        if control_connection:
            _update_run(
                control_connection,
                run_data,
                "FAILED",
                str(err)
            )

        raise

    finally:
        if control_connection:
            control_connection.close()
        if warehouse_connection:
            warehouse_connection.close()


# ============================================================================================
# Internal Helpers
# ============================================================================================
def _utc_now() -> str:
    """Return current UTC timestamp in ISO-8601 format."""
    return datetime.now(timezone.utc).isoformat()


def _get_run_id() -> str:
    """Generate a unique run identifier."""
    return str(uuid.uuid4())


def _get_control_connection(
    db_path: str,
    logger: logging.Logger
) -> sqlite3.Connection:
    """Create and return control DB connection."""
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def _get_warehouse_connection(
    db_path: str,
    logger: logging.Logger
) -> sqlite3.Connection:
    """Create and return warehouse DB connection."""
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def _is_pipeline_valid(
    pipeline_name: str,
    valid_pipelines: List[str],
    logger: logging.Logger
) -> None:
    """Validate pipeline name."""
    if pipeline_name not in valid_pipelines:
        raise ValueError(f"Invalid pipeline: {pipeline_name}")


def _insert_stage(
    connection: sqlite3.Connection,
    run_id: str,
    stage_name: str,
    status: str,
    rows_in: int | None
) -> Dict[str, Any]:
    """Insert a stage START record."""
    stage_data = {
        "run_id": run_id,
        "stage_name": stage_name,
        "status": status,
        "rows_in": rows_in,
        "rows_out": None,
        "start_time": _utc_now(),
        "end_time": None,
        "error_message": None,
    }
    log_table_helpers.insert_stage(connection, stage_data)
    return stage_data


def _update_stage(
    connection: sqlite3.Connection,
    stage_data: Dict[str, Any],
    status: str,
    rows_in: int | None,
    rows_out: int | None,
    error_message: str | None
) -> None:
    """Update terminal stage status."""
    stage_data.update(
        {
            "status": status,
            "rows_in": rows_in,
            "rows_out": rows_out,
            "end_time": _utc_now(),
            "error_message": error_message,
        }
    )
    log_table_helpers.update_stage_status(connection, stage_data)


def _insert_run(
    connection: sqlite3.Connection,
    run_id: str,
    pipeline_name: str,
    status: str
) -> Dict[str, Any]:
    """Insert pipeline run START record."""
    run_data = {
        "run_id": run_id,
        "pipeline_name": pipeline_name,
        "source_name": pipeline_name,
        "status": status,
        "start_time": _utc_now(),
        "end_time": None,
        "error_message": None,
    }
    log_table_helpers.insert_run(connection, run_data)
    return run_data


def _update_run(
    connection: sqlite3.Connection,
    run_data: Dict[str, Any],
    status: str,
    error_message: str | None
) -> None:
    """Update pipeline run terminal status."""
    run_data.update(
        {
            "status": status,
            "end_time": _utc_now(),
            "error_message": error_message,
        }
    )
    log_table_helpers.update_run_status(connection, run_data)


# ============================================================================================
# Entry point in code
# ============================================================================================
if __name__ == "__main__":
    main()
