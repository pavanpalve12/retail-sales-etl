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

import argparse
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


# ============================================================================================
# Main module to call pipelines
# ============================================================================================
def main():
    user_input = _parse_user_inputs()
    print(user_input)

    pipeline_name = user_input["pipeline_name"]
    dry_run = user_input["dry_run"]

    if not dry_run:
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
    run_id = _get_run_id()
    logger = text_logger.get_logger(run_id, pipeline_name)
    file_path = None
    control_connection = None
    warehouse_connection = None
    stage_data = None

    try:
        logger.info("Pipeline invocation started")
        logger.info("Pipeline name=%s, run_id=%s", pipeline_name, run_id)

        # ------------------------------------------------------------------
        # Validate pipeline
        # ------------------------------------------------------------------
        logger.info("Validating pipeline name")
        _is_pipeline_valid(pipeline_name, config.PIPELINES, logger)
        logger.info("Pipeline validation successful")

        # ------------------------------------------------------------------
        # Connect to databases
        # ------------------------------------------------------------------
        logger.info("Connecting to control database at path=%s", config.CONTROL_DB_PATH)
        control_connection = _get_control_connection(
            config.CONTROL_DB_PATH, logger
        )
        logger.info("Connected to control database")

        logger.info(
            "Connecting to warehouse database at path=%s",
            config.RETAIL_SALES_DB_PATH
        )
        warehouse_connection = _get_warehouse_connection(
            config.RETAIL_SALES_DB_PATH, logger
        )
        logger.info("Connected to warehouse database")

        # ------------------------------------------------------------------
        # Create pipeline run
        # ------------------------------------------------------------------
        logger.info("Creating pipeline run entry in control DB")
        run_data = _insert_run(
            control_connection,
            run_id,
            pipeline_name,
            "STARTED"
        )
        logger.info("Pipeline run entry created")

        # ------------------------------------------------------------------
        # Fetch pipeline configuration
        # ------------------------------------------------------------------
        logger.info("Fetching pipeline configuration")
        file_path = config.FILE_PATHS[pipeline_name]
        table_name = config.TABLE_NAMES[pipeline_name]
        expected_columns = config.EXPECTED_COLUMNS[table_name]
        primary_key = config.PRIMARY_KEYS[table_name]
        column_defaults = config.DEFAULT_VALUE_MAP[table_name]
        column_types = config.DATA_TYPE_MAP[table_name]

        logger.info(
            "Pipeline config resolved | file_path=%s | table=%s | pk=%s",
            file_path,
            table_name,
            primary_key
        )

        # ------------------------------------------------------------------
        # EXTRACT
        # ------------------------------------------------------------------
        logger.info("Starting EXTRACT stage")
        stage_data = _insert_stage(
            control_connection,
            run_id,
            "EXTRACT",
            "STARTED",
            None
        )
        logger.info("EXTRACT stage logged as STARTED.")

        sourced = extract.run_extract(
            run_id,
            pipeline_name,
            file_path,
            expected_columns,
            logger
        )

        logger.info("EXTRACT completed | rows=%d", len(sourced))

        _update_stage(
            control_connection,
            stage_data,
            "SUCCESS",
            len(sourced),
            len(sourced),
            None
        )
        logger.info("EXTRACT stage logged as SUCCESS")

        # ------------------------------------------------------------------
        # TRANSFORM CLEAN (T1)
        # ------------------------------------------------------------------
        logger.info("Starting TRANSFORM CLEAN (T1)")
        stage_data = _insert_stage(
            control_connection,
            run_id,
            "TRANSFORM_P1",
            "STARTED",
            len(sourced)
        )
        logger.info("TRANSFORM CLEAN (T1) stage logged as STARTED.")

        cleaned = transform_data_cleaning.run_transform_data_cleaning(
            pipeline_name,
            sourced,
            primary_key,
            column_defaults,
            column_types,
            logger
        )

        logger.info(
            "TRANSFORM CLEAN (T1) completed | rows_before=%d | rows_after=%d",
            len(sourced),
            len(cleaned)
        )

        _update_stage(
            control_connection,
            stage_data,
            "SUCCESS",
            len(sourced),
            len(cleaned),
            None
        )
        logger.info("TRANSFORM CLEAN (T1) stage logged as SUCCESS")

        # ------------------------------------------------------------------
        # TRANSFORM MODEL (T2)
        # ------------------------------------------------------------------
        logger.info("Starting TRANSFORM MODEL (T2)")
        stage_data = _insert_stage(
            control_connection,
            run_id,
            "TRANSFORM_P2",
            "STARTED",
            len(cleaned)
        )
        logger.info("TRANSFORM MODEL (T2) stage logged as STARTED.")

        modeled = transform_data_modeling.run_transform_data_modeling(
            pipeline_name,
            cleaned,
            expected_columns,
            primary_key,
            logger,
            state_region_map=config.STATE_REGION_MAP,
            as_of_date=config.AS_OF_DATE
        )

        logger.info(
            "TRANSFORM MODEL (T2) completed | rows_before=%d | rows_after=%d",
            len(cleaned),
            len(modeled)
        )

        _update_stage(
            control_connection,
            stage_data,
            "SUCCESS",
            len(cleaned),
            len(modeled),
            None
        )
        logger.info("TRANSFORM MODEL (T2) stage logged as SUCCESS")

        # ------------------------------------------------------------------
        # SALES-SPECIFIC DATE DIM
        # ------------------------------------------------------------------
        if pipeline_name == "sales":
            logger.info("Sales pipeline detected, building date_dim")

            min_sale_date = modeled["sale_date"].min()
            max_sale_date = modeled["sale_date"].max()

            logger.info(
                "Date dim range | min_date=%s | max_date=%s",
                min_sale_date,
                max_sale_date
            )

            date_dim = transform_data_modeling.build_date_dim(
                min_sale_date,
                max_sale_date,
                logger
            )

            logger.info("Date dim built | rows=%d", len(date_dim))

            stage_data = _insert_stage(
                control_connection,
                run_id,
                "LOAD_DATE_DIM",
                "STARTED",
                len(date_dim)
            )
            logger.info("LOAD stage logged as STARTED.")


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
            logger.info("LOAD_DATE_DIM stage logged as SUCCESS")

        # ------------------------------------------------------------------
        # LOAD
        # ------------------------------------------------------------------
        logger.info("Starting LOAD stage for table=%s", table_name)
        stage_data = _insert_stage(
            control_connection,
            run_id,
            "LOAD",
            "STARTED",
            len(modeled)
        )
        logger.info("LOAD stage logged as STARTED.")

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
        logger.info("LOAD stage logged as SUCCESS")

        # ------------------------------------------------------------------
        # Finalize run
        # ------------------------------------------------------------------
        logger.info("Marking pipeline run as SUCCESS")
        _update_run(control_connection, run_data, "SUCCESS", None)
        logger.info("Pipeline completed successfully")

    except Exception as err:
        logger.error("Pipeline execution failed | pipeline=%s | run_id=%s", pipeline_name, run_id)
        logger.exception("Failure details")

        if stage_data:
            logger.info("Updating failed stage log")
            _update_stage(
                control_connection,
                stage_data,
                "FAILED",
                None,
                None,
                str(err)
            )

        if control_connection:
            logger.info("Updating pipeline run status to FAILED")
            _update_run(
                control_connection,
                run_data,
                "FAILED",
                str(err)
            )

        raise

    finally:
        logger.info("Closing database connections")
        if control_connection:
            control_connection.close()
        if warehouse_connection:
            warehouse_connection.close()
        logger.info("Pipeline resources released")


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
# CLI input using argparse
# ============================================================================================
def _build_parser():
    prog = "retail_sales_etl"
    usage = "retail_sales_etl --pipeline_name {customers, products, stores, sales} [--dry-run]"
    description = ""
    epilog = ""

    formatter_class = argparse.RawDescriptionHelpFormatter
    parser = argparse.ArgumentParser(
        prog = prog,
        usage = usage,
        description = description,
        epilog = epilog,
        formatter_class = formatter_class
    )
    return parser

def _build_user_inputs():
    parser = _build_parser()
    parser.add_argument(
        "--pipeline_name", "-p",
        dest="pipeline_name",
        type=str,
        choices=["customers", "products", "stores", "sales"],
        help="Pipeline name"
    )

    parser.add_argument(
        "--dry-run", "-d",
        dest="dry_run",
        type=bool,
        default=False,
        choices=[True, False],
        help="validate config + connectivity only"
    )

    return parser

def _parse_user_inputs():
    parser = _build_user_inputs()

    try:
        args = parser.parse_args()
        user_input = vars(args)
    except SystemExit:
        print(f"\n{"-" * 50}\n{"Incorrect user input provided.".upper()}\n{"-" * 50}\n")
        parser.print_help()
        exit(1)
    else:
        return user_input


# ============================================================================================
# Entry point in code
# ============================================================================================
if __name__ == "__main__":
    main()
