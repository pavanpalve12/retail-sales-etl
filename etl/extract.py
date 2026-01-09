"""
------------------------------------------------------------------------------------
Module Name: extract_layer
------------------------------------------------------------------------------------
This module implements the **Extract layer** of the Retail Sales ETL pipeline.

The Extract layer is responsible for:
- Reading raw source files
- Performing structural validation
- Ensuring data shape correctness
- Producing a raw-but-validated DataFrame

No business transformations or data enrichment are performed in this layer.

------------------------------------------------------------------------------------
Scope & Responsibilities
------------------------------------------------------------------------------------
- File existence and readability checks
- File open validation with explicit encoding
- Schema validation against expected columns
- Lightweight data sanity checks
- Fail-fast behavior on validation errors

------------------------------------------------------------------------------------
Design Notes
------------------------------------------------------------------------------------
- Extract is read-only and idempotent
- All failures raise exceptions
- Text logging is used for diagnostics
- No database writes are performed here
------------------------------------------------------------------------------------
"""

from typing import List, Any
from pathlib import Path
import logging
import pandas as pd

# ==================================================================================================
# Extract Tasks
# ==================================================================================================
def _validate_source_file_exists(path: Path, logger: logging.Logger) -> None:
    """
    Validate source file existence and readability.

    :param path: Path to source file
    :param logger: Shared ETL logger instance
    :return: None
    :raises: FileNotFoundError, ValueError
    """
    logger.info("Validating source file existence and readability: %s", path)

    if not path.exists():
        raise FileNotFoundError(f"Source file not found: {path}")

    if not path.is_file():
        raise ValueError(f"Path is not a file: {path}")

    # Encoding + readability check
    with path.open(mode="r", encoding="utf-8") as f:
        f.read(1)


def _validate_source_file_schema(
    data: pd.DataFrame,
    expected_columns: List[str],
    logger: logging.Logger
) -> None:
    """
    Validate DataFrame schema against expected columns.

    :param data: Extracted DataFrame
    :param expected_columns: Authoritative list of expected column names
    :param logger: Shared ETL logger instance
    :return: None
    :raises: ValueError
    """
    logger.info("Validating source file schema")

    read_columns = sorted(data.columns.tolist())
    expected_columns_sorted = sorted(expected_columns)

    logger.info("Read columns: %s", read_columns)
    logger.info("Expected columns: %s", expected_columns_sorted)

    if read_columns != expected_columns_sorted:
        raise ValueError(
            "Schema mismatch detected.\n"
            f"Expected: {expected_columns_sorted}\n"
            f"Read: {read_columns}"
        )


def _perform_data_sanity_checks(data: pd.DataFrame, logger: logging.Logger) -> None:
    """
    Perform basic data sanity checks.

    :param data: Extracted DataFrame
    :param logger: Shared ETL logger instance
    :return: None
    :raises: ValueError
    """
    logger.info("Performing data sanity checks")

    if data.empty:
        raise ValueError("Source data is empty")

    row_count = len(data)
    logger.info("Row count: %d", row_count)

    # Column-wise NULL percentage check
    NULL_THRESHOLD_PCT = 95.0
    null_pct = data.isna().mean() * 100

    for col, pct in null_pct.items():
        if pct > NULL_THRESHOLD_PCT:
            logger.error("NULL distribution:\n%s", null_pct)
            raise ValueError(
                f"Column '{col}' has {pct:.2f}% NULL values"
            )

    # Duplicate detection (no removal)
    duplicates = data.duplicated(keep=False)
    if duplicates.any():
        dup_count = duplicates.sum()
        sample_dups = data.loc[duplicates].head(5)

        logger.error("Duplicate rows detected: %d", dup_count)
        raise ValueError(
            "Duplicate rows found in source data.\n"
            f"Sample:\n{sample_dups}"
        )


# ==================================================================================================
# Execute Extract
# ==================================================================================================
def run_extract(
        run_id: str,
        source_name: str,
        file_path: Path,
        expected_columns: List[str],
        logger: logging.Logger
) -> pd.DataFrame:
    """
    Execute the Extract phase for a given source.

    :param run_id: Pipeline run id
    :param source_name: Source entity name (e.g., sales, customers)
    :param file_path: Path to source file
    :param expected_columns: Authoritative column list
    :param logger: Shared ETL logger instance
    :return: Raw but validated DataFrame
    :raises: Exception
    """
    try:
        logger.info("Starting EXTRACT for source=%s", source_name)

        _validate_source_file_exists(file_path, logger)

        data = pd.read_csv(file_path)

        _validate_source_file_schema(data, expected_columns, logger)
        _perform_data_sanity_checks(data, logger)

        logger.info("EXTRACT completed successfully for source=%s", source_name)
        return data

    except Exception:
        logger.exception(
            "EXTRACT failed for source=%s at path=%s",
            source_name,
            file_path
        )
        raise
