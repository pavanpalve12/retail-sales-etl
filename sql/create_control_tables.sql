/*
------------------------------------------------------------------------------------
Module Name: create_control_tables
------------------------------------------------------------------------------------
This script defines the **control-plane schema** for the Retail Sales ETL system.

It creates all **logging and metadata tables** required to track:
- End-to-end ETL pipeline executions
- Per-stage execution status and row counts
- Pipeline configuration and activation control
- Table-level load strategy and watermark metadata
- Pipeline-to-table execution mapping

This schema acts as the **single source of truth** for ETL observability
and operational governance.

------------------------------------------------------------------------------------
Schema Coverage
------------------------------------------------------------------------------------
Log Tables:
- etl_run_log        : One record per pipeline execution
- etl_stage_log      : One record per stage within a pipeline run

Metadata Tables:
- pipeline_md        : Pipeline-level configuration and scheduling metadata
- table_md           : Table-level load strategy and watermark tracking
- pipeline_table_map : Pipeline-to-table execution mapping and order

------------------------------------------------------------------------------------
Design Notes
------------------------------------------------------------------------------------
- Designed for SQLite-based control DB (foreign keys enabled at runtime)
- All timestamps are stored as TEXT (ISO-8601, UTC expected)
- Logging tables are append-only; metadata tables are mutable
- No data warehouse tables are defined in this script
- This script is idempotent and safe to re-run
------------------------------------------------------------------------------------
*/

-- =================================================================================
-- Log Tables: etl_run_log
--     run_id	Unique identifier for this ETL run
--     pipeline_name	Name of the pipeline being executed
--     source_name	Source system/entity (sales, customers, etc.)
--     status	Overall run status (STARTED / SUCCESS / FAILED)
--     start_time	Timestamp when pipeline started
--     end_time	Timestamp when pipeline finished
--     error_message	Error details if the run failed
--     created_at	When this log record was created
-- =================================================================================
CREATE TABLE IF NOT EXISTS etl_run_log (
    run_id TEXT PRIMARY KEY,
    pipeline_name TEXT NOT NULL,
    source_name TEXT NOT NULL,
    status TEXT NOT NULL,
    start_time TEXT NOT NULL,
    end_time TEXT,
    error_message TEXT,
    created_at TEXT,
    updated_at TEXT
);
-- =================================================================================
-- Log Tables: etl_stage_log
--     run_id	Reference to parent ETL run
--     stage_name	Stage identifier (EXTRACT, TRANSFORM_P1, etc.)
--     status	Stage status (STARTED / SUCCESS / FAILED)
--     rows_in	Number of input rows processed
--     rows_out	Number of output rows produced
--     start_time	Timestamp when stage started
--     end_time	Timestamp when stage finished
--     error_message	Error details if stage failed
--     created_at	When this log record was created
-- =================================================================================
CREATE TABLE IF NOT EXISTS etl_stage_log (
    stage_name TEXT NOT NULL,
    run_id TEXT NOT NULL,
    status TEXT NOT NULL,
    rows_in INTEGER,
    rows_out INTEGER,
    start_time TEXT NOT NULL,
    end_time TEXT,
    error_message TEXT,
    created_at TEXT,
    updated_at TEXT,

    PRIMARY KEY (run_id, stage_name),
    FOREIGN KEY (run_id) REFERENCES etl_run_log(run_id)
);
-- =================================================================================
-- Metadata Tables: pipeline_md
--     pipeline_name	Unique pipeline identifier
--     source_name	Source entity handled by pipeline
--     load_strategy	full / incremental
--     schedule	Execution cadence (manual / daily)
--     is_active	Whether pipeline is enabled
--     created_at	Pipeline registration timestamp
--     updated_at   Track control changes
-- =================================================================================
CREATE TABLE IF NOT EXISTS pipeline_md (
    pipeline_name TEXT PRIMARY KEY NOT NULL,
    source_name TEXT NOT NULL,
    load_strategy TEXT NOT NULL,
    schedule TEXT NOT NULL,
    is_active BOOLEAN NOT NULL DEFAULT True,
    created_at TEXT,
    updated_at TEXT
);
-- =================================================================================
-- Metadata Tables: table_md
--     table_name	Target table name
--     layer	extract / transform / load
--     source_name	Source entity for this table
--     grain	Data grain (transaction, customer, etc.)
--     primary_key	Logical primary key column
--     load_strategy	full / incremental
--     watermark_column	Column used for incremental loads
--     last_loaded_value	Last successfully loaded watermark
--     row_count	Latest row count after load
--     is_active	Whether table is active
--     created_at   When metadata record was first created
--     updated_at	Last metadata update timestamp
-- =================================================================================
CREATE TABLE IF NOT EXISTS table_md (
    table_name TEXT PRIMARY KEY NOT NULL,
    layer TEXT NOT NULL,
    source_name TEXT NOT NULL,
    grain TEXT NOT NULL,
    primary_key TEXT NOT NULL,
    load_strategy TEXT NOT NULL,
    watermark_column TEXT,
    last_loaded_value TEXT,
    row_count INTEGER NOT NULL,
    is_active BOOLEAN NOT NULL DEFAULT True,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
-- =================================================================================
-- Mapping Tables: pipeline_table_map (pipeline -- tables map)
--     pipeline_name	Pipeline identifier
--     table_name	Table handled by pipeline
--     load_order	Execution order within pipeline
--     table_role	dimension / fact
-- =================================================================================
CREATE TABLE IF NOT EXISTS pipeline_table_map (
    pipeline_name TEXT NOT NULL,
    table_name TEXT NOT NULL,
    load_order TEXT NOT NULL,
    table_role TEXT NOT NULL,

    PRIMARY KEY (pipeline_name, table_name),
    FOREIGN KEY (pipeline_name) REFERENCES pipeline_md(pipeline_name),
    FOREIGN KEY (table_name) REFERENCES table_md(table_name)
);


