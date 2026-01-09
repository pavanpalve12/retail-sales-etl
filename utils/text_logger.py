import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path
from datetime import datetime, timezone

# ================================================================================
# Log directory & file
# ================================================================================
BASE_DIR = Path(__file__).resolve().parent.parent
LOG_DIR = BASE_DIR / "logs"
LOG_DIR.mkdir(exist_ok=True)
LOG_FILE = LOG_DIR / "etl_logs.log"

# ================================================================================
# Logger Configuration
# ================================================================================
LOGGER_NAME = "etl_logger"

def _utc_time(*args):
    return datetime.now(timezone.utc).timetuple()

logging.Formatter.converter = _utc_time

def get_logger(
        run_id: str,
        pipeline_name: str,
        level: int = logging.INFO
) -> logging.Logger:
    # -----------------------------------------------------------------------------
    # Get Logger
    # -----------------------------------------------------------------------------
    logger = logging.getLogger(LOGGER_NAME)
    logger.setLevel(level)

    # -----------------------------------------------------------------------------
    # Check if logger exists and handler is attached to it
    # check to ensure exising logger is used for all logs
    # Ex. different module (which use logger) can create logger
    # we need to safeguard that same logger is used
    # -----------------------------------------------------------------------------

    if not logger.handlers:
        # -----------------------------------------------------------------------------
        # Create handler
        # -----------------------------------------------------------------------------
        handler = RotatingFileHandler(
            filename = LOG_FILE,
            maxBytes = 5 * 1024 * 1024, # 5MB log
            backupCount = 3 # 3 backups to be kept before rotating logs
        )

        # -----------------------------------------------------------------------------
        # Configure format for handler
        # -----------------------------------------------------------------------------
        formatter = logging.Formatter(
            "%(asctime)s | %(levelname)s | %(module)s | %(pipeline_name)s | %(run_id)s | %(message)s"
        )
        handler.setFormatter(formatter)

        # -----------------------------------------------------------------------------
        # Add handler to logger
        # -----------------------------------------------------------------------------
        logger.addHandler(handler)
        logger.propagate = False

    return logging.LoggerAdapter(
        logger, extra={
            "run_id": run_id,
            "pipeline_name": pipeline_name
        }
    )
