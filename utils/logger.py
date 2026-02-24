import sys
from pathlib import Path
from loguru import logger

# ---------------------------------------------------------------------------
# ENTERPRISE LOGGING CONFIGURATION
# ---------------------------------------------------------------------------
# This module sets up a robust logging infrastructure.
# It handles console output for monitoring and file output for auditing.
# ---------------------------------------------------------------------------

# 1. Define Log Directory
# We ensure the 'logs' directory exists relative to the project root.
LOG_DIR = Path("logs")
LOG_DIR.mkdir(parents=True, exist_ok=True)

# 2. Remove Default Handler
# Loguru comes with a default handler. We remove it to apply our custom
# enterprise formatting and avoid duplicate log entries.
logger.remove()

# 3. Define Log Format
# Structure: Timestamp | Level | Module:Function:Line | Message
# This format ensures every log entry is traceable to its exact source code location.
LOG_FORMAT = (
    "<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
    "<level>{level: <8}</level> | "
    "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - "
    "<level>{message}</level>"
)

# 4. Add Console Handler (Standard Error)
# Purpose: Real-time monitoring for the operator.
# Level: INFO (Shows flow, success, warnings, and errors).
logger.add(
    sys.stderr,
    format=LOG_FORMAT,
    level="INFO",
    colorize=True,
    backtrace=True,
    diagnose=True
)

# 5. Add File Handler (Audit Trail)
# Purpose: Persistent storage for debugging and history.
# Level: DEBUG (Captures detailed variable states and low-level events).
# Features:
# - Rotation: Creates a new file every 10 MB.
# - Retention: Deletes logs older than 30 days to save disk space.
# - Compression: Zips old logs (e.g., .log.zip) to minimize storage footprint.
# - Enqueue: Ensures thread-safe logging (async safe).
logger.add(
    LOG_DIR / "scapile_ops.log",
    rotation="10 MB",
    retention="30 days",
    compression="zip",
    format=LOG_FORMAT,
    level="DEBUG",
    enqueue=True,
    backtrace=True,
    diagnose=True
)

# Expose the configured logger
__all__ = ["logger"]

logger.info("Logging infrastructure initialized successfully.")