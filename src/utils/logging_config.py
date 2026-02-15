"""
Centralized Logging Configuration
Provides consistent logging across all modules with file and console handlers
"""

import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path


def setup_logging(
    log_dir: str = "data/logs",
    log_file: str = "app.log",
    console_level: int = logging.INFO,
    file_level: int = logging.DEBUG,
    max_bytes: int = 10 * 1024 * 1024,  # 10 MB
    backup_count: int = 5,
) -> logging.Logger:
    """
    Configure application-wide logging with both file and console output.

    Args:
        log_dir: Directory to store log files
        log_file: Name of the log file
        console_level: Logging level for console output (INFO by default)
        file_level: Logging level for file output (DEBUG by default)
        max_bytes: Maximum size of log file before rotation (10 MB default)
        backup_count: Number of backup log files to keep

    Returns:
        Root logger instance
    """
    # Create log directory if it doesn't exist
    log_path = Path(log_dir)
    log_path.mkdir(parents=True, exist_ok=True)

    # Get root logger
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)  # Capture all levels, handlers will filter

    # Clear any existing handlers to avoid duplicates
    logger.handlers.clear()

    # Define log format
    log_format = logging.Formatter(
        fmt="%(asctime)s | %(levelname)-8s | %(name)-25s | %(funcName)-20s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # Console handler (INFO and above)
    console_handler = logging.StreamHandler()
    console_handler.setLevel(console_level)
    console_handler.setFormatter(log_format)
    logger.addHandler(console_handler)

    # File handler with rotation (DEBUG and above)
    file_handler = RotatingFileHandler(
        log_path / log_file,
        maxBytes=max_bytes,
        backupCount=backup_count,
        encoding="utf-8",
    )
    file_handler.setLevel(file_level)
    file_handler.setFormatter(log_format)
    logger.addHandler(file_handler)

    # Log startup message
    logger.info("=" * 80)
    logger.info("Logging system initialized")
    logger.info(f"Log file: {log_path / log_file}")
    logger.info(f"Console level: {logging.getLevelName(console_level)}")
    logger.info(f"File level: {logging.getLevelName(file_level)}")
    logger.info("=" * 80)

    return logger


def get_logger(name: str) -> logging.Logger:
    """
    Get a logger instance for a specific module.

    Args:
        name: Module name (typically __name__)

    Returns:
        Logger instance
    """
    return logging.getLogger(name)
