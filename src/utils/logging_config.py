"""Logging configuration with optional file output."""

import logging
from pathlib import Path

_LOG_FILE = "etl.log"
_configured_paths: set[Path] = set()


def configure_file_logging(log_dir: Path | str = "outputs/logs") -> Path:
    """
    Add a file handler so logs are written to disk as well as the console.

    Logs go to {log_dir}/logs/etl.log. Safe to call multiple times; skips adding
    a duplicate handler for the same path.

    Args:
        log_dir: Directory for the log file (default: outputs).

    Returns:
        Path to the log file.
    """
    log_path = Path(log_dir).resolve() / _LOG_FILE
    log_path.parent.mkdir(parents=True, exist_ok=True)

    if log_path in _configured_paths:
        return log_path

    handler = logging.FileHandler(log_path, encoding="utf-8")
    handler.setLevel(logging.INFO)
    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    handler.setFormatter(formatter)

    root = logging.getLogger()
    root.addHandler(handler)
    _configured_paths.add(log_path)

    return log_path
