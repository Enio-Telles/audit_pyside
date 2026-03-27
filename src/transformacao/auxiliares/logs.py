import logging
from pathlib import Path

def get_project_root() -> Path:
    """Returns the root directory of the project."""
    # Current file is in src/transformacao/auxiliares/logs.py
    return Path(__file__).resolve().parents[3]

def setup_logging():
    """Sets up the logging configuration with a single file handler."""
    log_dir = get_project_root() / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / "error.log"

    logger = logging.getLogger("secure_logger")
    if not logger.handlers:
        logger.setLevel(logging.ERROR)
        fh = logging.FileHandler(str(log_file), encoding="utf-8")
        fh.setLevel(logging.ERROR)
        formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        fh.setFormatter(formatter)
        logger.addHandler(fh)
    return logger

def log_exception(e: Exception):
    """Logs an exception with its traceback securely."""
    logger = setup_logging()
    logger.error("An unexpected error occurred", exc_info=e)
