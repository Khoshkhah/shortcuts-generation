"""
logging_config.py
=================

Logging configuration for Shortcuts Generation project.
Outputs to both terminal and file simultaneously.

Usage:
    from logging_config import get_logger
    logger = get_logger(__name__)
    logger.info("Processing started")
"""

import logging
from datetime import datetime
from pathlib import Path

# Project Root (parent of src/)
PROJECT_ROOT = Path(__file__).resolve().parent.parent

# Create logs directory
LOG_DIR = PROJECT_ROOT / "logs"
LOG_DIR.mkdir(exist_ok=True)

# Log filename with timestamp
TIMESTAMP = datetime.now().strftime("%Y%m%d_%H%M%S")
LOG_FILE = LOG_DIR / f"shortcuts_{TIMESTAMP}.log"

# Log format
LOG_FORMAT = logging.Formatter(
    fmt='[%(levelname)-8s] %(asctime)s - %(name)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)


def get_logger(name: str) -> logging.Logger:
    """Get a configured logger that writes to both terminal and file."""
    logger = logging.getLogger(name)
    
    if not logger.handlers:
        logger.setLevel(logging.INFO)
        
        # Console Handler
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        console_handler.setFormatter(LOG_FORMAT)
        logger.addHandler(console_handler)
        
        # File Handler
        file_handler = logging.FileHandler(LOG_FILE, mode='a', encoding='utf-8')
        file_handler.setLevel(logging.INFO)
        file_handler.setFormatter(LOG_FORMAT)
        logger.addHandler(file_handler)
    
    return logger


def log_section(logger: logging.Logger, title: str, width: int = 60):
    """Log a section header with separators."""
    separator = "=" * width
    logger.info(separator)
    logger.info(title)
    logger.info(separator)


def log_dict(logger: logging.Logger, data: dict, title: str = None):
    """Log dictionary as formatted key-value pairs."""
    if title:
        logger.info(f"--- {title} ---")
    max_key_len = max(len(str(k)) for k in data.keys())
    for key, value in data.items():
        logger.info(f"{str(key).ljust(max_key_len)} : {value}")
