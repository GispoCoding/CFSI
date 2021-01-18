import os
import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path

try:
    LOGGING_DIR = Path(os.environ["CFSI_LOG_DIR"])
except KeyError:
    LOGGING_DIR = Path.home()

if not LOGGING_DIR.exists():
    LOGGING_DIR.mkdir(parents=True)


def create_logger(
        name: str = "cfsi-logger",
        level: int = logging.INFO) -> logging.Logger:
    """ Sets up and returns a logger.
     TODO: add rotating log file handler. """
    logger = logging.getLogger(name)
    logger.setLevel(level)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(level)
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

    log_path = LOGGING_DIR.joinpath("cfsi.log")
    file_handler = RotatingFileHandler(log_path, maxBytes=10000000, backupCount=5)
    file_handler.setLevel(level)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    return logger
