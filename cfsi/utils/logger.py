import logging
import os
from logging.handlers import RotatingFileHandler
from pathlib import Path


def create_logger(name: str = "cfsi_logger",
                  level: int = logging.INFO) -> logging.Logger:
    """ Sets up and returns a logger. """
    logger = logging.getLogger(name)
    logger.setLevel(level)
    formatter = logging.Formatter('%(asctime)s %(levelname)8s %(name)20s - %(message)s')

    if not any([isinstance(handler, logging.StreamHandler) for handler in logger.handlers]):
        stream_handler = logging.StreamHandler()
        stream_handler.setLevel(level)
        stream_handler.setFormatter(formatter)
        logger.addHandler(stream_handler)

    if not any([isinstance(handler, RotatingFileHandler) for handler in logger.handlers]):
        try:
            log_file_path = Path(os.environ["CFSI_OUTPUT_CONTAINER"]).joinpath("log", "cfsi.log")
        except KeyError:
            return logger

        log_file_path.parent.mkdir(exist_ok=True)
        file_handler = RotatingFileHandler(str(log_file_path),
                                           maxBytes=1024 * 1024 * 2, backupCount=10)
        file_handler.setLevel(level)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    return logger
