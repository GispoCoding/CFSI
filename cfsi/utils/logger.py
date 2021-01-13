import logging


def create_logger(
        name: str = "cfsi-logger",
        level: int = logging.INFO) -> logging.Logger:
    """ Sets up and returns a logger.
     TODO: add rotating log file handler. """
    logger = logging.getLogger(name)
    logger.setLevel(level)
    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(level)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)
    return logger
