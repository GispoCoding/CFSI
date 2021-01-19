import logging


def create_logger(
        name: str = "cfsi-logger",
        level: int = logging.INFO) -> logging.Logger:
    """ Sets up and returns a logger.
     TODO: add rotating log file handler. """
    logger = logging.getLogger(name)
    logger.setLevel(level)
    formatter = logging.Formatter('%(asctime)s\t%(levelname)s\t%(name)s\t%(message)s')

    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(level)
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

    return logger
