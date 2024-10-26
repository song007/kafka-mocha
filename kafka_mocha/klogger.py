import logging


def get_custom_logger(name="kafka_mocha"):
    logger = logging.getLogger(name)
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(logging.Formatter("%(module)-15s > %(message)s"))

    if not logger.hasHandlers():
        logger.addHandler(stream_handler)
        logger.setLevel(logging.INFO)

    return logger
