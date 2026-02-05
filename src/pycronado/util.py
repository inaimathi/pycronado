import logging
import threading

_logger_lock = threading.Lock()


def getLogger(name, level=logging.DEBUG):
    logger = logging.getLogger(name)
    logger.setLevel(level)

    with _logger_lock:
        # If this logger OR any parent/root already has handlers, don't add another.
        # (This avoids the "root handler + local handler" double-print.)
        if logger.hasHandlers():
            return logger

        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

        # If we *do* attach a handler here, don't also bubble to root.
        logger.propagate = False

    return logger
