import sys
import logging

def get_logger(logger_name, log_level=logging.INFO):
    logger = logging.getLogger(logger_name)

    if not logger.hasHandlers():
        fmt = logging.Formatter(
            fmt="%(asctime)-11s %(name)s:%(lineno)d %(levelname)s: %(message)s", 
            datefmt="[%Y/%m/%d-%H:%M:%S]"
        )
        stream_handler = logging.StreamHandler(stream=sys.stdout)
        stream_handler.setFormatter(fmt)

        logger.addHandler(stream_handler)

    logger.setLevel(log_level)

    return logger
