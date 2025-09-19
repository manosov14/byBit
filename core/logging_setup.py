
import logging, sys
from logging.handlers import RotatingFileHandler

def setup_logging(file_path: str | None, level: str = "INFO"):
    logger = logging.getLogger()
    logger.handlers.clear()
    logger.setLevel(level.upper())

    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s"))
    logger.addHandler(ch)

    if file_path:
        fh = RotatingFileHandler(file_path, maxBytes=3_000_000, backupCount=4, encoding="utf-8")
        fh.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s"))
        logger.addHandler(fh)

    return logger
