import logging
import sys

def setup_logging(level=logging.INFO):
    handler = logging.StreamHandler(sys.stdout)
    fmt = "[%(asctime)s] [%(levelname)s] %(message)s"
    datefmt = "%Y-%m-%d %H:%M:%S"
    handler.setFormatter(logging.Formatter(fmt=fmt, datefmt=datefmt))

    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(level)

    # Reduce noise from dependencies
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("requests").setLevel(logging.WARNING)
