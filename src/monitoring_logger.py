import os
import logging
from logging.handlers import RotatingFileHandler

PROJECT_HOME = "/home/hdpuser/ecommerce-data-engineering"
LOG_DIR = os.path.join(PROJECT_HOME, "logs")
LOG_FILE = os.path.join(LOG_DIR, "ecommerce_pipeline.log")

os.makedirs(LOG_DIR, exist_ok=True)

logger = logging.getLogger("ecommerce_monitor")
logger.setLevel(logging.INFO)

if not logger.handlers:
    handler = RotatingFileHandler(
        LOG_FILE,
        maxBytes=5 * 1024 * 1024,
        backupCount=3
    )

    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(message)s"
    )

    handler.setFormatter(formatter)
    logger.addHandler(handler)


def log_event(task_name, status, message):
    logger.info(
        "TASK=%s | STATUS=%s | MESSAGE=%s",
        task_name,
        status,
        message
    )


if __name__ == "__main__":
    log_event(
        "monitoring_test",
        "SUCCESS",
        "Monitoring logger initialized"
    )
    print("Monitoring logger is working!")
    print("Log file:", LOG_FILE)
