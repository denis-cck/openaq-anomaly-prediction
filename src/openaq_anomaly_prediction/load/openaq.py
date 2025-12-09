from pathlib import Path

from openaq_anomaly_prediction.config import Configuration as config
from openaq_anomaly_prediction.utils.logging import logger

current_file_path = Path(__file__).resolve()
root_location = current_file_path.parent

logger.success(f"OPENAQ: {config.ROOT_PATH}")

# logger.info(f"current_file_path: {current_file_path}")
# logger.info(f"root_location: {root_location}")
# logger.info("Setup complete.")


def test():
    logger.trace("Loaded openaq data module")
    pass
