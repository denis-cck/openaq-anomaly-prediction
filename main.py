from pathlib import Path

from openaq_anomaly_prediction.config import Configuration as config
from openaq_anomaly_prediction.load.openaq import test
from openaq_anomaly_prediction.utils.logging import logger

logger.success(f"MAIN: {config.ROOT_PATH}")
test()
