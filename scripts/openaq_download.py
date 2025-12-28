import os
import sys
from pprint import pprint

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import seaborn as sns

from openaq_anomaly_prediction.config import Configuration as cfg
from openaq_anomaly_prediction.load.openaq import AreaDownloader
from openaq_anomaly_prediction.load.openaq import client as openaq
from openaq_anomaly_prediction.load.openmeteo import client as openmeteo
from openaq_anomaly_prediction.utils.helpers import get_trimestrial_periods
from openaq_anomaly_prediction.utils.logger import ProgressLogger, logger

# ---------------------------------------------------------------------
# INPUTS

# Seoul LARGE: https://bboxfinder.com/#37.376705,126.678543,37.754973,127.282104
# Seoul: https://bboxfinder.com/#37.439429,126.775360,37.693329,127.182884
# Paris: https://bboxfinder.com/#48.749172,2.155380,48.962187,2.538872

# New Delhi:    76.772461, 28.161110, 77.768372, 28.943516
# Seoul:        126.760597,37.422799,127.190437,37.709356
# Paris:        2.155380, 48.749172, 2.538872, 48.962187

# Los Angeles: -118.668153, 33.703935, -118.155358, 34.337306
# Nagoya:       136.822682, 35.058431, 137.050743, 35.233847


# CITY_ID = "paris"
# CITY_BBOX = 2.155380, 48.749172, 2.538872, 48.962187

# Seoul (small)
# CITY_ID = "seoul"
# CITY_NAME = "Seoul"
# CITY_BBOX = 126.760597, 37.422799, 127.190437, 37.709356


# Seoul (large)
CITY_ID = "seoul"
CITY_NAME = "Seoul"
CITY_BBOX = 126.678543, 37.376705, 127.282104, 37.754973


# ---------------------------------------------------------------------
# LOCATIONS

city = AreaDownloader(area_id=CITY_ID)
city.load_bbox(*CITY_BBOX)

logger.info(f"Area: {city.area_id.upper()}")
logger.info(f"Locations: {city.locations.shape}")
logger.info(f"Sensors: {city.sensors.shape}")
print()

# --------------------------------------------------------------------------------------
# TODO: Figure out the whole utc=True issue with to_datetime when doing to_datetime in request()

# --------------------------------------------------------------------------------------
# TODO: CREATE A "PROJECT" CLASS/TABLE TO MANAGE THE ACTUAL LOCATIONS WE WANT TO REMEMBER, DOWNLOAD AND TRACK
# Is supposed to replace both the hardcoded CITY_ID and CITY_BBOX above, as well as store additional metadata about the area
# e.g., population, area size, country, etc. but also which locations/sensors are used for the specific project

# --------------------------------------------------------------------------------------
# DOWNLOAD FROM OPENAQ API: Download measurements for filtered sensors in the date range

# # Testing: clear rate limits
# openaq.clear_ratelimits()

# ANSI Escape Codes for cursor control
HIDE_CURSOR = "\033[?25l"
SHOW_CURSOR = "\033[?25h"

sys.stdout.write(HIDE_CURSOR)
sys.stdout.flush()


try:
    # 2023 T4, 2025 T4
    all_logs = []
    # years = [2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024, 2025]
    years = [2025]
    for year in years:
        # Get trimesters for the year
        trimesters = get_trimestrial_periods(year)

        for i, trimester in enumerate(trimesters):
            # Skip trimesters (if already downloaded)
            # if year in [2024] and i in [0, 1]:
            #     continue
            # if year in [2025] and i in [0, 1, 2]:
            #     continue

            datetime_from = trimester[0]
            datetime_to = trimester[1]

            run_id_prefix = f"{CITY_ID}_{year}_T{i + 1}"

            trimester_logs = city.download_period_from_area(
                datetime_from=datetime_from,
                datetime_to=datetime_to,
                run_id_prefix=run_id_prefix,
                run_label=f"T{i + 1}/{year}",
            )
            all_logs.append(trimester_logs)

    # Summary of all download logs
    AreaDownloader.print_period_logs(all_logs, True)

except KeyboardInterrupt:
    print()
    logger.warning("Script interrupted by user.")
    print()

finally:
    # ALWAYS print the SHOW_CURSOR code before the script exits
    sys.stdout.write(SHOW_CURSOR)
    sys.stdout.flush()
