import os
import sys
import time
from datetime import datetime as dt
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
from openaq_anomaly_prediction.utils.helpers import (
    concat_pq_to_pq,
    exec_time,
    get_parquet_filepaths,
    get_trimestrial_periods,
    parquets_to_csv,
)
from openaq_anomaly_prediction.utils.logger import ProgressLogger, logger

# ---------------------------------------------------------------------

# TODO: We're not handling the different structures of the PQ files we're concatenating
# Some might have different columns depending on the parameters downloaded from Open-Meteo

# TODO: Make 2 separate loops: one for downloading raw data, one for processing/concatenating ALL files at once

# ---------------------------------------------------------------------
# INPUTS

# CITY_ID = "paris"
# CITY_BBOX = 2.155380, 48.749172, 2.538872, 48.962187

CITY_ID = "seoul"
CITY_BBOX = 126.760597, 37.422799, 127.190437, 37.709356
CITY_POLLUTANTS = ["pm25 µg/m³", "co ppm", "no2 ppm", "o3 ppm", "pm10 µg/m³", "so2 ppm"]
CITY_LOCATIONS = {
    "2623440": (37.593749, 126.94953399999999),
    "2622675": (37.53403500000001, 127.13917200000002),
    "2622950": (37.56865, 126.998083),
    "2622916": (37.570632999999994, 126.99678300000001),
    "2623214": (37.65450859999999, 126.7755571),
    "2623052": (37.6104714, 126.9335038),
    "2622627": (37.5203, 126.7737),
    "2622608": (37.516083, 127.019694),
    "2623586": (37.55561, 126.905457),
    "2623504": (37.449479, 127.155512),
    "2622932": (37.527800000000006, 126.7967),
    "2623346": (37.520222, 126.90496699999998),
    "2623422": (37.433169, 127.164372),
    "2622714": (37.4939, 126.7701),
    "2623394": (37.549389, 126.97151899999999),
    "2622851": (37.685514000000005, 126.813441),
    "2622782": (37.576169, 127.02964199999998),
    "2622858": (37.60672399999999, 126.762831),
    "2623344": (37.58495299999999, 127.094283),
    "2622689": (37.65741500000001, 127.067876),
    "2623321": (37.572025, 127.005028),
    "2623557": (37.544639, 127.09570600000002),
    "2623811": (37.474899, 126.898657),
    "2623080": (37.532057, 127.002371),
    "2623112": (37.654278, 127.029333),
    "2623681": (37.4800328, 126.7999056),
    "2623198": (37.49849799999999, 126.88969199999998),
    "2622918": (37.448806, 127.00232900000002),
    "2623154": (37.539283000000005, 127.04094299999998),
    "2622727": (37.526339, 126.896256),
    "2622796": (37.64793, 127.011952),
    "2622827": (37.65292399999999, 127.11586),
    "2623643": (37.452158, 126.884909),
    "2623424": (37.545089, 127.13680599999999),
    "2623444": (37.594755, 127.130141),
    "2623048": (37.452386, 126.90833299999998),
    "2623069": (37.6187, 127.1383),
    "2623563": (37.554936, 126.937619),
    "2622995": (37.476221, 126.86981000000002),
    "2623004": (37.482867, 127.035621),
    "2623465": (37.544656, 126.83509400000001),
    "2623259": (37.542036, 127.049685),
    "2623718": (37.603593, 127.026007),
    "2623345": (37.44310000000001, 126.78840000000001),
    "2622724": (37.4339, 127.12930000000001),
    "2622907": (37.480989, 126.971547),
    "2623532": (37.52328599999999, 126.85868900000001),
    "2622837": (37.564639, 126.975961),
    "2623538": (37.567014, 127.18602299999999),
    "2622586": (37.58016700000001, 127.044856),
    "2622825": (37.6251, 126.84230000000001),
    "2623430": (37.617315, 127.07511999999998),
    "2623116": (37.50268499999999, 127.092385),
    "2623078": (37.666362, 126.886708),
    "2623442": (37.609158, 127.163109),
    "2623194": (37.489495, 126.982489),
    "2622807": (37.4563598, 127.1304652),
    "2623679": (37.56282099999999, 126.826071),
}


# --------------------------------------------------------------------------------------
# DOWNLOAD FROM OPENMETEO API: Download weather data for pre-filtered locations in the date range

# ANSI Escape Codes for cursor control
HIDE_CURSOR = "\033[?25l"
SHOW_CURSOR = "\033[?25h"

sys.stdout.write(HIDE_CURSOR)
sys.stdout.flush()

progress = ProgressLogger()


# ---------------------------------------------------------------------
# If i only want to regenerate the weather parquet files

DISABLE_OPENMETEO_DOWNLOAD = True

# With ALL 29 parameters, each location eats around 20 api calls per trimester
# 1 trimester = 58 locations * 20 calls = 1160 calls
# Open-Meteo free tier = 600 calls per minute, 5000 calls per hour, 10000 calls per day
# So we can download a full year at most every hour without hitting rate limits, 2 years max per day

# estimated api calls per request (with all parameters, per trimester)
ESTIMATED_CALLS_PER_REQUEST = 20
used_calls = 0

# ---------------------------------------------------------------------


try:
    all_logs = []
    all_parquet_files = []

    years = [2024, 2025]
    for year in years:
        # Get trimesters for the year
        trimesters = get_trimestrial_periods(year)

        yearly_parquet_files = []
        for i, trimester in enumerate(trimesters):
            # # Skip trimesters (if already downloaded)
            # if year in [2024] and i in [0, 1, 2]:
            #     continue

            start_time = time.perf_counter()

            # ---------------------------------------------------------------------
            # PREPARE the input parameters for the run/trimester

            datetime_from = trimester[0]
            datetime_to = trimester[1]
            datetime_from_str = dt.fromisoformat(datetime_from).strftime("%Y-%m-%d")
            datetime_to_str = dt.fromisoformat(datetime_to).strftime("%Y-%m-%d")

            # Fix datetime if it's in the future (current ongoing trimester)
            today = dt.now().date()
            datetime_to_openmeteo = (
                datetime_to_str
                if dt.fromisoformat(datetime_to).date() <= today
                else today.strftime("%Y-%m-%d")
            )

            run_id_prefix = f"{CITY_ID}_{year}_T{i + 1}"
            run_id = f"{run_id_prefix}_{datetime_from_str}_{datetime_to_str}"

            logger.debug(
                f"[{run_id_prefix.upper()}] Downloading weather data for {len(CITY_LOCATIONS)} locations..."
            )
            logger.trace(f"RUN_ID: {run_id}")
            logger.trace(f"From: [{datetime_from_str}] to [{datetime_to_openmeteo}]")

            progress.print(
                f"Fetching weather data for {len(CITY_LOCATIONS)} locations..."
            )

            # ---------------------------------------------------------------------
            # DOWNLOAD weather data for each location in the CITY_LOCATIONS

            i = 0
            for location in CITY_LOCATIONS.items():
                location_id = int(location[0])
                location_latitude = CITY_LOCATIONS[location[0]][0]
                location_longitude = CITY_LOCATIONS[location[0]][1]

                progress.print(
                    f"Downloading weather data (location_id={location_id})...",
                    current_progress=i + 1,
                    total_progress=len(CITY_LOCATIONS),
                    prefix_msg=f"{i + 1}/{len(CITY_LOCATIONS)}",
                    last=False,
                )
                if not DISABLE_OPENMETEO_DOWNLOAD:
                    # TODO: Currently redownloading EVERYTHING for a whole trimester, until "today"
                    df_weather = openmeteo.download_weather_data(
                        run_id=run_id,
                        location_id=location_id,
                        latitude=location_latitude,
                        longitude=location_longitude,
                        start_date=datetime_from_str,
                        end_date=datetime_to_openmeteo,
                    )
                    used_calls += ESTIMATED_CALLS_PER_REQUEST

                    # After 500 calls/25 requests, wait for a minute
                    if used_calls >= 500:
                        print()
                        logger.warning(
                            f"Used {used_calls} API calls. Waiting for 70s to avoid rate limits..."
                        )
                        time.sleep(70)  # wait for 70 seconds
                        used_calls = 0

                if i >= (len(CITY_LOCATIONS) - 1):
                    progress.print(
                        f"Downloaded {i + 1} weather data in {exec_time(start_time, fmt=True)}",
                        current_progress=i + 1,
                        total_progress=len(CITY_LOCATIONS),
                        prefix_msg=f"{i + 1}/{len(CITY_LOCATIONS)}",
                        last=True,
                    )

                i += 1

            # ---------------------------------------------------------------------
            # SAVE all weather data for each trimester

            parquet_files = get_parquet_filepaths(
                os.path.join(run_id, "openmeteo"), "*weather.raw.parquet"
            )

            if len(parquet_files) == 0:
                logger.warning(
                    f"No parquet files found for run_id={run_id} in data/parquet/{run_id}/openmeteo/"
                )
                print()
                continue

            logger.trace(
                f"Found {len(parquet_files)} parquet files in data/parquet/{run_id}/openmeteo/*weather.raw.parquet"
            )
            # pprint(parquet_files)

            # CONCATENATE into a TRIMESTER parquet file
            concatenated_filename = f"{run_id}_weather.int.parquet"
            trimester_parquet_file = concat_pq_to_pq(
                parquet_files,
                concatenated_filename,
                os.path.join(cfg.DATA_PARQUET_PATH, run_id, "openmeteo"),
            )
            yearly_parquet_files.append(trimester_parquet_file)
            logger.trace(
                f"Concatenated all weather data in data/parquet/{run_id}/openmeteo/"
            )

            logger.success(
                f"[{run_id_prefix.upper()}] Downloaded weather data for {len(CITY_LOCATIONS)} locations in {exec_time(start_time, fmt=True)}"
            )
            print()

        # CONCATENATE into a YEAR parquet file
        logger.debug(
            f"[{CITY_ID.upper()}_{year}] Concatenating weather data for {year}..."
        )

        if len(yearly_parquet_files) == 0:
            logger.warning(f"No yearly parquet files for year={year}. Skipping...")
            print()
            continue

        concatenated_filename = f"{CITY_ID}_{year}_weather.int.parquet"
        year_parquet_file = concat_pq_to_pq(
            yearly_parquet_files,
            concatenated_filename,
            os.path.join(cfg.DATA_EXPORT_PATH),
        )
        all_parquet_files.append(year_parquet_file)

        logger.success(
            f"[{CITY_ID.upper()}_{year}] Concatenated all weather data from {year} in data/export/{concatenated_filename}"
        )
        print()

    # ---------------------------------------------------------------------
    # CONCATENATE ALL FILES for the CITY into a single parquet file
    logger.debug(f"[{CITY_ID.upper()}] Concatenating ALL weather data for {CITY_ID}...")

    parquet_files = get_parquet_filepaths(
        os.path.join(cfg.DATA_EXPORT_PATH), f"{CITY_ID}_*_weather.int.parquet"
    )
    for file in parquet_files:
        logger.trace(f"data/export/{file.split('/')[-1]}")

    concatenated_filename = f"{CITY_ID}_weather.int.parquet"
    city_parquet_file = concat_pq_to_pq(
        parquet_files,
        concatenated_filename,
        os.path.join(cfg.DATA_EXPORT_PATH),
    )

    logger.success(
        f"[{CITY_ID.upper()}] Concatenated all weather data for {CITY_ID} in data/export/{concatenated_filename}"
    )

except KeyboardInterrupt:
    print()
    logger.warning("Script interrupted by user.")
    print()

finally:
    # ALWAYS print the SHOW_CURSOR code before the script exits
    sys.stdout.write(SHOW_CURSOR)
    sys.stdout.flush()
