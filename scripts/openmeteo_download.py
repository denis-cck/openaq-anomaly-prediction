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
from openaq_anomaly_prediction.load.gcp import bq
from openaq_anomaly_prediction.load.openaq import client as openaq
from openaq_anomaly_prediction.load.openmeteo import OpenMeteoClient
from openaq_anomaly_prediction.load.openmeteo import client as openmeteo
from openaq_anomaly_prediction.load.schemas.openmeteo_historical import (
    OpenMeteoHistoricalTable,
)
from openaq_anomaly_prediction.utils.helpers import (
    concat_pq_to_pq,
    exec_time,
    get_parquet_filepaths,
    get_trimestrial_periods,
    parquets_to_csv,
)
from openaq_anomaly_prediction.utils.logger import (
    ProgressLogger,
    grey,
    hex,
    logger,
    rst,
)

# ---------------------------------------------------------------------

# TODO: We're not handling the different structures of the PQ files we're concatenating
# Some might have different columns depending on the parameters downloaded from Open-Meteo

# TODO: Make 2 separate loops: one for downloading raw data, one for processing/concatenating ALL files at once

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

PERFECT_LOCATIONS = [
    {
        "id": "2622689",
        "coordinates_latitude": 37.65741500000001,
        "coordinates_longitude": 127.067876,
    },
    {
        "id": "2623194",
        "coordinates_latitude": 37.489495,
        "coordinates_longitude": 126.982489,
    },
    {
        "id": "2622744",
        "coordinates_latitude": 37.4112,
        "coordinates_longitude": 127.25790000000002,
    },
    {
        "id": "2623190",
        "coordinates_latitude": 37.60666700000001,
        "coordinates_longitude": 127.02726400000002,
    },
    {
        "id": "2623536",
        "coordinates_latitude": 37.423889,
        "coordinates_longitude": 126.994512,
    },
    {
        "id": "2623173",
        "coordinates_latitude": 37.50560000000001,
        "coordinates_longitude": 126.759,
    },
    {
        "id": "2622918",
        "coordinates_latitude": 37.448806,
        "coordinates_longitude": 127.00232900000002,
    },
    {
        "id": "2623623",
        "coordinates_latitude": 37.44116400000001,
        "coordinates_longitude": 126.706817,
    },
    {
        "id": "2623276",
        "coordinates_latitude": 37.38569999999999,
        "coordinates_longitude": 126.8619,
    },
    {
        "id": "2623336",
        "coordinates_latitude": 37.384904,
        "coordinates_longitude": 126.931247,
    },
    {
        "id": "2622807",
        "coordinates_latitude": 37.4563598,
        "coordinates_longitude": 127.1304652,
    },
    {
        "id": "2623582",
        "coordinates_latitude": 37.52922,
        "coordinates_longitude": 126.71511,
    },
    {
        "id": "2623586",
        "coordinates_latitude": 37.55561,
        "coordinates_longitude": 126.905457,
    },
    {
        "id": "2622825",
        "coordinates_latitude": 37.6251,
        "coordinates_longitude": 126.84230000000001,
    },
    {
        "id": "2622687",
        "coordinates_latitude": 37.543224,
        "coordinates_longitude": 126.737849,
    },
    {
        "id": "2623186",
        "coordinates_latitude": 37.455474,
        "coordinates_longitude": 126.692888,
    },
    {
        "id": "2623538",
        "coordinates_latitude": 37.567014,
        "coordinates_longitude": 127.18602299999999,
    },
    {
        "id": "2623488",
        "coordinates_latitude": 37.382662,
        "coordinates_longitude": 127.119103,
    },
    {
        "id": "2623335",
        "coordinates_latitude": 37.384904,
        "coordinates_longitude": 126.931247,
    },
    {
        "id": "2805678",
        "coordinates_latitude": 37.487643999999996,
        "coordinates_longitude": 126.72556299999998,
    },
    {
        "id": "2623424",
        "coordinates_latitude": 37.545089,
        "coordinates_longitude": 127.13680599999999,
    },
    {
        "id": "4872069",
        "coordinates_latitude": 37.49918400000001,
        "coordinates_longitude": 126.723415,
    },
    {
        "id": "2623154",
        "coordinates_latitude": 37.539283000000005,
        "coordinates_longitude": 127.04094299999998,
    },
    {
        "id": "2622827",
        "coordinates_latitude": 37.65292399999999,
        "coordinates_longitude": 127.11586,
    },
    {
        "id": "2622782",
        "coordinates_latitude": 37.576169,
        "coordinates_longitude": 127.02964199999998,
    },
    {
        "id": "2623645",
        "coordinates_latitude": 37.449722,
        "coordinates_longitude": 126.72416699999998,
    },
    {
        "id": "2623198",
        "coordinates_latitude": 37.49849799999999,
        "coordinates_longitude": 126.88969199999998,
    },
    {
        "id": "4402010",
        "coordinates_latitude": 37.501782999999996,
        "coordinates_longitude": 126.996573,
    },
    {
        "id": "2622798",
        "coordinates_latitude": 37.48824200000001,
        "coordinates_longitude": 126.92708900000001,
    },
    {
        "id": "2623259",
        "coordinates_latitude": 37.542036,
        "coordinates_longitude": 127.049685,
    },
    {
        "id": "2623170",
        "coordinates_latitude": 37.51279,
        "coordinates_longitude": 126.73643,
    },
    {
        "id": "2623069",
        "coordinates_latitude": 37.6187,
        "coordinates_longitude": 127.1383,
    },
    {
        "id": "2623172",
        "coordinates_latitude": 37.50560000000001,
        "coordinates_longitude": 126.759,
    },
    {
        "id": "2622600",
        "coordinates_latitude": 37.4274,
        "coordinates_longitude": 126.7461,
    },
    {
        "id": "2622745",
        "coordinates_latitude": 37.4112,
        "coordinates_longitude": 127.25790000000002,
    },
    {
        "id": "2622858",
        "coordinates_latitude": 37.60672399999999,
        "coordinates_longitude": 126.762831,
    },
    {
        "id": "2623052",
        "coordinates_latitude": 37.6104714,
        "coordinates_longitude": 126.9335038,
    },
    {
        "id": "2623367",
        "coordinates_latitude": 37.4051,
        "coordinates_longitude": 126.91779999999999,
    },
    {
        "id": "2623760",
        "coordinates_latitude": 37.4903861,
        "coordinates_longitude": 126.7236421,
    },
    {
        "id": "2623430",
        "coordinates_latitude": 37.617315,
        "coordinates_longitude": 127.07511999999998,
    },
    {
        "id": "2623811",
        "coordinates_latitude": 37.474899,
        "coordinates_longitude": 126.898657,
    },
    {
        "id": "2622847",
        "coordinates_latitude": 37.72627,
        "coordinates_longitude": 127.189881,
    },
    {
        "id": "2623078",
        "coordinates_latitude": 37.666362,
        "coordinates_longitude": 126.886708,
    },
    {
        "id": "3916039",
        "coordinates_latitude": 37.477837,
        "coordinates_longitude": 126.959128,
    },
    {
        "id": "2622791",
        "coordinates_latitude": 37.735582,
        "coordinates_longitude": 127.040421,
    },
    {
        "id": "2622996",
        "coordinates_latitude": 37.3801,
        "coordinates_longitude": 126.80410000000002,
    },
    {
        "id": "2622727",
        "coordinates_latitude": 37.526339,
        "coordinates_longitude": 126.896256,
    },
    {
        "id": "2622688",
        "coordinates_latitude": 37.543224,
        "coordinates_longitude": 126.737849,
    },
    {
        "id": "2623681",
        "coordinates_latitude": 37.4800328,
        "coordinates_longitude": 126.7999056,
    },
    {
        "id": "2622724",
        "coordinates_latitude": 37.4339,
        "coordinates_longitude": 127.12930000000001,
    },
    {
        "id": "2622917",
        "coordinates_latitude": 37.570632999999994,
        "coordinates_longitude": 126.99678300000001,
    },
    {
        "id": "2623711",
        "coordinates_latitude": 37.698671999999995,
        "coordinates_longitude": 127.204729,
    },
    {
        "id": "2622932",
        "coordinates_latitude": 37.527800000000006,
        "coordinates_longitude": 126.7967,
    },
    {
        "id": "2623321",
        "coordinates_latitude": 37.572025,
        "coordinates_longitude": 127.005028,
    },
    {
        "id": "2623048",
        "coordinates_latitude": 37.452386,
        "coordinates_longitude": 126.90833299999998,
    },
    {
        "id": "2623445",
        "coordinates_latitude": 37.594755,
        "coordinates_longitude": 127.130141,
    },
    {
        "id": "2622627",
        "coordinates_latitude": 37.5203,
        "coordinates_longitude": 126.7737,
    },
    {
        "id": "2623346",
        "coordinates_latitude": 37.520222,
        "coordinates_longitude": 126.90496699999998,
    },
    {
        "id": "2623474",
        "coordinates_latitude": 37.723148,
        "coordinates_longitude": 126.756434,
    },
    {
        "id": "2622796",
        "coordinates_latitude": 37.64793,
        "coordinates_longitude": 127.011952,
    },
    {
        "id": "2623112",
        "coordinates_latitude": 37.654278,
        "coordinates_longitude": 127.029333,
    },
    {
        "id": "2623343",
        "coordinates_latitude": 37.58495299999999,
        "coordinates_longitude": 127.094283,
    },
    {
        "id": "2623444",
        "coordinates_latitude": 37.594755,
        "coordinates_longitude": 127.130141,
    },
    {
        "id": "2623442",
        "coordinates_latitude": 37.609158,
        "coordinates_longitude": 127.163109,
    },
    {
        "id": "2622606",
        "coordinates_latitude": 37.635600000000004,
        "coordinates_longitude": 127.2162,
    },
    {
        "id": "2622793",
        "coordinates_latitude": 37.390588,
        "coordinates_longitude": 127.077838,
    },
    {
        "id": "2623590",
        "coordinates_latitude": 37.404167,
        "coordinates_longitude": 126.72611099999999,
    },
    {
        "id": "2623679",
        "coordinates_latitude": 37.56282099999999,
        "coordinates_longitude": 126.826071,
    },
    {
        "id": "4864522",
        "coordinates_latitude": 37.49638199999999,
        "coordinates_longitude": 126.72549200000002,
    },
    {
        "id": "2623004",
        "coordinates_latitude": 37.482867,
        "coordinates_longitude": 127.035621,
    },
    {
        "id": "2623080",
        "coordinates_latitude": 37.532057,
        "coordinates_longitude": 127.002371,
    },
    {
        "id": "2623394",
        "coordinates_latitude": 37.549389,
        "coordinates_longitude": 126.97151899999999,
    },
    {
        "id": "2623465",
        "coordinates_latitude": 37.544656,
        "coordinates_longitude": 126.83509400000001,
    },
    {
        "id": "2623440",
        "coordinates_latitude": 37.593749,
        "coordinates_longitude": 126.94953399999999,
    },
    {
        "id": "2623344",
        "coordinates_latitude": 37.58495299999999,
        "coordinates_longitude": 127.094283,
    },
    {
        "id": "2622907",
        "coordinates_latitude": 37.480989,
        "coordinates_longitude": 126.971547,
    },
    {
        "id": "2622850",
        "coordinates_latitude": 37.685514000000005,
        "coordinates_longitude": 126.813441,
    },
    {
        "id": "2622848",
        "coordinates_latitude": 37.72627,
        "coordinates_longitude": 127.189881,
    },
    {
        "id": "2623114",
        "coordinates_latitude": 37.394327,
        "coordinates_longitude": 126.956859,
    },
    {
        "id": "2623277",
        "coordinates_latitude": 37.38569999999999,
        "coordinates_longitude": 126.8619,
    },
    {
        "id": "2623115",
        "coordinates_latitude": 37.394327,
        "coordinates_longitude": 126.956859,
    },
    {
        "id": "2622855",
        "coordinates_latitude": 37.504547,
        "coordinates_longitude": 126.994611,
    },
    {
        "id": "2622851",
        "coordinates_latitude": 37.685514000000005,
        "coordinates_longitude": 126.813441,
    },
    {
        "id": "2622718",
        "coordinates_latitude": 37.5941,
        "coordinates_longitude": 126.69859999999998,
    },
    {
        "id": "2623532",
        "coordinates_latitude": 37.52328599999999,
        "coordinates_longitude": 126.85868900000001,
    },
    {
        "id": "2622609",
        "coordinates_latitude": 37.668299999999995,
        "coordinates_longitude": 126.7564,
    },
    {
        "id": "2623537",
        "coordinates_latitude": 37.423889,
        "coordinates_longitude": 126.994512,
    },
    {
        "id": "2623426",
        "coordinates_latitude": 37.5175623,
        "coordinates_longitude": 127.0472893,
    },
    {
        "id": "2623473",
        "coordinates_latitude": 37.723148,
        "coordinates_longitude": 126.756434,
    },
    {
        "id": "2623504",
        "coordinates_latitude": 37.449479,
        "coordinates_longitude": 127.155512,
    },
    {
        "id": "2622586",
        "coordinates_latitude": 37.58016700000001,
        "coordinates_longitude": 127.044856,
    },
    {
        "id": "2623563",
        "coordinates_latitude": 37.554936,
        "coordinates_longitude": 126.937619,
    },
    {
        "id": "2623579",
        "coordinates_latitude": 37.457778,
        "coordinates_longitude": 126.690833,
    },
    {
        "id": "2622916",
        "coordinates_latitude": 37.570632999999994,
        "coordinates_longitude": 126.99678300000001,
    },
    {
        "id": "4953358",
        "coordinates_latitude": 37.515336,
        "coordinates_longitude": 127.049357,
    },
    {
        "id": "2622594",
        "coordinates_latitude": 37.539109,
        "coordinates_longitude": 127.214608,
    },
    {
        "id": "2622780",
        "coordinates_latitude": 37.546111,
        "coordinates_longitude": 126.730278,
    },
    {
        "id": "2623617",
        "coordinates_latitude": 37.38257620000001,
        "coordinates_longitude": 127.1027442,
    },
    {
        "id": "2623508",
        "coordinates_latitude": 37.619335,
        "coordinates_longitude": 126.71674800000001,
    },
    {
        "id": "2622950",
        "coordinates_latitude": 37.56865,
        "coordinates_longitude": 126.998083,
    },
    {
        "id": "2622860",
        "coordinates_latitude": 37.746355,
        "coordinates_longitude": 127.04765199999999,
    },
    {
        "id": "6136410",
        "coordinates_latitude": 37.506537,
        "coordinates_longitude": 127.163589,
    },
    {
        "id": "2622837",
        "coordinates_latitude": 37.564639,
        "coordinates_longitude": 126.975961,
    },
    {
        "id": "2622714",
        "coordinates_latitude": 37.4939,
        "coordinates_longitude": 126.7701,
    },
    {
        "id": "2623134",
        "coordinates_latitude": 37.7481,
        "coordinates_longitude": 127.106,
    },
    {
        "id": "2622995",
        "coordinates_latitude": 37.476221,
        "coordinates_longitude": 126.86981000000002,
    },
    {
        "id": "2623116",
        "coordinates_latitude": 37.50268499999999,
        "coordinates_longitude": 127.092385,
    },
    {
        "id": "2623191",
        "coordinates_latitude": 37.60666700000001,
        "coordinates_longitude": 127.02726400000002,
    },
    {
        "id": "2623345",
        "coordinates_latitude": 37.44310000000001,
        "coordinates_longitude": 126.78840000000001,
    },
    {
        "id": "2623647",
        "coordinates_latitude": 37.580842,
        "coordinates_longitude": 127.226766,
    },
    {
        "id": "4872070",
        "coordinates_latitude": 37.49044700000001,
        "coordinates_longitude": 126.723486,
    },
    {
        "id": "2622675",
        "coordinates_latitude": 37.53403500000001,
        "coordinates_longitude": 127.13917200000002,
    },
    {
        "id": "2623643",
        "coordinates_latitude": 37.452158,
        "coordinates_longitude": 126.884909,
    },
    {
        "id": "2623720",
        "coordinates_latitude": 37.404839,
        "coordinates_longitude": 126.69715799999999,
    },
    {
        "id": "2622608",
        "coordinates_latitude": 37.516083,
        "coordinates_longitude": 127.019694,
    },
    {
        "id": "2623718",
        "coordinates_latitude": 37.603593,
        "coordinates_longitude": 127.026007,
    },
    {
        "id": "2623557",
        "coordinates_latitude": 37.544639,
        "coordinates_longitude": 127.09570600000002,
    },
    {
        "id": "2623214",
        "coordinates_latitude": 37.65450859999999,
        "coordinates_longitude": 126.7755571,
    },
    {
        "id": "2623422",
        "coordinates_latitude": 37.433169,
        "coordinates_longitude": 127.164372,
    },
]

# Seoul (large)
CITY_ID = "seoul"
CITY_NAME = "Seoul"
CITY_BBOX = 126.678543, 37.376705, 127.282104, 37.754973

# 119 locations that all have the 6 pollutants (at least one measurement of each)
# fmt: off
CITY_LOCATIONS_IDS = ['2622594', '2622916', '2622586', '2623582', '2622714', '2623115', '2623538', '2622687', '2622858', '2623112', '2623504', '2623760', '2622718', '2623557', '2622825', '2623078', '2623426', '2623190', '2623345', '2623445', '2623186', '2622689', '2623069', '2623321', '2623422', '2623647', '2623424', '2622827', '2622782', '2623679', '2623276', '2623173', '2623617', '2623537', '2622807', '2623643', '2622727', '2623586', '2623645', '2623718', '2623623', '4864522', '2622837', '2623259', '2623154', '2622791', '2622688', '3916039', '2622627', '4953358', '2623590', '6136410', '2623430', '2622847', '2623394', '2805678', '2622600', '2623444', '2623579', '2623336', '2623442', '2623116', '2622793', '2623346', '2623488', '2623335', '2623681', '2623191', '2622724', '2623811', '2623134', '2623473', '2623277', '2622780', '2622744', '2622848', '2623474', '4872069', '2623114', '2622907', '2622796', '2623465', '2622798', '2622606', '2623344', '2623508', '2623080', '2623214', '2622932', '2622918', '2623532', '2623172', '2622995', '2623198', '2622950', '2622675', '2622608', '2622745', '2623711', '2623052', '2623367', '2623536', '2623048', '2623004', '2622855', '2623194', '2623563', '2622917', '2623343', '2623720', '2623170', '2622850', '4402010', '2622996', '2623440', '2622860', '2622851', '2622609', '4872070']
# fmt: on

# ids_formatted = ", ".join([f"'{id}'" for id in CITY_LOCATIONS_IDS])
# query = f"""
#     SELECT
#         id,
#         coordinates_latitude,
#         coordinates_longitude
#     FROM
#         `openaq-anomaly-prediction.dev_staging.stg_openaq__locations`
#     WHERE
#         id IN ({ids_formatted})
# """
# city_locations_with_coordinates = bq.query(query, dry_run=False)
# print(city_locations_with_coordinates)
# perfect_locations = []
# for index, row in city_locations_with_coordinates.iterrows():
#     perfect_locations.append(
#         {
#             "id": row["id"],
#             "coordinates_latitude": row["coordinates_latitude"],
#             "coordinates_longitude": row["coordinates_longitude"],
#         }
#     )
# print(perfect_locations)

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

DISABLE_OPENMETEO_DOWNLOAD = False

# With ALL 29 parameters, each location eats around 20 api calls per trimester
# 1 trimester = 58 locations * 20 calls = 1160 calls
# Open-Meteo free tier = 600 calls per minute, 5000 calls per hour, 10000 calls per day
# So we can download a full year at most every hour without hitting rate limits, 2 years max per day

# estimated api calls per request (with all parameters, per trimester)

# Open-Meteo Estimation for all listed parameters and 1 full trimester (1 location):
ESTIMATED_CALLS_PER_REQUEST = 20
used_calls = 0

# ---------------------------------------------------------------------


# datetime_from_str = "2025-12-01"
# datetime_to_str = "2025-12-02"

# df_weather = openmeteo.download_weather_data(
#     run_id="test_run_for_one_location",
#     location_id=PERFECT_LOCATIONS[0]["id"],
#     latitude=PERFECT_LOCATIONS[0]["coordinates_latitude"],
#     longitude=PERFECT_LOCATIONS[0]["coordinates_longitude"],
#     start_date=datetime_from_str,
#     end_date=datetime_to_str,
# )
# print(df_weather)

try:
    all_logs = []
    all_parquet_files = []

    years = [2024]
    for year in years:
        # Get trimesters for the year
        trimesters = get_trimestrial_periods(year)

        yearly_parquet_files = []
        for i, trimester in enumerate(trimesters):
            # Skip trimesters (if already downloaded)
            if year in [2024] and i in [0, 2]:
                continue

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

            # print()
            # print(datetime_from_str)
            # print(datetime_to_openmeteo)
            # print()

            run_id_prefix = f"{CITY_ID}_{year}_T{i + 1}"
            run_id = f"{run_id_prefix}_{datetime_from_str}_{datetime_to_str}"

            logger.debug(
                f"[{run_id_prefix.upper()}] Downloading weather data for {len(PERFECT_LOCATIONS)} locations..."
            )
            logger.trace(f"RUN_ID: {run_id}")
            logger.trace(f"From: [{datetime_from_str}] to [{datetime_to_openmeteo}]")

            progress.print(
                f"Fetching weather data for {len(PERFECT_LOCATIONS)} locations..."
            )

            # ---------------------------------------------------------------------
            # DOWNLOAD weather data for each location in the CITY_LOCATIONS

            i = 0
            for location in PERFECT_LOCATIONS:
                location_id = int(location["id"])
                location_latitude = location["coordinates_latitude"]
                location_longitude = location["coordinates_longitude"]

                progress.print(
                    f"Downloading weather data (location_id={location_id})...",
                    current_progress=i + 1,
                    total_progress=len(PERFECT_LOCATIONS),
                    prefix_msg=f"{i + 1}/{len(PERFECT_LOCATIONS)}",
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
                        logger.trace(
                            f"{hex('#dfa934')}WAIT{rst()}{grey()}: Waiting 70s to avoid rate limits (used {used_calls} requests)"
                        )
                        time.sleep(70)  # wait for 70 seconds
                        used_calls = 0
                # else:
                #     time.sleep(0.1)  # simulate some delay for the download

                if i >= (len(PERFECT_LOCATIONS) - 1):
                    progress.print(
                        f"Downloaded {i + 1} weather data in {exec_time(start_time, fmt=True)}",
                        current_progress=i + 1,
                        total_progress=len(PERFECT_LOCATIONS),
                        prefix_msg=f"{i + 1}/{len(PERFECT_LOCATIONS)}",
                        last=True,
                    )

                i += 1

            # ---------------------------------------------------------------------
            # SAVE PERIOD DATA:

            if OpenMeteoClient.SAVE_TO_GCS and not DISABLE_OPENMETEO_DOWNLOAD:
                save_start_time = time.perf_counter()

                logger.trace(
                    f"GCS > BIGQUERY: transfering all files in GCS to Big Query for RUN_ID [{run_id}]..."
                )

                # Trigger the load to Big Query from GCS Parquet files
                OpenMeteoHistoricalTable().transfer_all_data_to_bq()

                # period_logs["gcs_saving_duration"] = exec_time(save_start_time, 2)
                logger.debug(
                    f"[GCS > BIGQUERY] Upserted all measurements from GCS to BigQuery in {exec_time(save_start_time, fmt=True)}"
                )

            # Generate the period file from all downloaded Parquet files for the run_id.
            if OpenMeteoClient.SAVE_TO_DISK and not DISABLE_OPENMETEO_DOWNLOAD:
                save_start_time = time.perf_counter()

                logger.trace(
                    f"DISK: creating a consolidated file on disk for RUN_ID [{run_id}]..."
                )

                # Retrieve all Parquet files for the run manually
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

                # CONCATENATE into a TRIMESTER parquet file
                concatenated_filename = f"{run_id}_weather.int.parquet"
                trimester_parquet_file = concat_pq_to_pq(
                    parquet_files,
                    concatenated_filename,
                    os.path.join(cfg.DATA_PARQUET_PATH, run_id, "openmeteo"),
                )
                # yearly_parquet_files.append(trimester_parquet_file)  # honestly pointless
                all_parquet_files.append(trimester_parquet_file)

                logger.trace(
                    f"Concatenated all weather data in data/parquet/{run_id}/openmeteo/"
                )

                # period_logs["disk_saving_duration"] = exec_time(save_start_time, 2)
                logger.debug(
                    f"[DISK] Created data/csv/{run_id}.raw.csv from {len(parquet_files)} parquet files in {exec_time(save_start_time, fmt=True)}"
                )

            print()

            # END OF LOCATIONS LOOPS -----

        # END OF PERIOD/TRIMESTER LOOPS -----

    # END OF YEARS LOOPS -----

    # ---------------------------------------------------------------------
    # CONCATENATE ALL FILES for the CITY into a single parquet file

    logger.debug(f"[{CITY_ID.upper()}] Concatenating ALL weather data for {CITY_ID}...")

    if len(all_parquet_files) == 0:
        logger.warning("No parquet files found.")
        print()

    else:
        concatenated_filename = f"{CITY_ID}_weather.int.parquet"
        final_file = concat_pq_to_pq(
            all_parquet_files,
            concatenated_filename,
            os.path.join(cfg.DATA_EXPORT_PATH),
        )

        logger.success(
            f"[{CITY_ID.upper()}] Concatenated all weather data for {CITY_ID} in data/export/{concatenated_filename}"
        )

    # # OLD ARCHIVING OF THE PARQUET FILES PER YEAR THEN TOTAL

    # # CONCATENATE all periods into a consolidated file
    # logger.debug(
    #     f"[{CITY_ID.upper()}_{year}] Concatenating weather data for {year}..."
    # )

    # if len(yearly_parquet_files) == 0:
    #     logger.warning(f"No yearly parquet files for year={year}. Skipping...")
    #     print()
    #     continue

    # concatenated_filename = f"{CITY_ID}_{year}_weather.int.parquet"
    # year_parquet_file = concat_pq_to_pq(
    #     yearly_parquet_files,
    #     concatenated_filename,
    #     os.path.join(cfg.DATA_EXPORT_PATH),
    # )
    # all_parquet_files.append(year_parquet_file)

    # logger.success(
    #     f"[{CITY_ID.upper()}_{year}] Concatenated all weather data from {year} in data/export/{concatenated_filename}"
    # )
    # print()

    # # ---------------------------------------------------------------------
    # # CONCATENATE ALL FILES for the CITY into a single parquet file
    # logger.debug(f"[{CITY_ID.upper()}] Concatenating ALL weather data for {CITY_ID}...")

    # parquet_files = get_parquet_filepaths(
    #     os.path.join(cfg.DATA_EXPORT_PATH), f"{CITY_ID}_*_weather.int.parquet"
    # )
    # for file in parquet_files:
    #     logger.trace(f"data/export/{file.split('/')[-1]}")

    # concatenated_filename = f"{CITY_ID}_weather.int.parquet"
    # city_parquet_file = concat_pq_to_pq(
    #     parquet_files,
    #     concatenated_filename,
    #     os.path.join(cfg.DATA_EXPORT_PATH),
    # )

    # logger.success(
    #     f"[{CITY_ID.upper()}] Concatenated all weather data for {CITY_ID} in data/export/{concatenated_filename}"
    # )

except KeyboardInterrupt:
    print()
    logger.warning("Script interrupted by user.")
    print()

finally:
    # ALWAYS print the SHOW_CURSOR code before the script exits
    sys.stdout.write(SHOW_CURSOR)
    sys.stdout.flush()
