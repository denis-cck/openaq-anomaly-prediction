import os
import random
import time
from datetime import date
from datetime import datetime as dt
from pathlib import Path
from pprint import pprint
from typing import Any, Union

import numpy as np
import pandas as pd
import pyarrow.parquet as pq
import requests

# pandas typing aliases from their codebase (for date types hints)
from pandas._typing import (
    AnyArrayLike,
    # ArrayLike,
    # DateTimeErrorChoices,
)

from openaq_anomaly_prediction.config import Configuration as config
from openaq_anomaly_prediction.utils.helpers import (
    exec_time,
    get_iso_now,
    get_parquet_filepaths,
    parquets_to_csv,
    save_logs,
)
from openaq_anomaly_prediction.utils.logging import (
    ProgressLogger,
    b,
    grey,
    hex,
    logger,
    rst,
)


class OpenMeteo_Client:
    FULL_PARAMETERS = [
        "temperature_2m",
        "relative_humidity_2m",
        "dew_point_2m",
        "apparent_temperature",
        "precipitation",
        "rain",
        "snowfall",
        "snow_depth",
        "shortwave_radiation",
        "direct_radiation",
        "diffuse_radiation",
        "global_tilted_irradiance",
        "direct_normal_irradiance",
        "terrestrial_radiation",
        "weather_code",
        "pressure_msl",
        "surface_pressure",
        "cloud_cover",
        "cloud_cover_low",
        "cloud_cover_mid",
        "cloud_cover_high",
        "vapour_pressure_deficit",
        "et0_fao_evapotranspiration",
        "wind_speed_100m",
        "wind_speed_10m",
        "wind_direction_10m",
        "wind_direction_100m",
        "wind_gusts_10m",
        "is_day",
    ]

    def __init__(self, **kwargs) -> None:
        # KEYWORDS ARGUMENTS
        self.verbose = kwargs.get("verbose", 0)

    def request_api(self, url: str, request_params: dict, **kwargs) -> dict:
        """Make a request to the Open-Meteo API and return the results as a DataFrame."""
        verbose = kwargs.get("verbose", self.verbose)

        request_headers = {}

        # Make the Request
        try:
            start_time = time.perf_counter()

            response = requests.get(url, headers=request_headers, params=request_params)
            response.raise_for_status()  # Raises error for 4xx or 5xx

            # print(response.headers)

            # # ---------------------------------------------------------------------
            # # Randomly simulate HTTPError for testing purposes and add a status code
            # if random.random() < 0.01:  # 1% chance to simulate an error
            #     raise requests.exceptions.HTTPError(
            #         "Simulated HTTP error for testing.", response=response
            #     )
            # # ---------------------------------------------------------------------

            data = response.json()

            return data

        except requests.exceptions.HTTPError as err:
            # print(err.response.headers)
            # print(err.response.status_code)
            # print(err)
            # Rate limit handling: wait for reset if 429 Too Many Requests (if spamming 400/500 errors)
            if err.response.status_code == 429 or "429 Client Error" in str(err):
                print()
                logger.warning(
                    f"{hex('#dfa934')}[{err.response.status_code}] RATE LIMIT{rst()}{grey()}: Hit rate limit. Waiting for 90s...{rst()}"
                )
                time.sleep(90)  # Wait for 90 seconds before retrying

            # print(f"HTTP error occurred: {err}")
            # print(response.text)  # Print the error message from API
            raise err

    # ---------------------------------------------------------------------
    # PUBLIC METHODS

    def construct_weather_dataframe(self, location_id: int, data: dict) -> pd.DataFrame:
        """Create a standard DataFrame from Open-Meteo API response data."""

        df_weather = pd.DataFrame(data["hourly"])
        df_weather = df_weather.rename(columns={"time": "datetimeWeather.local"})
        df_weather_columns = df_weather.columns.tolist()[1:]  # exclude time column

        df_weather["datetimeWeather.local"] = pd.to_datetime(
            df_weather["datetimeWeather.local"]
        ).dt.tz_localize(str(data["timezone"]))
        df_weather["datetimeWeather.utc"] = df_weather[
            "datetimeWeather.local"
        ].dt.tz_convert("UTC")
        df_weather["location_id"] = location_id

        df_weather["elevation"] = data["elevation"]
        df_weather["weather_latitude"] = data["latitude"]
        df_weather["weather_longitude"] = data["longitude"]
        df_weather["timezone"] = data["timezone"]

        df_weather = df_weather[
            ["datetimeWeather.local", "datetimeWeather.utc", "location_id"]
            + ["weather_latitude", "weather_longitude", "timezone", "elevation"]
            + df_weather_columns
        ].sort_values(by=["datetimeWeather.local"])

        return df_weather

    def save_weather_dataframe(
        self, run_id: str, location_id: int, df_weather: pd.DataFrame
    ) -> None:
        """Save the weather DataFrame to a Parquet file for the given run and location."""

        output_path = os.path.join(config.DATA_PARQUET_PATH, run_id, "openmeteo")
        os.makedirs(output_path, exist_ok=True)

        parquet_file = os.path.join(
            output_path,
            f"{run_id}_location_{location_id}_weather.raw.parquet",
        )

        df_weather.to_parquet(
            parquet_file,
            index=False,
            compression="snappy",
        )

    def download_weather_data(
        self,
        run_id: str,
        location_id: int,
        latitude: float,
        longitude: float,
        start_date: str,
        end_date: str,
    ) -> pd.DataFrame:
        """Download historical weather data from Open-Meteo API for a given location and date range."""

        all_parameters_string = ",".join(OpenMeteo_Client.FULL_PARAMETERS)

        url = "https://archive-api.open-meteo.com/v1/archive"
        params = {
            "latitude": latitude,
            "longitude": longitude,
            "start_date": start_date,
            "end_date": end_date,
            "timezone": "auto",  # important so we get local time data
            "hourly": all_parameters_string,
            # "hourly": "temperature_2m,relative_humidity_2m,is_day",  # light version for testing
            # "hourly": "temperature_2m,relative_humidity_2m,dew_point_2m,apparent_temperature,precipitation,rain,snowfall,snow_depth,shortwave_radiation,direct_radiation,diffuse_radiation,global_tilted_irradiance,direct_normal_irradiance,terrestrial_radiation,weather_code,pressure_msl,surface_pressure,cloud_cover,cloud_cover_low,cloud_cover_mid,cloud_cover_high,vapour_pressure_deficit,et0_fao_evapotranspiration,wind_speed_100m,wind_speed_10m,wind_direction_10m,wind_direction_100m,wind_gusts_10m"
        }
        data = self.request_api(url, params)

        # Transform the data into a standard DataFrame
        df_weather = self.construct_weather_dataframe(location_id, data)
        # df_weather.head()

        # Save the weather data to a Parquet file (for the run/trimester/location)
        self.save_weather_dataframe(run_id, location_id, df_weather)

        return df_weather


client = OpenMeteo_Client()


if __name__ == "__main__":
    # TESTING THE OPEN-METEO CLIENT

    SEOUL_LOCATIONS_WITH_COORDINATES = {
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

    pollutants = ["pm25 µg/m³", "co ppm", "no2 ppm", "o3 ppm", "pm10 µg/m³", "so2 ppm"]

    year = 2025

    test_location_id = 2623440
    test_location_coordinates = (  # lat, long
        SEOUL_LOCATIONS_WITH_COORDINATES[str(test_location_id)][0],
        SEOUL_LOCATIONS_WITH_COORDINATES[str(test_location_id)][1],
    )

    full_url = "https://archive-api.open-meteo.com/v1/archive?latitude=37.59374&longitude=126.949534&start_date=2025-01-01&end_date=2025-12-15&hourly=temperature_2m,relative_humidity_2m"
    url = "https://archive-api.open-meteo.com/v1/archive"
    params = {
        "latitude": test_location_coordinates[0],
        "longitude": test_location_coordinates[1],
        "start_date": f"{year}-01-01",
        "end_date": f"{year}-12-17",
        "hourly": "temperature_2m,relative_humidity_2m",
    }
    client.request_api(url, params)

    pass
