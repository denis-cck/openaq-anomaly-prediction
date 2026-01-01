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
from shapely import length

from openaq_anomaly_prediction.config import Configuration as config  # noqa: F401
from openaq_anomaly_prediction.load.gcp import bq, gcs
from openaq_anomaly_prediction.load.schemas.openaq_locations import OpenAQLocationsTable
from openaq_anomaly_prediction.load.schemas.openaq_measurements import (
    OpenAQMeasurementsTable,
)
from openaq_anomaly_prediction.load.schemas.openaq_sensors import OpenAQSensorsTable
from openaq_anomaly_prediction.utils.helpers import (
    exec_time,
    format_duration,
    get_iso_now,
    get_parquet_filepaths,
    parquets_to_csv,
    save_logs,
)
from openaq_anomaly_prediction.utils.logger import (
    ProgressLogger,
    b,
    grey,
    hex,
    logger,
    rst,
)

ArrayConvertible = Union[list, tuple, AnyArrayLike]
Scalar = Union[float, str]
DatetimeScalar = Union[Scalar, date, np.datetime64]


class OpenAQClient:
    """OpenAQ API client wrapper."""

    def __init__(self, **kwargs) -> None:
        # KEYWORDS ARGUMENTS
        self.verbose = kwargs.get("verbose", 0)

        # API KEY
        self.apikey = config.getenv("OPENAQ_API_KEY")

        # RATE LIMIT TRACKING
        self.ratelimit_used = -1
        self.ratelimit_remaining = -1
        self.ratelimit_reset = -1

        # DATA STORAGE
        self.locations = None
        self.sensors = None

        # logger.success(f'OPENAQ: APIKEY="{config.getenv("OPENAQ_API_KEY")}"')

    def get_ratelimit_string(self) -> str:
        """Get a string representation of the current rate limit status."""
        return f"usd:{self.ratelimit_used:>2}, rem:{self.ratelimit_remaining:>2}, rst:{self.ratelimit_reset:>2}s"

    def clear_ratelimits(self) -> None:
        """Reset rate limit tracking variables."""
        self.ratelimit_used = -1
        self.ratelimit_remaining = -1
        self.ratelimit_reset = -1

    def should_wait(self) -> bool:
        """Get the current rate limit status."""
        return self.ratelimit_remaining >= 0 and self.ratelimit_remaining < 5

    def request_api(
        self, url: str, request_params: dict, **kwargs
    ) -> dict[str, pd.DataFrame]:
        """Make a request to the OpenAQ API and return the results as a DataFrame."""

        verbose = kwargs.get("verbose", self.verbose)

        request_headers = {
            "X-API-Key": self.apikey,
            "accept": "application/json",
        }

        # Check rate limits before making the request
        # if self.ratelimit_remaining >= 0 and self.ratelimit_remaining < 5:
        if self.should_wait():
            # if verbose >= 1:
            logger.trace(
                f"{hex('#dfa934')}WAIT{rst()}{grey()}: {f'Waiting for reset in {self.ratelimit_reset}s':<26} ({self.ratelimit_remaining} remaining)"
            )
            # logger.warning(f"{grey()}Approaching rate limit. Remaining requests: {self.ratelimit_remaining}. Waiting for reset in {self.ratelimit_reset} seconds.{rst()}")
            time.sleep(self.ratelimit_reset + 1)

            self.ratelimit_used = 0
            self.ratelimit_remaining = 60
            self.ratelimit_reset = 60

        # Make the Request
        try:
            start_time = time.perf_counter()

            response = requests.get(url, headers=request_headers, params=request_params)
            response.raise_for_status()  # Raises error for 4xx or 5xx

            self.ratelimit_used = int(response.headers.get("X-Ratelimit-Used", 0))
            self.ratelimit_remaining = int(
                response.headers.get("X-Ratelimit-Remaining", 0)
            )
            self.ratelimit_reset = int(response.headers.get("X-Ratelimit-Reset", 0))

            # # ---------------------------------------------------------------------
            # # Randomly simulate HTTPError for testing purposes and add a status code
            # if random.random() < 0.01:  # 1% chance to simulate an error
            #     raise requests.exceptions.HTTPError(
            #         "Simulated HTTP error for testing.", response=response
            #     )
            # # ---------------------------------------------------------------------

            if verbose >= 5:
                logger.trace(
                    f"RATE LIMITS: {self.ratelimit_used} used (remaining: {self.ratelimit_remaining}, reset in: {self.ratelimit_reset}s)"
                )

            # 4. Process Data
            data = response.json()

            data["meta"]["request_duration"] = exec_time(start_time)

            # Flatten results into a DataFrame
            results = pd.json_normalize(data["results"])

            # # Convert columns with "datetime" in their names to datetime types
            # datetime_columns = [col for col in results.columns if "datetime" in col]
            # for col in datetime_columns:
            #     if "local" in col:
            #         results[col] = pd.to_datetime(
            #             results[col], utc=False
            #         ).dt.tz_localize(None)  # remove timezone info (naive datetime)
            #     else:
            #         results[col] = pd.to_datetime(results[col], utc=True)

            return {"meta": data["meta"], "results": results}

        except requests.exceptions.HTTPError as err:
            # TODO: just test for string "429 Too Many Requests" directly in the error message (kinda broken but eh)
            # Rate limit handling: wait for reset if 429 Too Many Requests (if spamming 400/500 errors)
            if err.response.status_code == 429 or "429 Client Error" in str(err):
                print()
                logger.warning(
                    f"{hex('#dfa934')}[{err.response.status_code}] RATE LIMIT{rst()}{grey()}: Hit rate limit. Waiting for 60s...{rst()}"
                )
                time.sleep(60)  # Wait for 60 seconds before retrying

            # print(f"HTTP error occurred: {err}")
            # print(response.text)  # Print the error message from OpenAQ
            raise err


# ============================================================================


class AreaDownloader:
    SAVE_TO_GCS = True
    SAVE_TO_DISK = False

    def __init__(self, **kwargs) -> None:
        self.area_id = kwargs.get("area_id", "unknown_area_id")
        self.area_name = kwargs.get("area_name", "unknown_area_name")
        self.verbose = kwargs.get("verbose", 0)

        self.locations: pd.DataFrame = None
        self.sensors: pd.DataFrame = None

    # ---------------------------------------------------------------------
    # STATIC FUNCTIONS

    @staticmethod
    def standardized_measurements_sorting(measurements: pd.DataFrame) -> None:
        """Standardize the sorting of measurements DataFrame."""

        # This is happening BEFORE any cleaning, so the columns still have dots
        sorting_columns = ["period.datetimeTo.local", "sensor_id"]

        if not all(col in measurements.columns for col in sorting_columns):
            raise ValueError(
                f"Measurements DataFrame must contain the following columns for sorting: {sorting_columns}"
            )

        measurements.sort_values(
            by=["period.datetimeTo.local", "sensor_id"],
            ascending=True,
            ignore_index=True,
            inplace=True,
        )

    @staticmethod
    def print_period_logs(
        all_period_logs: list[dict[str, Any]], show_errors: bool = False
    ) -> None:
        # TODO: Fix errors:
        # Traceback (most recent call last):
        # File "/home/deniscck/code/denis-cck/openaq_anomaly_prediction/openaq_download.py", line 82, in <module>
        #     AreaDownloader.print_period_logs(all_logs, True)
        # File "/home/deniscck/code/denis-cck/openaq_anomaly_prediction/src/openaq_anomaly_prediction/load/openaq.py", line 187, in print_period_logs
        #     if period["status"] == "completed":
        #     ~~~~~~^^^^^^^^^^
        # TypeError: list indices must be integers or slices, not str

        for i, period in enumerate(all_period_logs):
            if i == 0:
                title = "DOWNLOADS SUMMARY"
                from_run_id = f"From RUN_ID: {all_period_logs[0]['run_id']}"
                to_run_id = f"To RUN_ID: {all_period_logs[-1]['run_id']}"
                max_length = max(len(from_run_id), len(to_run_id), len(title)) + 3

                print()
                logger.info(f"{'=' * max_length}")
                logger.info(title)
                logger.trace(from_run_id)
                logger.trace(to_run_id)
                print()

            if period["status"] == "completed":
                success_msg = f"[COMPLETED] Run completed in {format_duration(period['total_duration'])}"
                logger.success(success_msg)

            elif period["status"] == "aborted":
                aborted_msg = f"[ABORTED] Run cancelled after {format_duration(period['total_duration'])}"
                logger.error(aborted_msg)

            logger.trace(f"RUN_ID: {period['run_id']}")
            logger.trace(
                f"From: {period['datetime_from']}, to: {period['datetime_to']}"
            )
            logger.trace(
                f"Fetched data from {len(period['sensors'])} sensors ({period['errors']} errors and {period['retries']} retries)"
            )

            if AreaDownloader.SAVE_TO_GCS:
                logger.trace(
                    f"Saved all files in staging bucket in {period['gcs_saving_duration']:.2f}s"
                )

            if AreaDownloader.SAVE_TO_DISK:
                logger.trace(
                    f"Saved {period['saved']} parquet files in {period['disk_saving_duration']:.2f}s"
                )

            if show_errors:
                for run in period["logs"]:
                    if len(run["errors"]) > 0:
                        logger.warning(
                            f"> {run['status'].upper()} ({run['retries'] + 1}): {len(run['errors'])} error{'s' if len(run['errors']) != 1 else ''} in RUN_ID [{run['run_id']}]"
                        )
                        for error in run["errors"]:
                            logger.trace(
                                f"    [{error['type']}] sensor_id={error['sensor_id']}"
                            )

            # if period["status"] == "aborted":
            #     logger.error("=> [ABORTED]")

            # elif period["status"] == "completed":
            #     logger.success("=> [COMPLETED]")

            print()

    # ---------------------------------------------------------------------
    # PUBLIC METHODS

    def load_bbox(self, x1: float, y1: float, x2: float, y2: float) -> None:
        """Load locations and sensors within a bounding box defined by (x1, y1) and (x2, y2)."""

        res = client.request_api(
            "https://api.openaq.org/v3/locations",
            {
                "bbox": f"{x1},{y1},{x2},{y2}",
                "limit": 1000,
                "page": 1,
            },
        )
        results = res["results"]

        # print(results.head())

        # Get a full list of locations for the area
        self.locations = OpenAQLocationsTable().save_dataframe(
            res["results"], skip_bq=False
        )

        # Get a full list of sensors for the area (flatten "parameters")
        def _flatten_sensors(row):
            flattened = [
                {
                    "id": sensor.get("id"),
                    "location_id": row.get("id"),
                    "name": sensor.get("name"),
                    "parameter.id": sensor.get("parameter", {}).get("id"),
                    "parameter.name": sensor.get("parameter", {}).get("name"),
                    "parameter.units": sensor.get("parameter", {}).get("units"),
                    "parameter.displayName": sensor.get("parameter", {}).get(
                        "displayName"
                    ),
                }
                for sensor in row.get("sensors", [])
            ]
            return flattened

        results["sensors_flat"] = results.apply(_flatten_sensors, axis=1)
        df_sensors_exploded = pd.DataFrame(results["sensors_flat"].explode().tolist())

        self.sensors = OpenAQSensorsTable().save_dataframe(
            df_sensors_exploded, skip_bq=False
        )

        # Get a full list of instruments for the area
        # df_instruments_exploded = results["instruments"].explode()
        # self.instruments = pd.DataFrame(df_instruments_exploded.tolist())

    def get_sensors_with_dates(
        self, from_date: DatetimeScalar = "", to_date: DatetimeScalar = "", **kwargs
    ) -> pd.DataFrame:
        """Get sensors with their location's datetimeFirst_utc and datetimeLast_utc with an optional date filter."""

        verbose = kwargs.get("verbose", self.verbose)

        if self.locations is None or self.sensors is None:
            raise ValueError(
                "Locations and sensors data must be loaded before filtering."
            )

        # Filter locations before the JOIN to reduce the data size
        filtered_locations = self.locations[
            ["id", "datetimeFirst_utc", "datetimeLast_utc"]
        ]
        mask_filter = pd.Series(True, index=filtered_locations.index)

        if verbose >= 5:
            logger.trace(f"Locations found before filters: {mask_filter.sum()}")

        if from_date != "":
            mask_filter = mask_filter & (
                filtered_locations["datetimeLast_utc"]
                >= pd.to_datetime(from_date, utc=True)
            )
        if to_date != "":
            mask_filter = mask_filter & (
                filtered_locations["datetimeFirst_utc"]
                <= pd.to_datetime(to_date, utc=True)
            )

        if verbose >= 5:
            logger.trace(f"Locations found after filters: {mask_filter.sum()}")

        # Join sensors with filtered locations: only add "datetimeFirst_utc" and "datetimeLast_utc" columns
        sensors_with_dates = self.sensors[
            self.sensors["location_id"].isin(filtered_locations[mask_filter]["id"])
        ].join(
            filtered_locations.set_index("id"),
            on="location_id",
            how="left",
        )

        if verbose >= 5:
            logger.trace(f"Total number of sensors found: {len(sensors_with_dates)}")

        return sensors_with_dates

    def fetch_sensor_measurements(
        self,
        sensor_id: int,
        datetime_from: str | None = None,
        datetime_to: str | None = None,
        **kwargs,
    ) -> pd.DataFrame:
        """Get measurements for a specific sensor within a date range."""
        # Exemple: {'name': 'openaq-api', 'website': '/', 'page': 1, 'limit': 1000, 'found': 0}
        verbose = kwargs.get("verbose", self.verbose)
        inline_progress = kwargs.get("inline_progress", False)
        run_id = kwargs.get("run_id", "run")
        prefix_msg = kwargs.get("prefix_msg", None)
        suffix_msg = kwargs.get("suffix_msg", None)
        max_sensor_length = kwargs.get("max_sensor_length", 0)

        suffix_msg = "" if suffix_msg is None else suffix_msg + " "  # default
        inline_sensor_str = f"[sensor_id={sensor_id}]"

        progress = ProgressLogger()

        # 1. Start message (verbose 2+, 5+ or inline progress)
        start_message = (
            f"{'Fetching measurements...':<32} {suffix_msg}{inline_sensor_str}"
        )
        if verbose >= 2 and not inline_progress:
            logger.info(start_message)

        if verbose >= 5 and inline_progress:
            max_sensor_length += 12  # extra space for "[sensor_id=...]"

            progress.print(start_message, prefix_msg=prefix_msg)

        start_time = time.perf_counter()

        # 1.1. EXIT: Exlude banned sensors
        # Doesn't work because a sensor can be bad only for a specific period
        # if sensor_id in config.get_excluded_sensors():
        #     if verbose >= 5:
        #         progress_update = ""
        #         progress.print(
        #             f"{hex('#c1372e')}EXCL{rst() + grey()}  {'Excluded sensor':<26} {suffix_msg}{inline_sensor_str:<{max_sensor_length}} |",
        #             # f"----  {'Excluded sensor':<26} {suffix_msg}{inline_sensor_str:<{max_sensor_length}} | {'EXCLUDED SENSOR':<12}",
        #             prefix_msg=prefix_msg,
        #             suffix_msg=suffix_msg,
        #             last=True,
        #         )
        #     elif verbose >= 1:
        #         logger.success(f"Excluded sensor (sensor_id={sensor_id})")
        #     return pd.DataFrame()

        # 2. Start LOOP to fetch all pages
        page = 1
        found_value = -1  # -1 = first time, 0 = no results, >0 = number of results
        all_measurements = pd.DataFrame()
        all_results = []
        while True:
            if client.should_wait() and verbose >= 5:
                print()  # Move to a new line before waiting

            # 3. Make API Request
            res = client.request_api(
                f"https://api.openaq.org/v3/sensors/{sensor_id}/measurements/hourly",
                {
                    "datetime_from": datetime_from,
                    "datetime_to": datetime_to,
                    "limit": 1000,
                    "page": page,
                },
            )
            # req_duration = res["meta"]["request_duration"]
            req_duration = exec_time(start_time)  # overall duration
            req_message = ProgressLogger.time_gradient(
                f"{req_duration:.2f}s", req_duration, 20.0
            )

            # Process 'found' value the first request only
            if found_value == -1:
                # If found is a Series, get the first value
                found_value = res["meta"]["found"]
                if isinstance(found_value, pd.Series):
                    found_value = found_value.iloc[0]

                # 3.1 EXIT: if found == 0 for the first request, exit
                if found_value == 0 and page == 1:
                    # 6.1 EXIT: No measurements found
                    if verbose >= 5:
                        progress_update = ""
                        progress.print(
                            f"----  {'none':<26} {suffix_msg}{inline_sensor_str:<{max_sensor_length}} | {req_message}: -> {client.get_ratelimit_string()}",
                            prefix_msg=prefix_msg,
                            suffix_msg=suffix_msg,
                            last=True,
                        )
                    elif verbose >= 1:
                        logger.success(
                            f"Retrieved 0 measurement in {exec_time(start_time, fmt=True)} (sensor_id={sensor_id})"
                        )
                    break

            # 4. Save results (append to all_measurements)
            # res["results"]["ingested_at"] = get_iso_now()  # add [ingested_at] DONE IN MEASUREMENTS SCHEMA

            reordered_columns = ["sensor_id"] + res["results"].columns.tolist()
            res["results"]["sensor_id"] = sensor_id  # add [sensor_id]

            results = res["results"][reordered_columns]

            # Not great but whatever
            if len(results) == 0:
                logger.trace(
                    f"No results for sensor_id={sensor_id} in the given period (triggered warning on page {page})."
                )
            else:
                all_results.extend(results.to_dict(orient="records"))

            # FutureWarning: The behavior of DataFrame concatenation with empty or all-NA entries is deprecated.
            # In a future version, this will no longer exclude empty or all-NA columns when determining the result dtypes.
            # To retain the old behavior, exclude the relevant entries before the concat operation.
            # all_measurements = pd.concat([all_measurements, results])

            if verbose >= 5:
                progress_update = f"{len(all_results)}/{found_value} measurements"
                if len(all_results) == found_value:  # last step
                    message = f"{progress_update:<26} {suffix_msg}{inline_sensor_str:<{max_sensor_length}} | {req_message}: -> {client.get_ratelimit_string()}"
                    last = True
                    # DISABLE FOR GCS STREAMING
                    last = False
                else:
                    message = f"{progress_update:<26} {suffix_msg}{inline_sensor_str:<{max_sensor_length}} | {req_message}: -> {client.get_ratelimit_string()}"
                    last = False

                progress.print(
                    message,
                    current_progress=len(all_results),
                    total_progress=found_value,
                    prefix_msg=prefix_msg,
                    last=last,
                )

            # 5. EXIT: All measurements retrieved
            if len(all_results) >= found_value:
                if verbose >= 1 and not inline_progress:
                    # if verbose >= 5:
                    #     print()
                    # if not inline_progress:
                    measurements_msg = (
                        "0" if found_value == 0 else f"{len(all_results)}/{found_value}"
                    )
                    logger.success(
                        f"Retrieved {measurements_msg} measurements in {exec_time(start_time, fmt=True)} (sensor_id={sensor_id})"
                    )
                break

            page += 1

        if len(all_results) > 0:
            all_measurements = pd.DataFrame(all_results)
            AreaDownloader.standardized_measurements_sorting(all_measurements)

            parquet_filename = f"{run_id}_sensor_{sensor_id}.raw.parquet"
            final_message = message

            # Stream dataframe to GCS (staging bucket)
            if AreaDownloader.SAVE_TO_GCS:
                gcs_time = time.perf_counter()

                OpenAQMeasurementsTable().save_dataframe_to_gcs(
                    all_measurements,
                    "staging",
                    f"openaq/measurements/{parquet_filename}",
                )
                # gcs.stream_dataframe_to_gcs(
                #     all_measurements, "staging", f"openaq/{parquet_filename}"
                # )

                final_message += (
                    f" | {exec_time(gcs_time, fmt=True)} -> streamed to GCS"
                )

            # Save to Parquet files
            if AreaDownloader.SAVE_TO_DISK:
                output_path = os.path.join(config.DATA_PARQUET_PATH, run_id)
                parquet_file = os.path.join(output_path, parquet_filename)
                all_measurements.to_parquet(
                    parquet_file,
                    index=False,
                    compression="snappy",
                )

                final_message += " | saved to disk"

            progress.print(
                final_message,
                current_progress=1,
                total_progress=1,
                prefix_msg=prefix_msg,
                last=True,
            )

        return all_measurements

    def download_sensors_data(
        self,
        sensors_id: list[int],
        datetime_from: str | None = None,
        datetime_to: str | None = None,
        run_id: str = "default_run",
        **kwargs,
    ) -> dict:
        """Download all measurements for all sensors in the area between datetime_from and datetime_to."""

        verbose = kwargs.get("verbose", self.verbose)
        prefix_msg = kwargs.get("prefix_msg", None)
        suffix_msg = kwargs.get("suffix_msg", None)

        total = len(sensors_id)

        # Logging
        if verbose >= 4:
            logger.debug(f"[START] Downloading data for {total} sensors...")

        if verbose >= 5:
            logger.trace(f"RUN_ID: {run_id}")
            logger.trace(f"From: {datetime_from}, to: {datetime_to}")
            # logger.trace(f"Sensors to download: {total}")
            # print()

        start_time = time.perf_counter()

        # 1. Create output directories for the run (1 per sensor)
        if AreaDownloader.SAVE_TO_DISK:
            output_path = os.path.join(config.DATA_PARQUET_PATH, run_id)
            os.makedirs(output_path, exist_ok=True)

        saved = []
        errors = []
        total = len(sensors_id)
        max_sensor_length = len(str(max(sensors_id)))
        for i, sensor_id in enumerate(sensors_id):
            try:
                max_progress_length = len(str(total))
                progress_msg = f"{i + 1:>{max_progress_length}}/{total}"
                # 1. Fetch measurements for the sensor
                df_measurements = self.fetch_sensor_measurements(
                    sensor_id,
                    datetime_from=datetime_from,
                    datetime_to=datetime_to,
                    run_id=run_id,
                    prefix_msg=progress_msg if prefix_msg is None else prefix_msg,
                    suffix_msg="" if suffix_msg is None else suffix_msg,
                    # suffix_msg=progress_msg if suffix_msg is None else suffix_msg,
                    # suffix_msg=f"{progress_msg:>{len(str(total)) * 2 + 1}}"
                    # if suffix_msg is None
                    # else suffix_msg,
                    max_sensor_length=max_sensor_length,  # for alignement
                    inline_progress=True,
                    verbose=5,
                )

                if len(df_measurements) > 0:
                    parquet_filename = f"{run_id}_sensor_{sensor_id}.raw.parquet"
                    saved.append(parquet_filename)

            except requests.exceptions.HTTPError as err:
                print()
                logger.error(f"[SENSOR_ID={sensor_id}]: {str(err)}")
                errors.append(
                    {
                        "run_id": run_id,
                        "sensor_id": int(sensor_id),
                        "datetime_from": datetime_from,
                        "datetime_to": datetime_to,
                        "type": f"HTTPError{err.response.status_code}",
                        "error": err,
                    }
                )

            except Exception as e:
                print()
                logger.error(f"[SENSOR_ID={sensor_id}]: {e}")
                errors.append(
                    {
                        "run_id": run_id,
                        "sensor_id": int(sensor_id),
                        "datetime_from": datetime_from,
                        "datetime_to": datetime_to,
                        "type": "Exception",
                        "error": e,
                    }
                )
        if verbose >= 4:
            logger.debug(
                f"[OPENAQ] Downloaded data for {total - len(errors)}/{total} sensors in {exec_time(start_time, fmt=True)}"
            )

        if len(errors) > 0:
            logger.warning(
                f"[ERROR={len(errors)}] There were some errors during download."
            )
            # pprint(errors)

        return {"total": total, "saved": saved, "errors": errors}

    def download_data_with_retries(
        self,
        sensors_id: list[int],
        datetime_from: str,
        datetime_to: str,
        run_id: str,
        **kwargs,
    ) -> list:
        """
        Recursively downloads data for all sensors in the area between datetime_from and datetime_to until no errors remain.

        If ran again with the same run_id, it will overwrite existing files and refresh the trimester CSV.

        Returns a dictionary with the download logs including the saved parquet files and the errors.
        """

        verbose = kwargs.get("verbose", 5)

        retries = kwargs.get("retries", 0)
        max_retries = kwargs.get("max_retries", 5)

        prefix_msg = kwargs.get("prefix_msg", None)
        suffix_msg = kwargs.get("suffix_msg", None)

        # Overwrite the prefix message to indicate retries
        prefix_msg = f"({retries})" if retries > 0 else prefix_msg

        start_time = time.perf_counter()
        start_logs_datetime = get_iso_now()

        # --------------------------------------------------------------------------------------------
        # 1. DOWNLOAD the measurements for each sensor

        logs = []
        run_logs = self.download_sensors_data(
            sensors_id,
            datetime_from=datetime_from,
            datetime_to=datetime_to,
            run_id=run_id,
            verbose=5,
            prefix_msg=prefix_msg,
            suffix_msg=suffix_msg,
        )
        logs.append(
            {
                "run_id": run_id,
                "status": "downloaded",
                # "datetime": start_logs_datetime,
                "run_start": start_logs_datetime,
                "run_end": get_iso_now(),
                "download_duration": exec_time(start_time, 2),
                "saved": run_logs["saved"],
                "errors": run_logs["errors"],
                "retries": retries,
            }
        )

        # --------------------------------------------------------------------------------------------
        # 2. CHECK for errors and retry if needed

        retry_logs: list = []
        if len(run_logs["errors"]) > 0:
            # Abort if there are still errors after max retries
            if retries >= max_retries:
                logs[-1]["status"] = "aborted"  # Update the last log status to aborted
                logger.error(
                    f"[ABORT] Maximum retries reached ({max_retries}) for RUN_ID [{run_id}]"
                )
                # Break recursion, climb back to the first call

            # Retry downloading the sensors that had errors (recursion)
            else:
                logs[-1]["status"] = "retrying"  # Update the last log status to aborted

                logger.warning(
                    f"[RETRY={retries + 1}] RUN_ID [{run_id}] with {len(run_logs['errors'])} error{'s' if len(run_logs['errors']) != 1 else ''}..."
                )
                # print()

                sensors_to_retry = []
                for error in run_logs["errors"]:
                    sensors_to_retry.append(error["sensor_id"])

                retry_logs = self.download_data_with_retries(
                    sensors_id=sensors_to_retry,
                    datetime_from=datetime_from,
                    datetime_to=datetime_to,
                    run_id=run_id,
                    verbose=verbose,
                    prefix_msg=prefix_msg,
                    suffix_msg=suffix_msg,
                    retries=retries + 1,
                    max_retries=max_retries,
                )

        concatenated_logs: list = logs + retry_logs if len(retry_logs) > 0 else logs

        # --------------------------------------------------------------------------------------------
        # 3. EXIT: Last loop

        if retries == 0:
            total_errors = 0
            total_saved = 0
            total_retries = 0
            for run in concatenated_logs:
                total_errors += len(run["errors"])
                total_saved += len(run["saved"])
                if run["status"] == "retrying":
                    total_retries += 1

            # logger.info(f"[DOWNLOADED] Completed all downloads in {exec_time(start_time, fmt=True)}")
            # print()

            run_status = (
                "downloaded"
                if concatenated_logs[-1]["status"] != "aborted"
                else "aborted"
            )
            return [
                {
                    "run_id": run_id,
                    "status": run_status,
                    "run_start": start_logs_datetime,
                    "run_end": get_iso_now(),  # will be refreshed after saving
                    "datetime_from": datetime_from,
                    "datetime_to": datetime_to,
                    "download_duration": exec_time(start_time, 2),
                    "saving_duration": 0,  # to be filled later
                    "total_duration": 0,  # to be filled later
                    "errors": total_errors,
                    "saved": total_saved,
                    # "retries": len(concatenated_logs) - 1,
                    "retries": total_retries,
                    "sensors": sensors_id,
                    "logs": concatenated_logs,
                }
            ]

        return concatenated_logs

    def download_period_from_area(
        self, datetime_from: str, datetime_to: str, **kwargs
    ) -> list | dict:
        """Download data for a specific period from the area."""

        # Check if locations and sensors are loaded
        if self.locations is None or self.sensors is None:
            raise ValueError(
                "Locations and sensors data must be loaded before downloading period data."
            )

        # Overwrite parameters (if provided)
        sensors_id = kwargs.get("sensors_id", None)
        run_id = kwargs.get("run_id", None)
        max_retries = kwargs.get("max_retries", 5)

        # Customize logs
        # year = kwargs.get("year", 2023)
        run_id_prefix = kwargs.get("run_id_prefix", f"{self.area_id}_measurements")
        run_label = kwargs.get("run_label", None)

        start_time = time.perf_counter()

        logger.info(
            f"[{self.area_id.upper()}][{run_label}] Fetching all measurements..."
        )

        # FILTER the sensors with valid locations (with valid datetimeFirst_utc and datetimeLast_utc)
        if sensors_id is None:
            filtered_sensors = self.get_sensors_with_dates(
                from_date=datetime_from,
                to_date=datetime_to,
            )
            sensors_id = (
                filtered_sensors["id"].unique().tolist()
            )  # get unique sensor IDs

            # # TEMP: limit to first 5 sensors for testing
            # print()
            # logger.warning("TESTING: limiting to first 3 sensors only")
            # print()
            # sensors_id = sensors_id[:3]

        if len(sensors_id) == 0:
            logger.warning(
                f"[NO SENSORS] No sensors found between {datetime_from} and {datetime_to}."
            )
            print()
            return []

        # CREATE Run_ID from area_id, date range
        if run_id is None:  # or force a run_id (customization or overwriting)
            datetime_from_str = dt.fromisoformat(datetime_from).strftime("%Y-%m-%d")
            datetime_to_str = dt.fromisoformat(datetime_to).strftime("%Y-%m-%d")
            run_id = f"{run_id_prefix}_{datetime_from_str}_{datetime_to_str}"

        # DOWNLOAD PERIOD: Download all measurements for the sensors in the area for the given period
        period_logs = self.download_data_with_retries(
            sensors_id,
            datetime_from=datetime_from,
            datetime_to=datetime_to,
            run_id=run_id,
            # prefix_msg=f"T1/{year}",
            suffix_msg=run_label,
            verbose=5,
            max_retries=max_retries,
        )[0]  # when it's last loop, we get a dict

        if period_logs["status"] == "aborted":
            # Do stuff.
            print()
            pass

        # --------------------------------------------------------------------------------------------
        # SAVE PERIOD DATA:

        # elif period_logs["status"] == "downloaded":

        if AreaDownloader.SAVE_TO_GCS:
            save_start_time = time.perf_counter()

            logger.trace(
                f"GCS/BIGQUERY: saving all files in staging bucket for RUN_ID [{run_id}]..."
            )

            # Trigger the load to Big Query from GCS Parquet files
            OpenAQMeasurementsTable().save_from_staging_bucket()

            period_logs["gcs_saving_duration"] = exec_time(save_start_time, 2)
            logger.debug(
                f"[GCS/BIGQUERY] Upserted all measurements from GCS to BigQuery in {exec_time(save_start_time, fmt=True)}"
            )

        # Generate the period file from all downloaded Parquet files for the run_id.
        if AreaDownloader.SAVE_TO_DISK:
            save_start_time = time.perf_counter()

            logger.trace(
                f"DISK: creating a consolidated file on disk for RUN_ID [{run_id}]..."
            )

            # Retrieve all Parquet files for the run manually
            parquet_files = get_parquet_filepaths(run_id)
            logger.trace(
                f"Found {len(parquet_files)} parquet files in data/parquet/{run_id}/*.raw.parquet"
            )

            # Save the period CSV file
            # TODO: should switch to parquet files with DuckDB but now we'll be using GCS for 99.9% of the time so whatever
            parquets_to_csv(parquet_files, f"{run_id}.raw.csv", config.DATA_CSV_PATH)

            period_logs["disk_saving_duration"] = exec_time(save_start_time, 2)
            logger.debug(
                f"[DISK] Created data/csv/{run_id}.raw.csv from {len(parquet_files)} parquet files in {exec_time(save_start_time, fmt=True)}"
            )

        # --------------------------------------------------------------------------------------------
        # SAVE LOGS: Save period logs in log files

        # Cleanup and finalize logs
        if period_logs["status"] == "aborted":
            aborted_msg = f"[PARTIAL] Retrieved measurements with errors for [{self.area_id.upper()}][{run_label}] in {exec_time(start_time, fmt=True)}"
            logger.warning(aborted_msg)
            # print()
        else:
            period_logs["status"] = "completed"
            success_msg = f"[FINISHED] Retrieved all measurements for [{self.area_id.upper()}][{run_label}] in {exec_time(start_time, fmt=True)}"
            logger.success(success_msg)

        print()

        period_logs["total_duration"] = exec_time(start_time, 2)
        period_logs["run_end"] = get_iso_now()

        # Save logs to files (JSON): data/logs/{run_id}/{run_id}_{timestamp}.json
        save_logs(
            period_logs,
            # relative_path=run_id,
            filename=f"{run_id}_{int(time.time())}.json",
        )

        return period_logs

    def get_clean_measurements(self, df: pd.DataFrame) -> pd.DataFrame:
        # ---------------------------------------------------------------------
        # CLEAN MEASUREMENTS DATAFRAME
        # print(f"{'-' * 44}\nCLEANING [MEASUREMENTS] DATAFRAME:")
        clean_measurements = df[
            [
                "sensor_id",
                "value",
                "parameter.id",
                "parameter.name",
                "parameter.units",
                "period.datetimeFrom.local",
                "period.datetimeTo.local",
                "period.datetimeFrom.utc",
                "period.datetimeTo.utc",
                "coverage.expectedCount",
                "coverage.observedCount",
            ]
        ]
        # display(clean_measurements.head(1))

        # CLEAN SENSORS DATAFRAME
        # print(f"\n{'-' * 44}\nCLEANING [SENSORS] DATAFRAME:")
        clean_sensors = self.sensors[
            [
                "location_id",
                "id",
                "name",
                "parameter.displayName",
            ]
        ]
        # display(clean_sensors.head(1))

        # CLEAN LOCATIONS DATAFRAME
        # print(f"\n{'-' * 44}\nCLEANING [LOCATIONS] DATAFRAME:")
        clean_locations = self.locations[
            [
                "id",
                "name",
                "isMobile",
                "isMonitor",  # Maybe it's whether it's recognized as an "official" monitoring station or not?
                "country.id",
                "country.code",
                "country.name",
                "owner.id",
                "owner.name",
                "provider.id",
                "provider.name",
                "coordinates.latitude",
                "coordinates.longitude",
                # KEEP BUT DON'T NEED FOR MEASUREMENTS
                # "datetimeFirst.local",
                # "datetimeLast.local",
                "datetimeFirst_utc",
                "datetimeLast_utc",
                # CUSTOM FIELDS
                # "sensors_flat",  # custom field added in AreaDownloader
                # "instruments_flat",  # TODO: custom field added in AreaDownloader.
                #    Not very standardized (sometimes duplicates) and no way of linking it to sensors/measurements.
                #    You can only know which instruments are used in a location, but not which instrument is used for which sensor/parameter.
                #    So for now we just keep it for reference but don't use it.
                # EMPTY IN NEW DELHI
                # "locality",  # TODO: ???: 106/107 empty in New Delhi
                # "bounds",  # TODO: ???: all locations have fixed coordinates (no bounds just a point)
                # "distance",  # TODO: ???: fully empty in New Delhi
                # "licenses",  # TODO: ???: Vast majority of locations have NaN here, but there are some. Even then is that really useful? IDK just drop it
                # DON'T KEEP
                # "instruments",
                # "sensors",
                # "timezone",  # all the same usually for a city-sized area, and doesn't really influence the measurements themselves
                # "datetimeFirst",  # NaT (not a time) for all locations in New Delhi
                # "datetimeLast",  # NaT (not a time) for all locations in New Delhi
            ]
        ].rename(
            columns={
                "id": "location_id",
                "name": "location_name",
                "datetimeFirst_utc": "location.datetimeFirst_utc",
                "datetimeLast_utc": "location.datetimeLast_utc",
            }
        )

        # ---------------------------------------------------------------------
        # JOIN THE MEASUREMENTS WITH THE SENSORS ON SENSOR_ID
        df_joined = clean_measurements.join(
            clean_sensors.set_index("id"), on="sensor_id", how="left"
        )

        # JOIN THE PREVIOUS RESULT WITH THE LOCATIONS ON LOCATION_ID
        df_final = df_joined.join(
            clean_locations.set_index("location_id"), on="location_id", how="left"
        )

        # JOIN THE PREVIOUS RESULT WITH THE TEMPERATURES ON SENSOR_ID AND DATETIME
        # TODO: Yep.

        # REORDER COLUMNS
        ordered_columns = [
            "location_id",
            "sensor_id",
            "name",
            "value",
            "parameter.id",
            "parameter.name",
            "parameter.units",
            "parameter.displayName",
            "period.datetimeFrom.local",
            "period.datetimeTo.local",
            "period.datetimeFrom.utc",
            "period.datetimeTo.utc",
            "location.datetimeFirst_utc",
            "location.datetimeLast_utc",
            "coordinates.latitude",
            "coordinates.longitude",
            "location_name",
            "isMobile",
            "isMonitor",
            "country.id",
            "country.code",
            "country.name",
            "owner.id",
            "owner.name",
            "provider.id",
            "provider.name",
            "coverage.expectedCount",
            "coverage.observedCount",
        ]
        df_final = df_final[ordered_columns]

        # Convert columns with "datetime" in their names to datetime types
        datetime_columns = [col for col in df_final.columns if "datetime" in col]
        for col in datetime_columns:
            df_final[col] = pd.to_datetime(df_final[col])

        # print(f"Final measurements dataframe memory usage: {df_final.memory_usage(index=True, deep=True).sum() / 1024 ** 2:.2f} MB")

        return df_final


# ============================================================================


client = OpenAQClient()

if __name__ == "__main__":
    logger.info(
        f"This module is intended to be imported, not run directly: {__file__}\n"
    )
    pass
