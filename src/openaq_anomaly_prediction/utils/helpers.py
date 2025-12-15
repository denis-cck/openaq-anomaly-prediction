"""
Helper utility functions.
"""

import calendar
import glob
import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Tuple

import pandas as pd

from openaq_anomaly_prediction.config import Configuration as config
from openaq_anomaly_prediction.utils.logging import ProgressLogger, logger


def get_iso_now() -> str:
    """Get the current date and time in ISO 8601 format with UTC offset."""
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def exec_time(start_time: float, decimal: int | None = None) -> float:
    """Get a string representation of the current execution time."""

    duration = (
        time.perf_counter() - start_time
        if decimal is None
        else round(time.perf_counter() - start_time, decimal)
    )
    return duration


def get_monthly_periods(year: int) -> List[Tuple[str, str]]:
    """
    Generates a list of (start_datetime, end_datetime) strings for every
    month within the given year, formatted as ISO 8601 with UTC offset.
    """

    periods = []
    for month in range(1, 13):  # 12 months
        _, last_day = calendar.monthrange(
            year, month
        )  # weekday of first day, number of days

        start_date = datetime(year, month, 1, 0, 0, 0, tzinfo=timezone.utc)
        end_date = datetime(year, month, last_day, 23, 59, 59, tzinfo=timezone.utc)

        periods.append((start_date.isoformat(), end_date.isoformat()))

    return periods


def get_trimestrial_periods(year: int) -> List[Tuple[str, str]]:
    """
    Generates a list of (start_datetime, end_datetime) strings for every
    month within the given year, formatted as ISO 8601 with UTC offset.
    """

    trimesters = []
    monthly_periods = get_monthly_periods(year)
    for i in range(0, 12, 3):
        start_month = monthly_periods[i]
        end_month = monthly_periods[i + 2]
        trimesters.append((start_month[0], end_month[1]))

    return trimesters


# def concatenate_csv_files(
#     output_file: str,
#     output_path: str | None = None,  # todo: add an optional parent path
#     input_filepaths: list[str] | None = None,
#     parent_path: str = "",
#     # input_filenames: list[str] | None = None,
#     **kwargs,
# ) -> None:
#     """Concatenate multiple CSV files into a single CSV file."""

#     OUTPUT_CSV_DIR = os.path.join(config.DATA_PATH, "csv")
#     os.makedirs(OUTPUT_CSV_DIR, exist_ok=True)

#     if output_path is not None:
#         output_path = os.path.join(output_path, output_file)
#     else:
#         output_path = os.path.join(OUTPUT_CSV_DIR, output_file)  # default

#     # 1. Gather all input file paths
#     mode = None
#     all_files = []
#     if input_filepaths is not None:
#         # all_files = [os.path.join(input_path, f) for f in csv_filenames]
#         all_files = input_filepaths
#         mode = "FILEPATHS"
#         logger.trace(f"[{mode}] {len(input_filepaths)} CSV files found from filepaths")

#     elif parent_path != "":
#         all_files = glob.glob(os.path.join(parent_path, "*.csv"))
#         mode = "PARENT_PATH"
#         logger.trace(
#             f"[{mode}] {len(all_files)} CSV files found from {parent_path}/*.csv"
#         )

#     # 2. Iterate through files and write to the output file
#     for i, csv_path in enumerate(all_files):
#         # Read the file
#         try:
#             df = pd.read_csv(csv_path)
#         except Exception as e:
#             print(f"Error reading {csv_path}: {e}. Skipping.")
#             continue  # Skip to the next file

#         if i == 0:
#             # First file: Write the header row
#             df.to_csv(output_path, mode="w", index=False, header=True)
#         else:
#             # Subsequent files: Append without the header row
#             df.to_csv(output_path, mode="a", index=False, header=False)

#     # logger.success(f"Concatenated {len(all_files)} CSV files into {output_path}")


def get_parquet_filepaths(relative_path: str) -> list[str]:
    """Get all Parquet file paths from the default Parquet data directory."""
    parquet_path = config.DATA_PARQUET_PATH
    if relative_path is not None:
        parquet_path = os.path.join(parquet_path, relative_path)
    all_files = glob.glob(os.path.join(parquet_path, "*.parquet"))
    return all_files


def parquets_to_csv(
    files: list[str], filename: str, output_path: str | Path = config.DATA_CSV_PATH
) -> None:
    """Concatenate multiple Parquet files into a single CSV file."""

    progress = ProgressLogger()
    total_files = len(files)

    output_csv_path = os.path.join(output_path, filename)  # custom output path
    os.makedirs(output_path, exist_ok=True)

    # Iterate through each file and write to the output file
    for i, file in enumerate(files):
        try:
            df = pd.read_parquet(file)
        except Exception as e:
            print(f"Error reading {file}: {e}. Skipping.")
            continue  # Skip to the next file

        if i == 0:
            # First file: Write the header row and reset file (w)
            df.to_csv(output_csv_path, mode="w", index=False, header=True)
        else:
            # Subsequent files: Append without the header row (a)
            df.to_csv(output_csv_path, mode="a", index=False, header=False)

        progress.print(
            f"Appending parquet files to CSV -> data/csv/{filename}",
            current_progress=i + 1,
            total_progress=total_files,
            prefix_msg=f"{i + 1}/{total_files}",
            last=(i + 1 == total_files),
        )


def concat_csv_to_csv(
    files: list[str], filename: str, output_path: str | Path = config.DATA_CSV_PATH
) -> None:
    """Concatenate multiple CSV files into a single CSV file."""

    output_csv_path = os.path.join(output_path, filename)  # custom output path
    os.makedirs(output_path, exist_ok=True)

    # Iterate through each file and write to the output file
    for i, file in enumerate(files):
        try:
            df = pd.read_csv(file)
        except Exception as e:
            print(f"Error reading {file}: {e}. Skipping.")
            continue  # Skip to the next file

        if i == 0:
            # First file: Write the header row
            df.to_csv(output_csv_path, mode="w", index=False, header=True)
        else:
            # Subsequent files: Append without the header row
            df.to_csv(output_csv_path, mode="a", index=False, header=False)


def _safe_serialize(obj):
    """Recursively convert objects to JSON-serializable structures."""
    # Basic types
    if isinstance(obj, (str, int, float, bool)) or obj is None:
        return obj

    # Pandas / NumPy common cases
    try:
        import numpy as np
        import pandas as pd
    except Exception:
        np = None
        pd = None

    if pd is not None and isinstance(obj, pd.DataFrame):
        # Prefer records for logs
        return obj.to_dict(orient="records")
    if pd is not None and isinstance(obj, pd.Series):
        return obj.to_dict()
    if np is not None and isinstance(obj, (np.generic,)):
        return obj.item()

    # Exceptions
    if isinstance(obj, BaseException):
        if obj.__class__.__name__ == "HTTPError" and hasattr(obj, "response"):
            return {
                "type": obj.__class__.__name__,
                "message": str(obj),
                "status_code": obj.response.status_code,
                "url": obj.response.url,
            }
        return {
            "type": obj.__class__.__name__,
            "message": str(obj),
            "args": obj.args,
        }

    # Datetime-like
    from datetime import date, datetime

    if isinstance(obj, (datetime, date)):
        try:
            return obj.isoformat()
        except Exception:
            return str(obj)

    # Path
    from pathlib import Path

    if isinstance(obj, Path):
        return str(obj)

    # dict / list / tuple / set
    if isinstance(obj, dict):
        return {str(k): _safe_serialize(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple, set)):
        return [_safe_serialize(v) for v in obj]

    # Fallback: string representation
    return str(obj)


def save_logs(logs: list, **kwargs) -> None:
    """Save results to JSON file."""

    relative_path: str = kwargs.get("relative_path", "")
    filename: str = kwargs.get("filename", "logs.json")

    output_path = os.path.join(config.LOGS_PATH, relative_path, filename)
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    # Sanitize before dump
    safe_logs = _safe_serialize(logs)

    with open(output_path, "w") as f:
        json.dump(safe_logs, f, indent=4)


# def load_config(config_path: str) -> Dict[str, Any]:
#     """Load configuration from YAML or JSON file."""
#     path = Path(config_path)

#     if path.suffix.lower() in [".yaml", ".yml"]:
#         with open(path, "r") as f:
#             return yaml.safe_load(f)
#     elif path.suffix.lower() == ".json":
#         with open(path, "r") as f:
#             return json.load(f)
#     else:
#         raise ValueError(f"Unsupported config file format: {path.suffix}")


# def save_results(results: Dict[str, Any], output_path: str) -> None:
#     """Save results to JSON file."""
#     with open(output_path, "w") as f:
#         json.dump(results, f, indent=2)
