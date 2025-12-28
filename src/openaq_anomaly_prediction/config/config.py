"""Module for dynamic configuration variables of the project."""

import os
from pathlib import Path

from dotenv import load_dotenv

_ROOT_PATH = Path(__file__).parent.parent.parent.parent
load_dotenv(dotenv_path=_ROOT_PATH / "secrets" / ".env")
# print(f"Config loaded from {_ROOT_PATH / 'secrets' / '.env'}: {os.environ}")

# TODO: Get rid of this abomination, use OAuth instead
# Override GOOGLE_APPLICATION_CREDENTIALS to use the correct path in notebooks
_SECRETS_PATH = _ROOT_PATH / "secrets"
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = str(
    (_SECRETS_PATH / "openaq-anomaly-prediction-cf3c3c643a9f.json").absolute()
)


class Configuration:
    # Root path
    ROOT_PATH = _ROOT_PATH

    # Data paths
    DATA_PATH = ROOT_PATH / "data"
    DATA_CSV_PATH = DATA_PATH / "csv"
    DATA_PARQUET_PATH = DATA_PATH / "parquet"
    DATA_EXPORT_PATH = DATA_PATH / "export"

    os.makedirs(DATA_PATH, exist_ok=True)
    os.makedirs(DATA_CSV_PATH, exist_ok=True)
    os.makedirs(DATA_PARQUET_PATH, exist_ok=True)
    os.makedirs(DATA_EXPORT_PATH, exist_ok=True)

    # Logs path
    LOGS_PATH = ROOT_PATH / "logs"
    os.makedirs(LOGS_PATH, exist_ok=True)

    # BigQuery schemas
    BG_SCHEMA_PATH = (
        ROOT_PATH / "src" / "openaq_anomaly_prediction" / "load" / "schemas"
    )

    def __init__(self) -> None:
        print(f"BigQuery schemas path: {Configuration.BG_SCHEMA_PATH}")
        print(
            f"BigQuery GOOGLE_APPLICATION_CREDENTIALS: {Configuration.getenv('GOOGLE_APPLICATION_CREDENTIALS')}"
        )
        pass

    # TODO: Load the environment variables in the class
    @staticmethod
    def getenv(var_name: str) -> str:
        """Get environment variable by name."""
        load_dotenv(dotenv_path=_ROOT_PATH / "secrets" / ".env")
        return os.environ.get(var_name, "default")

    # Doesn't work because a sensor can be bad only for a specific period
    @staticmethod
    def get_excluded_sensors() -> list[int]:
        """Get the list of excluded sensor IDs."""
        SENSOR_BANNED_LIST = []  # List of sensor IDs to exclude from download

        # paris_2023_T4_2023-10-01_2023-12-31.json
        SENSOR_BANNED_LIST += [9593, 4275130, 9680]

        # paris_2025_T4_2025-10-01_2025-12-31.json
        SENSOR_BANNED_LIST += [
            5620,
            5588,
            9542,
            9539,
            9643,
            9736,
            9610,
            9716,
            9609,
            9590,
            9589,
            9680,
            9625,
            4661655,
            9614,
            9637,
            9863,
            15298,
            9647,
            9646,
            9652,
            9671,
            9672,
            9693,
            9729,
            9747,
            9778,
            9777,
            9779,
            9782,
            9850,
            9851,
            9860,
            9861,
            7773042,
            7773850,
            7773420,
            7775825,
            10124847,
            10124849,
            10124824,
            10124825,
        ]

        # paris_2021_T4_2021-10-01_2021-12-31.json
        SENSOR_BANNED_LIST += [
            9577,
            9609,
            9863,
            15298,
            9647,
            9646,
            9864,
            9652,
            9655,
            9662,
            9663,
        ]

        return SENSOR_BANNED_LIST
