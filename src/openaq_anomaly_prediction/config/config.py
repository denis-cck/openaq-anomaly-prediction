"""Module for dynamic configuration variables of the project."""

import os
from pathlib import Path

from dotenv import load_dotenv

_ROOT_PATH = Path(__file__).parent.parent.parent.parent
load_dotenv(dotenv_path=_ROOT_PATH / "secrets" / ".env")
# print(f"Config loaded from {_ROOT_PATH / 'secrets' / '.env'}: {os.environ}")


class Configuration:
    # Root path
    ROOT_PATH = _ROOT_PATH

    # Data paths
    DATA_PATH = ROOT_PATH / "data"
    DATA_CSV_PATH = DATA_PATH / "csv"
    DATA_PARQUET_PATH = DATA_PATH / "parquet"

    os.makedirs(DATA_PATH, exist_ok=True)
    os.makedirs(DATA_CSV_PATH, exist_ok=True)
    os.makedirs(DATA_PARQUET_PATH, exist_ok=True)

    # Logs path
    LOGS_PATH = ROOT_PATH / "logs"
    os.makedirs(LOGS_PATH, exist_ok=True)

    # TODO: Load the environment variables in the class
    @staticmethod
    def getenv(var_name: str) -> str:
        """Get environment variable by name."""
        load_dotenv(dotenv_path=_ROOT_PATH / "secrets" / ".env")
        return os.environ.get(var_name, "default")

    def __init__(self) -> None:
        pass
