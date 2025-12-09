"""Module for dynamic configuration variables of the project."""

import os
from pathlib import Path

from dotenv import load_dotenv

_ROOT_PATH = Path(__file__).parent.parent.parent.parent
load_dotenv(dotenv_path=_ROOT_PATH / "secrets" / ".env")


class Configuration:
    # Root path
    ROOT_PATH = _ROOT_PATH

    # Data paths
    DATA_PATH = ROOT_PATH / "data"

    # TODO: Load the environment variables in the class
    @staticmethod
    def getenv(var_name: str) -> str:
        """Get environment variable by name."""
        return os.environ.get(var_name, "default")

    def __init__(self) -> None:
        pass
