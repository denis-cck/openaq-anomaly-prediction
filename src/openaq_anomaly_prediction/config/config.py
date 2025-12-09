"""Module for dynamic configuration variables of the project."""

import os
from pathlib import Path


class Configuration:
    ROOT_PATH = Path(__file__).parent.parent.parent.parent

    # Data paths
    DATA_PATH = ROOT_PATH / "data"

    VERSION = "0.5"

    # DEPRECATED: We're using MLflow for experiment tracking and model registration.
    # Model paths
    # Results and logging
    # Model parameters

    # TODO: Load the environment variables in the class
    @staticmethod
    def getenv(var_name: str) -> str:
        """Get environment variable by name."""
        return os.getenv(var_name, "")

    def __init__(self) -> None:
        pass


# config = Configuration()
