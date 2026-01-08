import time
from pprint import pprint

import duckdb
import numpy as np
import pandas as pd
from mlforecast.target_transforms import GlobalSklearnTransformer
from sklearn.preprocessing import FunctionTransformer
from typing_extensions import Literal

from openaq_anomaly_prediction.load.gcp import bq
from openaq_anomaly_prediction.utils.logger import ProgressLogger, logger


class BaseModel:
    """Base class for all models."""

    def __init__(self, params: dict = {}) -> None:
        self.params = params
        self.train_df = pd.DataFrame()
        self.test_df = pd.DataFrame()

    def load_train_dataset(self, table_id: str):
        """Load training dataset."""
        raise NotImplementedError("This method should be overridden by subclasses.")

    def load_test_dataset(self, table_id: str):
        """Load testing dataset."""
        raise NotImplementedError("This method should be overridden by subclasses.")

    def load_dataset(self, dataset_type: Literal["train", "test"], **kwargs):
        """Load dataset based on type (train/test)."""
        raise NotImplementedError("This method should be overridden by subclasses.")

    @staticmethod
    def get_log1p_transformer():
        """Get a log1p transformer."""

        log1p_transformer = FunctionTransformer(func=np.log1p, inverse_func=np.expm1)
        return GlobalSklearnTransformer(log1p_transformer)
