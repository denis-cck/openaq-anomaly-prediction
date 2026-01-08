import random
import tempfile
import time
import warnings
from pathlib import Path
from pprint import pprint

import duckdb
import holidays
import lightgbm as lgb
import numpy as np
import pandas as pd
import xgboost as xgb
from mlforecast import MLForecast
from mlforecast.lag_transforms import (
    ExpandingMean,
    ExpandingStd,
    ExponentiallyWeightedMean,
    RollingMax,
    RollingMean,
    RollingStd,
    SeasonalRollingMean,
    SeasonalRollingStd,
)
from mlforecast.target_transforms import (
    Differences,
    LocalBoxCox,
    LocalMinMaxScaler,
    LocalRobustScaler,
    LocalStandardScaler,
)
from numba import njit
from sklearn.base import BaseEstimator, RegressorMixin
from sklearn.compose import ColumnTransformer, make_column_selector
from sklearn.pipeline import FunctionTransformer, Pipeline
from sklearn.preprocessing import (
    FunctionTransformer,
    MinMaxScaler,
    OneHotEncoder,
    StandardScaler,
)
from typing_extensions import Literal
from utilsforecast.evaluation import evaluate
from utilsforecast.losses import mae, mape, rmse
from utilsforecast.plotting import plot_series

from openaq_anomaly_prediction.core.base_model import BaseModel
from openaq_anomaly_prediction.core.timeseries_utils import tsu
from openaq_anomaly_prediction.load.gcp import bq
from openaq_anomaly_prediction.utils.helpers import exec_time, smart_display
from openaq_anomaly_prediction.utils.logger import ProgressLogger, logger


class WeightedPipeline(BaseEstimator, RegressorMixin):
    """
    Wrapper to force a Scikit-Learn Pipeline to accept 'sample_weight'
    directly from MLForecast and route it to the step named 'model'.
    """

    def __init__(self, pipeline):
        self.pipeline = pipeline

    def fit(self, X, y, sample_weight=None):
        # If weights are provided, route them to the 'model' step
        # using the 'stepname__parameter' syntax.
        if sample_weight is not None:
            self.pipeline.fit(X, y, model__sample_weight=sample_weight)
        else:
            self.pipeline.fit(X, y)
        return self

    def predict(self, X):
        return self.pipeline.predict(X)


class CustomXGBoostModel(BaseModel):
    """XGBoost model implementation."""

    POLLUTANTS_COLUMNS = [
        "pm25_ugm3",
        "pm10_ugm3",
        "no2_ppm",
        "o3_ppm",
        "so2_ppm",
        "co_ppm",
    ]
    DEFAULT_POLLUTANT = POLLUTANTS_COLUMNS[0]

    STATIC_COLUMNS = [
        "location_id",
        "oaq_latitude",
        "oaq_longitude",
    ]

    def __init__(self, params: dict = {}) -> None:
        super().__init__(params)
        # self.params = params
        self.pollutants_columns = []
        self.is_imputed_columns = []

        self.all_columns = []
        self.openaq_columns = []
        self.openmeteo_columns = []
        self.segments_columns = []
        self.features_columns = []
        self.other_columns = []
        # self.model = xgb.XGBClassifier(**self.params)

    @staticmethod
    def get_other_pollutant_columns(target_column: str) -> list[str]:
        """Get the list of pollutant columns excluding the target pollutant."""
        other_pollutants = [
            col for col in CustomXGBoostModel.POLLUTANTS_COLUMNS if col != target_column
        ]
        return other_pollutants

    # ---------------------------------------------------------------------
    # FEATURES TRANSFORMERS (sklearn)

    def _transform_trig(self, period: int):
        """Universal sin/cosine transformer."""

        def transform(X, period):
            X_scaled = 2 * np.pi * X / period
            return np.column_stack([np.sin(X_scaled), np.cos(X_scaled)])

        def feature_names(transformer, input_features):
            new_names = []
            for col in input_features:
                new_names.extend([f"{col}_sin", f"{col}_cos"])
            return new_names

        return FunctionTransformer(
            func=transform,
            feature_names_out=feature_names,
            kw_args={"period": period},
        )

    def _transform_date_features(self):
        """Date features sin/cosine transformer from local time."""

        def transform(X):
            dt_series = pd.to_datetime(X.iloc[:, 0])

            # # Actual transformations
            # date = dt_series.dt.date.values
            # day = dt_series.dt.day.values.astype(int)

            # hour = dt_series.dt.hour.values.astype(int)
            # dow = dt_series.dt.dayofweek.values.astype(int)
            # month = dt_series.dt.month.values.astype(int)
            # week = dt_series.dt.isocalendar().week.values.astype(int)

            # Holidays (using holidays package)
            unique_dates = dt_series.dt.date.unique()
            kr_holidays = holidays.CountryHoliday("KR")
            tomorrow_dates = dt_series.dt.date + pd.Timedelta(days=1)
            holiday_dates = [d for d in unique_dates if d in kr_holidays]
            is_holiday = np.isin(dt_series.dt.date, holiday_dates).astype(int)
            is_holiday_eve = np.isin(tomorrow_dates, holiday_dates).astype(int)

            # Exemples:
            # is_rush_hour = np.isin(hour, [7, 8, 9, 17, 18, 19]).astype(int)
            # is_party_night = (  # Friday night 9pm-2am and Saturday night 9pm-2am
            #     ((dow == 4) & (hour >= 21))
            #     | ((dow == 5) & ((hour >= 21) | (hour <= 2)))
            #     | ((dow == 6) & (hour <= 2))
            # ).astype(int)

            # # DEBUG VALUES --------------------
            # data = {
            #     "debug_datetime": dt_series.values,
            #     "debug_date": date,
            # }
            # return pd.DataFrame(data)
            # # END DEBUG -----------------------

            return np.column_stack(
                [
                    is_holiday,
                    is_holiday_eve,
                ]
            )

        def feature_names(transformer, input_features):
            return [
                "is_holiday",
                "is_holiday_eve",
            ]

        return FunctionTransformer(func=transform, feature_names_out=feature_names)

    # ---------------------------------------------------------------------
    # CREATE THE MODEL

    def create_preprocessor(
        self, example_df: pd.DataFrame, target_column: str
    ) -> ColumnTransformer:
        """Create a preprocessing pipeline for the given target column."""

        # Here we don't care what's a dynamic or a static feature,
        # it's ONLY when using MLForecast that we need to specify
        # the STATIC features. The dynamic features are all the rest.

        # ---------------------------------------------------------------------
        # Categorize every single column in the example dataframe

        # Refresh the columns categories
        self.categorize_columns(example_df)

        # Make sure the location_id is a category

        # MLForecast will drop these columns automatically (kept for remaining colunns identification)
        # TS_COLUMNS = ["datetimeto_utc", "segment_id", target_column]
        TS_COLUMNS = ["ds", "unique_id", "y"] + [target_column]  # after renaming

        # # We need to remove the segment_id from segments_columns to avoid dropping it twice
        # segments_columns = [col for col in self.segments_columns if col != "segment_id"]

        # Later use other pollutants as lags/features
        OTHER_POLLUTANTS = [
            col for col in self.pollutants_columns if col != target_column
        ]
        # OTHER_POLLUTANTS_LAGS = [
        #     col for col in self.POLLUTANTS_COLUMNS if col != target_column
        # ]

        num_columns = [
            "concurrent_sensors",
            "om_elevation",
            "om_temperature_2m",
            "om_dew_point_2m",
            # "om_apparent_temperature",
            "om_precipitation",
            "om_rain",
            "om_snowfall",
            "om_snow_depth",
            "om_shortwave_radiation",
            "om_direct_radiation",
            "om_diffuse_radiation",
            # "om_global_tilted_irradiance",
            # "om_direct_normal_irradiance",
            # "om_terrestrial_radiation",
            "om_pressure_msl",
            # "om_surface_pressure",
            "om_vapour_pressure_deficit",
            # "om_et0_fao_evapotranspiration",
            "om_wind_speed_100m",
            "om_wind_speed_10m",
            "om_wind_gusts_10m",
        ]
        num_columns_pct = [
            "om_relative_humidity_2m",
            "om_cloud_cover",
            "om_cloud_cover_low",
            "om_cloud_cover_mid",
            "om_cloud_cover_high",
        ]
        num_columns_deg = [
            # "om_wind_direction_10m",
            # "om_wind_direction_100m",
        ]
        num_columns_coordinates = ["oaq_latitude", "oaq_longitude"]
        date_columns = ["oaq_datetimeto_local"]
        cat_columns = [
            # "location_id",
        ]

        boolean_columns = ["om_is_day", "any_imputed"]

        # FEATURES COLUMNS ----------------
        features_time_columns = [
            col for col in self.features_columns if col.startswith("feat_time__")
        ]
        features_weather_columns = [
            col for col in self.features_columns if col.startswith("feat_weather__")
        ]
        features_pollutant_columns = [
            col for col in self.features_columns if col.startswith("feat_pollutant__")
        ]

        # KEPT COLUMNS --------------------
        # Summary of columns to transform, to passthrough and to drop
        kept_columns = (
            num_columns
            + num_columns_pct
            + num_columns_deg
            + cat_columns
            + num_columns_coordinates
            + date_columns
            # + ["feat_wind_10m_u", "feat_wind_10m_v"]  # engineered features
        )

        passthrough_columns = (
            boolean_columns
            + features_time_columns
            + features_weather_columns
            + features_pollutant_columns
            + ["location_id"]  # categorical feature (native xgboost support)
        )

        # DROPPED COLUMNS -----------------
        selective_drop_columns = [
            # "om_wind_direction_10m",
            "oaq_owner_id",
            "oaq_provider_id",
            "oaq_ismonitor",
            "om_weather_code",
        ]
        dropped_columns = (
            # Not used in exogenous features
            self.segments_columns
            # Should be dealt with separately, depending on the target pollutant
            # + OTHER_POLLUTANTS  # dropped BEFORE the preprocessor
            + self.is_imputed_columns
            # Processed in MLForecast
            + selective_drop_columns
            # + ["location_id"]
        )
        # Make sure to skip the TS_COLUMNS (errors otherwise)
        dropped_columns = [col for col in dropped_columns if col not in TS_COLUMNS]

        # SANITY CHECK --------------------
        remaining_columns = [
            col
            for col in example_df.columns
            if col
            not in TS_COLUMNS
            + kept_columns
            + passthrough_columns
            + dropped_columns
            + ["y_weight_penalty"]
        ]
        if len(remaining_columns) > 0:
            pprint(remaining_columns)
            raise ValueError(f"Some columns were not categorized: {remaining_columns}")

        # ---------------------------------------------------------------------
        # TRANSFORMERS PIPELINES

        # Removed for now, XGBoost can handle unscaled data
        # num_pipeline = Pipeline([("standard_scaler", StandardScaler())])  # RobustScaler
        # num_pipeline_pct = Pipeline(steps=[("minmax_scaler", MinMaxScaler())])

        cat_pipeline = Pipeline([("onehot", OneHotEncoder(handle_unknown="ignore"))])

        preprocessor = ColumnTransformer(
            transformers=[
                # -------------------------
                # Removed for now, XGBoost can handle unscaled data
                # ("num", num_pipeline, num_columns),
                # ("num_pct", num_pipeline_pct, num_columns_pct),
                ("num", "passthrough", num_columns),
                ("num_pct", "passthrough", num_columns_pct),
                # -------------------------
                # OneHotEncoder (could use other methods)
                ("cat", cat_pipeline, cat_columns),
                # -------------------------
                # Trigonometry (sin/cos)
                # ("num_deg", self._transform_trig(period=360), num_columns_deg),
                # ("hour_sin_cos", hour_pipeline, ["oaq_datetimeto_local"]),
                ("holidays", self._transform_date_features(), ["oaq_datetimeto_local"]),
                # -------------------------
                # Passthrough
                ("coords", "passthrough", num_columns_coordinates),
                ("passthrough", "passthrough", passthrough_columns),
                # -------------------------
                # Columns to drop
                ("dropped", "drop", dropped_columns),
            ],
            remainder="passthrough",  # make the rest of the columns from MLForecast passthrough
        )

        return preprocessor

    def create_xgb_model(
        self, example_df: pd.DataFrame, target_column: str, hyperparameters: dict = {}
    ) -> Pipeline | WeightedPipeline:
        """Create a XGBoost model pipeline for the given target column."""

        preprocessor = self.create_preprocessor(example_df, target_column)

        model = xgb.XGBRegressor(
            **hyperparameters,
            tree_method="hist",
            device="cuda",
            n_jobs=-1,
            enable_categorical=True,
            objective="reg:squarederror",
            # objective="reg:tweedie",
            # tweedie_variance_power=1.1,  # 1=spikier, 2=smoother
            missing=np.nan,
        )

        xgb_pipeline = Pipeline(
            steps=[
                ("preprocessor", preprocessor),
                ("model", model),
            ]
        )
        return WeightedPipeline(xgb_pipeline)
        # return xgb_pipeline

    def prepare_timeseries_dataframe(
        self, source_df: pd.DataFrame, target_column: str
    ) -> pd.DataFrame:
        """Create a time series dataframe compatible with MLForecast."""

        source_ts = (
            source_df.copy()
            .rename(
                columns={
                    "datetimeto_utc": "ds",
                    "segment_id": "unique_id",
                    target_column: "y",
                }
            )
            .drop(columns=CustomXGBoostModel.get_other_pollutant_columns(target_column))
        )

        # # Apply weight penalty (>0.9 quantile) to high pollution events
        # # QUANTILE: increase to focus on more extreme peaks. Decrease to include more values.
        # # WEIGHTS: increase weights to better detect peaks. Reduce to lessen overpredictions.
        # quantile_90 = source_ts["y"].quantile(0.9)
        # def assign_weights(value):
        #     return 5.0 if value > quantile_90 else 1.0
        # source_ts["y_weight_penalty"] = source_ts["y"].apply(assign_weights)

        source_ts["location_id"] = source_ts["location_id"].astype("category")

        # # NOTE: Ensure these column names match your dataset exactly!
        # # We use 10m because pollution is at ground level.
        # if (
        #     "om_wind_speed_10m" in source_ts.columns
        #     and "feat_weather__wind_direction_10m_sin" in source_ts.columns
        # ):
        #     # U-Component (East-West strength)
        #     source_ts["feat_wind_10m_u"] = (
        #         source_ts["om_wind_speed_10m"]
        #         * source_ts["feat_weather__wind_direction_10m_sin"]
        #     )

        #     # V-Component (North-South strength)
        #     source_ts["feat_wind_10m_v"] = (
        #         source_ts["om_wind_speed_10m"]
        #         * source_ts["feat_weather__wind_direction_10m_cos"]
        #     )

        return source_ts

    def create_mlforecaster(
        self,
        source_ts: pd.DataFrame,
        target_column: str,
        hyperparameters: dict | None = None,
        **kwargs,
    ) -> MLForecast:
        """Create an MLForecast object with the XGBoost model."""

        debug = kwargs.get("debug", False)

        # MLForecast warnings about the nulls in the exogenous features
        warnings.filterwarnings("ignore", message="Found null values in")

        MODEL_NAME = "xgb_" + target_column

        # ---------------------------------------------------------------------
        # XGB Hyperparameters

        if hyperparameters is not None:
            XGB_HYPERPARAMS = hyperparameters
        else:
            XGB_HYPERPARAMS = {
                "n_estimators": 441,
                "learning_rate": 0.18118585668338338,
                "max_depth": 8,
                "subsample": 0.8610061591784797,
                # "colsample_bytree": 0.7,  # add randomness to prevent overfitting
            }

            XGB_HYPERPARAMS = {
                "n_estimators": 700,
                "learning_rate": 0.05,
                "max_depth": 6,
                "min_child_weight": 15,
                # Prevent overfitting by adding randomness
                "colsample_bytree": 0.4,  # only use 40% of features per tree
                "subsample": 0.8,
            }

            XGB_HYPERPARAMS = {
                "n_estimators": 700,
                "learning_rate": 0.05,
                "max_depth": 6,
                "min_child_weight": 14,
                "subsample": 0.6,
                "colsample_bytree": 0.5,
                "reg_alpha": 9,
                "reg_lambda": 8,
            }

            XGB_HYPERPARAMS = {
                "n_estimators": 768,
                "learning_rate": 0.059529195355373835,
                "max_depth": 8,
                "min_child_weight": 9,
                "subsample": 0.6,
                "colsample_bytree": 0.34386241746623636,
                "reg_alpha": 11.27021981919661,  # L2
                "reg_lambda": 8,  # L2
            }

            XGB_HYPERPARAMS = {
                "n_estimators": 2000,
                "learning_rate": 0.03556179697145142,
                "max_depth": 5,
                "gamma": 2.289330777415043,
                "min_child_weight": 106,
                "subsample": 0.30868388328409396,
                "colsample_bytree": 0.49559683685651895,
                "reg_alpha": 11.350641812218562,
                "reg_lambda": 19.944262764273923,
            }

            # THIS SOMEHOW WORKS if shit hits the fan
            XGB_HYPERPARAMS = {
                "n_estimators": 500,
                "learning_rate": 0.02391361035408582,
                "max_depth": 9,
                "gamma": 1.0866108612280976,
                "min_child_weight": 144,
                "subsample": 0.6175065473012122,
                "colsample_bytree": 0.3872467446241419,
                "reg_alpha": 9.92187904640817,
                "reg_lambda": 14.20703165967658,
            }

            # # 24h window
            # XGB_HYPERPARAMS = {
            #     "n_estimators": 1100,
            #     "learning_rate": 0.04676087796507621,
            #     "max_depth": 8,
            #     "gamma": 6.19033208599029,
            #     "min_child_weight": 143,
            #     "subsample": 0.49209344888905693,
            #     "colsample_bytree": 0.37135007718333013,
            #     "reg_alpha": 10.197452902254163,
            #     "reg_lambda": 10.25566256129159,
            # }

        # pprint(XGB_HYPERPARAMS)
        # ---------------------------------------------------------------------
        # MLForecast preprocessing setup

        # # For 336h segments (14 days)
        # LAG_TRANSFORMS = {
        #     1: [
        #         ExpandingMean(),  # FIX: slow decay in ACF
        #         # ExponentiallyWeightedMean(alpha=0.1),
        #         # ExponentiallyWeightedMean(alpha=0.5),  # FIX: reaction to shocks  # overbearing
        #         RollingMean(window_size=24, min_samples=1),  # baseline
        #         RollingStd(window_size=12, min_samples=1),
        #         RollingMax(window_size=12, min_samples=1),  # FIX: volatility (peaks/spikes)
        #         # RollingMean(window_size=2),
        #         # RollingMean(window_size=12),
        #         # RollingMean(window_size=24),
        #     ],  # Short-term & Daily momentum
        #     # 12: [RollingMean(window_size=2)],  # Half-day momentum
        #     24: [
        #         SeasonalRollingMean(
        #             season_length=24, window_size=2, min_samples=1
        #         ),  # FIX: daily transitions
        #         # SeasonalRollingMean(season_length=24, window_size=4, min_samples=1),  # baseline
        #         SeasonalRollingMean(season_length=24, window_size=7, min_samples=1),  # rough on the burn in (192h)
        #     ],
        #     # 48: [
        #     #     RollingMean(window_size=24, min_samples=1),  # baseline
        #     #     # SeasonalRollingMean(
        #     #     #     season_length=24, window_size=2, min_samples=1
        #     #     # ),  # FIX: daily transitions
        #     #     # SeasonalRollingMean(season_length=24, window_size=5, min_samples=1),  # baseline
        #     #     # SeasonalRollingMean(season_length=24, window_size=7, min_samples=1),  # rough on the burn in (192h)
        #     # ],
        # }

        # 1. Define the function (Must be compiled with njit for speed)
        @njit
        def velocity_lag1(x):
            """
            Calculates (Lag1 - Lag2) for the entire series.
            Input x is the Lag 1 series (y_t-1).
            So x[i] is y_t-1, and x[i-1] is y_t-2.
            """
            n = len(x)
            out = np.full(n, np.nan)  # Create an array full of NaNs

            # We need at least index 1 to look back at index 0
            for i in range(1, n):
                out[i] = x[i] - x[i - 1]

            return out

        @njit
        def velocity_lag2(x):
            """
            Calculates (Lag2 - Lag3) for the entire series.
            Input x is the Lag 1 series.
            We look back 1 and 2 steps from the current Lag 1.
            """
            n = len(x)
            out = np.full(n, np.nan)

            # We need at least index 2 to look back at index 0
            for i in range(2, n):
                out[i] = x[i - 1] - x[i - 2]

            return out

        @njit
        def acceleration(x):
            """
            Calculates Change in Velocity: (Vel_Current - Vel_Previous)
            """
            n = len(x)
            out = np.full(n, np.nan)

            # We need at least index 2 to calculate two velocities
            for i in range(2, n):
                vel_curr = x[i] - x[i - 1]
                vel_prev = x[i - 1] - x[i - 2]
                out[i] = vel_curr - vel_prev

            return out

        # If i want to predict the peaks, i should not use log1p because it compresses the high values.
        LOG = [CustomXGBoostModel.get_log1p_transformer()]
        DIFFERENCES = [Differences([24])]
        SCALER = [
            # LocalStandardScaler(),
            # LocalRobustScaler(scale="iqr"),
        ]  # kinda need this for a global model across all locations

        # No target transform for Tweedie
        LOG = []
        DIFFERENCES = []
        SCALER = []

        LOG = [CustomXGBoostModel.get_log1p_transformer()]

        # It's recommended to have minimum lags = forecast horizon (h)
        # But MLForecast is built to handle recursive predictions internally so we can use shorter lags.
        # Max lag should capture seasonality and short-term patterns. More or less < 25% of the total series length.
        LAGS = [24, 48, 72, 168]  # don't use lag1, it's massively overfitting

        # ---------------------------------------------------------------------
        # NEW FEATURES
        # For 504h segments (21 days)
        LAG_TRANSFORMS = {
            1: [
                # Trends
                velocity_lag1,  # Instant change
                velocity_lag2,  # Previous change
                acceleration,  # Is the spike slowing down?
                # Smoothing (overall noise)
                # RollingMean(window_size=6, min_samples=1),  # Short-term trend
                RollingMean(window_size=12, min_samples=1),  # Medium-term trend
                # Spike detection
                RollingMax(
                    window_size=3, min_samples=1
                ),  # Did we see a huge spike recently?
                # Long term
                ExpandingMean(),
                RollingStd(
                    window_size=10000000, min_samples=2
                ),  # fake expandingstd with min_samples=2 to avoid 1-1
            ],
            24: [
                # Baseline seasonal: what happened at this hour over the last week
                SeasonalRollingMean(season_length=24, window_size=7, min_samples=1),
                # Context: was yesterday volatile?
                RollingMean(window_size=24, min_samples=1),  # REMOVED: 24h trend
                RollingStd(window_size=24, min_samples=2),  # volatility
                # RollingMax(window_size=24, min_samples=1),  # REMOVED: peak detection
            ],
            168: [
                # RollingMean(window_size=1, min_samples=1),  # baseline weekly level
            ],
        }

        # # ---------------------------------------------------------------------
        # # CLASSIC
        # # For 504h segments (21 days)
        # LAG_TRANSFORMS = {
        #     1: [
        #         ExpandingMean(),
        #         # ExpandingStd(),
        #         RollingStd(
        #             window_size=10000000, min_samples=2
        #         ),  # fake expandingstd with min_samples=2 to avoid 1-1
        #         # Capturing velocity of change
        #         RollingMean(window_size=2, min_samples=1),  # fast trend
        #         RollingMean(window_size=6, min_samples=1),  # slow-er trend
        #     ],
        #     24: [
        #         # Baseline seasonal: what happened at this hour over the last week
        #         SeasonalRollingMean(season_length=24, window_size=7, min_samples=1),
        #         # Context: was yesterday volatile?
        #         RollingMean(window_size=24, min_samples=1),  # 24h trend
        #         RollingStd(window_size=24, min_samples=2),  # volatility
        #         RollingMax(window_size=24, min_samples=1),  # peak detection
        #     ],
        #     168: [
        #         RollingMean(window_size=1, min_samples=1),  # baseline weekly level
        #     ],
        # }

        if debug:
            # CHECK BURN-IN
            SEGMENT_SIZE = 504  # 14 days
            burn_in = tsu.get_mlforecast_burn_in(
                segment_size=SEGMENT_SIZE,
                target_transforms=[],
                lags=LAGS,
                lag_transforms=LAG_TRANSFORMS,
            )

        # ---------------------------------------------------------------------
        # Create the MLForecast object

        fcst = MLForecast(
            models={
                # "naive": Naive(),
                MODEL_NAME: self.create_xgb_model(
                    source_ts, target_column, XGB_HYPERPARAMS
                ),
            },
            freq="h",
            target_transforms=LOG
            + DIFFERENCES
            + SCALER,  # differencing removes too much # type: ignore
            lags=LAGS,
            lag_transforms=LAG_TRANSFORMS,
        )

        return fcst

    # ---------------------------------------------------------------------
    # LOAD THE DATASETS

    def load_train_dataset(self, **kwargs):
        """Load training dataset."""

        verbose = kwargs.get("verbose", 5)

        if self.train_df.empty:
            start_time = time.perf_counter()
            if verbose >= 4:
                logger.debug("[TRAIN] Loading training dataset for XGBoost model.")

            self.train_df = self.load_dataset(dataset_type="train", verbose=verbose)

            if verbose >= 5:
                rows = self.train_df.shape[0]
                locations = self.train_df["location_id"].nunique()
                logger.trace(
                    f"Loaded {rows:,} rows from {locations} locations in {exec_time(start_time, fmt=True)}."
                )

        return self.train_df

    def load_test_dataset(self, **kwargs):
        """Load testing dataset."""

        verbose = kwargs.get("verbose", 5)

        if self.test_df.empty:
            start_time = time.perf_counter()
            if verbose >= 4:
                logger.debug("[TEST] Loading testing dataset for XGBoost model.")

            self.test_df = self.load_dataset(dataset_type="test", verbose=verbose)

            if verbose >= 5:
                rows = self.test_df.shape[0]
                locations = self.test_df["location_id"].nunique()
                logger.trace(
                    f"Loaded {rows:,} rows from {locations} locations in {exec_time(start_time, fmt=True)}."
                )

        return self.test_df

    def load_dataset(self, dataset_type: Literal["train", "test"], **kwargs):
        """Load dataset based on type (train/test)."""

        verbose = kwargs.get("verbose", 5)

        if dataset_type not in ["train", "test"]:
            raise ValueError("dataset_type must be either 'train' or 'test'.")

        # TODO: Put a local cache mechanism here (parquet file)

        # BigQuery
        start_time = time.perf_counter()
        query = f"""
            SELECT
                *
            FROM `openaq-anomaly-prediction.dev_serving.ml_seoul__xgb_{dataset_type}`
            WHERE datetimeto_utc <= '2026-01-01'
            --  AND datetimeto_utc >= '2025-12-15'
              AND segment_total_hours >= 504
            ORDER BY location_id, datetimeto_utc ASC
        """
        query_parameters = [
            # ("dataset", "STRING", dataset)
        ]
        df = bq.query(query, query_parameters=query_parameters, dry_run=False)

        if verbose >= 6:
            logger.trace(bq.get_query_result_summary(df, start_time))

        self.categorize_columns(df)

        # df = self.interpolate_missing_values(df) # type: ignore

        return df

    def categorize_columns(self, df: pd.DataFrame) -> None:
        """Categorize columns depending on their sources."""

        self.all_columns = df.columns.tolist()

        self.pollutants_columns = CustomXGBoostModel.POLLUTANTS_COLUMNS

        self.is_imputed_columns = [
            col for col in df.columns if col.endswith("is_imputed")
        ]
        # for pollutant in CustomXGBoostModel.POLLUTANTS_COLUMNS:
        #     self.pollutants_columns += [
        #         col for col in df.columns if col.startswith(pollutant + "_")
        #     ]

        self.openaq_columns = [col for col in df.columns if col.startswith("oaq_")]
        self.openmeteo_columns = [col for col in df.columns if col.startswith("om_")]
        self.segments_columns = [
            col
            for col in df.columns
            if col.startswith("segment_") or col.startswith("segments_")
        ]
        self.features_columns = [col for col in df.columns if col.startswith("feat_")]
        self.other_columns = [
            col
            for col in df.columns
            if col
            not in self.pollutants_columns
            + self.openmeteo_columns
            + self.openaq_columns
            + self.segments_columns
            + self.features_columns
        ]

    def interpolate_pollutants(self, df: pd.DataFrame, **kwargs) -> pd.DataFrame:
        debug = kwargs.get("debug", False)
        limit = kwargs.get("limit", 4)
        groupby_cols = ["location_id", "segment_id"]

        pollutant_cols = CustomXGBoostModel.POLLUTANTS_COLUMNS

        if debug:
            total_nans_before = df.loc[:, pollutant_cols].isna().sum().sum()
            print(
                f"{'=' * 64}\nINTERPOLATING {len(pollutant_cols)} POLLUTANTS\n{'-' * 64}"
            )
            print(f"BEFORE: {total_nans_before} total NaN values")

        # Add a "is_imputed" column for each pollutant
        imputed_cols = []
        for col in pollutant_cols:
            imputed_cols.append(f"{col}_is_imputed")
            df.loc[:, f"{col}_is_imputed"] = df.loc[:, col].isna().astype(int)

        # Interpolation function
        def fill_logic(x):
            return (
                x.interpolate(method="linear", limit=limit, limit_direction="both")
                .ffill(limit=limit)
                .bfill(limit=limit)
            )

        # Interpolate ALL pollutants at once
        df.loc[:, pollutant_cols] = df.groupby(groupby_cols)[pollutant_cols].transform(
            fill_logic
        )

        # Add a global "any_imputed" column for each row/observation
        df.loc[:, "any_imputed"] = df.loc[:, imputed_cols].max(axis=1)
        imputed_cols.append("any_imputed")

        if debug:
            limit_debug_string = (
                "all missing values" if limit is None else f"up to {limit} hours"
            )
            print(f"    -> Adding columns       : {imputed_cols}")
            print(f"    -> Grouped by           : {groupby_cols}")
            print(f"    -> Linear interpolation : {limit_debug_string}")
            print(f"    -> Forward fill         : {limit_debug_string}")
            print(f"    -> Backward fill        : {limit_debug_string}")

            total_nans_after = df.loc[:, pollutant_cols].isna().sum().sum()
            print(f"AFTER: {total_nans_after} total NaN values")

        return df

    # ---------------------------------------------------------------------
    # PRINT REPORTS

    def print_categories_report(self) -> None:
        """Print a report of NaN values in the dataframe."""
        sep_length = 64

        title = "COLUMNS CATEGORIZATION"
        print(f"{'=' * sep_length}\n{title}\n{'=' * sep_length}")

        # print(f"\nALL columns:\n{all_columns.columns.tolist()}")
        print(
            f"\nPollutants columns ({len(self.pollutants_columns)}):\n{self.pollutants_columns}"
        )
        print(f"\nOpenAQ columns ({len(self.openaq_columns)}):\n{self.openaq_columns}")
        print(
            f"\nOpenMeteo columns ({len(self.openmeteo_columns)}):\n{self.openmeteo_columns}"
        )
        print(
            f"\nSegments columns ({len(self.segments_columns)}):\n{self.segments_columns}"
        )
        print(f"\nFeatures columns ({len(self.features_columns)}):")

        time_features = [
            col for col in self.features_columns if col.startswith("feat_time__")
        ]
        weather_features = [
            col for col in self.features_columns if col.startswith("feat_weather__")
        ]
        pollutant_features = [
            col for col in self.features_columns if col.startswith("feat_pollutant__")
        ]

        print(f" > Time features ({len(time_features)})      : {time_features}")
        print(f" > Weather features ({len(weather_features)})   : {weather_features}")
        print(
            f" > Pollutant features ({len(pollutant_features)}) : {pollutant_features}"
        )
        print(f"\nOther columns:\n{self.other_columns}")

    def print_nan_report(self, df: pd.DataFrame) -> None:
        """Print a report of NaN values in the dataframe."""

        # pollutants_columns = CustomXGBoostModel.POLLUTANTS_COLUMNS

        # openaq_columns = [col for col in df.columns if col.startswith("oaq_")]
        # openmeteo_columns = [col for col in df.columns if col.startswith("om_")]
        # segments_columns = [col for col in df.columns if col.startswith("segment_")]
        # other_columns = [
        #     col
        #     for col in df.columns
        #     if col
        #     not in pollutants_columns
        #     + openmeteo_columns
        #     + openaq_columns
        #     + segments_columns
        # ]

        print()

        # Target columns NaN analysis
        nan_pollutants_columns = (
            df[self.pollutants_columns].isna().sum().sort_values(ascending=False)
        )
        total_nan_pollutants_columns = nan_pollutants_columns.sum()
        print(
            f"[ TARGET    ]: {total_nan_pollutants_columns} NaN values in the target columns"
        )
        if total_nan_pollutants_columns > 0:
            print(f"{nan_pollutants_columns}\n")

        # OpenAQ columns NaN analysis
        nan_openaq_columns = (
            df[self.openaq_columns].isna().sum().sort_values(ascending=False)
        )
        total_nan_openaq_columns = nan_openaq_columns.sum()
        print(f"[ OPENAQ    ]: {total_nan_openaq_columns} NaN values in OpenAQ columns")
        if total_nan_openaq_columns > 0:
            print(f"{nan_openaq_columns}\n")

        # OpenMeteo columns NaN analysis
        nan_openmeteo_columns = (
            df[self.openmeteo_columns].isna().sum().sort_values(ascending=False)
        )
        total_nan_openmeteo_columns = nan_openmeteo_columns.sum()
        print(
            f"[ OPENMETEO ]: {total_nan_openmeteo_columns} NaN values in OpenMeteo columns"
        )
        if total_nan_openmeteo_columns > 0:
            print(f"{nan_openmeteo_columns}\n")

        # Features columns NaN analysis
        nan_features_columns = (
            df[self.features_columns].isna().sum().sort_values(ascending=False)
        )
        total_nan_features_columns = nan_features_columns.sum()
        print(
            f"[ FEATURES  ]: {total_nan_features_columns} NaN values in features columns"
        )
        if total_nan_features_columns > 0:
            print(f"{nan_features_columns}\n")

        # Other columns NaN analysis
        nan_other_columns = (
            df[self.other_columns].isna().sum().sort_values(ascending=False)
        )
        total_nan_other_columns = nan_other_columns.sum()
        print(f"[ OTHERS    ]: {total_nan_other_columns} NaN values in other columns")
        if total_nan_other_columns > 0:
            print(f"{nan_other_columns}\n")

    def print_feature_names(self, fcst: MLForecast, train_df: pd.DataFrame) -> None:
        """Preview feature names without fitting the actual XGBoost model."""

        # Get the raw features produced by MLForecast (Lags, Windows, etc.)
        prep_df = fcst.preprocess(
            train_df, static_features=CustomXGBoostModel.STATIC_COLUMNS
        )

        xgb_pipeline = fcst.models["xgb"]  # returns the UNTRAINED object (no _)
        preprocessor = xgb_pipeline.named_steps["preprocessor"]

        # We drop 'ds', 'unique_id', and 'y' because the preprocessor doesn't expect them
        X_raw = prep_df.drop(columns=["unique_id", "ds", "y"])

        # # DEBUG: Print columns to verify
        # print(f"Columns in X_raw: {X_raw.columns.tolist()}")

        final_features = preprocessor.fit_transform(X_raw)
        # Manually fit ONLY the preprocessor on the preprocessed data
        # We drop 'ds', 'unique_id', and 'y' because the preprocessor doesn't expect them
        # X_sample = prep_df.drop(columns=["ds", "unique_id", "y"])

        # Get feature names from the preprocessor
        feature_names = preprocessor.get_feature_names_out()

        # DEBUG: Print shapes to catch the mismatch before the DataFrame fails
        # print(f"Data Shape: {final_features.shape}")
        # print(f"Names Length: {len(feature_names)}")

        print(f"Total Features: {len(feature_names)}")
        pprint(list(feature_names))

        # # This is bugging on the "location_id" not getting dropped properly, idc for now
        # final_df = pd.DataFrame(
        #     final_features,
        #     columns=feature_names,
        #     index=prep_df.index,  # Keep the original index for alignment # type: ignore
        # )  # type: ignore

        # return final_df
