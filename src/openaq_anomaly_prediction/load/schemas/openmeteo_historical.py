import time

import pandas as pd
from google.cloud import bigquery
from google.cloud.bigquery.table import (  # noqa: F401
    ColumnReference,
    ForeignKey,
    TableReference,
)

from openaq_anomaly_prediction.config import Configuration as cfg  # noqa: F401
from openaq_anomaly_prediction.load.gcp import bq, gcs
from openaq_anomaly_prediction.load.schemas.base_table import BaseTable
from openaq_anomaly_prediction.utils.helpers import exec_time
from openaq_anomaly_prediction.utils.logger import logger

OPENMETEO_HISTORICAL_TABLE_CONFIG = {
    "dataset_id": "raw",
    "table_id": "src_openmeteo__historical",
    "schema": [
        # --------------------------------------------------------------------
        # RAW FIELDS
        bigquery.SchemaField(
            "location_id",
            "STRING",
            mode="REQUIRED",
            description="Unique location identifier (OpenAQ ID)",
        ),
        bigquery.SchemaField("datetimeto_utc", "TIMESTAMP", mode="REQUIRED"),
        bigquery.SchemaField("datetimeto_local", "DATETIME", mode="NULLABLE"),
        bigquery.SchemaField("weather_latitude", "FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField("weather_longitude", "FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField("timezone", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("elevation", "FLOAT64", mode="NULLABLE"),
        #
        # --------------------------------------------------------------------
        # METEOROLOGICAL FIELDS
        bigquery.SchemaField("temperature_2m", "FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField("relative_humidity_2m", "INT64", mode="NULLABLE"),
        bigquery.SchemaField("dew_point_2m", "FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField("apparent_temperature", "FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField("precipitation", "FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField("rain", "FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField("snowfall", "FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField("snow_depth", "FLOAT64", mode="NULLABLE"),
        #
        # --------------------------------------------------------------------
        # RADIATION FIELDS
        bigquery.SchemaField("shortwave_radiation", "FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField("direct_radiation", "FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField("diffuse_radiation", "FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField("global_tilted_irradiance", "FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField("direct_normal_irradiance", "FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField("terrestrial_radiation", "FLOAT64", mode="NULLABLE"),
        #
        # --------------------------------------------------------------------
        # WIND AND ATMOSPHERIC FIELDS
        bigquery.SchemaField("weather_code", "INT64", mode="NULLABLE"),
        bigquery.SchemaField("pressure_msl", "FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField("surface_pressure", "FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField("cloud_cover", "INT64", mode="NULLABLE"),
        bigquery.SchemaField("cloud_cover_low", "INT64", mode="NULLABLE"),
        bigquery.SchemaField("cloud_cover_mid", "INT64", mode="NULLABLE"),
        bigquery.SchemaField("cloud_cover_high", "INT64", mode="NULLABLE"),
        bigquery.SchemaField("vapour_pressure_deficit", "FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField("et0_fao_evapotranspiration", "FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField("wind_speed_100m", "FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField("wind_speed_10m", "FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField("wind_direction_10m", "INT64", mode="NULLABLE"),
        bigquery.SchemaField("wind_direction_100m", "INT64", mode="NULLABLE"),
        bigquery.SchemaField("wind_gusts_10m", "FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField("is_day", "INT64", mode="NULLABLE"),
        #
        # --------------------------------------------------------------------
        # METADATA FIELDS
        bigquery.SchemaField("ingested_at", "TIMESTAMP", mode="REQUIRED"),
        bigquery.SchemaField("updated_at", "TIMESTAMP", mode="REQUIRED"),
        bigquery.SchemaField("refreshed_at", "TIMESTAMP", mode="REQUIRED"),
        #
        # --------------------------------------------------------------------
        # REMOVED FIELDS
        # None at the moment
    ],
    "partitioning": bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY,
        field="datetimeto_utc",
    ),
    "clustering_fields": ["location_id"],
    "primary_keys": ["location_id", "datetimeto_utc"],
    "foreign_keys": [],
}


class OpenMeteoHistoricalTable(BaseTable):
    """Table management for the raw OpenMeteo measurements table in BigQuery."""

    def __init__(self):
        super().__init__()

        self.dataset_id = OPENMETEO_HISTORICAL_TABLE_CONFIG["dataset_id"]
        self.table_id = OPENMETEO_HISTORICAL_TABLE_CONFIG["table_id"]

        self.schema = OPENMETEO_HISTORICAL_TABLE_CONFIG["schema"]
        self.partitioning = OPENMETEO_HISTORICAL_TABLE_CONFIG["partitioning"]
        self.clustering_fields = OPENMETEO_HISTORICAL_TABLE_CONFIG["clustering_fields"]
        self.primary_keys = OPENMETEO_HISTORICAL_TABLE_CONFIG["primary_keys"]
        self.foreign_keys = OPENMETEO_HISTORICAL_TABLE_CONFIG["foreign_keys"]

    def clean_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean and sanitize the measurements dataframe."""

        if self.schema is None:
            raise ValueError("Table schema must be defined.")

        cleaned_df = df.copy()

        # SANITIZE DATAFRAME -------------------------------------------------

        cleaned_df = self.sanitize_dataframe(cleaned_df)

        # CLEAN COLUMNS ------------------------------------------------------
        # No specific column cleaning needed for measurements at the moment

        # METADATA FIELDS ----------------------------------------------------

        now = pd.Timestamp.now(tz="UTC")
        cleaned_df["ingested_at"] = now
        cleaned_df["updated_at"] = now
        cleaned_df["refreshed_at"] = now

        # Reorder columns to match schema
        schema_fields = [field.name for field in self.schema]
        cleaned_df = cleaned_df.reindex(columns=schema_fields)

        return cleaned_df

    def save_dataframe_to_gcs(
        self, df: pd.DataFrame, bucket_suffix: str, blob_name: str, **kwargs
    ) -> pd.DataFrame:
        """Save a DataFrame to GCS as a Parquet file."""

        verbose = kwargs.get("verbose", -1)

        start_time = time.perf_counter()

        if verbose >= 4:
            print()
            logger.debug(
                f"[BIGQUERY] Saving {len(df)} historical weather data in Big Query..."
            )

        # Clean the dataframe
        cleaned_df = self.clean_dataframe(df)

        # Stream historical weather data to GCS as Parquet files
        gcs.stream_dataframe_to_gcs(cleaned_df, bucket_suffix, blob_name)

        if verbose >= 3:
            logger.success(
                f"Saved {len(cleaned_df)} historical weather data into [{self.get_full_table_id()}] in {exec_time(start_time, fmt=True)}.\n"
            )

        return cleaned_df

    def transfer_all_data_to_bq(self, **kwargs) -> None:
        """Save data from a GCS bucket to the a Big Query table."""

        verbose = kwargs.get("verbose", 5)
        debug_inline = kwargs.get("debug_inline", True)

        start_time = time.perf_counter()

        if verbose >= 4 and not debug_inline:
            print()
            logger.debug(
                "[GCS/BIGQUERY] Saving all historical weather data in the staging bucket to Big Query..."
            )

        bucket_id = f"{bq.client.project}-staging"
        bucket_uri = f"gs://{bucket_id}"

        bq.upsert_data(
            table=self,
            merge_keys=["location_id", "datetimeto_utc"],
            bucket_uri=bucket_uri,
            prefix_uri="openmeteo/historical",
        )

        if verbose >= 3 and not debug_inline:
            logger.success(
                f"Saved all historical weather data from the staging bucket in {exec_time(start_time, fmt=True)}.\n"
            )


if __name__ == "__main__":
    logger.info(
        f"This module is intended to be imported, not run directly: {__file__}\n"
    )
    pass
