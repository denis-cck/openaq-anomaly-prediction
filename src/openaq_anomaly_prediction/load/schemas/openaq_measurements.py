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

OPENAQ_MEASUREMENTS_TABLE_CONFIG = {
    "dataset_id": "raw",
    "table_id": "src_openaq__measurements",
    "schema": [
        # --------------------------------------------------------------------
        # RAW FIELDS
        bigquery.SchemaField(
            "sensor_id",
            "STRING",
            mode="REQUIRED",
            description="Unique sensor identifier (OpenAQ ID)",
        ),
        bigquery.SchemaField("value", "FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField("flagInfo_hasFlags", "BOOLEAN", mode="NULLABLE"),
        # --------------------------------------------------------------------
        # FLATTENED FIELDS
        bigquery.SchemaField("parameter_id", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("parameter_name", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("parameter_units", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("parameter_displayName", "STRING", mode="NULLABLE"),
        #
        bigquery.SchemaField("period_label", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("period_interval", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("period_datetimeFrom_utc", "TIMESTAMP", mode="NULLABLE"),
        bigquery.SchemaField("period_datetimeFrom_local", "DATETIME", mode="NULLABLE"),
        bigquery.SchemaField("period_datetimeTo_utc", "TIMESTAMP", mode="NULLABLE"),
        bigquery.SchemaField("period_datetimeTo_local", "DATETIME", mode="NULLABLE"),
        #
        bigquery.SchemaField("summary_min", "FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField("summary_q02", "FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField("summary_q25", "FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField("summary_median", "FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField("summary_q75", "FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField("summary_q98", "FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField("summary_max", "FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField("summary_avg", "FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField("summary_sd", "FLOAT64", mode="NULLABLE"),
        #
        bigquery.SchemaField("coverage_expectedCount", "INT64", mode="NULLABLE"),
        bigquery.SchemaField("coverage_expectedInterval", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("coverage_observedCount", "INT64", mode="NULLABLE"),
        bigquery.SchemaField("coverage_observedInterval", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("coverage_percentComplete", "FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField("coverage_percentCoverage", "FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField("coverage_datetimeFrom_utc", "TIMESTAMP", mode="NULLABLE"),
        bigquery.SchemaField(
            "coverage_datetimeFrom_local", "DATETIME", mode="NULLABLE"
        ),
        bigquery.SchemaField("coverage_datetimeTo_utc", "TIMESTAMP", mode="NULLABLE"),
        bigquery.SchemaField("coverage_datetimeTo_local", "DATETIME", mode="NULLABLE"),
        #
        # --------------------------------------------------------------------
        # METADATA FIELDS
        bigquery.SchemaField("ingested_at", "TIMESTAMP", mode="REQUIRED"),
        bigquery.SchemaField("updated_at", "TIMESTAMP", mode="REQUIRED"),
        bigquery.SchemaField("refreshed_at", "TIMESTAMP", mode="REQUIRED"),
        #
        # --------------------------------------------------------------------
        # REMOVED FIELDS
        # bigquery.SchemaField("coordinates"),  # mobile sensor
        # bigquery.SchemaField("coordinates_latitude"),  # after normalization
        # bigquery.SchemaField("coordinates_longitude"),  # after normalization
    ],
    "partitioning": bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY,
        field="period_datetimeTo_utc",
    ),
    "clustering_fields": ["sensor_id", "parameter_name"],
    "primary_keys": ["sensor_id", "period_datetimeTo_utc"],
    "foreign_keys": [
        ForeignKey(
            name="fk_measurements_sensor_id",
            referenced_table=TableReference.from_string(
                f"{cfg.getenv('GOOGLE_PROJECT_ID')}.raw.src_openaq__sensors"
            ),
            column_references=[
                ColumnReference(
                    referencing_column="sensor_id",
                    referenced_column="id",
                )
            ],
        )
    ],
}


class OpenAQMeasurementsTable(BaseTable):
    """Table management for the raw measurements table in BigQuery."""

    def __init__(self):
        super().__init__()

        self.dataset_id = OPENAQ_MEASUREMENTS_TABLE_CONFIG["dataset_id"]
        self.table_id = OPENAQ_MEASUREMENTS_TABLE_CONFIG["table_id"]

        self.schema = OPENAQ_MEASUREMENTS_TABLE_CONFIG["schema"]
        self.partitioning = OPENAQ_MEASUREMENTS_TABLE_CONFIG["partitioning"]
        self.clustering_fields = OPENAQ_MEASUREMENTS_TABLE_CONFIG["clustering_fields"]
        self.primary_keys = OPENAQ_MEASUREMENTS_TABLE_CONFIG["primary_keys"]
        self.foreign_keys = OPENAQ_MEASUREMENTS_TABLE_CONFIG["foreign_keys"]

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

    def save_from_staging_bucket(self, **kwargs) -> None:
        """Save data from a GCS bucket to the a Big Query table."""

        verbose = kwargs.get("verbose", 5)
        debug_inline = kwargs.get("debug_inline", True)

        start_time = time.perf_counter()

        if verbose >= 4 and not debug_inline:
            print()
            logger.debug(
                "[GCS/BIGQUERY] Saving all measurements in the staging bucket to Big Query..."
            )

        bucket_id = f"{bq.client.project}-staging"
        bucket_uri = f"gs://{bucket_id}"

        bq.upsert_data(
            table=self,
            merge_keys=["sensor_id", "period_datetimeTo_utc"],
            bucket_uri=bucket_uri,
            prefix_uri="openaq/measurements",
        )

        if verbose >= 3 and not debug_inline:
            logger.success(
                f"Saved all measurements from the staging bucket in {exec_time(start_time, fmt=True)}.\n"
            )

    def save_dataframe_to_gcs(
        self, df: pd.DataFrame, bucket_suffix: str, blob_name: str, **kwargs
    ) -> pd.DataFrame:
        """Save a DataFrame to the a Big Query table."""

        verbose = kwargs.get("verbose", -1)

        start_time = time.perf_counter()

        if verbose >= 4:
            print()
            logger.debug(f"[BIGQUERY] Saving {len(df)} measurements in Big Query...")

        # Clean the dataframe
        cleaned_df = self.clean_dataframe(df)

        # Stream measurements to GCS as Parquet files
        gcs.stream_dataframe_to_gcs(cleaned_df, bucket_suffix, blob_name)

        if verbose >= 3:
            logger.success(
                f"Saved {len(cleaned_df)} measurements into [{self.get_full_table_id()}] in {exec_time(start_time, fmt=True)}.\n"
            )

        return cleaned_df


if __name__ == "__main__":
    logger.info(
        f"This module is intended to be imported, not run directly: {__file__}\n"
    )
    pass
