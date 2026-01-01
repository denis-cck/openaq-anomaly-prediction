import time

import pandas as pd
from google.cloud import bigquery
from google.cloud.bigquery.table import (  # noqa: F401
    ColumnReference,
    ForeignKey,
    TableReference,
)

from openaq_anomaly_prediction.config import Configuration as cfg  # noqa: F401
from openaq_anomaly_prediction.load.gcp import bq
from openaq_anomaly_prediction.load.schemas.base_table import BaseTable
from openaq_anomaly_prediction.utils.helpers import exec_time
from openaq_anomaly_prediction.utils.logger import logger

OPENAQ_SENSORS_TABLE_CONFIG = {
    "dataset_id": "raw",
    "table_id": "src_openaq__sensors",
    "schema": [
        # --------------------------------------------------------------------
        # RAW FIELDS
        bigquery.SchemaField(
            "id",
            "STRING",
            mode="REQUIRED",
            description="Unique sensor identifier (OpenAQ ID)",
        ),
        bigquery.SchemaField("location_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("name", "STRING", mode="NULLABLE"),
        # --------------------------------------------------------------------
        # FLATTENED FIELDS
        bigquery.SchemaField("parameter_id", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("parameter_name", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("parameter_units", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("parameter_displayName", "STRING", mode="NULLABLE"),
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
    "partitioning": None,
    # "partitioning": bigquery.TimePartitioning(
    #     type_=bigquery.TimePartitioningType.DAY,
    #     field="ds",  # Partition by our timestamp
    # ),
    "clustering_fields": ["id", "location_id"],
    "primary_keys": ["id"],
    "foreign_keys": [
        ForeignKey(
            name="fk_sensors_location_id",
            referenced_table=TableReference.from_string(
                f"{cfg.getenv('GOOGLE_PROJECT_ID')}.raw.src_openaq__locations"
            ),
            column_references=[
                ColumnReference(
                    referencing_column="location_id",
                    referenced_column="id",
                )
            ],
        )
    ],
}


class OpenAQSensorsTable(BaseTable):
    """Table management for the raw sensors table in BigQuery."""

    def __init__(self):
        super().__init__()

        self.dataset_id = OPENAQ_SENSORS_TABLE_CONFIG["dataset_id"]
        self.table_id = OPENAQ_SENSORS_TABLE_CONFIG["table_id"]

        self.schema = OPENAQ_SENSORS_TABLE_CONFIG["schema"]
        self.partitioning = OPENAQ_SENSORS_TABLE_CONFIG["partitioning"]
        self.clustering_fields = OPENAQ_SENSORS_TABLE_CONFIG["clustering_fields"]
        self.primary_keys = OPENAQ_SENSORS_TABLE_CONFIG["primary_keys"]
        self.foreign_keys = OPENAQ_SENSORS_TABLE_CONFIG["foreign_keys"]

    def clean_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean and sanitize the sensors dataframe."""

        if self.schema is None:
            raise ValueError("Table schema must be defined.")

        cleaned_df = df.copy()

        # SANITIZE DATAFRAME -------------------------------------------------

        cleaned_df = self.sanitize_dataframe(cleaned_df)

        # # Convert ID columns to string types
        # id_columns = [col for col in cleaned_df.columns if "id" in col]
        # for col in id_columns:
        #     cleaned_df[col] = cleaned_df[col].astype(
        #         str
        #     )  # bq optimization for indexing

        # # Convert columns with "datetime" in their names to datetime types
        # datetime_columns = [col for col in cleaned_df.columns if "datetime" in col]
        # for col in datetime_columns:
        #     if "local" in col:
        #         cleaned_df[col] = pd.to_datetime(
        #             cleaned_df[col], utc=False
        #         ).dt.tz_localize(None)  # remove timezone info (naive datetime)
        #     else:
        #         cleaned_df[col] = pd.to_datetime(cleaned_df[col], utc=True)

        # # Replace dots in column names with underscores
        # cleaned_df.columns = [
        #     c.replace(".", "_") for c in cleaned_df.columns
        # ]  # bq compatibility

        # # Replace all empty or whitespace-only strings with NaN (only in text columns)
        # obj_cols = cleaned_df.select_dtypes(include=["object", "string"]).columns
        # cleaned_df[obj_cols] = cleaned_df[obj_cols].replace(r"^\s*$", pd.NA, regex=True)

        # CLEAN COLUMNS ------------------------------------------------------
        # No specific column cleaning needed for sensors at the moment

        # METADATA FIELDS ----------------------------------------------------

        now = pd.Timestamp.now(tz="UTC")
        cleaned_df["ingested_at"] = now
        cleaned_df["updated_at"] = now
        cleaned_df["refreshed_at"] = now

        # Reorder columns to match schema
        schema_fields = [field.name for field in self.schema]
        cleaned_df = cleaned_df.reindex(columns=schema_fields)

        return cleaned_df

    def save_dataframe(self, df: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """Save a DataFrame to the a Big Query table."""

        verbose = kwargs.get("verbose", 5)
        skip_bq = kwargs.get("skip_bq", False)

        if "ingested_at" in df.columns:
            raise ValueError("Dataframe was already saved.")

        start_time = time.perf_counter()

        if verbose >= 4:
            logger.debug(f"[BIGQUERY] Saving {len(df)} sensors in Big Query...")

        # Clean the dataframe
        cleaned_df = self.clean_dataframe(df)

        # Upsert sensors to BigQuery
        if skip_bq:
            if verbose >= 3:
                logger.info("[BIGQUERY] Skipping BigQuery upload")
        else:
            bq.upsert_data(self, merge_keys=["id"], df=cleaned_df)

            if verbose >= 3:
                logger.success(
                    f"Saved {len(df)} sensors into [{self.get_full_table_id()}] in {exec_time(start_time, fmt=True)}.\n"
                )

        return cleaned_df


if __name__ == "__main__":
    logger.info(
        f"This module is intended to be imported, not run directly: {__file__}\n"
    )
    pass
