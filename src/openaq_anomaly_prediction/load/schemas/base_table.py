import pandas as pd
from google.cloud import bigquery

from openaq_anomaly_prediction.config import Configuration as cfg  # noqa: F401
from openaq_anomaly_prediction.utils.logger import logger


class BaseTable:
    """Inheritable class for BigQuery table management."""

    def __init__(self):
        self.project_id = cfg.getenv("GOOGLE_PROJECT_ID")
        self.dataset_id = None
        self.table_id = None

        self.table_id = None
        self.schema: list[bigquery.SchemaField] | None = None
        self.partitioning = None
        self.clustering_fields = None
        self.primary_keys = []
        self.foreign_keys = []

    def get_full_table_id(self) -> str:
        """Get the full table ID in the format 'project.dataset.table'."""

        if self.dataset_id is None or self.table_id is None:
            raise ValueError("Dataset ID and Table ID must be defined.")

        return f"{self.project_id}.{self.dataset_id}.{self.table_id}"

    def sanitize_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """Sanitize the dataframe according to the schema."""

        if self.schema is None:
            raise ValueError("Table schema must be defined.")

        # Replace dots in column names with underscores
        df.columns = [c.replace(".", "_") for c in df.columns]  # bq compatibility

        # Convert ID columns to string types
        id_columns = [col for col in df.columns if "id" in col]
        for col in id_columns:
            df[col] = df[col].astype(str)  # bq optimization for indexing

        # Convert columns with "datetime" in their names to datetime types
        datetime_fields = [col for col in df.columns if "datetime" in col]
        for col in datetime_fields:
            if "local" in col:
                df[col] = (
                    pd.to_datetime(df[col], utc=False)
                    .dt.tz_localize(None)
                    .dt.floor("us")
                )  # remove timezone info (naive datetime)
            else:
                df[col] = pd.to_datetime(df[col], utc=True).dt.floor("us")

        # Replace all empty or whitespace-only strings with NaN (only in text columns)
        obj_cols = df.select_dtypes(include=["object", "string"]).columns
        df[obj_cols] = df[obj_cols].replace(r"^\s*$", pd.NA, regex=True)

        # Force Float64 dtypes for all FLOAT64 columns (to avoid issues with INT64s)
        float_fields = [
            field.name for field in self.schema if field.field_type == "FLOAT64"
        ]
        for col in float_fields:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce").astype("float64")

        # Force Int64 dtypes for all INT64 columns (to avoid issues with NaNs)
        int_fields = [
            field.name for field in self.schema if field.field_type == "INT64"
        ]
        for col in int_fields:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

        return df


if __name__ == "__main__":
    logger.info(
        f"This module is intended to be imported, not run directly: {__file__}\n"
    )
    pass
