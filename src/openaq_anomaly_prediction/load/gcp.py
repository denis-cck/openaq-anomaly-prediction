import io
import os
import time
from typing import Any, Literal, Union, overload

import pandas as pd
import pyarrow.parquet as pq
from google.cloud import bigquery, storage
from google.cloud.bigquery.table import PrimaryKey, TableConstraints
from google.cloud.exceptions import BadRequest, GoogleCloudError, NotFound

from openaq_anomaly_prediction.config import Configuration as cfg  # noqa: F401
from openaq_anomaly_prediction.load.schemas.base_table import BaseTable
from openaq_anomaly_prediction.utils.helpers import exec_time, format_duration
from openaq_anomaly_prediction.utils.logger import ProgressLogger, logger

GCS_BUCKETS = [
    {
        "name": "staging",
        "soft_delete_policy.duration": 0,
        "lifecycle_rules": [
            {"action": {"type": "Delete"}, "condition": {"age": 1}}  # 1 day
        ],
    },
    {"name": "archives"},
    {"name": "logs"},
    {"name": "exports"},
    {"name": "assets"},
    {"name": "backups"},
]


class CloudStorageClient:
    """Google Cloud Storage client wrapper."""

    def __init__(self):
        self.client = storage.Client()

    def _init_buckets(self, **kwargs) -> None:
        """Initialize GCS buckets to reserve namespaces."""

        verbose = kwargs.get("verbose", 6)

        project_id = self.client.project

        if verbose >= 4:
            logger.debug(f"[GCS] Initializing GCS buckets for project: [{project_id}]")

        for bucket_cfg in GCS_BUCKETS:
            bucket_name = f"{project_id}-{bucket_cfg.get('name')}"
            full_bucket_name = f"gs://{bucket_name}"

            try:
                self.client.get_bucket(bucket_name)

                if verbose >= 6:
                    logger.trace(f"Bucket already exists: {full_bucket_name}")
            except NotFound:
                bucket = self.client.create_bucket(
                    bucket_name, project=project_id, location="EU"
                )

                patched = False
                if bucket_cfg.get("soft_delete_policy.duration", None) is not None:
                    bucket.soft_delete_policy.retention_duration_seconds = 0
                    patched = True

                if bucket_cfg.get("lifecycle_rules", None) is not None:
                    bucket.lifecycle_rules = bucket_cfg["lifecycle_rules"]
                    patched = True

                if patched:
                    bucket.patch()

                if verbose >= 5:
                    logger.trace(f"Created Bucket: {full_bucket_name}")

    def stream_dataframe_to_gcs(
        self, df: pd.DataFrame, bucket_name: str, blob_name: str
    ) -> None:
        """Stream a dataFrame to a GCS bucket."""

        bucket_id = f"{self.client.project}-{bucket_name}"

        # Load the dataframe in the RAM (buffer)
        with io.BytesIO() as buffer:
            df.to_parquet(
                buffer,
                index=False,
                engine="pyarrow",
                coerce_timestamps="us",  # forces microseconds
                allow_truncated_timestamps=True,
            )

            # Seek back to the start of the buffer so GCS can read it
            buffer.seek(0)

            # Upload to GCS
            bucket = self.client.bucket(bucket_id)
            blob = bucket.blob(blob_name)
            blob.upload_from_file(buffer, content_type="application/octet-stream")

    def clear_staging_bucket(self, prefix: str = "") -> None:
        """Deletes all objects in the staging bucket."""

        bucket_name = f"{self.client.project}-staging"
        bucket = self.client.bucket(bucket_name)
        blobs = bucket.list_blobs(prefix=prefix)

        if blobs:
            bucket.delete_blobs(list(blobs))


# ============================================================================


class BigQueryClient:
    """BigQuery client wrapper."""

    def __init__(self):
        self.client = bigquery.Client(location="EU")

    @overload
    def query(self, query: str, **kwargs: Any) -> Any: ...

    @overload
    def query(
        self, query: str, *, dry_run: Literal[False] = False, **kwargs: Any
    ) -> pd.DataFrame: ...

    @overload
    def query(self, query: str, *, dry_run: Literal[True], **kwargs: Any) -> None: ...

    def query(self, query: str, **kwargs) -> Union[pd.DataFrame, None]:
        dry_run = kwargs.get("dry_run", False)
        query_cache = kwargs.get("query_cache", True)
        query_parameters = kwargs.get("query_parameters", None)

        query_parameters_fmt = []
        if query_parameters is not None:
            for param in query_parameters:
                if isinstance(param[2], list):
                    query_parameters_fmt.append(
                        bigquery.ArrayQueryParameter(param[0], param[1], param[2])
                    )
                else:
                    query_parameters_fmt.append(
                        bigquery.ScalarQueryParameter(param[0], param[1], param[2])
                    )

        try:
            job_config = bigquery.QueryJobConfig(
                dry_run=dry_run,
                use_query_cache=query_cache,
                query_parameters=query_parameters_fmt,
            )
            query_job = self.client.query(query, job_config=job_config)

            if dry_run:
                print(
                    f"This query will process {query_job.total_bytes_processed / 10**6:.2f} MB."
                )
                return None

            return query_job.result().to_dataframe(create_bqstorage_client=True)

        except GoogleCloudError as e:
            error = "BigQuery Error: "
            # Table or Dataset not found (404)
            status_code = getattr(e, "code", "Unknown")
            if status_code == 404:
                error += f"Not Found ({status_code})"
            if status_code == 400:
                error += f"Bad Request ({status_code})"

            logger.error(error)
            print({e.message})

            return None  # Or raise a custom, cleaner error

        except Exception as e:
            raise e

        # EXAMPLE QUERIES:

        # IN CLAUSE (UNNEST)
        # query = """
        #     SELECT
        #         *
        #     FROM `openaq-anomaly-prediction.dev_intermediate.int_seoul__measurements_segments`
        #     WHERE location_id IN UNNEST(@test_locations_ids)
        #     ORDER BY location_id, sensor_name, datetimeto_utc ASC
        # """
        # query_parameters = [
        #     ("test_locations_ids", "STRING", ["12", "34", "56"]),
        # ]
        # df = bq.query(query, query_parameters=query_parameters, dry_run=False)

    def create_dataset_if_not_exists(self, dataset_id: str, **kwargs) -> None:
        """Create a BigQuery dataset if it does not exist."""

        verbose = kwargs.get("verbose", 5)

        start_time = time.perf_counter()

        # GOOGLE_PROJECT_ID = cfg.getenv("GOOGLE_PROJECT_ID")
        # DATASET_NAME = cfg.getenv("GOOGLE_BIGQUERY_DATASET")  # "default"
        try:
            # Check if dataset exists
            self.client.get_dataset(dataset_id)  # Make an API request.
            if verbose >= 6:
                logger.trace(
                    f"Dataset already exists: [{self.client.project}.{dataset_id}] in {exec_time(start_time, fmt=True)}"
                )
        except NotFound:
            # logger.debug(f"Dataset not found: [{dataset_id}]")
            dataset = bigquery.Dataset(self.client.dataset(dataset_id))
            dataset.location = "EU"
            dataset = self.client.create_dataset(
                dataset, timeout=30
            )  # Make an API request.

            if verbose >= 5:
                logger.trace(
                    f"Created dataset: [{self.client.project}.{dataset.dataset_id}] in {exec_time(start_time, fmt=True)}"
                )

    def create_table_if_not_exists(self, table: BaseTable, **kwargs) -> None:
        """Create the BigQuery table if it does not exist."""

        verbose = kwargs.get("verbose", 5)
        table_id = kwargs.get("table_id", table.get_full_table_id())

        start_time = time.perf_counter()

        if table.dataset_id is None or table.table_id is None or table.schema is None:
            raise ValueError("Table ID and schema must be defined.")

        # Ensure dataset exists
        self.create_dataset_if_not_exists(table.dataset_id)

        try:
            # Check if table exists
            found_table = self.client.get_table(table_id)  # Make an API request.
            if verbose >= 6:
                logger.trace(
                    f"Table already exists: [{self.client.project}.{found_table.dataset_id}.{found_table.table_id}] in {exec_time(start_time, fmt=True)}"
                )
        except NotFound:
            # TABLE DEFINITION
            new_table = bigquery.Table(table_id, schema=table.schema)

            # PRIMARY KEYS (UNENFORCED)
            new_table.table_constraints = TableConstraints(
                primary_key=PrimaryKey(columns=table.primary_keys),
                foreign_keys=table.foreign_keys,
            )

            # PARTITIONING
            if table.partitioning is not None:
                new_table.time_partitioning = table.partitioning

            # CLUSTERING
            if table.clustering_fields is not None:
                new_table.clustering_fields = table.clustering_fields

            # CREATE TABLE
            created_table = self.client.create_table(new_table, exists_ok=True)

            if verbose >= 5:
                logger.trace(
                    f"Created table: [{self.client.project}.{created_table.dataset_id}.{created_table.table_id}] in {exec_time(start_time, fmt=True)}"
                )

    def load_dataframe_to_bq(
        self, df: pd.DataFrame, table: BaseTable, **kwargs
    ) -> None:
        """Save a pandas DataFrame to a BigQuery table."""

        verbose = kwargs.get("verbose", 6)
        mode = kwargs.get("mode", "truncate")
        table_id = kwargs.get("table_id", table.get_full_table_id())

        start_time = time.perf_counter()

        if mode not in ["append", "truncate"]:
            raise ValueError("Mode must be either 'append' or 'truncate'.")

        # Ensure table exists
        self.create_table_if_not_exists(table, table_id=table_id)

        # EXECUTE THE LOAD QUERY ---------------------------------------------

        job_config = bigquery.LoadJobConfig(
            # 'WRITE_APPEND' to add data, 'WRITE_TRUNCATE' to overwrite
            write_disposition="WRITE_TRUNCATE"
            if mode == "truncate"
            else "WRITE_APPEND",
            schema=table.schema,
            # source_format=bigquery.SourceFormat.PARQUET, # Needed?
        )

        job = self.client.load_table_from_dataframe(df, table_id, job_config=job_config)
        job.result()  # Wait for the load to finish

        if verbose >= 5:
            logger.trace(
                f"Loaded dataframe in [{table_id}] in {exec_time(start_time, fmt=True)}"
            )

    def load_bucket_to_bq(self, uri: str, table: BaseTable, **kwargs) -> None:
        """Save a bucket to a BigQuery table."""

        verbose = kwargs.get("verbose", 6)
        mode = kwargs.get("mode", "truncate")
        table_id = kwargs.get("table_id", table.get_full_table_id())

        start_time = time.perf_counter()

        if mode not in ["append", "truncate"]:
            raise ValueError("Mode must be either 'append' or 'truncate'.")

        # Ensure table exists
        self.create_table_if_not_exists(table, table_id=table_id)

        # EXECUTE THE LOAD QUERY ---------------------------------------------
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.PARQUET,
            write_disposition="WRITE_TRUNCATE"
            if mode == "truncate"
            else "WRITE_APPEND",
            schema=table.schema,
        )

        job = self.client.load_table_from_uri(uri, table_id, job_config=job_config)
        job.result()  # Wait for the load to finish

        if verbose >= 5:
            logger.trace(
                f"Loaded dataframe in [{table_id}] in {exec_time(start_time, fmt=True)}"
            )

    def get_staging_table_id(self, target_table_id: str) -> str:
        """Get the staging table ID for a given target table ID."""
        return f"{target_table_id}_staged"

    def generate_merge_query(self, table: BaseTable, merge_keys: list[str]) -> str:
        """Generate a BigQuery MERGE query string."""

        if table.schema is None:
            raise ValueError("Table schema must be defined.")

        # INIT ---------------------------------------------------------------
        target_table_id = table.get_full_table_id()
        staging_table_id = self.get_staging_table_id(target_table_id)

        all_columns = [field.name for field in table.schema]
        meta_columns = ["ingested_at", "updated_at", "refreshed_at"]
        immutable_columns = merge_keys + ["ingested_at"]

        data_columns = [
            col
            for col in all_columns
            if col not in merge_keys and col not in meta_columns
        ]

        # MERGE ON: primary keys
        join_condition = " AND ".join(
            [f"target.{pk} = staged.{pk}" for pk in merge_keys]
        )

        # MATCHED WITH CHANGES: update all columns except "id" and "ingested_at"
        change_condition = " OR ".join(
            [
                f"target.{col} IS DISTINCT FROM staged.{col}" for col in data_columns
            ]  # check all except primary keys and meta columns
        )
        full_update = ", ".join(
            [
                f"target.{col} = staged.{col}"
                for col in all_columns
                if col not in immutable_columns
            ]  # update all except primary keys and ingested_at
        )

        # MATCHED WITHOUT CHANGES: update only "refreshed_at" field
        refreshed_update = "target.refreshed_at = staged.refreshed_at"

        # NOT MATCHED: insert new row
        insert_cols = ", ".join(all_columns)
        insert_vals = ", ".join([f"staged.{col}" for col in all_columns])

        merge_query = f"""
            MERGE `{target_table_id}` target
            USING `{staging_table_id}` staged
            ON {join_condition}

            -- MATCHED WITH CHANGES: update all columns except "id" and "ingested_at"
            WHEN MATCHED AND ({change_condition}) THEN
                UPDATE SET {full_update}

            -- MATCHED WITHOUT CHANGES: update "refreshed_at" field
            WHEN MATCHED THEN
                UPDATE SET {refreshed_update}

            -- NOT MATCHED: insert new row
            WHEN NOT MATCHED THEN
                INSERT ({insert_cols}) VALUES ({insert_vals})
        """

        return merge_query

    def upsert_data(
        self,
        table: BaseTable,
        merge_keys: list[str],
        df: pd.DataFrame | None = None,
        bucket_uri: str = "",
        prefix_uri: str = "",
        **kwargs,
    ) -> None:
        """Perform an upsert (merge) of data into a BigQuery table."""

        verbose = kwargs.get("verbose", 6)
        debug_inline = kwargs.get("debug_inline", True)

        if df is None and bucket_uri == "":
            raise ValueError("Either df or bucket_uri must be provided.")

        if df is not None and bucket_uri != "":
            raise ValueError("Only one of df or bucket_uri must be provided.")

        if table.schema is None:
            raise ValueError("Table schema must be defined.")

        if df is not None and df.empty:
            logger.warning("Upsert skipped: DataFrame is empty.")
            return

        start_time = time.perf_counter()

        # INIT ---------------------------------------------------------------
        target_table_id = table.get_full_table_id()
        staging_table_id = self.get_staging_table_id(target_table_id)

        self.create_table_if_not_exists(table)  # Ensure target table exists

        if verbose >= 4 and not debug_inline and df is not None:
            print()
            logger.debug(f"Starting upsert of {len(df)} rows...")
        elif verbose >= 4 and not debug_inline and bucket_uri != "":
            print()
            logger.debug(f"Starting upsert of data from bucket_uri [{bucket_uri}]...")

        # GENERATE THE MERGE QUERY -------------------------------------------
        merge_query = self.generate_merge_query(table, merge_keys)
        # print(merge_query)

        # LOAD THE STAGING TABLE ---------------------------------------------
        # From dataframe
        if df is not None:
            self.load_dataframe_to_bq(df, table=table, table_id=staging_table_id)

        # From GCS bucket (staging)
        elif bucket_uri != "":
            full_uri = (
                f"{bucket_uri}/{prefix_uri}/*.parquet"
                if prefix_uri != ""
                else f"{bucket_uri}/*.parquet"
            )
            self.load_bucket_to_bq(full_uri, table=table, table_id=staging_table_id)

        # EXECUTE THE MERGE QUERY --------------------------------------------
        query_time = time.perf_counter()

        job_config = bigquery.QueryJobConfig(
            use_query_cache=False,  # Optional: Ensures it doesn't use old results
            labels={"process": "openaq_upsert"},
        )
        query_job = self.client.query(merge_query, job_config=job_config)
        query_job.result()  # This blocks the script until the database is done

        if verbose >= 5:
            logger.trace(
                f"Merged the dataframe in [{target_table_id}] in {exec_time(query_time, fmt=True)}"
            )

        # CLEAN UP -----------------------------------------------------------
        deleted_time = time.perf_counter()

        # Delete the staging table
        self.client.delete_table(staging_table_id, not_found_ok=True)
        if verbose >= 6:
            logger.trace(
                f"Deleted the staging table [{staging_table_id}] in {exec_time(deleted_time, fmt=True)}"
            )

        # Delete the staging files from GCS if applicable
        if bucket_uri != "":
            deleted_time = time.perf_counter()
            gcs.clear_staging_bucket(prefix=prefix_uri)
            if verbose >= 6:
                logger.trace(
                    f"Deleted the staging bucket [{prefix_uri}] in {exec_time(deleted_time, fmt=True)}"
                )

        # FINAL LOGGING ------------------------------------------------------
        if verbose >= 3 and not debug_inline:
            if df is not None:
                logger.success(
                    f"[DATAFRAME LOADING] Saved dataframe into [{target_table_id}] in {exec_time(start_time, fmt=True)}.\n"
                )
            elif bucket_uri != "":
                logger.success(
                    f"[GCS LOADING] Saved all files from {prefix_uri} into [{target_table_id}] in {exec_time(start_time, fmt=True)}.\n"
                )

    def export_table_to_disk(
        self, query: str, filename: str, dry_run: bool = True, **kwargs
    ) -> None:
        """Export a BigQuery query to a local parquet file."""

        start_time = time.perf_counter()

        query_job = bq.client.query(
            query,
            job_config=bigquery.QueryJobConfig(dry_run=dry_run, use_query_cache=True),
        )
        query_time = exec_time(start_time)

        if dry_run:
            print(
                f"This query will process {query_job.total_bytes_processed / 10**6:.2f} MB. If you want to execute it, set dry_run=False."
            )

        else:
            logger.info(f"Executed the query in {format_duration(query_time)}")
            # This is slow AF, but it works, so we'll go with it for now
            # We should really use GCS for this though
            result_iterator = query_job.result()
            iterable = result_iterator.to_arrow_iterable()

            export_path = os.path.join(cfg.DATA_EXPORT_PATH, filename)
            logger.debug(f"Exporting the BigQuery table to [{filename}]...")

            writer = None
            current_rows = 0
            total_rows = result_iterator.total_rows
            progress = ProgressLogger()
            for batch in iterable:
                if writer is None:
                    # Initialize the writer with the schema from the first batch
                    writer = pq.ParquetWriter(export_path, batch.schema)

                writer.write_batch(batch)
                current_rows += batch.num_rows

                progress.print(
                    "Streaming the table to a local file...",
                    current_progress=current_rows,
                    total_progress=total_rows,
                    prefix_msg="PROGRESS",
                    last=(current_rows >= total_rows),
                )

            logger.success(
                f"Exported the BigQuery table to a local file in {exec_time(start_time, fmt=True)}"
            )

            if writer:
                writer.close()


# ============================================================================


bq = BigQueryClient()
gcs = CloudStorageClient()


# ============================================================================


# # URI (source), destination (bq table)
# def upsert_bucket_to_bq(bucket_short_name: str, uri: str, table: BaseTable, **kwargs) -> None:
#     """Load data from GCS URI to a BigQuery table."""

#     verbose = kwargs.get("verbose", 5)

#     bucket_name = f"{gcs.client.project}-{bucket_short_name}"

#     if table.schema is None:
#         raise ValueError("Table schema must be defined.")

#     # Check if URI is valid and if there are files
#     if gcs.client.get_bucket

#     start_time = time.perf_counter()

#     # if mode not in ["append", "truncate"]:
#     #     raise ValueError("Mode must be either 'append' or 'truncate'.")

#     # Ensure table exists
#     bq.create_table_if_not_exists(table)

#     # EXECUTE THE LOAD QUERY ---------------------------------------------

#     job_config = bigquery.LoadJobConfig(
#         source_format=bigquery.SourceFormat.PARQUET,
#         # 'WRITE_APPEND' to add data, 'WRITE_TRUNCATE' to overwrite
#         write_disposition="WRITE_TRUNCATE" if mode == "truncate" else "WRITE_APPEND",
#         schema=table.schema,
#     )

#     job = bq.client.load_table_from_uri(
#         uri, table.get_full_table_id(), job_config=job_config
#     )
#     job.result()  # Wait for the load to finish

#     if verbose >= 5:
#         logger.trace(
#             f"Loaded data from [{uri}] into [{table.get_full_table_id()}] in {exec_time(start_time, fmt=True)}"
#         )


# def test_query():
#     """Function to test BigQuery connection."""
#     # Perform a query.
#     QUERY = (
#         "SELECT name FROM `bigquery-public-data.usa_names.usa_1910_2013` "
#         'WHERE state = "TX" '
#         "LIMIT 1"
#     )
#     query_job = client.query(QUERY)  # API request
#     rows = query_job.result()  # Waits for query to finish

#     for row in rows:
#         print(row.name)


if __name__ == "__main__":
    logger.info(
        f"This module is intended to be imported, not run directly: {__file__}\n"
    )
    pass
