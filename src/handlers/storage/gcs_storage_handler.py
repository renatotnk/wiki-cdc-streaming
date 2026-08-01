"""GcsStorageHandler — StorageBackend implementation for GCS.

Bucket creation is deliberately left as a cloud-side setup step
(documented in RUNBOOK.md), never an implicit side effect of application
code — per P3 (no cloud resource turned on by default).
"""

import os
from typing import Any

from google.cloud import storage

from src.handlers.storage.base import (
    dataframe_to_parquet_bytes,
    parquet_bytes_to_dataframe,
    require_supported_format,
)


class GcsStorageHandler:
    def __init__(self) -> None:
        self._bucket_name = os.environ["BUCKET_NAME"]
        self._client = storage.Client()

    def write(self, df: Any, path: str, format: str = "delta") -> None:
        require_supported_format(format)
        payload = dataframe_to_parquet_bytes(df)
        blob = self._client.bucket(self._bucket_name).blob(path)
        blob.upload_from_string(payload)

    def read(self, path: str, format: str = "delta") -> Any:
        require_supported_format(format)
        blob = self._client.bucket(self._bucket_name).blob(path)
        return parquet_bytes_to_dataframe(blob.download_as_bytes())

    def resolve_uri(self, logical_path: str) -> str:
        return f"gs://{self._bucket_name}/{logical_path}"
