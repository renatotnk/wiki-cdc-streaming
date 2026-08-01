"""GcsStorageHandler — StorageBackend implementation for GCS.

Bucket creation is deliberately left as a cloud-side setup step
(documented in RUNBOOK.md), never an implicit side effect of application
code — per P3 (no cloud resource turned on by default).
"""

import os
from typing import Any

from google.cloud import storage

from src.handlers.storage.base import bytes_to_dataframe, dataframe_to_bytes


class GcsStorageHandler:
    def __init__(self) -> None:
        self._bucket_name = os.environ["BUCKET_NAME"]
        self._client = storage.Client()

    def write(self, df: Any, path: str, format: str = "delta") -> None:
        payload = dataframe_to_bytes(df, format)
        blob = self._client.bucket(self._bucket_name).blob(path)
        blob.upload_from_string(payload)

    def read(self, path: str, format: str = "delta") -> Any:
        blob = self._client.bucket(self._bucket_name).blob(path)
        return bytes_to_dataframe(blob.download_as_bytes(), format)

    def resolve_uri(self, logical_path: str) -> str:
        return f"gs://{self._bucket_name}/{logical_path}"
