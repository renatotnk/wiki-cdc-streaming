"""MinioStorageHandler — StorageBackend implementation for local dev.

Ensures the configured bucket exists on construction — local and free, so
automating that is harmless. GcsStorageHandler deliberately does NOT do
the cloud equivalent (see its module docstring).
"""

import io
import os
from typing import Any

from minio import Minio

from src.handlers.storage.base import (
    dataframe_to_parquet_bytes,
    parquet_bytes_to_dataframe,
    require_supported_format,
)


class MinioStorageHandler:
    def __init__(self) -> None:
        self._bucket = os.environ["BUCKET_NAME"]
        self._client = Minio(
            os.environ["MINIO_ENDPOINT"],
            access_key=os.environ["MINIO_ACCESS_KEY"],
            secret_key=os.environ["MINIO_SECRET_KEY"],
            secure=False,
        )
        if not self._client.bucket_exists(self._bucket):
            self._client.make_bucket(self._bucket)

    def write(self, df: Any, path: str, format: str = "delta") -> None:
        require_supported_format(format)
        payload = dataframe_to_parquet_bytes(df)
        self._client.put_object(self._bucket, path, io.BytesIO(payload), length=len(payload))

    def read(self, path: str, format: str = "delta") -> Any:
        require_supported_format(format)
        response = self._client.get_object(self._bucket, path)
        try:
            return parquet_bytes_to_dataframe(response.read())
        finally:
            response.close()
            response.release_conn()

    def resolve_uri(self, logical_path: str) -> str:
        return f"s3a://{self._bucket}/{logical_path}"
