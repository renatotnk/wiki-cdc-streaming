"""S3CompatibleStorageHandler — StorageBackend implementation serving any
S3-compatible endpoint: MinIO (local dev) and real AWS S3 (cloud, required
by Databricks Free Edition's S3-only External Volumes) are the same API,
differing only in endpoint/credentials — one class, not two near-identical
ones (see docs/trade-offs.md).

S3_ENDPOINT_URL set -> MinIO (or another self-hosted S3-compatible store):
local and free, so auto-creating the bucket on construction is harmless.
S3_ENDPOINT_URL unset -> real AWS S3: bucket creation is deliberately NOT
automatic there (P3: no cloud resource turned on by default) — same
reasoning as GcsStorageHandler.
"""

import io
import os
from typing import Any
from urllib.parse import urlparse

from minio import Minio

from src.handlers.storage.base import (
    dataframe_to_parquet_bytes,
    parquet_bytes_to_dataframe,
    require_supported_format,
)

AWS_S3_ENDPOINT = "s3.amazonaws.com"


class S3CompatibleStorageHandler:
    def __init__(self) -> None:
        self._bucket = os.environ["BUCKET_NAME"]
        access_key = os.environ["AWS_ACCESS_KEY_ID"]
        secret_key = os.environ["AWS_SECRET_ACCESS_KEY"]
        endpoint_url = os.environ.get("S3_ENDPOINT_URL")

        if endpoint_url:
            parsed_endpoint = urlparse(endpoint_url)
            self._client = Minio(
                parsed_endpoint.netloc,
                access_key=access_key,
                secret_key=secret_key,
                secure=parsed_endpoint.scheme == "https",
            )
            if not self._client.bucket_exists(self._bucket):
                self._client.make_bucket(self._bucket)
        else:
            self._client = Minio(
                AWS_S3_ENDPOINT,
                access_key=access_key,
                secret_key=secret_key,
                secure=True,
                region=os.environ["AWS_REGION"],
            )

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
