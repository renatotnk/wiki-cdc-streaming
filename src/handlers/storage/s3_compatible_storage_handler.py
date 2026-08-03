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

The Minio client (and the AWS credentials it needs) is built lazily, on
first write()/read(), not in __init__ — resolve_uri() only needs
BUCKET_NAME, and pipelines/bronze/*.py call only resolve_uri(). On
Databricks, real S3 access happens through Spark's own Hadoop S3A
connector, using whatever credential Unity Catalog vends for a registered
External Location — genuinely no AWS_ACCESS_KEY_ID/SECRET needed for that
path, so this class shouldn't demand them just to resolve a URI string.
"""

import io
import os
from typing import Any
from urllib.parse import urlparse

from minio import Minio

from src.handlers.storage.base import bytes_to_dataframe, dataframe_to_bytes

AWS_S3_ENDPOINT = "s3.amazonaws.com"


class S3CompatibleStorageHandler:
    def __init__(self) -> None:
        self._bucket = os.environ["BUCKET_NAME"]
        self._client: Minio | None = None

    def _get_client(self) -> Minio:
        if self._client is not None:
            return self._client

        access_key = os.environ["AWS_ACCESS_KEY_ID"]
        secret_key = os.environ["AWS_SECRET_ACCESS_KEY"]
        endpoint_url = os.environ.get("S3_ENDPOINT_URL")

        if endpoint_url:
            parsed_endpoint = urlparse(endpoint_url)
            client = Minio(
                parsed_endpoint.netloc,
                access_key=access_key,
                secret_key=secret_key,
                secure=parsed_endpoint.scheme == "https",
            )
            if not client.bucket_exists(self._bucket):
                client.make_bucket(self._bucket)
        else:
            client = Minio(
                AWS_S3_ENDPOINT,
                access_key=access_key,
                secret_key=secret_key,
                secure=True,
                region=os.environ["AWS_REGION"],
            )

        self._client = client
        return client

    def write(self, df: Any, path: str, format: str = "delta") -> None:
        payload = dataframe_to_bytes(df, format)
        self._get_client().put_object(self._bucket, path, io.BytesIO(payload), length=len(payload))

    def read(self, path: str, format: str = "delta") -> Any:
        response = self._get_client().get_object(self._bucket, path)
        try:
            return bytes_to_dataframe(response.read(), format)
        finally:
            response.close()
            response.release_conn()

    def resolve_uri(self, logical_path: str) -> str:
        return f"s3a://{self._bucket}/{logical_path}"
