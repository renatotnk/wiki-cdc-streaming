"""Single point of backend selection (SPEC-phase1-ingestion.md section 3).

No other module reads MESSAGING_BACKEND/STORAGE_BACKEND directly — always
through these two functions (DRY: one place decides, everywhere else just
consumes the interface).

Imports are deliberately function-local, not at module level: a caller
that only needs get_storage_backend() (e.g. pipelines/bronze/*.py, which
never touches messaging at all) shouldn't have to transitively import
google-cloud-pubsub just because this module also knows how to build a
MessagingBackend. Matters concretely on Databricks, where the pipeline's
Python environment only needs to satisfy whichever backend is actually
selected by STORAGE_BACKEND/MESSAGING_BACKEND, not every backend this
project has ever supported.
"""

import os

from src.interfaces.messaging import MessagingBackend
from src.interfaces.storage import StorageBackend


def get_messaging_backend() -> MessagingBackend:
    from src.handlers.messaging.pubsub_cloud_handler import PubSubCloudHandler
    from src.handlers.messaging.pubsub_emulator_handler import PubSubEmulatorHandler

    backend = os.environ["MESSAGING_BACKEND"]  # "emulator" | "cloud"
    return {"emulator": PubSubEmulatorHandler, "cloud": PubSubCloudHandler}[backend]()


def get_storage_backend() -> StorageBackend:
    backend = os.environ["STORAGE_BACKEND"]  # "minio" | "s3" | "gcs"
    if backend in ("minio", "s3"):
        from src.handlers.storage.s3_compatible_storage_handler import S3CompatibleStorageHandler

        return S3CompatibleStorageHandler()  # reads S3_ENDPOINT_URL itself -- set for minio, unset for s3

    from src.handlers.storage.gcs_storage_handler import GcsStorageHandler

    return {"gcs": GcsStorageHandler}[backend]()
