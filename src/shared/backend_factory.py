"""Single point of backend selection (SPEC-phase1-ingestion.md section 3).

No other module reads MESSAGING_BACKEND/STORAGE_BACKEND directly — always
through these two functions (DRY: one place decides, everywhere else just
consumes the interface).
"""

import os

from src.handlers.messaging.pubsub_cloud_handler import PubSubCloudHandler
from src.handlers.messaging.pubsub_emulator_handler import PubSubEmulatorHandler
from src.handlers.storage.gcs_storage_handler import GcsStorageHandler
from src.handlers.storage.minio_storage_handler import MinioStorageHandler
from src.interfaces.messaging import MessagingBackend
from src.interfaces.storage import StorageBackend


def get_messaging_backend() -> MessagingBackend:
    backend = os.environ["MESSAGING_BACKEND"]  # "emulator" | "cloud"
    return {"emulator": PubSubEmulatorHandler, "cloud": PubSubCloudHandler}[backend]()


def get_storage_backend() -> StorageBackend:
    backend = os.environ["STORAGE_BACKEND"]  # "minio" | "gcs"
    return {"minio": MinioStorageHandler, "gcs": GcsStorageHandler}[backend]()
