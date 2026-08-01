"""S3CompatibleStorageHandler: confirms the branch on S3_ENDPOINT_URL builds
the right client (MinIO vs. real AWS S3), with no real network call -- per
FIRST (Isolated), the Minio client construction is mocked, not the config
logic under test.
"""

from unittest.mock import MagicMock, patch

import pytest

from src.handlers.storage.s3_compatible_storage_handler import S3CompatibleStorageHandler

MINIO_MODULE_PATH = "src.handlers.storage.s3_compatible_storage_handler.Minio"


@pytest.fixture
def storage_env(monkeypatch):
    monkeypatch.setenv("BUCKET_NAME", "test-bucket")
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "test-access-key")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "test-secret-key")


def test_endpoint_set_builds_a_client_pointed_at_that_minio_host(storage_env, monkeypatch):
    monkeypatch.setenv("S3_ENDPOINT_URL", "http://localhost:9000")
    mock_client = MagicMock()
    mock_client.bucket_exists.return_value = True

    with patch(MINIO_MODULE_PATH, return_value=mock_client) as mock_minio:
        S3CompatibleStorageHandler()

    mock_minio.assert_called_once_with(
        "localhost:9000",
        access_key="test-access-key",
        secret_key="test-secret-key",
        secure=False,
    )


def test_endpoint_set_creates_the_bucket_when_it_does_not_exist_yet(storage_env, monkeypatch):
    monkeypatch.setenv("S3_ENDPOINT_URL", "http://localhost:9000")
    mock_client = MagicMock()
    mock_client.bucket_exists.return_value = False

    with patch(MINIO_MODULE_PATH, return_value=mock_client):
        S3CompatibleStorageHandler()

    mock_client.make_bucket.assert_called_once_with("test-bucket")


def test_https_endpoint_builds_a_secure_client(storage_env, monkeypatch):
    monkeypatch.setenv("S3_ENDPOINT_URL", "https://minio.internal:9000")
    mock_client = MagicMock()
    mock_client.bucket_exists.return_value = True

    with patch(MINIO_MODULE_PATH, return_value=mock_client) as mock_minio:
        S3CompatibleStorageHandler()

    mock_minio.assert_called_once_with(
        "minio.internal:9000",
        access_key="test-access-key",
        secret_key="test-secret-key",
        secure=True,
    )


def test_no_endpoint_set_builds_a_client_pointed_at_real_aws_s3(storage_env, monkeypatch):
    monkeypatch.delenv("S3_ENDPOINT_URL", raising=False)
    monkeypatch.setenv("AWS_REGION", "us-east-1")
    mock_client = MagicMock()

    with patch(MINIO_MODULE_PATH, return_value=mock_client) as mock_minio:
        S3CompatibleStorageHandler()

    mock_minio.assert_called_once_with(
        "s3.amazonaws.com",
        access_key="test-access-key",
        secret_key="test-secret-key",
        secure=True,
        region="us-east-1",
    )


def test_no_endpoint_set_never_touches_bucket_existence_or_creation(storage_env, monkeypatch):
    """P3 (no cloud resource turned on by default) -- unlike the MinIO path,
    real S3 never auto-creates a bucket, same reasoning as GcsStorageHandler.
    """
    monkeypatch.delenv("S3_ENDPOINT_URL", raising=False)
    monkeypatch.setenv("AWS_REGION", "us-east-1")
    mock_client = MagicMock()

    with patch(MINIO_MODULE_PATH, return_value=mock_client):
        S3CompatibleStorageHandler()

    mock_client.bucket_exists.assert_not_called()
    mock_client.make_bucket.assert_not_called()
