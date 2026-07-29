"""Shared Parquet serialization helpers for storage handlers.

MinioStorageHandler and GcsStorageHandler both read/write polars
DataFrames as Parquet bytes — the bucket/object I/O differs per SDK, but
this conversion is identical, so it's extracted here (DRY) instead of
duplicated in both files.
"""

import io

import polars as pl

SUPPORTED_FORMATS = {"parquet"}


def require_supported_format(format: str) -> None:
    if format not in SUPPORTED_FORMATS:
        raise ValueError(
            f"unsupported format {format!r} — Phase 1 storage handlers only write "
            "Parquet (Delta is introduced in Phase 2)"
        )


def dataframe_to_parquet_bytes(df: pl.DataFrame) -> bytes:
    buffer = io.BytesIO()
    df.write_parquet(buffer)
    return buffer.getvalue()


def parquet_bytes_to_dataframe(data: bytes) -> pl.DataFrame:
    return pl.read_parquet(io.BytesIO(data))
