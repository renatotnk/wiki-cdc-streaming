"""Shared serialization helpers for storage handlers.

S3CompatibleStorageHandler and GcsStorageHandler both read/write polars
DataFrames as bytes — the bucket/object I/O differs per SDK, but this
conversion is identical, so it's extracted here (DRY) instead of duplicated
in both files. Parquet is Phase 1's format (raw ingestion); ndjson is
Phase 2's (scripts/fetch_wiki_sitematrix.py's snapshot files, read back via
Spark's standard line-delimited JSON source).
"""

import io

import polars as pl


def _dataframe_to_parquet_bytes(df: pl.DataFrame) -> bytes:
    buffer = io.BytesIO()
    df.write_parquet(buffer)
    return buffer.getvalue()


def _parquet_bytes_to_dataframe(data: bytes) -> pl.DataFrame:
    return pl.read_parquet(io.BytesIO(data))


def _dataframe_to_ndjson_bytes(df: pl.DataFrame) -> bytes:
    buffer = io.BytesIO()
    df.write_ndjson(buffer)
    return buffer.getvalue()


def _ndjson_bytes_to_dataframe(data: bytes) -> pl.DataFrame:
    return pl.read_ndjson(io.BytesIO(data))


_SERIALIZE_BY_FORMAT = {"parquet": _dataframe_to_parquet_bytes, "ndjson": _dataframe_to_ndjson_bytes}
_DESERIALIZE_BY_FORMAT = {"parquet": _parquet_bytes_to_dataframe, "ndjson": _ndjson_bytes_to_dataframe}


def dataframe_to_bytes(df: pl.DataFrame, format: str) -> bytes:
    if format not in _SERIALIZE_BY_FORMAT:
        raise ValueError(f"unsupported format {format!r} — supported: {sorted(_SERIALIZE_BY_FORMAT)}")
    return _SERIALIZE_BY_FORMAT[format](df)


def bytes_to_dataframe(data: bytes, format: str) -> pl.DataFrame:
    if format not in _DESERIALIZE_BY_FORMAT:
        raise ValueError(f"unsupported format {format!r} — supported: {sorted(_DESERIALIZE_BY_FORMAT)}")
    return _DESERIALIZE_BY_FORMAT[format](data)
