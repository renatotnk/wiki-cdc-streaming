"""StorageBackend contract (SPEC-agnostic-architecture.md, section 4.2).

`df` is typed as Any because it's polymorphic across phases: a polars
DataFrame in Phase 1 (producer/consumer), a Spark DataFrame from Phase 2
onward — the contract itself has no opinion on which.
"""

from typing import Any, Protocol


class StorageBackend(Protocol):
    def write(self, df: Any, path: str, format: str = "delta") -> None: ...

    def read(self, path: str, format: str = "delta") -> Any: ...

    def resolve_uri(self, logical_path: str) -> str: ...
