"""MessagingBackend contract (SPEC-agnostic-architecture.md, section 4.1).

Business logic (producer, consumer) depends only on this Protocol, never on a
concrete backend (Pub/Sub Emulator, real Pub/Sub) — Dependency Inversion.
"""

from typing import Callable, Protocol


class MessagingBackend(Protocol):
    def publish(self, topic: str, message: dict) -> None: ...

    def subscribe(self, subscription: str, callback: Callable[[dict], None]) -> None: ...

    def ensure_topic(self, topic: str) -> None: ...
