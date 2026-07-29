"""Shared Pub/Sub publish/subscribe/ensure_topic logic.

PubSubEmulatorHandler and PubSubCloudHandler talk to the identical Pub/Sub
API — the only real difference between them is how the underlying client is
constructed (emulator channel vs. real credentials). That one seam lives in
each subclass's `__init__`; everything else is genuine duplication avoided
here, not a speculative abstraction (both concrete implementations already
exist and are required by MESSAGING_BACKEND, per SPEC section 3).
"""

import json
from collections.abc import Callable

from google.api_core.exceptions import AlreadyExists
from google.cloud import pubsub_v1

from src.shared.logger import get_logger

logger = get_logger("pubsub_handler")


def default_subscription_name(topic: str) -> str:
    """Single, authoritative subscription-naming convention (DRY) — used by
    `ensure_topic` when creating the subscription, and by producer/consumer
    code when deciding which subscription name to pass to `subscribe()`.
    """
    return f"{topic}-sub"


class _BasePubSubHandler:
    def __init__(
        self,
        project_id: str,
        publisher: pubsub_v1.PublisherClient,
        subscriber: pubsub_v1.SubscriberClient,
    ) -> None:
        self._project_id = project_id
        self._publisher = publisher
        self._subscriber = subscriber

    def ensure_topic(self, topic: str) -> None:
        topic_path = self._publisher.topic_path(self._project_id, topic)
        try:
            self._publisher.create_topic(request={"name": topic_path})
        except AlreadyExists:
            pass

        subscription_path = self._subscriber.subscription_path(
            self._project_id, default_subscription_name(topic)
        )
        try:
            self._subscriber.create_subscription(
                request={"name": subscription_path, "topic": topic_path}
            )
        except AlreadyExists:
            pass

    def publish(self, topic: str, message: dict) -> None:
        topic_path = self._publisher.topic_path(self._project_id, topic)
        payload = json.dumps(message, default=str).encode("utf-8")
        future = self._publisher.publish(topic_path, payload)
        future.result()

    def subscribe(self, subscription: str, callback: Callable[[dict], None]) -> None:
        subscription_path = self._subscriber.subscription_path(self._project_id, subscription)

        def _on_message(message: pubsub_v1.subscriber.message.Message) -> None:
            payload = json.loads(message.data.decode("utf-8"))
            callback(payload)
            message.ack()

        streaming_pull_future = self._subscriber.subscribe(subscription_path, callback=_on_message)
        logger.info(f"subscribed to {subscription_path}")
        try:
            streaming_pull_future.result()
        except KeyboardInterrupt:
            streaming_pull_future.cancel()
            streaming_pull_future.result()
