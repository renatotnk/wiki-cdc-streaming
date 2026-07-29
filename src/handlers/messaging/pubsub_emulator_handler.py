"""PubSubEmulatorHandler — MessagingBackend implementation for local dev.

Relies on google-cloud-pubsub's built-in emulator detection: when
PUBSUB_EMULATOR_HOST is set, PublisherClient/SubscriberClient route to the
local emulator over an insecure channel instead of real Pub/Sub.
"""

import os

from google.cloud import pubsub_v1

from src.handlers.messaging.base import _BasePubSubHandler


class PubSubEmulatorHandler(_BasePubSubHandler):
    def __init__(self) -> None:
        if "PUBSUB_EMULATOR_HOST" not in os.environ:
            raise RuntimeError(
                "PUBSUB_EMULATOR_HOST must be set in the environment when "
                "MESSAGING_BACKEND=emulator"
            )
        super().__init__(
            project_id=os.environ["PUBSUB_PROJECT_ID"],
            publisher=pubsub_v1.PublisherClient(),
            subscriber=pubsub_v1.SubscriberClient(),
        )
