"""PubSubCloudHandler — MessagingBackend implementation for real Pub/Sub.

Uses the client library's default credential resolution (Application
Default Credentials / GOOGLE_APPLICATION_CREDENTIALS) — no emulator
channel involved.
"""

import os

from google.cloud import pubsub_v1

from src.handlers.messaging.base import _BasePubSubHandler


class PubSubCloudHandler(_BasePubSubHandler):
    def __init__(self) -> None:
        super().__init__(
            project_id=os.environ["PUBSUB_PROJECT_ID"],
            publisher=pubsub_v1.PublisherClient(),
            subscriber=pubsub_v1.SubscriberClient(),
        )
