"""Topic/subscription naming — single source of truth shared by producer
and consumer, so the convention only lives in one place (DRY).
"""

from src.handlers.messaging.base import default_subscription_name

TOPIC_RECENTCHANGE_RAW = "recentchange-raw"
SUBSCRIPTION_RECENTCHANGE_RAW = default_subscription_name(TOPIC_RECENTCHANGE_RAW)
