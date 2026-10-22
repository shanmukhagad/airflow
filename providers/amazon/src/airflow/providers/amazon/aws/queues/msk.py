from __future__ import annotations

import os
from typing import TYPE_CHECKING, Any
from urllib.parse import parse_qs, urlparse

try:
    from airflow.providers.common.messaging.providers.base_provider import BaseMessageQueueProvider
except ImportError:
    from airflow.exceptions import AirflowOptionalProviderFeatureException

    raise AirflowOptionalProviderFeatureException(
        "The 'common.messaging' provider version >= 2.0.0 is required to use message queue providers. "
        "Install it with: pip install 'apache-airflow-providers-amazon[common.messaging]'"
    )

if TYPE_CHECKING:
    from airflow.triggers.base import BaseTrigger


class MskMessageQueueProvider(BaseMessageQueueProvider):
    """Message Queue Provider adapter for Amazon Managed Streaming for Apache Kafka (MSK)."""

    scheme = "msk"

    def queue_matches(self, queue_name: str) -> bool:
        """Return True if the given queue name or URI matches this provider's scheme."""
        return queue_name.startswith(f"{self.scheme}://") or self.scheme_matches(queue_name)

    def trigger_class(self) -> type[BaseTrigger]:
        """Return the underlying trigger class responsible for executing event watching."""
        try:
            from airflow.providers.apache.kafka.triggers.await_message import AwaitMessageTrigger

            return AwaitMessageTrigger
        except ImportError as e:
            from airflow.exceptions import AirflowOptionalProviderFeatureException

            raise AirflowOptionalProviderFeatureException(
                "The 'apache-kafka' provider is required to use the Amazon MSK queue provider. "
                "Install it with: pip install 'apache-airflow-providers-apache-kafka[msk]'"
            ) from e

    def trigger_kwargs(self, queue_name: str, **kwargs: Any) -> dict[str, Any]:
        """Parse queue URIs and keyword arguments into trigger arguments."""
        topics = kwargs.pop("topics", None)
        bootstrap_servers = kwargs.pop("bootstrap_servers", None)
        conn_id = kwargs.pop("kafka_config_id", None)
        uri_kwargs: dict[str, Any] = {}

        if queue_name.startswith(f"{self.scheme}://"):
            try:
                parsed = urlparse(queue_name)
                if parsed.netloc and not bootstrap_servers:
                    bootstrap_servers = parsed.netloc

                path_clean = parsed.path.strip("/")
                if path_clean and not topics:
                    topics = [t for t in path_clean.split(",") if t]

                if parsed.query:
                    for k, v in parse_qs(parsed.query).items():
                        if v:
                            uri_kwargs[k] = v[0] if len(v) == 1 else v
            except Exception as err:
                raise ValueError(f"Invalid MSK queue URI '{queue_name}': {err}") from err

        if not topics:
            raise ValueError(
                "At least one topic must be provided via 'topics' or in the queue URI "
                "(e.g., 'msk://broker:9098/my-topic')."
            )

        kafka_config_id = (
            conn_id
            or uri_kwargs.get("kafka_config_id")
            or uri_kwargs.get("conn_id")
            or os.getenv("KAFKA_DEFAULT_CONN_ID", "kafka_default")
        )

        result = {
            "topics": topics,
            "kafka_config_id": kafka_config_id,
            **uri_kwargs,
            **kwargs,
        }

        if bootstrap_servers:
            result["bootstrap_servers"] = bootstrap_servers

        return result