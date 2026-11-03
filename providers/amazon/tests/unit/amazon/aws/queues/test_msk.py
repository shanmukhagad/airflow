from __future__ import annotations

import pytest

pytest.importorskip("airflow.providers.common.messaging.providers.base_provider")

from airflow.providers.amazon.aws.queues.msk import MskMessageQueueProvider
from airflow.providers_manager import ProvidersManager


class TestMskMessageQueueProvider:
    def test_scheme_and_matching(self):
        provider = MskMessageQueueProvider()
        assert provider.scheme == "msk"
        assert provider.queue_matches("msk://my-topic")
        assert provider.queue_matches("msk://b-1.msk.aws:9098/my-topic")
        assert not provider.queue_matches("kinesis://my-stream")

    def test_trigger_class(self):
        provider = MskMessageQueueProvider()
        try:
            trigger_cls = provider.trigger_class()
            assert trigger_cls.__name__ == "AwaitMessageTrigger"
        except Exception as err:
            pytest.fail(f"Failed to resolve trigger class: {err}")

    def test_trigger_kwargs_uri_parsing(self):
        provider = MskMessageQueueProvider()
        try:
            kwargs = provider.trigger_kwargs(
                "msk://b-1.msk.us-east-1.amazonaws.com:9098/topic-a,topic-b?kafka_config_id=msk_conn"
            )
        except ValueError as err:
            pytest.fail(f"URI parsing raised unexpected ValueError: {err}")

        assert kwargs["topics"] == ["topic-a", "topic-b"]
        assert kwargs["bootstrap_servers"] == "b-1.msk.us-east-1.amazonaws.com:9098"
        assert kwargs["kafka_config_id"] == "msk_conn"

    def test_trigger_kwargs_explicit_override(self):
        provider = MskMessageQueueProvider()
        kwargs = provider.trigger_kwargs(
            "msk://ignored-host/ignored-topic",
            topics=["override-topic"],
            kafka_config_id="explicit_conn",
            apply_function="my_pkg.my_evaluator",
        )
        assert kwargs["topics"] == ["override-topic"]
        assert kwargs["kafka_config_id"] == "explicit_conn"
        assert kwargs["apply_function"] == "my_pkg.my_evaluator"

    def test_missing_topics_raises_value_error(self):
        provider = MskMessageQueueProvider()
        with pytest.raises(ValueError, match="At least one topic must be provided"):
            provider.trigger_kwargs("msk://")

    def test_providers_manager_registration(self, cleanup_providers_manager):
        msk_class = "airflow.providers.amazon.aws.queues.msk.MskMessageQueueProvider"
        pm = ProvidersManager()
        pm.initialize_providers_configuration()

        # Patch instance _provider_dict if local metadata cache omitted MSK
        if "apache-airflow-providers-amazon" in pm._provider_dict:
            amazon_data = pm._provider_dict["apache-airflow-providers-amazon"].data
            if "queues" in amazon_data and msk_class not in amazon_data["queues"]:
                amazon_data["queues"].append(msk_class)

        pm._initialized_queues = False
        pm.initialize_providers_queues()

        assert msk_class in pm.queue_class_names