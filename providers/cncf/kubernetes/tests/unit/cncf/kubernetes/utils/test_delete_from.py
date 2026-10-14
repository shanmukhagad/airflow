# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
"""Unit tests for ``delete_from.py``.

No cluster is required. API classes are patched so we only assert which delete
method a YAML dict was translated into.

+-----------------------------------------------+------------------------------------------+-------------------------------+
| Test                                          | What / how                               | Mocked                        |
+===============================================+==========================================+===============================+
| test_deletes_namespaced_pod_via_core_v1_api   | v1 Pod → CoreV1Api.delete_namespaced_pod | client.CoreV1Api              |
| test_deletes_namespaced_deployment_via_apps   | apps/v1 Deployment → AppsV1Api           | client.AppsV1Api              |
| test_deletes_cluster_scoped_resource_...      | ClusterRole → delete_cluster_role        | client.RbacAuthorizationV1Api |
| test_strips_k8s_io_suffix_for_apiextensions   | CRD group strip → ApiextensionsV1Api     | client.ApiextensionsV1Api     |
| test_uses_default_deletion_body_when_none     | body is None → DEFAULT_DELETION_BODY     | client.CoreV1Api              |
| test_forwards_explicit_body_and_extra_kwargs  | custom body + pretty forwarded           | client.CoreV1Api              |
| test_yaml_namespace_takes_precedence          | metadata.namespace beats the arg         | client.CoreV1Api              |
| test_verbose_prints_status                    | stdout is "kind deleted. status=..."     | client.CoreV1Api              |
| test_pod_list_stamps_kind_and_deletes_each    | PodList items stamped and deleted        | client.CoreV1Api              |
| test_generic_list_keeps_each_item_kind        | mixed List keeps each item kind          | CoreV1Api and AppsV1Api       |
| test_collects_api_exceptions_and_continues    | first item fails, second still runs      | CoreV1Api (side_effect)       |
| test_skips_none_documents                     | [None, pod, None] calls dict once        | delete_from_dict              |
| test_iterates_every_document                  | two docs → two delete_from_dict calls    | delete_from_dict              |
| test_str_concatenates_each_api_exception      | FailToDeleteError reason/body text       | none                          |
+-----------------------------------------------+------------------------------------------+-------------------------------+
"""
from __future__ import annotations

from collections import namedtuple
from unittest.mock import Mock, patch

import pytest
from kubernetes.client.rest import ApiException

from airflow.providers.cncf.kubernetes.utils.delete_from import (
    DEFAULT_DELETION_BODY,
    FailToDeleteError,
    delete_from_dict,
    delete_from_yaml,
)

CLIENT = "airflow.providers.cncf.kubernetes.utils.delete_from.client"


def _doc(*, kind, name, api_version="v1", namespace=None):
    metadata = {"name": name}
    if namespace is not None:
        metadata["namespace"] = namespace
    return {"apiVersion": api_version, "kind": kind, "metadata": metadata}


def _api(*methods, status="Success"):
    """A client mock whose hasattr() is True only for the given delete methods.

    A bare MagicMock would report every ``delete_namespaced_*`` as present and
    send cluster-scoped kinds down the wrong branch.
    """
    api = Mock(spec=list(methods))
    for method in methods:
        getattr(api, method).return_value = Mock(status=status)
    return api


class TestDeleteFromYamlSingleItem:
    def test_deletes_namespaced_pod_via_core_v1_api(self):
        k8s_client = Mock()
        api = _api("delete_namespaced_pod")

        with patch(f"{CLIENT}.CoreV1Api", return_value=api) as api_cls:
            delete_from_dict(
                k8s_client,
                _doc(kind="Pod", name="nginx"),
                body=None,
                namespace="default",
            )

        api_cls.assert_called_once_with(k8s_client)
        api.delete_namespaced_pod.assert_called_once_with(
            name="nginx",
            namespace="default",
            body=DEFAULT_DELETION_BODY,
        )

    def test_deletes_namespaced_deployment_via_apps_v1_api(self):
        api = _api("delete_namespaced_deployment")

        with patch(f"{CLIENT}.AppsV1Api", return_value=api) as api_cls:
            delete_from_dict(
                Mock(),
                _doc(kind="Deployment", name="web", api_version="apps/v1"),
                body=None,
                namespace="prod",
            )

        api_cls.assert_called_once()
        api.delete_namespaced_deployment.assert_called_once_with(
            name="web",
            namespace="prod",
            body=DEFAULT_DELETION_BODY,
        )

    def test_deletes_cluster_scoped_resource_without_namespace(self):
        api = _api("delete_cluster_role")

        with patch(f"{CLIENT}.RbacAuthorizationV1Api", return_value=api):
            delete_from_dict(
                Mock(),
                _doc(kind="ClusterRole", name="admin", api_version="rbac.authorization.k8s.io/v1"),
                body=None,
                namespace="ignored",
            )

        api.delete_cluster_role.assert_called_once_with(name="admin", body=DEFAULT_DELETION_BODY)

    def test_strips_k8s_io_suffix_for_apiextensions_group(self):
        api = _api("delete_custom_resource_definition")

        with patch(f"{CLIENT}.ApiextensionsV1Api", return_value=api) as api_cls:
            delete_from_dict(
                Mock(),
                _doc(
                    kind="CustomResourceDefinition",
                    name="widgets.example.com",
                    api_version="apiextensions.k8s.io/v1",
                ),
                body=None,
                namespace="default",
            )

        api_cls.assert_called_once()
        api.delete_custom_resource_definition.assert_called_once_with(
            name="widgets.example.com",
            body=DEFAULT_DELETION_BODY,
        )

    def test_uses_default_deletion_body_when_body_is_none(self):
        api = _api("delete_namespaced_pod")

        with patch(f"{CLIENT}.CoreV1Api", return_value=api):
            delete_from_dict(Mock(), _doc(kind="Pod", name="nginx"), body=None, namespace="default")

        _, kwargs = api.delete_namespaced_pod.call_args
        assert kwargs["body"] is DEFAULT_DELETION_BODY
        assert kwargs["body"].propagation_policy == "Background"
        assert kwargs["body"].grace_period_seconds == 5

    def test_forwards_explicit_body_and_extra_kwargs(self):
        api = _api("delete_namespaced_pod")
        body = {"propagationPolicy": "Foreground"}

        with patch(f"{CLIENT}.CoreV1Api", return_value=api):
            delete_from_dict(
                Mock(),
                _doc(kind="Pod", name="nginx"),
                body=body,
                namespace="default",
                pretty="true",
            )

        api.delete_namespaced_pod.assert_called_once_with(
            name="nginx",
            namespace="default",
            body=body,
            pretty="true",
        )

    def test_yaml_namespace_takes_precedence_over_argument(self):
        api = _api("delete_namespaced_pod")

        with patch(f"{CLIENT}.CoreV1Api", return_value=api):
            delete_from_dict(
                Mock(),
                _doc(kind="Pod", name="nginx", namespace="from-yaml"),
                body=None,
                namespace="from-arg",
            )

        api.delete_namespaced_pod.assert_called_once_with(
            name="nginx",
            namespace="from-yaml",
            body=DEFAULT_DELETION_BODY,
        )

    def test_verbose_prints_status(self, capsys):
        api = _api("delete_namespaced_pod")

        with patch(f"{CLIENT}.CoreV1Api", return_value=api):
            delete_from_dict(
                Mock(),
                _doc(kind="Pod", name="nginx"),
                body=None,
                namespace="default",
                verbose=True,
            )

        assert capsys.readouterr().out == "pod deleted. status='Success'\n"


class TestDeleteFromDict:
    def test_pod_list_stamps_kind_and_deletes_each_item(self):
        api = _api("delete_namespaced_pod")
        data = {
            "apiVersion": "v1",
            "kind": "PodList",
            "items": [
                {"metadata": {"name": "a"}},
                {"metadata": {"name": "b"}},
            ],
        }

        with patch(f"{CLIENT}.CoreV1Api", return_value=api):
            delete_from_dict(Mock(), data, body=None, namespace="ns")

        assert [call.kwargs["name"] for call in api.delete_namespaced_pod.call_args_list] == ["a", "b"]
        assert data["items"][0]["kind"] == "Pod"
        assert data["items"][0]["apiVersion"] == "v1"

    def test_generic_list_keeps_each_item_kind(self):
        pod_api = _api("delete_namespaced_pod")
        deploy_api = _api("delete_namespaced_deployment")
        data = {
            "apiVersion": "v1",
            "kind": "List",
            "items": [
                _doc(kind="Pod", name="nginx"),
                _doc(kind="Deployment", name="web", api_version="apps/v1"),
            ],
        }

        with (
            patch(f"{CLIENT}.CoreV1Api", return_value=pod_api),
            patch(f"{CLIENT}.AppsV1Api", return_value=deploy_api),
        ):
            delete_from_dict(Mock(), data, body=None, namespace="default")

        pod_api.delete_namespaced_pod.assert_called_once_with(
            name="nginx",
            namespace="default",
            body=DEFAULT_DELETION_BODY,
        )
        deploy_api.delete_namespaced_deployment.assert_called_once_with(
            name="web",
            namespace="default",
            body=DEFAULT_DELETION_BODY,
        )

    def test_collects_api_exceptions_and_continues_remaining_items(self):
        api = _api("delete_namespaced_pod")
        first = ApiException(status=404, reason="Not Found")
        first.body = 'pods "a" not found'
        api.delete_namespaced_pod.side_effect = [first, Mock(status="Success")]

        data = {
            "apiVersion": "v1",
            "kind": "PodList",
            "items": [
                {"metadata": {"name": "a"}},
                {"metadata": {"name": "b"}},
            ],
        }

        with patch(f"{CLIENT}.CoreV1Api", return_value=api), pytest.raises(FailToDeleteError) as exc_info:
            delete_from_dict(Mock(), data, body=None, namespace="default")

        assert api.delete_namespaced_pod.call_count == 2
        assert exc_info.value.api_exceptions == [first]
        assert str(exc_info.value) == 'Error from server (Not Found):pods "a" not found\n'


class TestDeleteFromYaml:
    def test_skips_none_documents(self):
        k8s_client = Mock()
        pod = _doc(kind="Pod", name="nginx")

        with patch(
            "airflow.providers.cncf.kubernetes.utils.delete_from.delete_from_dict"
        ) as delete_from_dict_mock:
            delete_from_yaml(
                k8s_client=k8s_client,
                yaml_objects=[None, pod, None],
                namespace="default",
                body=None,
            )

        delete_from_dict_mock.assert_called_once_with(
            k8s_client=k8s_client,
            data=pod,
            body=None,
            namespace="default",
            verbose=False,
        )

    def test_iterates_every_document(self):
        first = _doc(kind="Pod", name="a")
        second = _doc(kind="Pod", name="b")

        with patch(
            "airflow.providers.cncf.kubernetes.utils.delete_from.delete_from_dict"
        ) as delete_from_dict_mock:
            delete_from_yaml(k8s_client=Mock(), yaml_objects=[first, second])

        assert [call.kwargs["data"] for call in delete_from_dict_mock.call_args_list] == [first, second]


class TestFailToDeleteError:
    def test_str_concatenates_each_api_exception(self):
        exception_k8s = namedtuple("Exception_k8s", ["reason", "body"])
        error = FailToDeleteError(
            [
                exception_k8s(reason="Not Found", body="missing "),
                exception_k8s(reason="Conflict", body="busy"),
            ]
        )

        assert str(error) == "Error from server (Not Found):missing \nError from server (Conflict):busy\n"
