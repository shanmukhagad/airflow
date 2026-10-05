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

from __future__ import annotations

import importlib

import pytest
import yaml

from airflow.plugins_manager import is_valid_plugin
from airflow.providers.apache.hive.macros import hive as hive_macros
from airflow.providers.apache.hive.plugins.hive import HivePlugin

from tests_common.test_utils.paths import AIRFLOW_PROVIDERS_ROOT_PATH

HIVE_PROVIDER_YAML = AIRFLOW_PROVIDERS_ROOT_PATH / "apache" / "hive" / "provider.yaml"


class TestHivePlugin:
    def test_name_is_hive(self):
        assert HivePlugin.name == "hive"

    def test_macros_are_exposed_under_their_function_names(self):
        assert {macro.__name__: macro for macro in HivePlugin.macros} == {
            "max_partition": hive_macros.max_partition,
            "closest_ds_partition": hive_macros.closest_ds_partition,
        }

    def test_plugin_passes_loader_validation(self):
        assert is_valid_plugin(HivePlugin)

    @pytest.mark.skipif(
        not HIVE_PROVIDER_YAML.exists(),
        reason="provider.yaml is only available when tests are run from sources",
    )
    def test_provider_yaml_plugin_class_resolves_to_hive_plugin(self):
        provider_info = yaml.safe_load(HIVE_PROVIDER_YAML.read_text())
        hive_plugins = [plugin for plugin in provider_info.get("plugins", []) if plugin["name"] == "hive"]
        assert len(hive_plugins) == 1

        module_path, class_name = hive_plugins[0]["plugin-class"].rsplit(".", maxsplit=1)
        assert getattr(importlib.import_module(module_path), class_name) is HivePlugin
