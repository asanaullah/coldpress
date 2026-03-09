# Assisted by: Gemini 3, Claude Sonnet 4.5
# pyright: reportUnusedCallResult=false

from typing import Callable
import pytest
import pydantic
import yaml

import models

valid_config = """
tasks:
  - name: "inference-server"
    template: "vllm-parser"
    node: 0
    params:
      num_gpus: 1
      model: "ibm-granite/granite-3.3-8b-instruct"
      max_model_len: 10000
      port: 8000
      gpu_memory_utilization: 0.6
storage:
  results: researcher-a-storage
"""

wrong_data_type = valid_config.replace("node: 0", "node: foo")

unknown_top_level_field = (
    valid_config
    + """
unknown_field: "this shouldn't be here"
"""
)

missing_required_fields = """
storage:
  results: researcher-a-storage
"""


config_tests = [
    [
        "empty config",
        "",
        False,
        lambda x: (
            x.value.errors()[0]["msg"]
            == "Input should be a valid dictionary or instance of ConfigFile"
        ),
    ],
    ["valid config", valid_config, True, None],
    [
        "wrong data type",
        wrong_data_type,
        False,
        lambda x: (
            x.value.errors()[0]["msg"]
            == "Input should be a valid integer, unable to parse string as an integer"
        ),
    ],
    [
        "unknown top level field",
        unknown_top_level_field,
        False,
        lambda x: x.value.errors()[0]["msg"] == "Extra inputs are not permitted",
    ],
    [
        "missing required fields",
        missing_required_fields,
        False,
        lambda x: (
            x.value.errors()[0]["msg"] == "Field required"
            and x.value.errors()[0]["loc"] == ("tasks",)
        ),
    ],
]
config_test_ids = [test[0] for test in config_tests]


@pytest.mark.parametrize("name,config,valid,check", config_tests, ids=config_test_ids)
def test_config_parser(
    name: str, config: str, valid: bool, check: Callable[[Exception], None]
):
    parsed_config = yaml.safe_load(config)

    if valid:
        models.ConfigFile.model_validate(parsed_config)
    else:
        with pytest.raises(pydantic.ValidationError) as err:
            models.ConfigFile.model_validate(parsed_config)

        if callable(check):
            assert check(err)


class TestConfigEdgeCases:
    """Tests for edge cases in config validation."""

    def test_task_params_mixed_types(self):
        """Verifies params dict accepts str/int/float/bool."""
        config_yaml = """
tasks:
  - name: "test-task"
    template: "test-parser"
    node: 0
    params:
      model: "gpt-4"
      port: 8000
      temperature: 0.7
      debug: true
storage:
  results: test-pvc
"""
        parsed = yaml.safe_load(config_yaml)
        config = models.ConfigFile.model_validate(parsed)

        # Verify mixed types are preserved
        assert config.tasks[0].params["model"] == "gpt-4"
        assert config.tasks[0].params["port"] == 8000
        assert config.tasks[0].params["temperature"] == 0.7
        assert config.tasks[0].params["debug"] is True

    def test_storage_optional_pvc_namespace(self):
        """Verifies pvc_namespace defaults correctly."""
        config_yaml = """
tasks:
  - name: "test-task"
    template: "test-parser"
    node: 0
storage:
  results: test-pvc
"""
        parsed = yaml.safe_load(config_yaml)
        config = models.ConfigFile.model_validate(parsed)

        # pvc_namespace should be None (optional)
        assert config.storage.pvc_namespace is None

    def test_underscore_to_dash_alias(self):
        """Verifies YAML pvc-namespace maps to pvc_namespace."""
        config_yaml = """
tasks:
  - name: "test-task"
    template: "test-parser"
    node: 0
storage:
  results: test-pvc
  pvc-namespace: "admin"
"""
        parsed = yaml.safe_load(config_yaml)
        config = models.ConfigFile.model_validate(parsed)

        # Dash notation should populate underscore field
        assert config.storage.pvc_namespace == "admin"
