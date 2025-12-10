# pyright: reportUnusedCallResult=false

from typing import Callable
import pytest
import pydantic
import yaml

import models

valid_config = """
benchmarks:
  - name: guidellm
    launch_node: 0 
    target_node: 0
    log: True
    port: 8000
    args: 
      max-seconds: 30
      rate-type: throughput
      data: '"prompt_tokens=256,output_tokens=128"'


model: 
  name: 'ibm-granite/granite-3.3-8b-instruct'
  max_model_len: 10000

model_server:
  - framework:
      name: vllm
      env:
        VLLM_USE_V1: 1 
      args:
        port: 8000
        gpu-memory-utilization: 0.6
    hardware:
      node: 0
      gpu: 0
    log: True
"""

wrong_data_type = valid_config.replace("launch_node: 0", "launch_node: foo")

unknown_top_level_field = (
    valid_config
    + """
unknown_field: "this shouldn't be here"
"""
)

missing_required_fields = """
benchmarks:
model:
model_server:
"""


config_tests = [
    [
        "empty config",
        "",
        False,
        lambda x: x.value.errors()[0]["msg"]
        == "Input should be a valid dictionary or instance of ConfigFile",
    ],
    ["valid config", valid_config, True, None],
    [
        "wrong data type",
        wrong_data_type,
        False,
        lambda x: x.value.errors()[0]["msg"]
        == "Input should be a valid integer, unable to parse string as an integer",
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
        lambda x: x.value.errors()[0]["msg"] == "Input should be a valid list"
        and x.value.errors()[0]["loc"] == ("benchmarks",),
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
