from pydantic import BaseModel
from pydantic import ConfigDict
from pydantic import field_validator


def underscore_to_dash(name: str) -> str:
    """Converts underscores in names to dashes, since the existing
    config files seem to prefer dashes."""
    return name.replace("_", "-")


class StrictBase(BaseModel):
    model_config: ConfigDict = ConfigDict(
        extra="forbid", alias_generator=underscore_to_dash, populate_by_name=True
    )


class BenchmarkArgs(StrictBase):
    max_seconds: int
    rate_type: str
    data: str


class Benchmark(StrictBase):
    name: str
    launch_node: int
    target_node: int
    port: int
    args: BenchmarkArgs
    log: bool = True
    target_nodeip: str | None = None


class Model(StrictBase):
    name: str
    max_model_len: int


class FrameworkArgs(StrictBase):
    port: int
    gpu_memory_utilization: float


class Framework(StrictBase):
    name: str
    env: dict[str, str]
    args: FrameworkArgs

    @field_validator("env", mode="before")
    @classmethod
    def ensure_string_vals(
        cls, value: dict[str, str | int | float | bool]
    ) -> dict[str, str]:
        """Ensure that environment variable values are strings."""
        newvalue: dict[str, str] = {}
        for k, v in value.items():
            newvalue[k] = str(v)

        return newvalue


class Hardware(StrictBase):
    node: int
    gpu: int


class MountSpec(StrictBase):
    name: str
    mount_point: str
    uri: str


class Storage(StrictBase):
    mounts: list[MountSpec]


class ModelServer(StrictBase):
    framework: Framework
    hardware: Hardware
    storage: Storage | None = None
    log: bool = True


class ConfigFile(StrictBase):
    benchmarks: list[Benchmark]
    model: Model
    model_server: list[ModelServer]
