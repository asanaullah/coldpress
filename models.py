# Assisted by: Gemini 3
from pydantic import BaseModel
from pydantic import ConfigDict


def underscore_to_dash(name: str) -> str:
    """Converts underscores in names to dashes, since the existing
    config files seem to prefer dashes."""
    return name.replace("_", "-")


class StrictBase(BaseModel):
    model_config: ConfigDict = ConfigDict(
        extra="forbid", alias_generator=underscore_to_dash, populate_by_name=True
    )


class ComputeJobStorage(StrictBase):
    results: str
    models: str = "coldpress-model-storage"
    pvc_namespace: str | None = None  # Optional, defaults to job's namespace


class Task(StrictBase):
    name: str
    template: str
    node: int
    # Params captures all the user variables (e.g. max_seconds, model_name, etc.)
    params: dict[str, str | int | float | bool] = {}
    # Optional overrides maintained from original design
    log: bool = True


class ConfigFile(StrictBase):
    tasks: list[Task]
    storage: ComputeJobStorage | None = None
