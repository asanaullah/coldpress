# Coldpress YAML Validation

Coldpress includes comprehensive YAML validation using Pydantic models. This ensures that configuration errors are caught early at file load time, rather than during job execution.

## Overview

The validation system is implemented in the shared `coldpress_common` module and provides type-safe, schema-validated models for all Coldpress configuration files used by both `coldpress` and `coldpress-setup` tools:

- **config.yaml** - Workload configuration (`coldpress` tool)
- **job-spec.yaml** - Task specifications (`coldpress` tool)
- **projects/*.yaml** - Project configuration (both tools)
- **users/*.yaml** - User configuration (`coldpress-setup` tool)

## How It Works

Both `coldpress` and `coldpress-setup` tools validate YAML files automatically:

### `coldpress generate`
1. **Config validation** - Checks project, discovery, output, and files fields
2. **Task spec validation** - Validates container specs, resources, volumes, and dependencies
3. **Project config validation** - Ensures namespace and storage are properly configured

### `coldpress-setup apply`
1. **Project config validation** - Validates namespace, storage, cluster_queue
2. **User config validation** - Validates username and namespaces list

If validation fails, you'll get clear error messages indicating exactly what's wrong and the operation will be aborted before applying to the cluster.

## Example Error Messages

### Missing required field:
```
Error: Task spec validation failed: 1 validation error for TaskSpec
name
  Field required [type=missing, input_value={'containers': [...]}, input_type=dict]
```

### Invalid blocking configuration:
```
Error: Task 'my-task' has blocking='endpoint' but no health_check or readinessProbe
```

### Invalid resource specification:
```
Error: Task spec validation failed: 1 validation error for TaskSpec
containers.0.resources.requests
  Field required [type=missing]
```

## Testing Validation

Run the validation test script to verify the system is working:

```bash
./test_validation.py
```

This will test both valid and invalid configurations to ensure proper error detection.

## Using Validation in Code

You can import and use the validation functions directly from the shared module:

```python
import yaml
from coldpress_common import (
    validate_config,
    validate_task_specs,
    validate_project_config,
    validate_user_config,
)
from pydantic import ValidationError

# Validate config.yaml
try:
    with open("config.yaml") as f:
        config_data = yaml.safe_load(f)
        config = validate_config(config_data)
        print(f"Valid config: {config.project}")
except ValidationError as e:
    print(f"Validation error: {e}")

# Validate user config
try:
    with open("users/username.yaml") as f:
        user_data = yaml.safe_load(f)
        user = validate_user_config(user_data)
        print(f"Valid user: {user.username}")
except ValidationError as e:
    print(f"Validation error: {e}")
```

## Supported Validations

### Config File (`config.yaml`)
- ✓ project (optional, can be overridden via CLI)
- ✓ discovery (optional)
- ✓ output (optional)
- ✓ files (optional list of strings)

### Task Spec (`job-spec.yaml`)
**Required:**
- ✓ name (string)
- ✓ containers (list with at least one container)

**Optional:**
- ✓ blocking ("completion" or "endpoint")
- ✓ health_check (required if blocking="endpoint")
- ✓ resources (requests and limits)
- ✓ volumes (emptyDir, PVC, configMap)
- ✓ env (environment variables)
- ✓ tolerate_all (boolean)
- ✓ network_mode ("host" or "default")
- ✓ privileged (boolean)
- ✓ sys_mounts (host path mounts)

**Container-level:**
- ✓ name, image (required)
- ✓ command, args (optional)
- ✓ workingDir (optional)
- ✓ resources (optional)
- ✓ env (optional)
- ✓ ports (optional)
- ✓ readinessProbe (optional)

### Project Config (`projects/*.yaml`)
**Required:**
- ✓ namespace (string)

**Optional:**
- ✓ cluster_queue (string)
- ✓ storage_class (string)
- ✓ storage (object with results, models, size)

### User Config (`users/*.yaml`)
**Required:**
- ✓ username (string)
- ✓ namespaces (list of strings, at least one required)

**Validation Rules:**
- namespaces list cannot be empty

## Advanced Validation Rules

1. **Endpoint Blocking**: If `blocking: "endpoint"`, task must have either:
   - A `health_check` URL, OR
   - A `readinessProbe` in the first container

2. **Resource Consistency**: GPU resources are automatically tracked and validated

3. **Volume References**: Volume names are validated for internal consistency

4. **Environment Variables**: Can be specified as dict or list format

## Benefits

- **Early Error Detection**: Catch configuration errors before job submission
- **Clear Error Messages**: Pydantic provides detailed, actionable error messages
- **Type Safety**: Ensures fields have correct types (string, int, list, etc.)
- **Schema Documentation**: Pydantic models serve as living documentation
- **IDE Support**: Better autocomplete and type hints when working with configs

## Architecture

### Shared Validation Module

The validation logic is centralized in `coldpress_common/model.py`, which is imported by both:
- `coldpress/cli.py` - Workload generation tool
- `coldpress_setup/cli.py` - Cluster setup tool

This ensures:
- **Consistency**: Same validation rules across both tools
- **Maintainability**: Single source of truth for schemas
- **Early detection**: Errors caught before cluster operations

### Why Shared Module?

Previously, `model.py` was in `coldpress/` and only the `coldpress` tool validated configs. The `coldpress-setup` tool would accept invalid YAMLs and fail during cluster application. Now both tools validate upfront using the same shared models.

## Migration from Legacy

This replaces the old validation system with modern Pydantic 2.0+ models. The validation is:

- More comprehensive
- Better error messages
- Type-safe
- Easier to extend
- Compatible with modern Python tooling
- **Shared between both tools** (new!)

## Adding New Validations

To add new validation rules, edit `coldpress_common/model.py`:

1. Add fields to the appropriate Pydantic model
2. Add `@field_validator` or `@model_validator` decorators for custom logic
3. Export the function in `coldpress_common/__init__.py`
4. Update this documentation
5. Add test cases to `test_validation.py`

Example:

```python
# In coldpress_common/model.py
class TaskSpec(BaseModel):
    name: str
    
    @field_validator("name")
    @classmethod
    def validate_name_format(cls, v):
        if not v.islower():
            raise ValueError("Task name must be lowercase")
        return v
```
