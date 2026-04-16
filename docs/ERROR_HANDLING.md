# Error Handling

Coldpress implements robust error handling with clear error messages and proper exit codes.

## Exit Codes

All Coldpress CLI commands return standard Unix exit codes:

| Exit Code | Meaning | Example |
|-----------|---------|---------|
| 0 | Success | Job generated successfully |
| 1 | Application error | File not found, validation failed, template error |
| 2 | Usage error | Missing required option, invalid argument |

### Testing Exit Codes

```bash
# Test success (exit 0)
coldpress generate --config examples/pytorch_ddp_training/config.yaml
echo $?  # Should print: 0

# Test error (exit 1)
coldpress-setup generate cluster non-existent.yaml
echo $?  # Should print: 1

# Test usage error (exit 2)
coldpress generate
echo $?  # Should print: 2
```

## Exception Handling

### Specific Exception Types

Coldpress uses specific exception types instead of broad `except Exception` blocks:

```python
# ✅ Good: Specific exceptions
try:
    with open(template_path, "r") as f:
        template = yaml.safe_load(f)
except (FileNotFoundError, yaml.YAMLError, KeyError, IndexError) as e:
    print(f"Warning: Could not load template {template_path}: {e}")
    return None

# ❌ Bad: Overly broad exception
try:
    with open(template_path, "r") as f:
        template = yaml.safe_load(f)
except Exception as e:  # Catches too much
    return None
```

### Exception Categories

**File Operations:**
- `FileNotFoundError` - File or directory doesn't exist
- `PermissionError` - Insufficient permissions

**YAML Parsing:**
- `yaml.YAMLError` - Invalid YAML syntax
- `yaml.scanner.ScannerError` - YAML scanning errors

**Validation:**
- `pydantic.ValidationError` - Schema validation failed
- `ValueError` - Invalid value or configuration
- `KeyError` - Required key missing from dict
- `IndexError` - List index out of range

**URL/Service:**
- `ValueError` - URL parsing failed
- `AttributeError` - Missing attribute in object
- `KeyError` - Service configuration incomplete

## Error Messages

Coldpress provides clear, actionable error messages:

### Configuration Errors

```bash
$ coldpress-setup generate project invalid-project.yaml
Error: Project config validation failed:
1 validation error for ProjectConfig
namespace
  Field required [type=missing, input_value={...}, input_type=dict]
```

### File Not Found

```bash
$ coldpress generate --config missing.yaml
Error: Config file not found: missing.yaml
```

### Discovery Template Errors

```bash
Warning: Could not load discovery template discovery/invalid.yaml: 
  while scanning for the next token
  found character '\t' that cannot start any token
```

## Validation

Coldpress validates configurations at load time using Pydantic models:

```python
from coldpress_common import validate_config, validate_project_config
from pydantic import ValidationError

try:
    config = validate_config(config_data)
except ValidationError as e:
    print(f"Validation failed: {e}")
    sys.exit(1)
```

This catches errors **before** attempting to generate manifests, saving time and providing clear feedback.

## Best Practices

### 1. Check Exit Codes in Scripts

```bash
#!/bin/bash
set -e  # Exit on any error

coldpress-setup generate cluster cluster/prod.yaml
if [ $? -ne 0 ]; then
    echo "Cluster setup failed!"
    exit 1
fi

coldpress generate --config job.yaml
if [ $? -ne 0 ]; then
    echo "Job generation failed!"
    exit 1
fi
```

### 2. Validate Before Deploying

```bash
# Generate manifests (validates config)
coldpress-setup generate project projects/my-project.yaml

# Review generated manifests
cat manifests/project-my-project-*.yaml

# Apply only after manual review
oc apply -f manifests/project-my-project-*.yaml
```

### 3. Handle Errors Gracefully

```python
import subprocess
import sys

result = subprocess.run(
    ["coldpress", "generate", "--config", "job.yaml"],
    capture_output=True,
    text=True
)

if result.returncode != 0:
    print(f"Error: {result.stderr}")
    sys.exit(result.returncode)
```

## Debugging

### Enable Traceback

For unexpected errors, Coldpress prints a full traceback:

```bash
$ coldpress generate --config bad.yaml
Error generating JobSet: division by zero
Traceback (most recent call last):
  File "coldpress/cli.py", line 365, in generate
    ...
ZeroDivisionError: division by zero
```

### Verbose YAML Errors

YAML parsing errors show line and column numbers:

```bash
Warning: Could not load discovery template discovery/bad.yaml:
  while parsing a block mapping
  in "discovery/bad.yaml", line 5, column 3
  expected <block end>, but found '<block mapping start>'
  in "discovery/bad.yaml", line 6, column 4
```

## Related

- [Validation](VALIDATION.md) - Pydantic model validation
- [Labels](LABELS.md) - Resource labels for querying and cleanup
