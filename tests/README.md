# Coldpress Test Suite

This directory contains the comprehensive test suite for Coldpress.

## Test Files

### Validation Tests
**`test_validation.py`** - Tests Pydantic model validation
- Validates config.yaml schema
- Validates job-spec.yaml schema
- Validates project config schema
- Tests error handling for invalid configs
- Verifies validation catches errors early

### Label Tests
**`test_labels.py`** - Tests standard Kubernetes labels
- Verifies all resources have `app.kubernetes.io/managed-by: coldpress`
- Verifies version labels
- Tests job-specific labels (`coldpress.io/job-id`)
- Validates labels on: JobSet, Service, ConfigMap, Namespace, LocalQueue, PVC, RBAC

### Security Tests
**`test_security.py`** - Tests security improvements
- Tests JSON injection prevention (json.dumps vs f-strings)
- Tests Kubernetes name validation (lowercase, alphanumeric, dashes, dots)
- Tests Pydantic validators enforce naming rules
- Audits for hardcoded temp file vulnerabilities
- Validates user input sanitization

### Error Handling Tests
**`test_error_handling.py`** - Tests error handling
- Tests exit code propagation (0/1/2)
- Tests specific exception types (FileNotFoundError, yaml.YAMLError, etc.)
- Tests validation error messages
- Verifies Click command error handling

### Exit Code Tests
**`test_exit_codes.sh`** - Shell script tests for exit codes
- Tests success case (exit 0)
- Tests error cases (exit 1)
- Tests usage errors (exit 2)
- Verifies shell-level exit code propagation

### RoCE Disabled Tests
**`test_roce_disabled.py`** - Tests RoCE NIC support is disabled
- Verifies no RDMA resources in ClusterQueue
- Verifies no NetworkAttachmentDefinitions generated
- Verifies `roce_nics` field still validates in config
- Tests simplified cluster setup

## Running Tests

### Run All Tests
```bash
# From repository root
./tests/run_all_tests.sh
```

### Run Individual Tests
```bash
# Validation
python tests/test_validation.py

# Labels
python tests/test_labels.py

# Security
python tests/test_security.py

# Error handling
python tests/test_error_handling.py

# RoCE disabled
python tests/test_roce_disabled.py

# Exit codes
bash tests/test_exit_codes.sh
```

### GitHub Actions
Tests run automatically on every push and pull request via GitHub Actions (`.github/workflows/tests.yml`).

The workflow runs all tests on Python 3.9, 3.10, 3.11, and 3.12.

## Test Coverage

These tests verify fixes for GitHub issue #37:
- ✅ Issue #1: No Input Validation → `test_validation.py`
- ✅ Issue #2: Poor Separation of Concerns → Validated by integration tests
- ✅ Issue #3: Error Handling → `test_error_handling.py`, `test_exit_codes.sh`
- ✅ Issue #6: Security → `test_security.py`
- ✅ Issue #7: Error Handling → `test_error_handling.py`
- ✅ Issue #8: No Consistent Labeling → `test_labels.py`

## Adding New Tests

When adding new tests:
1. Create `test_*.py` in this directory
2. Add to `run_all_tests.sh`
3. Add to `.github/workflows/tests.yml`
4. Update this README

## Dependencies

Tests require:
- Python 3.9+
- pyyaml
- click
- pydantic

Install with:
```bash
pip install pyyaml click pydantic
```

Or use the development environment:
```bash
source .venv/bin/activate
```
