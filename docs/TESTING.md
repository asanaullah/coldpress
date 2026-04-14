# Coldpress Testing Guide

This document describes the comprehensive test suite for Coldpress and how to use it.

## Test Organization

All tests are located in the `tests/` directory:

```
tests/
├── README.md                  # Detailed test documentation
├── run_all_tests.sh          # Run all tests locally
├── test_validation.py        # Pydantic model validation
├── test_labels.py            # Resource labels (issue #37-8)
├── test_security.py          # Security & input validation (issue #37-6)
├── test_error_handling.py    # Error handling (issue #37-7)
├── test_exit_codes.sh        # Shell exit codes
└── test_roce_disabled.py     # RoCE NIC disabled tests
```

## Running Tests Locally

### Run All Tests

```bash
./tests/run_all_tests.sh
```

This runs all 6 test suites and provides a summary:

```
════════════════════════════════════════════════════════════
  TEST SUMMARY
════════════════════════════════════════════════════════════

  ✅ Passed: 6
  ❌ Failed: 0

  🎉 All tests passed!
════════════════════════════════════════════════════════════
```

### Run Individual Tests

```bash
# Validation tests
python tests/test_validation.py

# Label tests
python tests/test_labels.py

# Security tests
python tests/test_security.py

# Error handling tests
python tests/test_error_handling.py

# RoCE disabled tests
python tests/test_roce_disabled.py

# Exit code tests (shell script)
bash tests/test_exit_codes.sh
```

## Continuous Integration

### GitHub Actions

Tests run automatically on:
- Every push to `main` or `v0.2` branches
- Every pull request to `main` or `v0.2` branches

**Workflow:** `.github/workflows/tests.yml`

**Python versions tested:**
- Python 3.9
- Python 3.10
- Python 3.11
- Python 3.12

**View results:**
- Check the Actions tab on GitHub
- Badge on README shows latest status: [![Tests](https://github.com/asanaullah/coldpress/workflows/Tests/badge.svg)](https://github.com/asanaullah/coldpress/actions)

### What Gets Tested

1. **Validation Tests** - Verifies Pydantic models catch errors
   - Config validation (config.yaml)
   - Task spec validation (job-spec.yaml)
   - Project config validation
   - User config validation
   - Fixes: Issue #37-1 (No Input Validation)

2. **Label Tests** - Verifies resource labeling
   - All resources have `app.kubernetes.io/managed-by: coldpress`
   - Version labels present
   - Job-specific labels on job resources
   - Fixes: Issue #37-8 (No Consistent Resource Labelling)

3. **Security Tests** - Verifies security improvements
   - JSON constructed safely (no f-string injection)
   - Kubernetes names validated
   - User input sanitized
   - No hardcoded temp files
   - Fixes: Issue #37-6 (Security and Reliability Issues)

4. **Error Handling Tests** - Verifies error handling
   - Exit codes propagate correctly (0/1/2)
   - Specific exception types used
   - Clear error messages
   - Validation errors caught early
   - Fixes: Issue #37-7 (Error Handling)

5. **Exit Code Tests** - Verifies shell-level exit codes
   - Success returns 0
   - Errors return 1
   - Usage errors return 2

6. **RoCE Disabled Tests** - Verifies RoCE NIC support disabled
   - No RDMA resources in ClusterQueue
   - No NetworkAttachmentDefinitions generated
   - `roce_nics` field still validates

## Test Dependencies

**Required packages:**
```bash
pip install pyyaml click pydantic
```

**Or use the development environment:**
```bash
source .venv/bin/activate  # or setup-env.sh
```

## Adding New Tests

When adding new functionality:

1. **Create test file:** `tests/test_<feature>.py`
2. **Add to test runner:** Edit `tests/run_all_tests.sh`
3. **Add to CI:** Edit `.github/workflows/tests.yml`
4. **Document:** Update `tests/README.md`

### Test Template

```python
#!/usr/bin/env python3
"""Test description."""

import sys

def test_feature():
    """Test specific feature."""
    print("=" * 60)
    print("Testing Feature")
    print("=" * 60)

    # Test logic here
    if condition:
        print("✅ Test passed")
        return True
    else:
        print("❌ Test failed")
        return False

def main():
    """Run all tests."""
    if test_feature():
        print("\n✅ All tests passed!")
        return 0
    else:
        print("\n❌ Some tests failed")
        return 1

if __name__ == "__main__":
    sys.exit(main())
```

## Debugging Test Failures

### Local Debugging

1. **Run specific test:**
   ```bash
   python tests/test_<name>.py
   ```

2. **Check error output:**
   Tests print detailed error messages showing what failed

3. **Run with verbose output:**
   Most tests print progress as they run

### CI Debugging

1. **View Actions logs:**
   - Go to GitHub repository
   - Click "Actions" tab
   - Click on failed workflow run
   - Expand failed step

2. **Common issues:**
   - Import errors → Check `PYTHONPATH` in workflow
   - Missing dependencies → Check `pip install` step
   - Python version issues → Check matrix strategy

## Test Coverage

Current test coverage addresses all major categories from GitHub issue #37:

| Category | Issue # | Test File | Status |
|----------|---------|-----------|--------|
| Input Validation | #37-1 | test_validation.py | ✅ |
| Separation of Concerns | #37-2 | Integration tests | ✅ |
| Error Handling | #37-3, #37-7 | test_error_handling.py | ✅ |
| Security | #37-6 | test_security.py | ✅ |
| Resource Labeling | #37-8 | test_labels.py | ✅ |
| RoCE Disabled | N/A | test_roce_disabled.py | ✅ |

## Related Documentation

- [tests/README.md](tests/README.md) - Detailed test documentation
- [docs/ERROR_HANDLING.md](docs/ERROR_HANDLING.md) - Error handling guide
- [docs/LABELS.md](docs/LABELS.md) - Resource labels guide
- [docs/ROCE_DISABLED.md](docs/ROCE_DISABLED.md) - RoCE NIC disabled guide
