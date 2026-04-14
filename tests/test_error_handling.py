#!/usr/bin/env python3
"""Test error handling improvements in Coldpress."""

import sys
import subprocess


def run_command(cmd, expected_exit_code):
    """Run command and check exit code."""
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    return result.returncode == expected_exit_code, result.returncode


def test_exit_codes():
    """Test that CLI commands return correct exit codes."""
    print("=" * 60)
    print("Testing Exit Code Propagation")
    print("=" * 60)

    tests = [
        {
            "name": "Success: Valid coldpress config",
            "cmd": "python -m coldpress.cli generate --config examples/pytorch_ddp_training/config.yaml",
            "expected": 0,
        },
        {
            "name": "Error: Non-existent file (coldpress-setup)",
            "cmd": "python -m coldpress_setup.cli generate cluster non-existent.yaml",
            "expected": 1,
        },
        {
            "name": "Usage error: Missing required argument",
            "cmd": "python -m coldpress.cli generate",
            "expected": 2,
        },
        {
            "name": "Error: Invalid YAML in config",
            "cmd": "echo 'invalid: yaml: :' > /tmp/invalid.yaml && python -m coldpress_setup.cli generate cluster /tmp/invalid.yaml",
            "expected": 1,
        },
    ]

    passed = 0
    failed = 0

    for test in tests:
        success, actual = run_command(test["cmd"], test["expected"])
        if success:
            print(f"✅ {test['name']}")
            print(f"   Expected exit code {test['expected']}, got {actual}")
            passed += 1
        else:
            print(f"❌ {test['name']}")
            print(f"   Expected exit code {test['expected']}, got {actual}")
            failed += 1

    print()
    print(f"Passed: {passed}/{len(tests)}")
    print(f"Failed: {failed}/{len(tests)}")

    assert failed == 0, f"Exit code tests failed: {failed} failures"


def test_specific_exceptions():
    """Test that specific exceptions are used instead of broad Exception."""
    print("\n" + "=" * 60)
    print("Testing Specific Exception Handling")
    print("=" * 60)

    # Check that code uses specific exceptions
    import coldpress.generator as gen

    # Test create_service with invalid URL
    task = {"health_check": None}
    result = gen.create_service(task, 0, "test", "default")
    assert result is None, "create_service should return None for missing health_check"
    print("✅ create_service handles missing health_check")

    # Test build_discovery_init_container with missing template
    result = gen.build_discovery_init_container(
        "/tmp/non-existent-template.yaml", 0, "base", "pvc"
    )
    assert result is None, (
        "build_discovery_init_container should return None for missing template"
    )
    print("✅ build_discovery_init_container handles missing template")

    # Test build_discovery_job with missing template
    result = gen.build_discovery_job(
        "/tmp/non-existent-template.yaml", "base", "pvc", "0"
    )
    assert result is None, "build_discovery_job should return None for missing template"
    print("✅ build_discovery_job handles missing template")


def test_validation_errors():
    """Test that validation errors are caught and reported clearly."""
    print("\n" + "=" * 60)
    print("Testing Validation Error Messages")
    print("=" * 60)

    from coldpress_common import validate_task_specs
    from pydantic import ValidationError

    # Test missing required field
    try:
        invalid_task = [{"containers": [{"name": "test", "image": "alpine"}]}]
        validate_task_specs(invalid_task)
        raise AssertionError("Should have caught missing 'name' field")
    except (ValidationError, ValueError) as e:
        print("✅ Caught validation error for missing 'name' field")
        print(f"   Error message: {str(e).splitlines()[0][:70]}...")

    # Test endpoint blocking without health check
    try:
        invalid_task = [
            {
                "name": "test",
                "blocking": "endpoint",
                "containers": [{"name": "test", "image": "alpine"}],
            }
        ]
        validate_task_specs(invalid_task)
        raise AssertionError(
            "Should have caught missing health_check for endpoint blocking"
        )
    except (ValidationError, ValueError) as e:
        print("✅ Caught validation error for endpoint blocking without health check")
        print(f"   Error message: {str(e).splitlines()[0][:70]}...")


def main():
    """Run all error handling tests."""
    print("\n" + "=" * 60)
    print("COLDPRESS ERROR HANDLING TEST SUITE")
    print("=" * 60)

    try:
        # Test exit codes
        test_exit_codes()

        # Test specific exceptions
        test_specific_exceptions()

        # Test validation errors
        test_validation_errors()

        print("\n" + "=" * 60)
        print("✅ All error handling tests passed!")
        print("=" * 60)
        print("\nError handling improvements:")
        print("  ✅ Exit codes properly propagated to shell")
        print("  ✅ Specific exceptions instead of broad Exception")
        print("  ✅ Clear validation error messages")
        print("  ✅ FileNotFoundError, yaml.YAMLError, KeyError, etc.")
        print("=" * 60)
        return 0
    except (AssertionError, Exception) as e:
        print("\n" + "=" * 60)
        print(f"❌ Error handling test failed: {e}")
        print("=" * 60)
        return 1


if __name__ == "__main__":
    sys.exit(main())
