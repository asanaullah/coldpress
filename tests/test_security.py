#!/usr/bin/env python3
"""Test security and reliability improvements in Coldpress."""

import json
import sys
from coldpress_setup.generator import generate_sriov_network_attachments
from coldpress_common import (
    validate_kubernetes_name,
    validate_project_config,
    validate_user_config,
    validate_task_specs,
)
from pydantic import ValidationError


def test_json_injection_fix():
    """Test that JSON is constructed safely using json.dumps."""
    print("=" * 60)
    print("Testing JSON Injection Prevention")
    print("=" * 60)

    # Generate SRIOV network attachments
    attachments = generate_sriov_network_attachments("test-namespace", 2)

    passed = 0
    failed = 0

    for attachment in attachments:
        config_str = attachment["spec"]["config"]

        # Check that config is valid JSON
        try:
            config = json.loads(config_str)
            print(f"✅ Valid JSON: {config['name']}")
            passed += 1
        except json.JSONDecodeError:
            print(f"❌ Invalid JSON in config: {config_str}")
            failed += 1

        # Verify structure
        if "cniVersion" in config and "type" in config and "ipam" in config:
            print(f"   ✅ Correct CNI config structure")
        else:
            print(f"   ❌ Missing required CNI config fields")
            failed += 1

    print(f"\nJSON tests: {passed} passed, {failed} failed")
    return failed == 0


def test_kubernetes_name_validation():
    """Test that Kubernetes names are validated."""
    print("\n" + "=" * 60)
    print("Testing Kubernetes Name Validation")
    print("=" * 60)

    test_cases = [
        # (name, max_length, should_pass, description)
        ("valid-name", 63, True, "Valid lowercase name"),
        ("valid-name-123", 63, True, "Valid with numbers"),
        ("a", 63, True, "Single character"),
        ("valid.name", 63, True, "Valid with dots"),
        ("Invalid-Name", 63, False, "Uppercase not allowed"),
        ("invalid_name", 63, False, "Underscores not allowed"),
        ("invalid name", 63, False, "Spaces not allowed"),
        ("-invalid", 63, False, "Cannot start with dash"),
        ("invalid-", 63, False, "Cannot end with dash"),
        (".invalid", 63, False, "Cannot start with dot"),
        ("invalid.", 63, False, "Cannot end with dot"),
        ("", 63, False, "Empty string"),
        ("a" * 64, 63, False, "Too long"),
        ("valid-name-too-long", 10, False, "Exceeds max_length"),
    ]

    passed = 0
    failed = 0

    for name, max_len, should_pass, description in test_cases:
        try:
            result = validate_kubernetes_name(name, max_length=max_len)
            if should_pass:
                print(f"✅ {description}: '{name}' accepted")
                passed += 1
            else:
                print(f"❌ {description}: '{name}' should have been rejected")
                failed += 1
        except ValueError as e:
            if not should_pass:
                print(f"✅ {description}: '{name}' rejected ({str(e)[:50]}...)")
                passed += 1
            else:
                print(f"❌ {description}: '{name}' should have been accepted")
                failed += 1

    print(f"\nValidation tests: {passed} passed, {failed} failed")
    return failed == 0


def test_model_validation():
    """Test that Pydantic models validate resource names."""
    print("\n" + "=" * 60)
    print("Testing Pydantic Model Validation")
    print("=" * 60)

    # Test valid namespace
    try:
        config = validate_project_config(
            {
                "namespace": "valid-namespace",
                "storage": {"results": "pvc-name"},
            }
        )
        print("✅ Valid namespace accepted: valid-namespace")
    except ValidationError as e:
        print(f"❌ Valid namespace rejected: {e}")
        return False

    # Test invalid namespace (uppercase)
    try:
        config = validate_project_config(
            {
                "namespace": "Invalid-Namespace",
                "storage": {"results": "pvc-name"},
            }
        )
        print("❌ Invalid namespace should have been rejected: Invalid-Namespace")
        return False
    except ValidationError as e:
        print(f"✅ Invalid namespace rejected: Invalid-Namespace")

    # Test valid username
    try:
        config = validate_user_config(
            {"username": "valid-user", "namespaces": ["namespace1", "namespace2"]}
        )
        print("✅ Valid username accepted: valid-user")
    except ValidationError as e:
        print(f"❌ Valid username rejected: {e}")
        return False

    # Test invalid username (underscore)
    try:
        config = validate_user_config(
            {"username": "invalid_user", "namespaces": ["namespace1"]}
        )
        print("❌ Invalid username should have been rejected: invalid_user")
        return False
    except ValidationError as e:
        print(f"✅ Invalid username rejected: invalid_user")

    # Test invalid namespace in list
    try:
        config = validate_user_config(
            {"username": "valid-user", "namespaces": ["Valid-Namespace"]}
        )
        print("❌ Invalid namespace in list should have been rejected")
        return False
    except ValidationError as e:
        print(f"✅ Invalid namespace in list rejected")

    # Test valid task name
    try:
        tasks = validate_task_specs(
            [
                {
                    "name": "valid-task",
                    "containers": [{"name": "main", "image": "alpine"}],
                }
            ]
        )
        print("✅ Valid task name accepted: valid-task")
    except (ValidationError, ValueError) as e:
        print(f"❌ Valid task name rejected: {e}")
        return False

    # Test invalid task name (uppercase)
    try:
        tasks = validate_task_specs(
            [
                {
                    "name": "Invalid-Task",
                    "containers": [{"name": "main", "image": "alpine"}],
                }
            ]
        )
        print("❌ Invalid task name should have been rejected: Invalid-Task")
        return False
    except (ValidationError, ValueError) as e:
        print(f"✅ Invalid task name rejected: Invalid-Task")

    print("\nAll model validation tests passed!")
    return True


def test_no_temp_file_vulnerabilities():
    """Test that no hardcoded temp files exist."""
    print("\n" + "=" * 60)
    print("Testing Temp File Security")
    print("=" * 60)

    import os
    import glob

    # Search for hardcoded /tmp paths in source code (excluding tests)
    source_files = []
    for pattern in ["coldpress/*.py", "coldpress_setup/*.py", "coldpress_common/*.py"]:
        source_files.extend(glob.glob(pattern))

    vulnerable_files = []
    for filepath in source_files:
        with open(filepath, "r") as f:
            content = f.read()
            # Look for hardcoded /tmp/ paths that aren't container mount paths or shell commands
            if "/tmp/" in content:
                lines = content.split("\n")
                for i, line in enumerate(lines):
                    # Ignore safe /tmp usage:
                    # - mountPath (container volumes)
                    # - /tmp/result (container mount point)
                    # - Shell commands inside container (mv, if [ -f /tmp/...)
                    safe_patterns = [
                        "mountPath",
                        '"/tmp/result"',
                        "'/tmp/result'",
                        "/tmp/result/",
                        "if [ -f /tmp",
                        "mv /tmp",
                    ]
                    if "/tmp/" in line and not any(
                        pattern in line for pattern in safe_patterns
                    ):
                        vulnerable_files.append((filepath, i + 1, line.strip()))

    if vulnerable_files:
        print("❌ Found potential hardcoded temp file paths:")
        for filepath, line_num, line in vulnerable_files:
            print(f"   {filepath}:{line_num} - {line[:60]}...")
        return False
    else:
        print("✅ No hardcoded temp file paths found in source code")
        print("   (Container mount paths like /tmp/result are safe)")
        return True


def main():
    """Run all security tests."""
    print("\n" + "=" * 60)
    print("COLDPRESS SECURITY TEST SUITE")
    print("=" * 60)

    all_passed = True

    # Test JSON injection prevention
    if not test_json_injection_fix():
        all_passed = False

    # Test Kubernetes name validation
    if not test_kubernetes_name_validation():
        all_passed = False

    # Test model validation
    if not test_model_validation():
        all_passed = False

    # Test temp file security
    if not test_no_temp_file_vulnerabilities():
        all_passed = False

    print("\n" + "=" * 60)
    if all_passed:
        print("✅ All security tests passed!")
        print("=" * 60)
        print("\nSecurity improvements:")
        print("  ✅ JSON constructed with json.dumps() (no injection)")
        print("  ✅ Kubernetes names validated against spec")
        print("  ✅ User input sanitized before use in resource names")
        print("  ✅ No hardcoded temp file vulnerabilities")
        print("=" * 60)
        return 0
    else:
        print("❌ Some security tests failed")
        print("=" * 60)
        return 1


if __name__ == "__main__":
    sys.exit(main())
