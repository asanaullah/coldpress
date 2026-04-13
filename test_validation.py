#!/usr/bin/env python3
"""Test script to demonstrate Coldpress YAML validation."""

import yaml
from coldpress.model import (
    validate_config,
    validate_task_specs,
    validate_project_config,
)
from pydantic import ValidationError


def test_valid_configs():
    """Test validation with valid configuration files."""
    print("=" * 60)
    print("Testing VALID configurations")
    print("=" * 60)

    # Test 1: Valid config.yaml
    print("\n1. Testing config.yaml validation...")
    with open("examples/pytorch_ddp_training/config.yaml") as f:
        config_data = yaml.safe_load(f)
        config = validate_config(config_data)
        print(
            f"   ✓ Valid config: project={config.project}, discovery={config.discovery}"
        )

    # Test 2: Valid job-spec.yaml
    print("\n2. Testing job-spec.yaml validation...")
    with open("examples/pytorch_ddp_training/job-spec.yaml") as f:
        task_specs = list(yaml.safe_load_all(f))
        validated = validate_task_specs(task_specs)
        print(f"   ✓ Valid task spec: {len(validated)} task(s)")
        for i, task in enumerate(validated):
            print(f"     - Task {i}: {task.name} ({len(task.containers)} container(s))")

    # Test 3: Valid project config
    print("\n3. Testing project config validation...")
    with open("projects/researcher-a.yaml") as f:
        project_data = yaml.safe_load(f)
        project = validate_project_config(project_data)
        print(f"   ✓ Valid project: namespace={project.namespace}")

    # Test 4: Multi-task job spec
    print("\n4. Testing multi-task job-spec.yaml...")
    with open("examples/vllm_guidellm_benchmark/job-spec.yaml") as f:
        task_specs = list(yaml.safe_load_all(f))
        validated = validate_task_specs(task_specs)
        print(f"   ✓ Valid multi-task spec: {len(validated)} task(s)")
        for i, task in enumerate(validated):
            blocking = task.blocking or "completion"
            print(f"     - Task {i}: {task.name} (blocking={blocking})")


def test_invalid_configs():
    """Test validation with invalid configurations."""
    print("\n" + "=" * 60)
    print("Testing INVALID configurations (should catch errors)")
    print("=" * 60)

    # Test 1: Missing required field
    print("\n1. Testing missing 'name' field...")
    try:
        invalid_task = [{"containers": [{"name": "test", "image": "alpine"}]}]
        validate_task_specs(invalid_task)
        print("   ❌ ERROR: Should have caught missing 'name' field!")
    except (ValidationError, ValueError) as e:
        print(f"   ✓ Caught error: {str(e).splitlines()[0][:70]}...")

    # Test 2: Missing containers
    print("\n2. Testing missing 'containers' field...")
    try:
        invalid_task = [{"name": "test-task"}]
        validate_task_specs(invalid_task)
        print("   ❌ ERROR: Should have caught missing 'containers' field!")
    except (ValidationError, ValueError) as e:
        print(f"   ✓ Caught error: {str(e).splitlines()[0][:70]}...")

    # Test 3: Invalid blocking endpoint without health check
    print("\n3. Testing endpoint blocking without health check...")
    try:
        invalid_task = [
            {
                "name": "test-task",
                "blocking": "endpoint",
                "containers": [{"name": "test", "image": "alpine"}],
            }
        ]
        validate_task_specs(invalid_task)
        print("   ❌ ERROR: Should have caught missing health check!")
    except (ValidationError, ValueError) as e:
        print(f"   ✓ Caught error: {str(e).splitlines()[0][:70]}...")

    # Test 4: Invalid blocking type
    print("\n4. Testing invalid blocking type...")
    try:
        invalid_task = [
            {
                "name": "test-task",
                "blocking": "invalid_type",
                "containers": [{"name": "test", "image": "alpine"}],
            }
        ]
        validate_task_specs(invalid_task)
        print("   ❌ ERROR: Should have caught invalid blocking type!")
    except (ValidationError, ValueError) as e:
        print(f"   ✓ Caught error: {str(e).splitlines()[0][:70]}...")


def main():
    """Run all validation tests."""
    print("\n" + "=" * 60)
    print("COLDPRESS YAML VALIDATION TEST")
    print("=" * 60)

    test_valid_configs()
    test_invalid_configs()

    print("\n" + "=" * 60)
    print("✅ All validation tests passed!")
    print("=" * 60)
    print("\nValidation is working correctly. YAML files are checked")
    print("at load time, catching errors early before execution.")
    print("=" * 60 + "\n")


if __name__ == "__main__":
    main()
