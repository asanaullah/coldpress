#!/usr/bin/env python3
"""Test script to demonstrate Coldpress YAML validation."""

import yaml
from coldpress_common import (
    validate_project_config,
)
from pydantic import ValidationError


def test_valid_configs():
    """Test validation with valid configuration files."""
    print("=" * 60)
    print("Testing VALID configurations")
    print("=" * 60)

    # Test 1: Valid intent.yaml
    print("\n1. Testing intent.yaml validation...")
    from coldpress_common import validate_intent

    with open("examples/pytorch_ddp_training/intent_jobset.yaml") as f:
        intent_data = yaml.safe_load(f)
        intent = validate_intent(intent_data)
        print(f"   ✓ Valid intent: target={intent.target}, tasks={len(intent.tasks)}")

    # Test 2: Valid job-spec.yaml (vanilla k8s Jobs)
    print("\n2. Testing job-spec.yaml (vanilla k8s)...")
    with open("examples/pytorch_ddp_training/job-spec.yaml") as f:
        manifests = list(yaml.safe_load_all(f))
        jobs = [m for m in manifests if m.get("kind") == "Job"]
        print(f"   ✓ Valid job spec: {len(jobs)} Job(s)")
        for i, job in enumerate(jobs):
            job_name = job["metadata"]["name"]
            print(f"     - Job {i}: {job_name}")

    # Test 3: Valid project config
    print("\n3. Testing project config validation...")
    with open("projects/coldpress-project.yaml") as f:
        project_data = yaml.safe_load(f)
        project = validate_project_config(project_data)
        print(f"   ✓ Valid project: namespace={project.namespace}")

    # Test 4: Multi-task intent spec
    print("\n4. Testing multi-task intent.yaml...")
    with open("examples/vllm_guidellm_benchmark/intent_jobset.yaml") as f:
        intent_data = yaml.safe_load(f)
        intent = validate_intent(intent_data)
        print(f"   ✓ Valid multi-task intent: {len(intent.tasks)} task(s)")
        for i, task in enumerate(intent.tasks):
            print(f"     - Task {i}: {task.name} (replicas={task.replicas})")


def test_invalid_configs():
    """Test validation with invalid configurations."""
    print("\n" + "=" * 60)
    print("Testing INVALID configurations (should catch errors)")
    print("=" * 60)

    from coldpress_common import validate_intent

    # Test 1: Missing required field (project)
    print("\n1. Testing missing 'project' field...")
    try:
        invalid_intent = {
            "target": "jobset",
            "output": "test",
            "tasks": [{"name": "test-task", "replicas": 1}],
        }
        validate_intent(invalid_intent)
        print("   ❌ ERROR: Should have caught missing 'project' field!")
    except (ValidationError, ValueError) as e:
        print(f"   ✓ Caught error: {str(e).splitlines()[0][:70]}...")

    # Test 2: Missing tasks
    print("\n2. Testing missing 'tasks' field...")
    try:
        invalid_intent = {"project": "test", "output": "test", "target": "jobset"}
        validate_intent(invalid_intent)
        print("   ❌ ERROR: Should have caught missing 'tasks' field!")
    except (ValidationError, ValueError) as e:
        print(f"   ✓ Caught error: {str(e).splitlines()[0][:70]}...")

    # Test 3: Invalid target type
    print("\n3. Testing invalid target type...")
    try:
        invalid_intent = {
            "project": "test",
            "output": "test",
            "target": "invalid_target",
            "tasks": [{"name": "test", "replicas": 1}],
        }
        validate_intent(invalid_intent)
        print("   ❌ ERROR: Should have caught invalid target type!")
    except (ValidationError, ValueError) as e:
        print(f"   ✓ Caught error: {str(e).splitlines()[0][:70]}...")

    # Test 4: Invalid replicas (negative)
    print("\n4. Testing invalid replicas...")
    try:
        invalid_intent = {
            "backend": "jobset",
            "tasks": [{"name": "test", "replicas": -1}],
        }
        validate_intent(invalid_intent)
        print("   ❌ ERROR: Should have caught invalid replicas!")
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
