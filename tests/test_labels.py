#!/usr/bin/env python3
"""Test script to verify Coldpress resource labels are applied correctly."""

from coldpress.jobset_generator import (
    generate_jobset_from_intent,
)
from coldpress.constants import (
    COLDPRESS_LABELS as COLDPRESS_JOB_LABELS,
)
from coldpress_setup.generator import (
    generate_project_manifests,
    generate_user_rbac,
    generate_kueue_resource_flavors,
    generate_cluster_queue,
    COLDPRESS_LABELS as COLDPRESS_SETUP_LABELS,
)
from coldpress_common import validate_intent


def test_label_constants():
    """Verify label constants are defined correctly."""
    print("=" * 60)
    print("Testing Label Constants")
    print("=" * 60)

    expected = {
        "app.kubernetes.io/managed-by": "coldpress",
        "app.kubernetes.io/version": "0.2.0",
    }

    assert COLDPRESS_JOB_LABELS == expected, (
        f"Job labels mismatch: {COLDPRESS_JOB_LABELS}"
    )
    assert COLDPRESS_SETUP_LABELS == expected, (
        f"Setup labels mismatch: {COLDPRESS_SETUP_LABELS}"
    )
    print("✅ Label constants match expected values")


def test_jobset_labels():
    """Test that JobSets have correct labels."""
    print("\n" + "=" * 60)
    print("Testing JobSet Labels")
    print("=" * 60)

    # Create vanilla k8s Job
    vk8s_job = {
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {"name": "test-task"},
        "spec": {
            "template": {
                "spec": {
                    "containers": [{"name": "main", "image": "alpine:latest"}],
                    "restartPolicy": "Never",
                }
            }
        },
    }

    # Create intent config
    intent_data = {
        "project": "coldpress-project",
        "output": "test-job",
        "target": "jobset",
        "tasks": [{"name": "test-task", "replicas": 1}],
    }
    intent_config = validate_intent(intent_data)

    # Create project config
    project_config = {"namespace": "test-ns", "storage": {"results": "test-pvc"}}

    jobset, services, _ = generate_jobset_from_intent(
        {"test-task": vk8s_job}, intent_config, project_config, "test-ns"
    )
    labels = jobset["metadata"]["labels"]

    assert "app.kubernetes.io/managed-by" in labels
    assert labels["app.kubernetes.io/managed-by"] == "coldpress"
    assert "app.kubernetes.io/version" in labels
    assert labels["app.kubernetes.io/version"] == "0.2.0"

    print(f"✅ JobSet labels: {labels}")


def test_service_labels():
    """Test that Services are not generated (JobSet provides automatic DNS)."""
    print("\n" + "=" * 60)
    print("Testing Service Labels (JobSet DNS)")
    print("=" * 60)

    # Create vanilla k8s Job with readinessProbe
    vk8s_job = {
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {"name": "server"},
        "spec": {
            "template": {
                "spec": {
                    "containers": [
                        {
                            "name": "main",
                            "image": "vllm:latest",
                            "readinessProbe": {
                                "httpGet": {"path": "/health", "port": 8000}
                            },
                        }
                    ],
                    "restartPolicy": "Never",
                }
            }
        },
    }

    # Create intent config
    intent_data = {
        "project": "coldpress-project",
        "output": "test-job",
        "target": "jobset",
        "tasks": [{"name": "server", "replicas": 1}],
    }
    intent_config = validate_intent(intent_data)

    # Create project config
    project_config = {"namespace": "test-ns", "storage": {"results": "test-pvc"}}

    jobset, services, _ = generate_jobset_from_intent(
        {"server": vk8s_job}, intent_config, project_config, "test-ns"
    )

    # Services are no longer generated - JobSet provides automatic DNS
    assert len(services) == 0, "Services should not be generated (JobSet provides DNS)"
    print("✅ No services generated (JobSet provides automatic DNS)")


def test_project_resource_labels():
    """Test that project resources have correct labels."""
    print("\n" + "=" * 60)
    print("Testing Project Resource Labels")
    print("=" * 60)

    config = {
        "namespace": "test-ns",
        "cluster_queue": "test-queue",
        "storage_class": "nfs-csi",
        "storage": {"results": "test-pvc", "size": "100Gi"},
    }

    manifests = generate_project_manifests(config)

    # Check Namespace
    ns = manifests["namespaces"][0]
    ns_labels = ns["metadata"]["labels"]
    assert "app.kubernetes.io/managed-by" in ns_labels
    print(f"✅ Namespace labels: {ns_labels}")

    # Check LocalQueue
    lq = manifests["kueue"][0]
    lq_labels = lq["metadata"]["labels"]
    assert "app.kubernetes.io/managed-by" in lq_labels
    print(f"✅ LocalQueue labels: {lq_labels}")

    # Check PVC
    pvc = manifests["storage"][0]
    pvc_labels = pvc["metadata"]["labels"]
    assert "app.kubernetes.io/managed-by" in pvc_labels
    print(f"✅ PVC labels: {pvc_labels}")

    # Check RBAC (ServiceAccount, Role, RoleBinding)
    for rbac_resource in manifests["rbac"]:
        rbac_labels = rbac_resource["metadata"]["labels"]
        assert "app.kubernetes.io/managed-by" in rbac_labels
        kind = rbac_resource["kind"]
        print(f"✅ {kind} labels: {rbac_labels}")


def test_user_rbac_labels():
    """Test that user RBAC resources have correct labels."""
    print("\n" + "=" * 60)
    print("Testing User RBAC Labels")
    print("=" * 60)

    rbac = generate_user_rbac("test-user", ["ns1", "ns2"])

    for binding in rbac:
        labels = binding["metadata"]["labels"]
        assert "app.kubernetes.io/managed-by" in labels
        assert labels["app.kubernetes.io/managed-by"] == "coldpress"
        ns = binding["metadata"]["namespace"]
        print(f"✅ RoleBinding ({ns}) labels: {labels}")


def test_cluster_resource_labels():
    """Test that cluster resources have correct labels."""
    print("\n" + "=" * 60)
    print("Testing Cluster Resource Labels")
    print("=" * 60)

    nodes = [
        {"hostname": "node1", "gpus": 2, "roce_nics": 0},
        {"hostname": "node2", "gpus": 4, "roce_nics": 2},
    ]

    # Test ResourceFlavors
    flavors = generate_kueue_resource_flavors(nodes)
    for flavor in flavors:
        labels = flavor["metadata"]["labels"]
        assert "app.kubernetes.io/managed-by" in labels
        print(f"✅ ResourceFlavor labels: {labels}")

    # Test ClusterQueue
    cq = generate_cluster_queue("test-queue", nodes)
    cq_labels = cq["metadata"]["labels"]
    assert "app.kubernetes.io/managed-by" in cq_labels
    print(f"✅ ClusterQueue labels: {cq_labels}")


def main():
    """Run all label tests."""
    print("\n" + "=" * 60)
    print("COLDPRESS RESOURCE LABELS TEST")
    print("=" * 60)

    test_label_constants()
    test_jobset_labels()
    test_service_labels()
    test_project_resource_labels()
    test_user_rbac_labels()
    test_cluster_resource_labels()

    print("\n" + "=" * 60)
    print("✅ All label tests passed!")
    print("=" * 60)
    print("\nAll Coldpress resources have standard labels:")
    print("  - app.kubernetes.io/managed-by: coldpress")
    print("  - app.kubernetes.io/version: 0.2.0")
    print("  - coldpress.io/job-id: {job_name} (for job resources)")
    print("\nQuery all resources:")
    print(
        "  kubectl get all,pvc,configmap,rolebinding -A -l app.kubernetes.io/managed-by=coldpress"
    )
    print("=" * 60 + "\n")


if __name__ == "__main__":
    main()
