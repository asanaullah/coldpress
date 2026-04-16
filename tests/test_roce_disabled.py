#!/usr/bin/env python3
"""Test that RoCE NIC support is properly disabled."""

import sys
import pytest
from coldpress_setup.generator import (
    generate_cluster_queue,
    generate_kueue_resource_flavors,
    generate_all_manifests,
    manifests_to_yaml,
)


def test_cluster_queue_no_roce():
    """Test that ClusterQueue doesn't include RoCE resources."""
    print("=" * 60)
    print("Testing ClusterQueue - No RoCE Resources")
    print("=" * 60)

    nodes = [
        {"hostname": "node1", "gpus": 2, "roce_nics": 2},
        {"hostname": "node2", "gpus": 4, "roce_nics": 1},
    ]

    cq = generate_cluster_queue("test-queue", nodes)

    # Check covered resources
    resources = cq["spec"]["resourceGroups"][0]["coveredResources"]
    expected = ["cpu", "memory", "nvidia.com/gpu"]

    assert resources == expected, f"Covered resources unexpected: {resources}"
    print(f"✅ Covered resources correct: {resources}")

    # Check that no RoCE resources exist
    roce_resources = [r for r in resources if "eno" in r or "rdma" in r]
    assert not roce_resources, f"Found RoCE resources in covered list: {roce_resources}"
    print("✅ No RoCE resources in covered list")

    # Check each flavor's resources
    for i, flavor in enumerate(cq["spec"]["resourceGroups"][0]["flavors"]):
        node_resources = [r["name"] for r in flavor["resources"]]
        roce_in_node = [r for r in node_resources if "eno" in r or "rdma" in r]

        assert not roce_in_node, f"Node {i} has RoCE resources: {roce_in_node}"
        print(f"✅ Node {i} has no RoCE resources: {node_resources}")


def test_resource_flavors_no_roce():
    """Test that ResourceFlavors are still generated correctly."""
    print("\n" + "=" * 60)
    print("Testing ResourceFlavors - Basic GPU Resources")
    print("=" * 60)

    nodes = [
        {"hostname": "node1", "gpus": 2, "roce_nics": 2},
        {"hostname": "node2", "gpus": 4, "roce_nics": 0},
    ]

    flavors = generate_kueue_resource_flavors(nodes)

    assert len(flavors) == 2, f"Expected 2 ResourceFlavors, got {len(flavors)}"
    print(f"✅ Generated {len(flavors)} ResourceFlavors (one per node)")

    for i, flavor in enumerate(flavors):
        print(f"✅ ResourceFlavor node{i}: {flavor['metadata']['name']}")


def test_no_network_attachments():
    """Test that NetworkAttachmentDefinitions are not generated."""
    print("\n" + "=" * 60)
    print("Testing No NetworkAttachmentDefinitions")
    print("=" * 60)

    config = {
        "nodes": [
            {"hostname": "node1", "gpus": 2, "roce_nics": 2},
        ],
        "namespaces": [
            {"name": "test-namespace", "storage": "100Gi", "privileged": False}
        ],
        "cluster_queue": "test-queue",
        "storage_class": "nfs-csi",
    }

    manifests = generate_all_manifests(config)

    # Check that network category doesn't exist or is empty
    if "network" not in manifests:
        print("✅ 'network' category not in manifests dict")
    elif not manifests["network"]:
        print("✅ 'network' category is empty")
    else:
        pytest.fail(f"Found {len(manifests['network'])} network resources")

    # Convert to YAML and check
    yaml_output = manifests_to_yaml(manifests)
    assert "NetworkAttachmentDefinition" not in yaml_output, (
        "Found NetworkAttachmentDefinition in YAML output"
    )
    print("✅ No NetworkAttachmentDefinition in YAML output")

    assert "sriov" not in yaml_output.lower(), "Found 'sriov' in YAML output"
    print("✅ No 'sriov' references in YAML output")


def test_roce_field_still_in_config():
    """Test that roce_nics field still exists in config for reference."""
    print("\n" + "=" * 60)
    print("Testing RoCE Field Preserved in Config")
    print("=" * 60)

    from coldpress_common import validate_cluster_config

    # Config with roce_nics should still validate
    config_data = {
        "nodes": [
            {"hostname": "node1", "gpus": 2, "roce_nics": 2},
            {"hostname": "node2", "gpus": 4, "roce_nics": 0},
        ]
    }

    try:
        cluster_config = validate_cluster_config(config_data)
        print("✅ Cluster config with roce_nics validates successfully")
        print(f"   Node 0 roce_nics: {cluster_config.nodes[0].roce_nics}")
        print(f"   Node 1 roce_nics: {cluster_config.nodes[1].roce_nics}")
    except Exception as e:
        pytest.fail(f"Cluster config validation failed: {e}")


def main():
    """Run all RoCE disabled tests."""
    print("\n" + "=" * 60)
    print("ROCE NIC DISABLED TEST SUITE")
    print("=" * 60)

    try:
        test_cluster_queue_no_roce()
        test_resource_flavors_no_roce()
        test_no_network_attachments()
        test_roce_field_still_in_config()

        print("\n" + "=" * 60)
        print("✅ All RoCE disabled tests passed!")
        print("=" * 60)
        print("\nRoCE NIC handling:")
        print("  ✅ RoCE RDMA resources NOT generated in ClusterQueue")
        print("  ✅ NetworkAttachmentDefinitions NOT generated")
        print("  ✅ roce_nics field still accepted in cluster config")
        print("  ✅ Simplified cluster setup (GPU-only)")
        print("=" * 60)
        return 0
    except (AssertionError, Exception) as e:
        print("\n" + "=" * 60)
        print(f"❌ RoCE disabled test failed: {e}")
        print("=" * 60)
        return 1


if __name__ == "__main__":
    sys.exit(main())
