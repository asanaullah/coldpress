#!/usr/bin/env python3
"""Test that RoCE NIC support is properly disabled."""

import sys
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

    if resources == expected:
        print(f"✅ Covered resources correct: {resources}")
    else:
        print(f"❌ Covered resources unexpected: {resources}")
        return False

    # Check that no RoCE resources exist
    roce_resources = [r for r in resources if "eno" in r or "rdma" in r]
    if roce_resources:
        print(f"❌ Found RoCE resources in covered list: {roce_resources}")
        return False
    else:
        print("✅ No RoCE resources in covered list")

    # Check each flavor's resources
    for i, flavor in enumerate(cq["spec"]["resourceGroups"][0]["flavors"]):
        node_resources = [r["name"] for r in flavor["resources"]]
        roce_in_node = [r for r in node_resources if "eno" in r or "rdma" in r]

        if roce_in_node:
            print(f"❌ Node {i} has RoCE resources: {roce_in_node}")
            return False
        else:
            print(f"✅ Node {i} has no RoCE resources: {node_resources}")

    return True


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

    if len(flavors) == 2:
        print(f"✅ Generated {len(flavors)} ResourceFlavors (one per node)")
    else:
        print(f"❌ Expected 2 ResourceFlavors, got {len(flavors)}")
        return False

    for i, flavor in enumerate(flavors):
        print(f"✅ ResourceFlavor node{i}: {flavor['metadata']['name']}")

    return True


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
        print(f"❌ Found {len(manifests['network'])} network resources")
        return False

    # Convert to YAML and check
    yaml_output = manifests_to_yaml(manifests)
    if "NetworkAttachmentDefinition" in yaml_output:
        print("❌ Found NetworkAttachmentDefinition in YAML output")
        return False
    else:
        print("✅ No NetworkAttachmentDefinition in YAML output")

    if "sriov" in yaml_output.lower():
        print("❌ Found 'sriov' in YAML output")
        return False
    else:
        print("✅ No 'sriov' references in YAML output")

    return True


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
        return True
    except Exception as e:
        print(f"❌ Cluster config validation failed: {e}")
        return False


def main():
    """Run all RoCE disabled tests."""
    print("\n" + "=" * 60)
    print("ROCE NIC DISABLED TEST SUITE")
    print("=" * 60)

    all_passed = True

    if not test_cluster_queue_no_roce():
        all_passed = False

    if not test_resource_flavors_no_roce():
        all_passed = False

    if not test_no_network_attachments():
        all_passed = False

    if not test_roce_field_still_in_config():
        all_passed = False

    print("\n" + "=" * 60)
    if all_passed:
        print("✅ All RoCE disabled tests passed!")
        print("=" * 60)
        print("\nRoCE NIC handling:")
        print("  ✅ RoCE RDMA resources NOT generated in ClusterQueue")
        print("  ✅ NetworkAttachmentDefinitions NOT generated")
        print("  ✅ roce_nics field still accepted in cluster config")
        print("  ✅ Simplified cluster setup (GPU-only)")
        print("=" * 60)
        return 0
    else:
        print("❌ Some RoCE disabled tests failed")
        print("=" * 60)
        return 1


if __name__ == "__main__":
    sys.exit(main())
