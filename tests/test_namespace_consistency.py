#!/usr/bin/env python3
"""Test that namespace generation is consistent and not duplicated."""

import pytest
from coldpress_setup.generator import generate_namespace, generate_project_manifests


def test_generate_namespace_function():
    """Test that generate_namespace function works correctly."""
    # Test regular namespace
    ns = generate_namespace("test-namespace", "100Gi", privileged=False)

    assert ns["apiVersion"] == "v1"
    assert ns["kind"] == "Namespace"
    assert ns["metadata"]["name"] == "test-namespace"
    assert ns["metadata"]["labels"]["kueue.openshift.io/managed"] == "true"
    assert ns["metadata"]["labels"]["app.kubernetes.io/managed-by"] == "coldpress"
    assert ns["metadata"]["labels"]["pod-security.kubernetes.io/enforce"] == "restricted"

    # Test privileged namespace
    ns_priv = generate_namespace("privileged-namespace", "100Gi", privileged=True)

    assert ns_priv["metadata"]["labels"]["pod-security.kubernetes.io/enforce"] == "privileged"
    assert ns_priv["metadata"]["labels"]["pod-security.kubernetes.io/audit"] == "privileged"
    assert ns_priv["metadata"]["labels"]["pod-security.kubernetes.io/warn"] == "privileged"


def test_project_manifests_uses_generate_namespace():
    """Test that generate_project_manifests uses the shared generate_namespace function."""
    # Test regular project
    config = {
        "namespace": "my-project",
        "storage": {
            "results": "my-pvc",
            "size": "200Gi"
        }
    }

    manifests = generate_project_manifests(config)
    ns = manifests["namespaces"][0]

    # Should have standard Coldpress labels
    assert ns["metadata"]["labels"]["app.kubernetes.io/managed-by"] == "coldpress"
    assert ns["metadata"]["labels"]["kueue.openshift.io/managed"] == "true"
    # Should default to restricted
    assert ns["metadata"]["labels"]["pod-security.kubernetes.io/enforce"] == "restricted"

    # Test privileged project
    config_priv = {
        "namespace": "privileged-project",
        "privileged": True,
        "storage": {
            "results": "priv-pvc",
            "size": "100Gi"
        }
    }

    manifests_priv = generate_project_manifests(config_priv)
    ns_priv = manifests_priv["namespaces"][0]

    # Should have privileged security labels
    assert ns_priv["metadata"]["labels"]["pod-security.kubernetes.io/enforce"] == "privileged"
    assert ns_priv["metadata"]["labels"]["pod-security.kubernetes.io/audit"] == "privileged"
    assert ns_priv["metadata"]["labels"]["pod-security.kubernetes.io/warn"] == "privileged"


def test_no_namespace_duplication():
    """Test that namespace generation logic is not duplicated."""
    import inspect
    from coldpress_setup import generator

    # Read the source code of generate_project_manifests
    source = inspect.getsource(generator.generate_project_manifests)

    # Should call generate_namespace function, not duplicate the logic
    assert "generate_namespace(" in source

    # Should NOT have duplicated inline namespace creation
    assert source.count('ns_labels["kueue.openshift.io/managed"]') == 0
    assert source.count('"kind": "Namespace"') == 0


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
