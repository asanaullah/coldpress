#!/usr/bin/env python3
"""Test that namespace generation is consistent and not duplicated."""

import sys
from coldpress_setup.generator import generate_namespace, generate_project_manifests


def test_generate_namespace_function():
    """Test that generate_namespace function works correctly."""
    print("\n" + "=" * 60)
    print("Testing generate_namespace Function")
    print("=" * 60)

    passed = 0
    failed = 0

    # Test regular namespace
    ns = generate_namespace("test-namespace", "100Gi", privileged=False)

    if ns["apiVersion"] == "v1":
        print("✅ Namespace has correct apiVersion")
        passed += 1
    else:
        print("❌ Namespace apiVersion is wrong")
        failed += 1

    if ns["kind"] == "Namespace":
        print("✅ Namespace has correct kind")
        passed += 1
    else:
        print("❌ Namespace kind is wrong")
        failed += 1

    if ns["metadata"]["name"] == "test-namespace":
        print("✅ Namespace has correct name")
        passed += 1
    else:
        print("❌ Namespace name is wrong")
        failed += 1

    if ns["metadata"]["labels"]["kueue.openshift.io/managed"] == "true":
        print("✅ Namespace has kueue.openshift.io/managed label")
        passed += 1
    else:
        print("❌ Namespace missing kueue.openshift.io/managed label")
        failed += 1

    if ns["metadata"]["labels"]["app.kubernetes.io/managed-by"] == "coldpress":
        print("✅ Namespace has app.kubernetes.io/managed-by label")
        passed += 1
    else:
        print("❌ Namespace missing app.kubernetes.io/managed-by label")
        failed += 1

    if ns["metadata"]["labels"]["pod-security.kubernetes.io/enforce"] == "restricted":
        print("✅ Namespace has restricted security enforcement")
        passed += 1
    else:
        print("❌ Namespace security enforcement is wrong")
        failed += 1

    # Test privileged namespace
    ns_priv = generate_namespace("privileged-namespace", "100Gi", privileged=True)

    if (
        ns_priv["metadata"]["labels"]["pod-security.kubernetes.io/enforce"]
        == "privileged"
    ):
        print("✅ Privileged namespace has privileged enforcement")
        passed += 1
    else:
        print("❌ Privileged namespace enforcement is wrong")
        failed += 1

    if (
        ns_priv["metadata"]["labels"]["pod-security.kubernetes.io/audit"]
        == "privileged"
    ):
        print("✅ Privileged namespace has privileged audit")
        passed += 1
    else:
        print("❌ Privileged namespace audit is wrong")
        failed += 1

    if ns_priv["metadata"]["labels"]["pod-security.kubernetes.io/warn"] == "privileged":
        print("✅ Privileged namespace has privileged warn")
        passed += 1
    else:
        print("❌ Privileged namespace warn is wrong")
        failed += 1

    print(f"\ngenerate_namespace tests: {passed} passed, {failed} failed")
    assert failed == 0, f"generate_namespace tests failed: {failed} failures"


def test_project_manifests_uses_generate_namespace():
    """Test that generate_project_manifests uses the shared generate_namespace function."""
    print("\n" + "=" * 60)
    print("Testing Project Manifests Use Shared Function")
    print("=" * 60)

    passed = 0
    failed = 0

    # Test regular project
    config = {
        "namespace": "my-project",
        "storage": {"results": "my-pvc", "size": "200Gi"},
    }

    manifests = generate_project_manifests(config)
    ns = manifests["namespaces"][0]

    # Should have standard Coldpress labels
    if ns["metadata"]["labels"]["app.kubernetes.io/managed-by"] == "coldpress":
        print("✅ Project namespace has Coldpress managed-by label")
        passed += 1
    else:
        print("❌ Project namespace missing Coldpress managed-by label")
        failed += 1

    if ns["metadata"]["labels"]["kueue.openshift.io/managed"] == "true":
        print("✅ Project namespace has kueue managed label")
        passed += 1
    else:
        print("❌ Project namespace missing kueue managed label")
        failed += 1

    # Should default to restricted
    if ns["metadata"]["labels"]["pod-security.kubernetes.io/enforce"] == "restricted":
        print("✅ Project namespace defaults to restricted security")
        passed += 1
    else:
        print("❌ Project namespace security is wrong")
        failed += 1

    # Test privileged project
    config_priv = {
        "namespace": "privileged-project",
        "privileged": True,
        "storage": {"results": "priv-pvc", "size": "100Gi"},
    }

    manifests_priv = generate_project_manifests(config_priv)
    ns_priv = manifests_priv["namespaces"][0]

    # Should have privileged security labels
    if (
        ns_priv["metadata"]["labels"]["pod-security.kubernetes.io/enforce"]
        == "privileged"
    ):
        print("✅ Privileged project has privileged enforcement")
        passed += 1
    else:
        print("❌ Privileged project enforcement is wrong")
        failed += 1

    if (
        ns_priv["metadata"]["labels"]["pod-security.kubernetes.io/audit"]
        == "privileged"
    ):
        print("✅ Privileged project has privileged audit")
        passed += 1
    else:
        print("❌ Privileged project audit is wrong")
        failed += 1

    if ns_priv["metadata"]["labels"]["pod-security.kubernetes.io/warn"] == "privileged":
        print("✅ Privileged project has privileged warn")
        passed += 1
    else:
        print("❌ Privileged project warn is wrong")
        failed += 1

    print(f"\nProject manifest tests: {passed} passed, {failed} failed")
    assert failed == 0, f"Project manifest tests failed: {failed} failures"


def test_no_namespace_duplication():
    """Test that namespace generation logic is not duplicated."""
    print("\n" + "=" * 60)
    print("Testing No Namespace Duplication")
    print("=" * 60)

    import inspect
    from coldpress_setup import generator

    # Read the source code of generate_project_manifests
    source = inspect.getsource(generator.generate_project_manifests)

    # Should call generate_namespace function, not duplicate the logic
    assert "generate_namespace(" in source, (
        "generate_project_manifests does not call generate_namespace()"
    )
    print("✅ generate_project_manifests calls generate_namespace()")

    # Should NOT have duplicated inline namespace creation
    ns_labels_count = source.count('ns_labels["kueue.openshift.io/managed"]')
    namespace_kind_count = source.count('"kind": "Namespace"')

    assert ns_labels_count == 0, (
        f"Found {ns_labels_count} inline ns_labels assignments (duplication)"
    )
    print("✅ No inline ns_labels assignment found")

    assert namespace_kind_count == 0, (
        f"Found {namespace_kind_count} inline Namespace definitions (duplication)"
    )
    print("✅ No inline Namespace kind definition found")

    print("\nNo duplication tests: all passed")


def main():
    """Run all namespace consistency tests."""
    print("\n" + "=" * 60)
    print("NAMESPACE CONSISTENCY TEST SUITE")
    print("=" * 60)

    try:
        # Test generate_namespace function
        test_generate_namespace_function()

        # Test project manifests use shared function
        test_project_manifests_uses_generate_namespace()

        # Test no duplication
        test_no_namespace_duplication()

        print("\n" + "=" * 60)
        print("✅ All namespace consistency tests passed!")
        print("=" * 60)
        return 0
    except (AssertionError, Exception) as e:
        print("\n" + "=" * 60)
        print(f"❌ Namespace consistency test failed: {e}")
        print("=" * 60)
        return 1


if __name__ == "__main__":
    sys.exit(main())
