#!/usr/bin/env python3
"""Test script generation security - verify filenames are sanitized."""

import sys
import pytest
from coldpress.script_gen import (
    generate_run_script,
    sanitize_filename,
    sanitize_identifier,
)


def test_sanitize_identifier():
    """Test identifier (job name, namespace) sanitization."""
    print("\n" + "=" * 60)
    print("Testing Identifier Sanitization (job names, namespaces)")
    print("=" * 60)

    # Valid identifiers should pass
    valid_cases = [
        "test-job",
        "my-namespace",
        "job-123",
        "coldpress-test",
        "prod-cluster",
    ]

    for identifier in valid_cases:
        try:
            result = sanitize_identifier(identifier, "job name")
            assert result == identifier
            print(f"✅ Valid: '{identifier}' -> '{result}'")
        except ValueError as e:
            pytest.fail(f"Valid identifier rejected: {identifier} - {e}")

    # Invalid identifiers should fail
    invalid_cases = [
        ("job;rm -rf /", "command injection"),
        ("$(whoami)", "command substitution"),
        ("`id`", "backtick substitution"),
        ("job|cat", "pipe"),
        ("job&background", "background"),
        ("job>output", "redirection"),
        ("job name", "space"),
        ("'job'", "single quote"),
        ('"job"', "double quote"),
        ("/etc/passwd", "path separator"),
        ("..\\windows", "backslash"),
        ("job$VAR", "dollar sign"),
    ]

    for identifier, reason in invalid_cases:
        try:
            result = sanitize_identifier(identifier, "job name")
            pytest.fail(
                f"Invalid identifier should have been rejected: {identifier} ({reason})"
            )
        except ValueError:
            print(f"✅ Rejected: '{identifier}' ({reason})")


def test_sanitize_filename_edge_cases():
    """Test edge cases for filename sanitization."""
    print("\n" + "=" * 60)
    print("Testing Filename Sanitization Edge Cases")
    print("=" * 60)

    # Valid filenames should pass
    valid_cases = [
        "config.yaml",
        "script_v2.sh",
        "data.tar.gz",
        "README.md",
        "file-123.txt",
    ]

    for filename in valid_cases:
        try:
            result = sanitize_filename(filename)
            assert result == filename
            print(f"✅ Valid: '{filename}' -> '{result}'")
        except ValueError as e:
            pytest.fail(f"Valid filename rejected: {filename} - {e}")

    # Invalid filenames should fail
    invalid_cases = [
        ("../../../etc/passwd", "path traversal"),
        ("file;rm -rf /", "command injection"),
        ("$(whoami).txt", "command substitution"),
        ("`id`.txt", "backtick substitution"),
        ("file|cat", "pipe"),
        ("file&background", "background"),
        ("file>output", "redirection"),
        ("file name.txt", "space"),
        ("'file'.txt", "single quote"),
        ('"file".txt', "double quote"),
    ]

    for filename, reason in invalid_cases:
        try:
            result = sanitize_filename(filename)
            pytest.fail(
                f"Invalid filename should have been rejected: {filename} ({reason})"
            )
        except ValueError:
            print(f"✅ Rejected: '{filename}' ({reason})")


def test_generate_run_script_with_safe_files():
    """Test that run script generation works with safe filenames."""
    print("\n" + "=" * 60)
    print("Testing Run Script Generation with Safe Files")
    print("=" * 60)

    safe_files = ["config.yaml", "script.sh", "data.json"]

    try:
        script = generate_run_script(
            job_name="test-job",
            namespace="test-ns",
            configmap_name="test-config",
            configmap_files=safe_files,
            manifest_type="jobset",
        )

        # Verify the script contains the expected files
        assert "--from-file=config.yaml" in script
        assert "--from-file=script.sh" in script
        assert "--from-file=data.json" in script
        assert "coldpress-test-config" in script
        print("✅ Run script generated successfully with safe files")
        print(f"   Files: {', '.join(safe_files)}")
    except Exception as e:
        pytest.fail(f"Failed to generate script with safe files: {e}")


def test_generate_run_script_with_dangerous_files():
    """Test that run script generation rejects dangerous filenames."""
    print("\n" + "=" * 60)
    print("Testing Run Script Generation with Dangerous Files")
    print("=" * 60)

    dangerous_test_cases = [
        (["config.yaml", "file;rm -rf /"], "command injection"),
        (["../../../etc/passwd"], "path traversal"),
        (["$(whoami).txt"], "command substitution"),
        (["`id`.txt"], "backtick substitution"),
        (["file|cat"], "pipe character"),
        (["file name.txt"], "space in filename"),
    ]

    for files, reason in dangerous_test_cases:
        try:
            generate_run_script(
                job_name="test-job",
                namespace="test-ns",
                configmap_name="test-config",
                configmap_files=files,
                manifest_type="jobset",
            )
            pytest.fail(
                f"Script generation should have rejected dangerous files: {files} ({reason})"
            )
        except ValueError as e:
            print(f"✅ Rejected: {files} ({reason})")
            assert "Invalid" in str(e) or "security" in str(e).lower()


def test_no_injection_in_generated_script():
    """Test that user-provided filenames don't inject into generated scripts."""
    print("\n" + "=" * 60)
    print("Testing Generated Scripts for User Input Injection")
    print("=" * 60)

    # Generate a script with safe files
    safe_files = ["config.yaml", "app.json"]
    script = generate_run_script(
        job_name="test-job",
        namespace="test-ns",
        configmap_name="test-config",
        configmap_files=safe_files,
        manifest_type="jobset",
    )

    # Check that user filenames appear safely in the script
    # They should appear as --from-file=<filename> with no dangerous characters
    assert "--from-file=config.yaml" in script
    assert "--from-file=app.json" in script

    # Check that the ConfigMap section doesn't contain dangerous user input patterns
    # Extract just the ConfigMap creation line
    configmap_section = ""
    for line in script.split("\n"):
        if "oc create configmap" in line:
            configmap_section = line
            break

    # Verify filenames in the configmap command are safe
    assert "../" not in configmap_section  # No path traversal
    assert ";rm" not in configmap_section  # No command injection
    assert "|cat" not in configmap_section  # No piping
    assert "&background" not in configmap_section  # No background execution
    assert ">output" not in configmap_section  # No redirection
    # Note: $(dirname "$0") is legitimate bash and appears in the template, not user input

    print("✅ User-provided filenames safely embedded in generated script")
    print(f"   Safe files: {', '.join(safe_files)}")


def main():
    """Run all script generation security tests."""
    print("\n" + "=" * 60)
    print("SCRIPT GENERATION SECURITY TEST SUITE")
    print("=" * 60)

    try:
        test_sanitize_identifier()
        test_sanitize_filename_edge_cases()
        test_generate_run_script_with_safe_files()
        test_generate_run_script_with_dangerous_files()
        test_no_injection_in_generated_script()

        print("\n" + "=" * 60)
        print("✅ All script generation security tests passed!")
        print("=" * 60)
        print("\nSecurity improvements:")
        print("  ✅ Job names and namespaces sanitized")
        print("  ✅ Filenames sanitized before use in shell commands")
        print("  ✅ Path traversal attempts blocked")
        print("  ✅ Command injection attempts blocked")
        print("  ✅ Shell metacharacters rejected")
        print("=" * 60)
        return 0
    except (AssertionError, Exception) as e:
        print("\n" + "=" * 60)
        print(f"❌ Script generation security test failed: {e}")
        print("=" * 60)
        import traceback

        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())
