#!/bin/bash
# Run all Coldpress tests

# Don't exit on first failure - run all tests and report at end
set +e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$SCRIPT_DIR/.."
cd "$REPO_ROOT"

# Ensure Python can find the modules
export PYTHONPATH="$REPO_ROOT:$PYTHONPATH"

echo ""
echo "════════════════════════════════════════════════════════════"
echo "  COLDPRESS TEST SUITE"
echo "════════════════════════════════════════════════════════════"
echo ""

FAILED=0
PASSED=0

# Function to run a test
run_test() {
    local test_name="$1"
    local test_cmd="$2"

    echo "──────────────────────────────────────────────────────────"
    echo "Running: $test_name"
    echo "──────────────────────────────────────────────────────────"

    if eval "$test_cmd"; then
        echo "✅ PASSED: $test_name"
        ((PASSED++))
    else
        echo "❌ FAILED: $test_name"
        ((FAILED++))
    fi
    echo ""
}

# Run all tests
run_test "Label Tests" "python tests/test_labels.py"
run_test "Security Tests" "python tests/test_security.py"
run_test "Error Handling Tests" "python tests/test_error_handling.py"
run_test "RoCE Disabled Tests" "python tests/test_roce_disabled.py"
run_test "Namespace Consistency Tests" "python tests/test_namespace_consistency.py"
run_test "Script Generation Security Tests" "python tests/test_script_gen_security.py"
run_test "Exit Code Tests" "bash tests/test_exit_codes.sh"

# Summary
echo "════════════════════════════════════════════════════════════"
echo "  TEST SUMMARY"
echo "════════════════════════════════════════════════════════════"
echo ""
echo "  ✅ Passed: $PASSED"
echo "  ❌ Failed: $FAILED"
echo ""

if [ $FAILED -eq 0 ]; then
    echo "  All tests passed!"
    echo ""
    echo "════════════════════════════════════════════════════════════"
    exit 0
else
    echo "  Some tests failed!"
    echo ""
    echo "════════════════════════════════════════════════════════════"
    exit 1
fi
