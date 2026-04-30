#!/bin/bash
# Test script to verify CLI exit codes are properly propagated

set +e  # Don't exit on error

echo "Testing Exit Code Propagation"
echo "=============================="

# Test 1: coldpress-setup with missing file (should return 1)
echo ""
echo "Test 1: coldpress-setup with non-existent file..."
python -m coldpress_setup.cli generate cluster non-existent-file.yaml >/dev/null 2>&1
EXIT_CODE=$?
if [ $EXIT_CODE -eq 1 ]; then
    echo "✅ Exit code 1 (expected for error)"
else
    echo "❌ Exit code $EXIT_CODE (expected 1)"
fi

# Test 2: coldpress with missing intent file (should return 2 for usage error)
echo ""
echo "Test 2: coldpress with non-existent intent..."
python -m coldpress.cli generate --intent non-existent.yaml >/dev/null 2>&1
EXIT_CODE=$?
if [ $EXIT_CODE -eq 2 ]; then
    echo "✅ Exit code 2 (expected for Click usage error)"
else
    echo "❌ Exit code $EXIT_CODE (expected 2)"
fi

# Test 3: coldpress with valid intent (should return 0)
echo ""
echo "Test 3: coldpress with valid intent..."
python -m coldpress.cli generate --intent examples/pytorch_ddp_training/intent_jobset.yaml >/dev/null 2>&1
EXIT_CODE=$?
if [ $EXIT_CODE -eq 0 ]; then
    echo "✅ Exit code 0 (expected for success)"
else
    echo "❌ Exit code $EXIT_CODE (expected 0)"
fi

echo ""
echo "=============================="
echo "Exit code tests complete!"
