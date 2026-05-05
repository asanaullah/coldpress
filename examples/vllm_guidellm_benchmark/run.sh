#!/bin/bash
# Manual Kubernetes orchestration - coordinate server and client

set -e

NAMESPACE="coldpress-project"

echo "========================================="
echo "vLLM + GuideLLM Benchmark Workflow"
echo "========================================="
echo ""

# Step 1: Apply server and service
echo "Step 1: Deploying vLLM inference server..."
oc apply -f vllm-server.yaml
oc apply -f service.yaml

# Step 2: Wait for pod to be created
echo "Step 2: Waiting for server pod to be created..."
sleep 5

# Get pod name
POD_NAME=$(oc get pods -n $NAMESPACE -l app=vllm-server --no-headers -o custom-columns=":metadata.name" | head -1)

if [ -z "$POD_NAME" ]; then
    echo "ERROR: Server pod not found!"
    exit 1
fi

echo "Server pod: $POD_NAME"

# Step 3: Wait for pod to be ready (this is the hard part!)
echo "Step 3: Waiting for server to be ready..."
echo "  - Checking readiness probe..."

MAX_WAIT=600  # 10 minutes
ELAPSED=0
READY=false

while [ $ELAPSED -lt $MAX_WAIT ]; do
    # Check if pod is ready
    READY_STATUS=$(oc get pod $POD_NAME -n $NAMESPACE -o jsonpath='{.status.conditions[?(@.type=="Ready")].status}' 2>/dev/null || echo "Unknown")

    if [ "$READY_STATUS" == "True" ]; then
        echo "  ✓ Server is ready!"
        READY=true
        break
    fi

    # Show progress
    if [ $((ELAPSED % 10)) -eq 0 ]; then
        PHASE=$(oc get pod $POD_NAME -n $NAMESPACE -o jsonpath='{.status.phase}' 2>/dev/null || echo "Unknown")
        echo "  - Still waiting... (${ELAPSED}s elapsed, phase: $PHASE)"
    fi

    sleep 2
    ELAPSED=$((ELAPSED + 2))
done

if [ "$READY" != "true" ]; then
    echo "ERROR: Server never became ready after ${MAX_WAIT}s"
    echo "Pod status:"
    oc describe pod $POD_NAME -n $NAMESPACE
    exit 1
fi

# Step 4: Verify service endpoint is accessible
echo "Step 4: Verifying service endpoint..."
echo "  - Testing http://vllm-server:8000/health"

# We need a test pod to check from inside the cluster
TEST_POD="endpoint-test-$$"
oc run $TEST_POD -n $NAMESPACE --image=curlimages/curl:latest --rm -i --restart=Never --command -- \
    curl -s http://vllm-server:8000/health > /dev/null

if [ $? -eq 0 ]; then
    echo "  ✓ Service endpoint is accessible!"
else
    echo "ERROR: Service endpoint not accessible"
    exit 1
fi

# Step 5: Launch benchmark client
echo "Step 5: Launching benchmark client..."
oc apply -f guidellm-client.yaml

# Step 6: Wait for client to complete
echo "Step 6: Waiting for benchmark to complete..."
oc wait --for=condition=complete job/guidellm-benchmark -n $NAMESPACE --timeout=5m || {
    echo "Benchmark job status:"
    oc get job guidellm-benchmark -n $NAMESPACE
    oc get pods -n $NAMESPACE -l app=guidellm-client
    exit 1
}

echo ""
echo "========================================="
echo "✓ Benchmark workflow completed!"
echo "========================================="
echo ""
echo "To cleanup:"
echo "  oc delete job vllm-inference-server guidellm-benchmark -n $NAMESPACE"
echo "  oc delete service vllm-server -n $NAMESPACE"
