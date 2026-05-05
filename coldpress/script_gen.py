# Assisted by: Claude Sonnet 4.5
"""Bash script generation for Coldpress jobs."""

import os
from datetime import datetime
from .constants import (
    DEFAULT_JOB_TIMEOUT,
    DEFAULT_SLEEP_DURATION,
    DEFAULT_SLEEP_INFINITY,
    EXPLORER_IMAGE,
    COPIER_IMAGE,
    MKDIR_IMAGE,
    MANIFEST_CONFIG,
)


def sanitize_identifier(identifier: str, identifier_type: str = "identifier") -> str:
    """
    Sanitize an identifier (job name, namespace) for safe use in shell commands.

    Args:
        identifier: Identifier to sanitize
        identifier_type: Type of identifier for error messages (e.g., "job name", "namespace")

    Returns:
        Sanitized identifier

    Raises:
        ValueError: If identifier contains dangerous characters
    """
    if not identifier or not isinstance(identifier, str):
        raise ValueError(f"Invalid {identifier_type}: {identifier}")

    # Check for dangerous shell metacharacters
    dangerous_chars = [
        "`",
        "$",
        "|",
        ";",
        "&",
        ">",
        "<",
        "\n",
        "\r",
        " ",
        "'",
        '"',
        "/",
        "\\",
    ]
    for char in dangerous_chars:
        if char in identifier:
            raise ValueError(
                f"Invalid character '{char}' in {identifier_type} '{identifier}'. "
                f"This could be a security risk."
            )

    # Ensure it's not empty
    if not identifier or identifier in [".", ".."]:
        raise ValueError(f"Invalid {identifier_type}: '{identifier}'")

    return identifier


def sanitize_filename(filename: str) -> str:
    """
    Sanitize a filename for safe use in shell commands.

    Args:
        filename: Filename to sanitize

    Returns:
        Sanitized filename

    Raises:
        ValueError: If filename contains dangerous characters or path traversal
    """
    if not filename or not isinstance(filename, str):
        raise ValueError(f"Invalid filename: {filename}")

    # Get basename to prevent path traversal
    basename = os.path.basename(filename)
    if basename != filename:
        raise ValueError(
            f"Filename contains path separators: '{filename}'. "
            "Only basenames are allowed."
        )

    # Check for dangerous shell metacharacters
    dangerous_chars = ["`", "$", "|", ";", "&", ">", "<", "\n", "\r", " ", "'", '"']
    for char in dangerous_chars:
        if char in basename:
            raise ValueError(
                f"Invalid character '{char}' in filename '{basename}'. "
                f"This could be a security risk."
            )

    # Ensure it's not empty after basename
    if not basename or basename in [".", ".."]:
        raise ValueError(f"Invalid filename: '{basename}'")

    return basename


def generate_run_script(
    job_name,
    namespace,
    timeout=None,
    configmap_name=None,
    configmap_files=None,
    manifest_type="jobset",
):
    """
    Generate run.sh script to apply and monitor the job.

    Args:
        job_name: Name of the job
        namespace: Kubernetes namespace
        timeout: Timeout for job completion (default: from DEFAULT_JOB_TIMEOUT)
        configmap_name: Optional ConfigMap name
        configmap_files: Optional list of files to include in ConfigMap
        manifest_type: Type of manifest (jobset, pytorchjob, tfjob, mpijob, inferenceservice)

    Returns:
        str: Bash script content
    """
    # Validate inputs to prevent shell injection
    sanitize_identifier(job_name, "job name")
    sanitize_identifier(namespace, "namespace")

    if timeout is None:
        timeout = DEFAULT_JOB_TIMEOUT

    configmap_apply = ""
    if configmap_name and configmap_files:
        # Sanitize filenames to prevent command injection
        sanitized_files = []
        for f in configmap_files:
            try:
                safe_filename = sanitize_filename(f)
                sanitized_files.append(safe_filename)
            except ValueError as e:
                raise ValueError(f"Invalid ConfigMap filename: {e}") from e

        from_files = " ".join([f"--from-file={f}" for f in sanitized_files])
        # Add coldpress- prefix if not already present
        if not configmap_name.startswith("coldpress-"):
            actual_configmap_name = f"coldpress-{configmap_name}"
        else:
            actual_configmap_name = configmap_name
        configmap_apply = f"""
# Create ConfigMap from files
echo "Creating ConfigMap from files..."
oc create configmap {actual_configmap_name} -n {namespace} {from_files} --dry-run=client -o yaml | oc apply -f -
"""

    # Get config or use defaults for unknown types
    config = MANIFEST_CONFIG.get(
        manifest_type,
        {
            "file": "manifest.yaml",
            "type": manifest_type,
            "apply_msg": manifest_type,
        },
    )

    manifest_file = config["file"]
    resource_type = config["type"]
    resource_name = f"coldpress-{job_name}"
    apply_cmd = f'echo "Applying {config["apply_msg"]}..."\noc apply -f {manifest_file}'

    # Generate services apply command if applicable
    services_apply = (
        """
# Apply services if they exist
if [ -f services.yaml ]; then
    echo "Applying Services..."
    oc apply -f services.yaml
fi"""
        if config.get("has_services", False)
        else ""
    )

    # Determine wait condition based on resource type
    if manifest_type == "inferenceservice":
        wait_message = (
            f"Waiting for InferenceService to be ready (timeout: {timeout})..."
        )
        wait_cmd = f"oc wait --for=condition=Ready $RESOURCE_TYPE/$JOB_NAME -n $NAMESPACE --timeout={timeout} 2>/dev/null"
        pod_selector = f"serving.kserve.io/inferenceservice={job_name}"
    else:
        wait_message = f"Waiting for job to complete (timeout: {timeout})..."
        wait_cmd = f"oc wait --for=condition=complete $RESOURCE_TYPE/$JOB_NAME -n $NAMESPACE --timeout={timeout} 2>/dev/null"
        pod_selector = f"coldpress.io/job-id={job_name}"

    script = f'''#!/bin/bash
# Generated by coldpress on {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}

set -e

# Change to script directory to handle relative paths
cd "$(dirname "$0")"

NAMESPACE="{namespace}"
JOB_NAME="{resource_name}"
RESOURCE_TYPE="{resource_type}"
{configmap_apply}
{apply_cmd}
{services_apply}

echo "{wait_message}"
{wait_cmd} || {{
    echo "Job may still be running or condition not supported for $RESOURCE_TYPE"
    echo "Current status:"
    oc get $RESOURCE_TYPE/$JOB_NAME -n $NAMESPACE
    oc get pods -n $NAMESPACE -l training.kubeflow.org/job-name=$JOB_NAME 2>/dev/null || \
    oc get pods -n $NAMESPACE -l {pod_selector} 2>/dev/null || \
    oc get pods -n $NAMESPACE | grep {job_name}
}}

echo "Job applied successfully!"
echo ""
echo "To monitor: ./monitor.sh"
echo "To check logs: ./logs.sh"
echo "To cleanup: ./cleanup.sh"
'''
    return script


def generate_cleanup_script(
    job_name, namespace, configmap_name=None, manifest_type="jobset"
):
    """
    Generate cleanup.sh script to delete job resources.

    Args:
        job_name: Name of the job (without coldpress- prefix)
        namespace: Kubernetes namespace
        configmap_name: Optional ConfigMap name to delete
        manifest_type: Type of manifest (jobset, pytorchjob, tfjob, mpijob, inferenceservice)

    Returns:
        str: Bash script content
    """
    # Validate inputs to prevent shell injection
    sanitize_identifier(job_name, "job name")
    sanitize_identifier(namespace, "namespace")

    configmap_delete = ""
    if configmap_name:
        # Add coldpress- prefix if not already present
        if not configmap_name.startswith("coldpress-"):
            actual_configmap_name = f"coldpress-{configmap_name}"
        else:
            actual_configmap_name = configmap_name
        configmap_delete = f"""
# Delete ConfigMap
echo "Deleting ConfigMap..."
oc delete configmap/{actual_configmap_name} -n $NAMESPACE --ignore-not-found=true
"""

    # Generate manifest-specific commands
    if manifest_type == "jobset":
        resource_type = "jobset"
        resource_name = f"coldpress-{job_name}"
        delete_cmd = f"oc delete {resource_type}/{resource_name} -n $NAMESPACE --ignore-not-found=true"
        services_delete = f"oc delete services -n $NAMESPACE -l coldpress/gid={resource_name} --ignore-not-found=true"
    elif manifest_type in ["pytorchjob", "tfjob", "mpijob"]:
        resource_name = f"coldpress-{job_name}"
        delete_cmd = f"oc delete {manifest_type}/{resource_name} -n $NAMESPACE --ignore-not-found=true"
        services_delete = ""  # Kubeflow jobs don't create separate services
    elif manifest_type == "inferenceservice":
        resource_name = f"coldpress-{job_name}"
        delete_cmd = f"oc delete {manifest_type}/{resource_name} -n $NAMESPACE --ignore-not-found=true"
        services_delete = ""
    else:
        resource_name = f"coldpress-{job_name}"
        delete_cmd = f"oc delete {manifest_type}/{resource_name} -n $NAMESPACE --ignore-not-found=true"
        services_delete = ""

    services_section = (
        f"""
# Delete Services
echo "Deleting Services..."
{services_delete}
"""
        if services_delete
        else ""
    )

    script = f'''#!/bin/bash
# Generated by coldpress on {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}

set -e

# Change to script directory to handle relative paths
cd "$(dirname "$0")"

NAMESPACE="{namespace}"
JOB_NAME="{resource_name}"
RESOURCE_TYPE="{manifest_type}"

echo "Cleaning up resources for job: $JOB_NAME"

# Delete main resource (this cascades to Jobs and Pods)
echo "Deleting $RESOURCE_TYPE..."
{delete_cmd}
{services_section}{configmap_delete}

# Delete helper pods (log-saver, coldpress-explorer)
echo "Deleting helper pods..."
oc delete pods -n $NAMESPACE -l app=coldpress-explorer,coldpress.io/job-id=$JOB_NAME --ignore-not-found=true 2>/dev/null || true
oc get pods -n $NAMESPACE --no-headers 2>/dev/null | grep -E "log-saver-${{JOB_NAME}}-|coldpress-explorer-${{JOB_NAME}}-|coldpress-copier-${{JOB_NAME}}-" | awk '{{print $1}}' | xargs -r oc delete pod -n $NAMESPACE --ignore-not-found=true 2>/dev/null || true

echo "Cleanup complete!"
'''
    return script


def generate_monitor_script(job_name, namespace, manifest_type="jobset"):
    """
    Generate monitor.sh script to watch job progress.

    Args:
        job_name: Name of the job
        namespace: Kubernetes namespace
        manifest_type: Type of manifest (jobset, pytorchjob, etc.)

    Returns:
        str: Bash script content
    """
    # Validate inputs to prevent shell injection
    sanitize_identifier(job_name, "job name")
    sanitize_identifier(namespace, "namespace")

    # Generate manifest-specific commands
    if manifest_type == "jobset":
        resource_type = "jobset"
        resource_name = f"coldpress-{job_name}"
        label_selector = f"coldpress.io/job-id={resource_name}"
    elif manifest_type == "pytorchjob":
        resource_type = "pytorchjob"
        resource_name = f"coldpress-{job_name}"
        label_selector = f"training.kubeflow.org/job-name={resource_name}"
    elif manifest_type == "tfjob":
        resource_type = "tfjob"
        resource_name = f"coldpress-{job_name}"
        label_selector = f"training.kubeflow.org/job-name={resource_name}"
    elif manifest_type == "mpijob":
        resource_type = "mpijob"
        resource_name = f"coldpress-{job_name}"
        label_selector = f"training.kubeflow.org/job-name={resource_name}"
    elif manifest_type == "inferenceservice":
        resource_type = "inferenceservice"
        resource_name = f"coldpress-{job_name}"
        label_selector = f"serving.kserve.io/inferenceservice={resource_name}"
    else:
        resource_type = manifest_type
        resource_name = f"coldpress-{job_name}"
        label_selector = f"coldpress.io/job-id={resource_name}"

    script = f'''#!/bin/bash
# Generated by coldpress on {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}

NAMESPACE="{namespace}"
JOB_NAME="{resource_name}"
RESOURCE_TYPE="{resource_type}"

echo "Monitoring $RESOURCE_TYPE: $JOB_NAME in namespace: $NAMESPACE"
echo "Press Ctrl+C to exit"
echo ""

watch -n 2 "oc get $RESOURCE_TYPE,job,pod -n $NAMESPACE -l {label_selector}"
'''
    return script


def generate_logs_script(job_name, namespace, storage_pvc, base_dir):
    """
    Generate logs.sh script to save and view logs from all pods.

    Args:
        job_name: Name of the job (without coldpress- prefix)
        namespace: Kubernetes namespace
        storage_pvc: PVC name for results storage
        base_dir: Base directory path in PVC

    Returns:
        str: Bash script content
    """
    # Validate inputs to prevent shell injection
    sanitize_identifier(job_name, "job name")
    sanitize_identifier(namespace, "namespace")
    sanitize_identifier(storage_pvc, "storage PVC")

    full_job_name = f"coldpress-{job_name}"

    script = f'''#!/bin/bash
# Generated by coldpress on {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}

NAMESPACE="{namespace}"
JOB_NAME="{full_job_name}"
STORAGE_PVC="{storage_pvc}"
BASE_DIR="{base_dir}"
LOGS_DIR="$BASE_DIR/logs"

echo "Fetching and saving logs for job: $JOB_NAME"
echo ""

# Get all pods for this job (try both label formats for compatibility)
PODS=$(oc get pods -n $NAMESPACE -l coldpress.io/job-id=$JOB_NAME -o jsonpath='{{.items[*].metadata.name}}' 2>/dev/null)
if [ -z "$PODS" ]; then
    PODS=$(oc get pods -n $NAMESPACE -l coldpress/gid=$JOB_NAME -o jsonpath='{{.items[*].metadata.name}}' 2>/dev/null)
fi

if [ -z "$PODS" ]; then
    echo "No pods found for job $JOB_NAME"
    exit 1
fi

# Create helper pod to save logs to PVC
POD_NAME="log-saver-${{JOB_NAME}}-$(date +%s)"

# Cleanup function
cleanup() {{
    if [ -n "$POD_NAME" ]; then
        echo ""
        echo "Cleaning up log saver pod..."
        oc delete pod/$POD_NAME -n $NAMESPACE --ignore-not-found=true 2>/dev/null
    fi
}}

# Register cleanup trap for EXIT, INT, TERM
trap cleanup EXIT INT TERM

echo "Creating log saver pod..."
cat <<EOF | oc apply -f -
apiVersion: v1
kind: Pod
metadata:
  name: $POD_NAME
  namespace: $NAMESPACE
spec:
  restartPolicy: Never
  containers:
  - name: saver
    image: {MKDIR_IMAGE}
    command: ["sleep", "{DEFAULT_SLEEP_DURATION}"]
    volumeMounts:
    - name: data
      mountPath: /data
  volumes:
  - name: data
    persistentVolumeClaim:
      claimName: $STORAGE_PVC
EOF

# Wait for pod to be ready
oc wait --for=condition=ready pod/$POD_NAME -n $NAMESPACE --timeout=30s

# Create logs directory
oc exec -n $NAMESPACE $POD_NAME -- mkdir -p /data/$LOGS_DIR

# Fetch and save logs from each pod
for POD in $PODS; do
    echo "===== Saving logs from $POD ====="

    # Get logs and save to PVC (pipe directly to avoid tar dependency)
    oc logs -n $NAMESPACE $POD --all-containers=true 2>&1 | oc exec -i -n $NAMESPACE $POD_NAME -- sh -c "cat > /data/$LOGS_DIR/$POD.log" || echo "Could not save logs from $POD"

    # Also show to stdout
    cat <<LOGEOF
----- $POD -----
$(oc logs -n $NAMESPACE $POD --all-containers=true 2>&1 || echo "Could not fetch logs")

LOGEOF
done

# Create combined log file
echo "Creating combined log file..."
oc exec -n $NAMESPACE $POD_NAME -- sh -c "cat /data/$LOGS_DIR/*.log > /data/$LOGS_DIR/combined.log 2>/dev/null || true"

echo ""
echo "Logs saved to: /data/$LOGS_DIR/"
echo "  - Individual pod logs: *.log"
echo "  - Combined log: combined.log"
'''
    return script


def generate_explore_script(job_name, namespace, storage_pvc, base_dir):
    """
    Generate explore.sh script to interactively explore results directory.

    Creates a temporary helper pod, mounts the PVC, and opens an interactive shell.
    Automatically cleans up the pod when the shell session exits.

    Args:
        job_name: Name of the job (without coldpress- prefix)
        namespace: Kubernetes namespace
        storage_pvc: Name of the storage PVC
        base_dir: Base directory path in PVC

    Returns:
        str: Bash script content
    """
    # Validate inputs to prevent shell injection
    sanitize_identifier(job_name, "job name")
    sanitize_identifier(namespace, "namespace")
    sanitize_identifier(storage_pvc, "storage PVC")

    full_job_name = f"coldpress-{job_name}"

    script = f'''#!/bin/bash
# Generated by coldpress on {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}

set -e

NAMESPACE="{namespace}"
JOB_NAME="{full_job_name}"
STORAGE_PVC="{storage_pvc}"
BASE_DIR="{base_dir}"
POD_NAME="coldpress-explorer-${{JOB_NAME}}-$(date +%s)"

echo "Starting interactive explorer pod..."
echo "PVC: $STORAGE_PVC"
echo "Base directory: $BASE_DIR"
echo ""

# Create temporary pod with PVC mounted
cat <<EOF | oc apply -f -
apiVersion: v1
kind: Pod
metadata:
  name: $POD_NAME
  namespace: $NAMESPACE
  labels:
    app: coldpress-explorer
    coldpress.io/job-id: $JOB_NAME
spec:
  restartPolicy: Never
  containers:
  - name: explorer
    image: {EXPLORER_IMAGE}
    command: ["sleep", "{DEFAULT_SLEEP_INFINITY}"]
    volumeMounts:
    - name: data
      mountPath: /data
  volumes:
  - name: data
    persistentVolumeClaim:
      claimName: $STORAGE_PVC
EOF

# Wait for pod to be ready
echo "Waiting for pod to start..."
oc wait --for=condition=ready pod/$POD_NAME -n $NAMESPACE --timeout=60s

echo ""
echo "===== Interactive Shell ====="
echo "You are now in the explorer pod."
echo "Results directory: /data/$BASE_DIR"
echo "PVC root: /data"
echo ""
echo "Useful commands:"
echo "  cd /data/$BASE_DIR    # Go to results directory"
echo "  ls -lh                # List files"
echo "  cat <file>            # View file contents"
echo "  tree                  # View directory tree (if installed)"
echo ""
echo "Type 'exit' to leave and cleanup the pod."
echo "============================================="
echo ""

# Cleanup function
cleanup() {{
    echo ""
    echo "Cleaning up explorer pod..."
    oc delete pod/$POD_NAME -n $NAMESPACE --ignore-not-found=true
    echo "Done!"
}}

# Register cleanup on exit
trap cleanup EXIT

# Open interactive shell
oc rsh -n $NAMESPACE $POD_NAME
'''
    return script


def generate_copy_script(job_name, namespace, storage_pvc, base_dir):
    """
    Generate cp.sh script to copy results from PVC to local directory.

    Creates a temporary helper pod, mounts the PVC, and copies results using tar.
    Automatically cleans up the pod when the copy is complete.

    Args:
        job_name: Name of the job (without coldpress- prefix)
        namespace: Kubernetes namespace
        storage_pvc: Name of the storage PVC
        base_dir: Base directory path in PVC

    Returns:
        str: Bash script content
    """
    # Validate inputs to prevent shell injection
    sanitize_identifier(job_name, "job name")
    sanitize_identifier(namespace, "namespace")
    sanitize_identifier(storage_pvc, "storage PVC")

    full_job_name = f"coldpress-{job_name}"

    script = f'''#!/bin/bash
# Generated by coldpress on {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}

set -e

NAMESPACE="{namespace}"
JOB_NAME="{full_job_name}"
STORAGE_PVC="{storage_pvc}"
BASE_DIR="{base_dir}"
POD_NAME="coldpress-copier-${{JOB_NAME}}-$(date +%s)"

# Determine destination directory
DEST_DIR="${{1:-$(dirname "$0")/results}}"

echo "Copying results from PVC to local directory..."
echo "PVC: $STORAGE_PVC"
echo "Source directory: /data/$BASE_DIR"
echo "Destination: $DEST_DIR"
echo ""

# Create temporary pod with PVC mounted
echo "Creating temporary pod..."
cat <<EOF | oc apply -f -
apiVersion: v1
kind: Pod
metadata:
  name: $POD_NAME
  namespace: $NAMESPACE
  labels:
    app: coldpress-copier
    coldpress.io/job-id: $JOB_NAME
spec:
  restartPolicy: Never
  containers:
  - name: copier
    image: {COPIER_IMAGE}
    command: ["sleep", "{DEFAULT_SLEEP_DURATION}"]
    volumeMounts:
    - name: data
      mountPath: /data
  volumes:
  - name: data
    persistentVolumeClaim:
      claimName: $STORAGE_PVC
EOF

# Wait for pod to be ready
echo "Waiting for pod to start..."
oc wait --for=condition=ready pod/$POD_NAME -n $NAMESPACE --timeout=60s

# Cleanup function
cleanup() {{
    echo ""
    echo "Cleaning up temporary pod..."
    oc delete pod/$POD_NAME -n $NAMESPACE --ignore-not-found=true
}}

# Register cleanup on exit
trap cleanup EXIT

# Create destination directory if it doesn't exist
mkdir -p "$DEST_DIR"

echo ""
echo "Copying files..."
# Use tar to copy files (more reliable than rsync)
oc exec -n $NAMESPACE $POD_NAME -- tar czf - -C /data/$BASE_DIR . | tar xzf - -C "$DEST_DIR"

echo ""
echo "===== Copy Complete ====="
echo "Results copied to: $DEST_DIR"
echo "=========================="
'''
    return script


def write_scripts(
    output_dir,
    job_name,
    namespace,
    storage_pvc=None,
    base_dir=None,
    configmap_name=None,
    configmap_files=None,
    manifest_type="jobset",
):
    """
    Write all bash scripts to output directory.

    Args:
        output_dir: Directory to write scripts to
        job_name: Name of the job
        namespace: Kubernetes namespace
        storage_pvc: Name of the storage PVC (optional)
        base_dir: Base directory path in PVC (optional)
        configmap_name: Name of ConfigMap to apply/delete (optional)
        configmap_files: List of files for ConfigMap (optional)
        manifest_type: Type of manifest (jobset, pytorchjob, etc.)
    """
    import os

    scripts = {
        "run.sh": generate_run_script(
            job_name,
            namespace,
            configmap_name=configmap_name,
            configmap_files=configmap_files,
            manifest_type=manifest_type,
        ),
        "cleanup.sh": generate_cleanup_script(
            job_name,
            namespace,
            configmap_name=configmap_name,
            manifest_type=manifest_type,
        ),
        "monitor.sh": generate_monitor_script(
            job_name, namespace, manifest_type=manifest_type
        ),
    }

    # Add logs, explore, and copy scripts if storage info is available
    if storage_pvc and base_dir:
        scripts["logs.sh"] = generate_logs_script(
            job_name, namespace, storage_pvc, base_dir
        )
        scripts["explore.sh"] = generate_explore_script(
            job_name, namespace, storage_pvc, base_dir
        )
        scripts["cp.sh"] = generate_copy_script(
            job_name, namespace, storage_pvc, base_dir
        )

    for filename, content in scripts.items():
        filepath = os.path.join(output_dir, filename)
        with open(filepath, "w") as f:
            f.write(content)
        # Make executable
        os.chmod(filepath, 0o755)
