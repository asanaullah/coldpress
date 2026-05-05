# Assisted by: Claude Sonnet 4.5
"""Coldpress constants - centralized configuration values."""

# Version
COLDPRESS_VERSION = "0.2.1"

# Resource naming
COLDPRESS_PREFIX = "coldpress"
COLDPRESS_LABEL_MANAGED_BY = "coldpress"

# Images
MKDIR_IMAGE = "registry.access.redhat.com/ubi9/ubi-minimal:latest"
EXPLORER_IMAGE = "registry.access.redhat.com/ubi9/ubi-minimal:latest"
COPIER_IMAGE = "registry.access.redhat.com/ubi9/ubi:latest"

# Labels
COLDPRESS_LABELS = {
    "app.kubernetes.io/managed-by": COLDPRESS_LABEL_MANAGED_BY,
    "app.kubernetes.io/version": COLDPRESS_VERSION,
}

# Directory defaults
DEFAULT_DISCOVERY_DIR = "discovery"

# Script generation defaults
DEFAULT_JOB_TIMEOUT = "1h"
DEFAULT_MASTER_PORT = "29500"
DEFAULT_SLEEP_DURATION = "300"  # seconds for helper pods
DEFAULT_SLEEP_INFINITY = "infinity"  # for explorer pod


# Kueue
def get_kueue_queue_label(namespace: str) -> str:
    """Get Kueue local queue label for a namespace."""
    return f"{COLDPRESS_PREFIX}-local-queue-{namespace}"


def get_jobset_name(job_id: str) -> str:
    """Get JobSet name from job ID."""
    return f"{COLDPRESS_PREFIX}-{job_id}"


def get_service_name(jobset_name: str, task_id: int) -> str:
    """Get Service name for a task."""
    return f"{COLDPRESS_PREFIX}-s-{jobset_name}-{task_id}"


def get_pvc_name(namespace: str) -> str:
    """Get default PVC name for a namespace."""
    return f"{COLDPRESS_PREFIX}-{namespace}-storage"


# Manifest type configuration for script generation
MANIFEST_CONFIG = {
    "jobset": {
        "file": "jobset.yaml",
        "type": "jobset",
        "apply_msg": "JobSet",
        "has_services": True,
    },
    "pytorchjob": {
        "file": "pytorchjob.yaml",
        "type": "pytorchjob",
        "apply_msg": "PyTorchJob",
    },
    "tfjob": {
        "file": "tfjob.yaml",
        "type": "tfjob",
        "apply_msg": "TFJob",
    },
    "mpijob": {
        "file": "mpijob.yaml",
        "type": "mpijob",
        "apply_msg": "MPIJob",
    },
    "inferenceservice": {
        "file": "kservejob.yaml",
        "type": "inferenceservice",
        "apply_msg": "KServe InferenceService",
    },
    "rayjob": {
        "file": "rayjob.yaml",
        "type": "rayjob",
        "apply_msg": "rayjob",
    },
}

# Backend display names
BACKEND_NAME_MAP = {
    "kubeflow": "Kubeflow",
    "kserve": "KServe",
    "kuberay": "KubeRay",
    "jobset": "JobSet",
}

# Default directories (fallback values when env vars not set)
DEFAULT_PROJECT_DIR = "projects"
DEFAULT_OUTPUT_DIR = "output"
