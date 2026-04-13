# Assisted by: Claude Sonnet 4.5
"""Node allocation and resource scoring for Coldpress jobs.

This module allocates tasks to nodes based on:
- Total GPU quantity per node (not specific GPU device IDs)
- Current GPU usage tracked by Kueue and actual pod requests
- Node capacity and availability

The generated JobSet requests GPU quantities (e.g., nvidia.com/gpu: "1")
and lets Kubernetes scheduler assign actual GPU devices.
"""

import math
import logging
import subprocess
import json
import shutil


def _get_kubectl_cmd():
    """Detect kubectl or oc command."""
    if shutil.which("oc"):
        return "oc"
    elif shutil.which("kubectl"):
        return "kubectl"
    else:
        raise Exception("Neither kubectl nor oc found in PATH")


def get_nodes():
    """
    Get all coldpress-labeled nodes and their GPU availability.

    Returns:
        dict: Mapping of node ID to node info (name, gpus)
    """
    kubectl_cmd = _get_kubectl_cmd()
    result = subprocess.run(
        [kubectl_cmd, "get", "nodes", "-o", "json"],
        capture_output=True,
        text=True,
    )

    if result.returncode != 0:
        logging.warning(f"Failed to get nodes: {result.stderr}")
        return {}

    try:
        data = json.loads(result.stdout)
    except json.JSONDecodeError:
        logging.warning("Failed to parse nodes JSON")
        return {}

    node_data = {}
    for node in data.get("items", []):
        labels = node.get("metadata", {}).get("labels", {})
        if "coldpress.node" in labels:
            nodeid = labels["coldpress.node"]
            allocatable = node.get("status", {}).get("allocatable", {})
            gpu_count_str = allocatable.get("nvidia.com/gpu", "0")
            try:
                gpu_count = int(gpu_count_str)
            except ValueError:
                gpu_count = 0

            node_data[str(nodeid)] = {
                "name": node.get("metadata", {}).get("name", ""),
                "gpu_count": gpu_count,  # Total GPU quantity, not specific GPU IDs
            }

    return node_data


def get_actual_node_gpu_usage(node_name):
    """
    Get actual GPU usage on a node by summing up GPU requests from all running pods.
    This catches GPU workloads that might be running outside of kueue's tracking.

    Args:
        node_name: Name of the node to check

    Returns:
        float: Total GPUs in use on the node
    """
    kubectl_cmd = _get_kubectl_cmd()
    result = subprocess.run(
        [
            kubectl_cmd,
            "get",
            "pods",
            "--all-namespaces",
            "--field-selector",
            f"spec.nodeName={node_name},status.phase=Running",
            "-o",
            "json",
        ],
        capture_output=True,
        text=True,
    )

    if result.returncode != 0:
        logging.warning(f"Could not fetch pods for node {node_name}: {result.stderr}")
        return 0

    try:
        data = json.loads(result.stdout)
    except json.JSONDecodeError:
        logging.warning("Failed to parse pods JSON")
        return 0

    total_gpus_used = 0
    for pod in data.get("items", []):
        for container in pod.get("spec", {}).get("containers", []):
            resources = container.get("resources", {})

            # Check requests first
            requests = resources.get("requests", {})
            gpu_request = requests.get("nvidia.com/gpu", "0")
            try:
                total_gpus_used += float(gpu_request)
            except ValueError:
                pass

            # Also check limits in case requests aren't set
            if gpu_request == "0":
                limits = resources.get("limits", {})
                gpu_limit = limits.get("nvidia.com/gpu", "0")
                try:
                    gpu_val = float(gpu_limit)
                    if gpu_val > 0:
                        total_gpus_used += gpu_val
                except ValueError:
                    pass

    return total_gpus_used


def _get_node_name(node_id):
    """Get node name from node ID."""
    nodes = get_nodes()
    node_info = nodes.get(str(node_id), {})
    return node_info.get("name", "unknown")


def _fetch_kueue_status(kubectl_cmd):
    """Fetch Kueue ClusterQueue status."""
    result = subprocess.run(
        [kubectl_cmd, "get", "clusterqueue", "cluster-queue-test", "-o", "json"],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        logging.warning(
            f"Could not fetch Kueue queue status for scoring: {result.stderr}"
        )
        return None

    try:
        return json.loads(result.stdout)
    except json.JSONDecodeError:
        logging.warning("Failed to parse ClusterQueue JSON")
        return None


def _parse_cpu_usage(cpu_str):
    """Parse CPU usage string (handles 'm' suffix for millicores)."""
    if cpu_str.endswith("m"):
        return float(cpu_str[:-1]) / 1000.0
    return float(cpu_str)


def _calculate_resource_score(
    res_name, total_str, req_gpus, req_nics, actual_gpu_usage, kueue_gpu_usage, node_id
):
    """Calculate score contribution for a specific resource."""
    score = 0
    try:
        if res_name == "nvidia.com/gpu":
            kueue_gpu = float(total_str)
            effective_gpu_usage = max(kueue_gpu, actual_gpu_usage)
            score = (effective_gpu_usage / max(req_gpus, 1)) * 10

            if effective_gpu_usage != kueue_gpu:
                logging.info(
                    f"Node {node_id}: Actual GPU usage ({actual_gpu_usage}) "
                    f"differs from Kueue tracking ({kueue_gpu}). "
                    f"Using max: {effective_gpu_usage}"
                )
        elif "rdma" in res_name:
            score = (float(total_str) / max(req_nics, 1)) * 5
        elif res_name == "cpu":
            score = _parse_cpu_usage(total_str) * 0.1
    except ValueError:
        pass

    return score


def _calculate_score_from_kueue(
    kueue_data, node_id, req_gpus, req_nics, actual_gpu_usage
):
    """Calculate score from Kueue flavor usage data."""
    score = 0
    flavor_name = f"node{node_id}"
    flavors_usage = kueue_data.get("status", {}).get("flavorsUsage", [])

    for usage in flavors_usage:
        if usage.get("name") == flavor_name:
            for res in usage.get("resources", []):
                res_name = res.get("name")
                total_str = str(res.get("total", "0"))
                score += _calculate_resource_score(
                    res_name,
                    total_str,
                    req_gpus,
                    req_nics,
                    actual_gpu_usage,
                    0,
                    node_id,
                )

    return score


def get_node_demand_score(node_id, req_gpus, req_nics):
    """
    Calculate demand score for a node based on current Kueue usage and actual GPU usage.
    Lower score = less loaded node = better choice.

    Args:
        node_id: Coldpress node ID (0, 1, etc.)
        req_gpus: Required GPUs for this task
        req_nics: Required RoCE NICs for this task

    Returns:
        float: Demand score (lower is better)
    """
    kubectl_cmd = _get_kubectl_cmd()
    node_name = _get_node_name(node_id)
    actual_gpu_usage = (
        get_actual_node_gpu_usage(node_name) if node_name != "unknown" else 0
    )

    kueue_data = _fetch_kueue_status(kubectl_cmd)
    if not kueue_data:
        return (actual_gpu_usage / max(req_gpus, 1)) * 10 if actual_gpu_usage > 0 else 0

    score = _calculate_score_from_kueue(
        kueue_data, node_id, req_gpus, req_nics, actual_gpu_usage
    )
    return (
        score
        if score > 0
        else (actual_gpu_usage / max(req_gpus, 1)) * 10
        if actual_gpu_usage > 0
        else 0
    )


def allocate_node(req_gpus, req_nics):
    """
    Find the best node for a task based on resource requirements.

    Args:
        req_gpus: Required GPUs
        req_nics: Required RoCE NICs

    Returns:
        str: Node ID (e.g., "0", "1")

    Raises:
        Exception: If no suitable node is found
    """
    nodes = get_nodes()
    best_node = None
    lowest_score = math.inf

    for node_id, info in nodes.items():
        total_gpus = info.get("gpu_count", 0)
        if total_gpus < req_gpus:
            continue
        score = get_node_demand_score(node_id, req_gpus, req_nics)
        if score < lowest_score:
            lowest_score = score
            best_node = node_id

    if best_node is None:
        raise Exception(
            f"No nodes found satisfying requirements: GPUs: {req_gpus}, NICs: {req_nics}"
        )

    return best_node
