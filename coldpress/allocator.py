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
from kubernetes import client, config


def get_nodes():
    """
    Get all coldpress-labeled nodes and their GPU availability.

    Returns:
        dict: Mapping of node ID to node info (name, gpus)
    """
    try:
        config.load_incluster_config()
    except Exception:
        config.load_kube_config()

    v1 = client.CoreV1Api()
    nodes = v1.list_node().items
    node_data = {}

    for node in nodes:
        labels = node.metadata.labels or {}
        if "coldpress.node" in labels:
            nodeid = labels["coldpress.node"]
            allocatable = node.status.allocatable or {}
            gpu_count_str = allocatable.get("nvidia.com/gpu", "0")
            try:
                gpu_count = int(gpu_count_str)
            except ValueError:
                gpu_count = 0

            node_data[str(nodeid)] = {
                "name": node.metadata.name,
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
    try:
        core_api = client.CoreV1Api()
        pods = core_api.list_pod_for_all_namespaces(
            field_selector=f"spec.nodeName={node_name},status.phase=Running"
        )

        total_gpus_used = 0
        for pod in pods.items:
            for container in pod.spec.containers:
                if container.resources and container.resources.requests:
                    gpu_request = container.resources.requests.get(
                        "nvidia.com/gpu", "0"
                    )
                    try:
                        total_gpus_used += float(gpu_request)
                    except ValueError:
                        pass

                # Also check limits in case requests aren't set
                if container.resources and container.resources.limits:
                    gpu_limit = container.resources.limits.get("nvidia.com/gpu", "0")
                    try:
                        # Use max of request and limit for this container
                        gpu_val = float(gpu_limit)
                        if gpu_val > 0 and (
                            not container.resources.requests
                            or not container.resources.requests.get("nvidia.com/gpu")
                        ):
                            total_gpus_used += gpu_val
                    except ValueError:
                        pass

        return total_gpus_used
    except Exception as e:
        logging.warning(f"Could not fetch actual GPU usage for node {node_name}: {e}")
        return 0


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
    custom_api = client.CustomObjectsApi()
    score = 0

    # Get the node name for actual usage check
    nodes = get_nodes()
    node_info = nodes.get(str(node_id), {})
    node_name = node_info.get("name", "unknown")

    # Get actual GPU usage from pods running on the node
    actual_gpu_usage = (
        get_actual_node_gpu_usage(node_name) if node_name != "unknown" else 0
    )

    try:
        cq = custom_api.get_cluster_custom_object(
            group="kueue.x-k8s.io",
            version="v1beta1",
            plural="clusterqueues",
            name="cluster-queue-test",
        )
        flavor_name = f"node{node_id}"
        flavors_usage = cq.get("status", {}).get("flavorsUsage", [])
        for usage in flavors_usage:
            if usage.get("name") == flavor_name:
                for res in usage.get("resources", []):
                    res_name = res.get("name")
                    total_str = str(res.get("total", "0"))
                    try:
                        if res_name == "nvidia.com/gpu":
                            kueue_gpu_usage = float(total_str)
                            # Use maximum of actual vs kueue tracked usage
                            # This ensures we account for workloads outside kueue's view
                            effective_gpu_usage = max(kueue_gpu_usage, actual_gpu_usage)
                            score += (effective_gpu_usage / max(req_gpus, 1)) * 10
                            if effective_gpu_usage != kueue_gpu_usage:
                                logging.info(
                                    f"Node {node_id}: Actual GPU usage ({actual_gpu_usage}) "
                                    f"differs from Kueue tracking ({kueue_gpu_usage}). "
                                    f"Using max: {effective_gpu_usage}"
                                )
                        elif "rdma" in res_name:
                            score += (float(total_str) / max(req_nics, 1)) * 5
                        elif res_name == "cpu":
                            if total_str.endswith("m"):
                                total_used = float(total_str[:-1]) / 1000.0
                            else:
                                total_used = float(total_str)
                            score += total_used * 0.1
                    except ValueError:
                        pass
        return score
    except Exception as e:
        logging.warning(f"Could not fetch Kueue queue status for scoring: {e}")
        # If kueue query fails, still use actual usage if available
        if actual_gpu_usage > 0:
            score = (actual_gpu_usage / max(req_gpus, 1)) * 10
        return score


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
