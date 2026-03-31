# Assisted by: Gemini 3, Claude Sonnet 4.5
import kopf
import math
import logging
import yaml
from kubernetes import client
from models import ConfigFile
from openshift_runtime import runtime
from datetime import datetime, timezone


rt = runtime()


def get_node_ip(target_node, namespace, job_id, node_task_map):
    target_node = str(target_node)
    if target_node in node_task_map:
        task_id = node_task_map[target_node]
        return f"s-{job_id}-{task_id}.{namespace}.svc"
    return "127.0.0.1"


@kopf.on.startup()
def configure(settings: kopf.OperatorSettings, **_):
    settings.posting.level = logging.INFO


def render_template(
    template_name, namespace, user_params, admin_namespace="coldpress-admin"
):
    api = client.CustomObjectsApi()
    try:
        # HARDCODED SECURITY: Always fetch from the admin namespace
        tpl = api.get_namespaced_custom_object(
            "coldpress.io", "v1", admin_namespace, "workload-templates", template_name
        )
        spec = tpl.get("spec", {})
    except Exception as e:
        raise kopf.PermanentError(
            f"Template '{template_name}' not found in admin namespace '{admin_namespace}': {e}"
        )

    def fmt(val):
        if isinstance(val, str):
            try:
                return val.format(**user_params)
            except KeyError:
                return val
        return val

    def format_nested(data):
        if isinstance(data, dict):
            return {str(fmt(k)): format_nested(v) for k, v in data.items()}
        elif isinstance(data, list):
            return [format_nested(v) for v in data]
        else:
            return str(fmt(data))

    run_params = {
        "label": f"{template_name}-task",
        "image": spec.get("image"),
        "command": [fmt(c) for c in spec.get("command", [])]
        if spec.get("command")
        else None,
        "args": [fmt(a) for a in spec.get("args", [])],
        "env": [
            {"name": e["name"], "value": fmt(e["value"])} for e in spec.get("env", [])
        ],
        "blocking": spec.get("blocking", {"type": "completion"}),
        "annotations": format_nested(spec.get("annotations", {})),
        "resources": format_nested(spec.get("resources", {})),
        "files_to_copy": [fmt(f) for f in spec.get("files_to_copy", [])],
        "ephemeral_mounts": [
            {k: fmt(v) for k, v in m.items()} for m in spec.get("ephemeral_mounts", [])
        ],
        "sys_mounts": [
            {k: fmt(v) for k, v in m.items()} for m in spec.get("sys_mounts", [])
        ],
        "tolerate_all": spec.get("tolerate_all", False),
        "log": True,
    }
    if run_params["blocking"].get("address"):
        run_params["blocking"]["address"] = fmt(run_params["blocking"]["address"])
    return run_params


@kopf.on.create("coldpress.io", "v1", "discovery-jobs")
def create_discovery(spec, name, namespace, body, **kwargs):
    job_id = f"{name}"
    template_name = spec.get("template")
    admin_namespace = "coldpress-admin"
    if not template_name:
        raise kopf.PermanentError("Discovery job must specify a 'template'")
    core_api = client.CoreV1Api()
    ns_obj = core_api.read_namespace(namespace)
    annotations = ns_obj.metadata.annotations or {}
    allowed_parsers = [
        p.strip()
        for p in annotations.get("coldpress.io/allowed-parsers", "").split(",")
        if p.strip()
    ]
    if template_name not in allowed_parsers and "*" not in allowed_parsers:
        raise kopf.PermanentError(
            f"Security Violation: Discovery parser '{template_name}' is not authorized for namespace '{namespace}'."
        )
    logging.info(
        f"Launching discovery {template_name} (ID: {job_id}) in namespace {namespace}"
    )
    labels = body.get("metadata", {}).get("labels", {})
    batch_id = labels.get("coldpress/batch")
    if batch_id:
        group_folder = batch_id
    else:
        creation_ts = body.get("metadata", {}).get("creationTimestamp")
        if creation_ts:
            dt = datetime.fromisoformat(creation_ts.replace("Z", "+00:00"))
            group_folder = dt.strftime("%Y%m%d_%H%M")
        else:
            group_folder = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M")
    api = client.CustomObjectsApi()
    try:
        tpl = api.get_namespaced_custom_object(
            "coldpress.io", "v1", admin_namespace, "discovery-templates", template_name
        )
        tpl_spec = tpl.get("spec", {})
    except Exception as e:
        raise kopf.PermanentError(f"DiscoveryTemplate '{template_name}' not found: {e}")
    image = tpl_spec.get("image", "alpine:latest")
    script = tpl_spec.get("script", "echo 'No script provided'")
    result_dir = tpl_spec.get("result_dir", template_name)
    nodes = rt.get_nodes()
    if not nodes:
        raise kopf.PermanentError("No nodes found with label coldpress.node")
    core_api = client.CoreV1Api()
    is_admin = False
    try:
        ns_obj = core_api.read_namespace(namespace)
        ns_labels = ns_obj.metadata.labels or {}
        if ns_labels.get("pod-security.kubernetes.io/enforce") == "privileged":
            is_admin = True
    except Exception:
        pass
    storage_pvc = f"{namespace}-storage"
    task_list = []
    for idx, (node_id, node_info) in enumerate(nodes.items()):
        node_name = node_info.get("name", "unknown")
        wrapper_script = f"""
mkdir -p /tmp/result
cd /tmp/result
{script}
"""
        run_params = {
            "label": f"discovery-{template_name}-{node_id}",
            "image": image,
            "command": ["bash", "-c"],
            "args": [wrapper_script],
            "env": [],
            "blocking": {"type": "completion"},
            "annotations": {},
            "resources": {},
            "ephemeral_mounts": [{"target": "/tmp/result", "size": "1Gi"}],
            "sys_mounts": [],
            "folders_to_copy": ["."],
            "files_to_copy": [],
            "log": True,
        }
        if is_admin:
            run_params["network_mode"] = "host"
            run_params["privileged"] = True
            run_params["tolerate_all"] = True
        params = {
            "run_params": run_params,
            "node_id": node_id,
            "node_name": node_name,
            "tmpdir": f"/tmp/{job_id}/{idx}",
            "result_path": f"discovery/{group_folder}/{result_dir}/{node_id}",
            "tag": f"{name}-{node_id}",
        }
        task_list.append(
            {
                "label": f"Discovery {template_name} on Node: {node_id}",
                "params": params,
                "task_id": idx,
            }
        )
    try:
        rt.run(job_id, task_list, namespace, storage_pvc)
        return {"status": "JobSubmitted", "job_id": job_id, "tasks": len(task_list)}
    except Exception as e:
        raise kopf.PermanentError(f"Failed to launch discovery: {e}")


@kopf.on.create("coldpress.io", "v1", "compute-jobs")
def create_compute_job(spec, name, namespace, body, **kwargs):
    job_id = f"{name}"
    logging.info(f"Launching compute job {job_id}")
    is_allocated = False
    owner_refs = body.get("metadata", {}).get("ownerReferences", [])
    for ref in owner_refs:
        if ref.get("kind") == "ColdpressResourceAllocator":
            is_allocated = True
            break
    try:
        config = ConfigFile.model_validate(spec)
    except Exception as e:
        raise kopf.PermanentError(f"Invalid configuration: {e}")
    core_api = client.CoreV1Api()
    try:
        ns_obj = core_api.read_namespace(namespace)
        annotations = ns_obj.metadata.annotations or {}
        if is_allocated:
            allowed_parsers = [
                p.strip()
                for p in annotations.get(
                    "coldpress.io/allowed-allocator-parsers", ""
                ).split(",")
                if p.strip()
            ]
            error_msg_type = "Resource Allocator"
        else:
            allowed_parsers = [
                p.strip()
                for p in annotations.get(
                    "coldpress.io/allowed-compute-parsers", ""
                ).split(",")
                if p.strip()
            ]
            error_msg_type = "Direct ComputeJ"
    except Exception as e:
        raise kopf.PermanentError(f"Failed to verify namespace permissions: {e}")
    for task in config.tasks:
        if task.template not in allowed_parsers and "*" not in allowed_parsers:
            raise kopf.PermanentError(
                f"Security Violation: Parser '{task.template}' is not authorized for {error_msg_type} use in namespace '{namespace}'. Allowed: {allowed_parsers}"
            )
    base_dir = f"coldpress_results/{name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    task_list = []
    node_task_map = {}
    for i, task in enumerate(config.tasks):
        s_node = str(task.node)
        if s_node not in node_task_map:
            node_task_map[s_node] = i
    for i, task in enumerate(config.tasks):
        ctx_params = task.params.copy()
        if "target_task" in ctx_params:
            target_task_id = ctx_params["target_task"]
            ip = f"s-{job_id}-{target_task_id}.{namespace}.svc"
            ctx_params["target_nodeip"] = ip
        elif "target_node" in ctx_params:
            ip = get_node_ip(
                ctx_params["target_node"], namespace, job_id, node_task_map
            )
            ctx_params["target_nodeip"] = i
        ctx_params.update({"job_id": job_id, "task_id": i, "namespace": namespace})
        run_params = render_template(task.template, namespace, ctx_params)
        node_info = rt.get_nodes().get(str(task.node), {})
        params = {
            "run_params": run_params,
            "node_id": str(task.node),
            "node_name": node_info.get("name", "unknown"),
            "tmpdir": f"/tmp/{job_id}/{i}",
            "result_path": f"{base_dir}/{i}",
            "tag": f"{name}-{i}",
        }
        task_list.append(
            {"label": f"Task {i}: {task.name}", "params": params, "task_id": i}
        )
    try:
        rt.run(job_id, task_list, namespace, config.storage.results)
        return {"job_id": job_id, "tasks": len(task_list)}
    except Exception as e:
        raise kopf.PermanentError(f"Failed to launch job: {e}")


def dump_provenance_to_pvc(namespace, job_name, compute_job_body, allocator_name=None):
    """Dump complete provenance to PVC before cleanup"""
    try:
        api = client.CustomObjectsApi()
        core_api = client.CoreV1Api()

        # Get the ColdpressResourceAllocator if we have the name
        allocator_data = None
        if allocator_name:
            try:
                allocator_data = api.get_namespaced_custom_object(
                    group="coldpress.io",
                    version="v1",
                    namespace=namespace,
                    plural="coldpressresourceallocators",
                    name=allocator_name
                )
            except Exception as e:
                logging.warning(f"Could not retrieve allocator {allocator_name}: {e}")

        # Reconstruct base_dir from compute job metadata
        # base_dir format: coldpress_results/{name}_{timestamp}
        creation_time = compute_job_body.get("metadata", {}).get("creationTimestamp")
        base_dir = None
        if creation_time:
            try:
                from datetime import datetime
                dt = datetime.fromisoformat(creation_time.replace('Z', '+00:00'))
                timestamp_str = dt.strftime('%Y%m%d_%H%M%S')
                base_dir = f"coldpress_results/{job_name}_{timestamp_str}"
            except Exception as e:
                logging.warning(f"Could not parse creation timestamp: {e}")

        # Get the JobSet (contains actual rendered pod specs)
        jobset_name = f"cpj-{job_name}"
        jobset_data = None
        try:
            jobset_data = api.get_namespaced_custom_object(
                group="jobset.x-k8s.io",
                version="v1alpha2",
                namespace=namespace,
                plural="jobsets",
                name=jobset_name
            )
        except Exception as e:
            logging.warning(f"Could not retrieve JobSet {jobset_name}: {e}")

        # Get all pods created by this job (for final status/logs info)
        pods_data = []
        try:
            # List all pods in namespace and filter by name prefix
            pods = core_api.list_namespaced_pod(namespace=namespace)
            # Filter pods that belong to this jobset
            for pod in pods.items:
                pod_name = pod.metadata.name
                # Pod names follow pattern: cpj-{job_name}-{task}-{replica}-{index}-{hash}
                if pod_name.startswith(f"cpj-{job_name}-"):
                    pods_data.append({
                        "name": pod.metadata.name,
                        "node": pod.spec.node_name,
                        "phase": pod.status.phase,
                        "start_time": pod.status.start_time.isoformat() if pod.status.start_time else None,
                        "container_statuses": [
                            {
                                "name": cs.name,
                                "state": str(cs.state),
                                "ready": cs.ready,
                                "restart_count": cs.restart_count,
                                "image": cs.image
                            } for cs in (pod.status.container_statuses or [])
                        ],
                        "spec": api.api_client.sanitize_for_serialization(pod.spec)
                    })
        except Exception as e:
            logging.warning(f"Could not retrieve pods for {job_name}: {e}")

        # Get template definitions (workload-templates from admin namespace)
        templates_data = {}
        try:
            tasks = compute_job_body.get("spec", {}).get("tasks", [])
            template_names = set(task.get("template") for task in tasks if task.get("template"))

            for template_name in template_names:
                try:
                    template = api.get_namespaced_custom_object(
                        group="coldpress.io",
                        version="v1",
                        namespace="coldpress-admin",
                        plural="workload-templates",
                        name=template_name
                    )
                    templates_data[template_name] = template
                except Exception as e:
                    logging.warning(f"Could not retrieve template {template_name}: {e}")
        except Exception as e:
            logging.warning(f"Could not retrieve templates: {e}")

        # Get node information (GPU types available at the time)
        nodes_data = {}
        try:
            # Get all nodes that were used
            nodes_used = set(pod_info.get("node") for pod_info in pods_data if pod_info.get("node"))
            core_api_client = client.CoreV1Api()
            for node_name in nodes_used:
                try:
                    node = core_api_client.read_node(node_name)
                    nodes_data[node_name] = {
                        "labels": node.metadata.labels,
                        "allocatable": node.status.allocatable,
                        "capacity": node.status.capacity,
                    }
                except Exception as e:
                    logging.warning(f"Could not retrieve node {node_name}: {e}")
        except Exception as e:
            logging.warning(f"Could not retrieve node info: {e}")

        # Create provenance record
        provenance = {
            "job_name": job_name,
            "namespace": namespace,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "compute_job": compute_job_body,
            "resource_allocator": allocator_data,
            "templates": templates_data,
            "jobset": jobset_data,
            "nodes": nodes_data,
            "pods": pods_data
        }

        # Write to PVC via a temporary pod
        provenance_yaml = yaml.dump(provenance, default_flow_style=False)

        # Find the results PVC
        storage_pvc = f"{namespace}-storage"

        # Escape single quotes in YAML for shell heredoc
        provenance_yaml_escaped = provenance_yaml.replace("'", "'\\''")

        # Determine provenance file path
        if base_dir:
            # Save inside the timestamped results directory
            provenance_path = f"/data/{base_dir}/provenance.yaml"
            mkdir_cmd = f"mkdir -p /data/{base_dir} && "
        else:
            # Fallback to root if base_dir not found
            provenance_path = f"/data/provenance_{job_name}.yaml"
            mkdir_cmd = ""

        # Create a simple pod to write the file
        write_pod = client.V1Pod(
            metadata=client.V1ObjectMeta(
                name=f"provenance-writer-{job_name}"[:63],
                namespace=namespace
            ),
            spec=client.V1PodSpec(
                restart_policy="Never",
                containers=[
                    client.V1Container(
                        name="writer",
                        image="registry.access.redhat.com/ubi9/ubi-minimal:latest",
                        command=["sh", "-c", f"{mkdir_cmd}cat > {provenance_path} << 'EOF'\n{provenance_yaml_escaped}\nEOF"],
                        volume_mounts=[
                            client.V1VolumeMount(
                                name="results",
                                mount_path="/data"
                            )
                        ]
                    )
                ],
                volumes=[
                    client.V1Volume(
                        name="results",
                        persistent_volume_claim=client.V1PersistentVolumeClaimVolumeSource(
                            claim_name=storage_pvc
                        )
                    )
                ]
            )
        )

        core_api.create_namespaced_pod(namespace, write_pod)
        logging.info(f"Provenance dumped to PVC {storage_pvc}/{provenance_path}")

        # Wait for provenance writer to complete, then clean it up
        import time
        max_wait = 60
        for i in range(max_wait):
            try:
                pod_status = core_api.read_namespaced_pod(
                    name=write_pod.metadata.name,
                    namespace=namespace
                )
                if pod_status.status.phase in ["Succeeded", "Failed"]:
                    # Delete the provenance writer pod
                    try:
                        core_api.delete_namespaced_pod(
                            name=write_pod.metadata.name,
                            namespace=namespace
                        )
                        logging.info(f"Cleaned up provenance writer pod {write_pod.metadata.name}")
                    except Exception as del_e:
                        logging.warning(f"Could not delete provenance writer pod: {del_e}")
                    break
            except Exception as read_e:
                # RBAC permission issue or pod not ready yet
                if i > 10:  # Only log after 10 seconds
                    logging.warning(f"Could not read provenance writer pod status: {read_e}")
                pass
            time.sleep(1)

    except Exception as e:
        logging.error(f"Failed to dump provenance for {job_name}: {e}")


@kopf.on.delete("coldpress.io", "v1", "compute-jobs")
@kopf.on.delete("coldpress.io", "v1", "discovery-jobs")
def delete_job(name, namespace, **kwargs):
    job_id = f"{name}"
    logging.info(f"Cleaning up job {job_id}")
    rt.delete(job_id, namespace)


@kopf.timer("coldpress.io", "v1", "compute-jobs", interval=10.0)
@kopf.timer("coldpress.io", "v1", "discovery-jobs", interval=10.0)
def monitor_status(name, namespace, body, **kwargs):
    job_id = f"{name}"
    status_info = rt.status(job_id, namespace)
    state = status_info.get("state", "Unknown")
    should_cleanup = False
    if state in ["Completed", "Failed"]:
        should_cleanup = True
    elif state == "Unknown":
        creation_timestamp = body.get("metadata", {}).get("creationTimestamp")
        if creation_timestamp:
            try:
                created_dt = datetime.fromisoformat(creation_timestamp)
                now_dt = datetime.now(timezone.utc)
                age = (now_dt - created_dt).total_seconds()
                if age > 60:
                    logging.warning(
                        f"Job {job_id} is in Unknown state (JobSet missing) for {age}s. Treating as zombie."
                    )
                    should_cleanup = True
            except Exception as e:
                logging.warning(f"Could not parse creationTimestamp for {job_id}: {e}")
    if should_cleanup:
        logging.info(
            f"Job {job_id} finished with state {state}. Initiating complete garbage collection."
        )

        # DUMP PROVENANCE BEFORE CLEANUP
        owner_refs = body.get("metadata", {}).get("ownerReferences", [])
        allocator_name = None
        for ref in owner_refs:
            if ref.get("kind") == "ColdpressResourceAllocator":
                allocator_name = ref.get("name")
                break

        dump_provenance_to_pvc(namespace, name, body, allocator_name)

        rt.delete(job_id, namespace)
        api = client.CustomObjectsApi()
        group = "coldpress.io"
        version = "v1"
        kind = body.get("kind", kwargs.get("kind"))
        plural = "discovery-jobs" if kind == "DiscoveryJ" else "compute-jobs"
        try:
            logging.info(
                f"Deleting CR {name} in {namespace} (kind: {kind}, plural: {plural})"
            )
            api.delete_namespaced_custom_object(
                group=group,
                version=version,
                namespace=namespace,
                plural=plural,
                name=name,
                body=client.V1DeleteOptions(),
            )
        except client.exceptions.ApiException as e:
            if e.status != 404:
                logging.error(f"Failed to delete CR {name}: {e}")

        # Delete parent ColdpressResourceAllocator if exists
        if allocator_name:
            try:
                logging.info(
                    f"Deleting parent Allocator CR {allocator_name} in {namespace}"
                )
                api.delete_namespaced_custom_object(
                    group=group,
                    version=version,
                    namespace=namespace,
                    plural="coldpressresourceallocators",
                    name=allocator_name,
                    body=client.V1DeleteOptions(),
                )
            except client.exceptions.ApiException as e:
                if e.status != 404:
                    logging.error(f"Failed to delete parent CRA {allocator_name}: {e}")
    return status_info


def get_actual_node_gpu_usage(node_name):
    """
    Get actual GPU usage on a node by summing up GPU requests from all running pods.
    This catches GPU workloads that might be running outside of kueue's tracking.
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
    custom_api = client.CustomObjectsApi()
    score = 0

    # Get the node name for actual usage check
    nodes = rt.get_nodes()
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
    nodes = rt.get_nodes()
    best_node = None
    lowest_score = math.inf
    for node_id, info in nodes.items():
        total_gpus = len(info.get("gpus", {}))
        if total_gpus < req_gpus:
            continue
        score = get_node_demand_score(node_id, req_gpus, req_nics)
        if score < lowest_score:
            lowest_score = score
            best_node = node_id
    if best_node is None:
        raise kopf.PermanentError(
            f"No nodes found satisfying requirements: GPUs: {req_gpus}, NICs: {req_nics}"
        )
    return best_node


@kopf.on.create("coldpress.io", "v1", "coldpressresourceallocators")
def handle_allocator(spec, name, namespace, body, **kwargs):
    logging.info(f"Allocating resources for {name}...")
    core_api = client.CoreV1Api()
    try:
        ns_obj = core_api.read_namespace(namespace)
        annotations = ns_obj.metadata.annotations or {}
        allowed_allocator_parsers = [
            p.strip()
            for p in annotations.get(
                "coldpress.io/allowed-allocator-parsers", ""
            ).split(",")
            if p.strip()
        ]
    except Exception as e:
        raise kopf.PermanentError(f"Failed to verify namespace permissions: {e}")
    api = client.CustomObjectsApi()
    tasks = spec.get("tasks", [])
    allocated_tasks = []
    for idx, task in enumerate(tasks):
        template_name = task.get("template")
        if (
            template_name not in allowed_allocator_parsers
            and "*" not in allowed_allocator_parsers
        ):
            raise kopf.PermanentError(
                f"Security Violation: Parser '{template_name}' is not authorized for Resource Allocator use in namespace '{namespace}'. Allowed: {allowed_allocator_parsers}"
            )
        user_params = task.get("params", {})
        try:
            tpl = api.get_namespaced_custom_object(
                "coldpress.io",
                "v1",
                "coldpress-admin",
                "workload-templates",
                template_name,
            )
            requirements = tpl.get("spec", {}).get("requirements", {})
        except Exception as e:
            raise kopf.PermanentError(f"Failed to fetch template {template_name}: {e}")
        req_gpus_str = requirements.get("gpus_per_node", "0").format(**user_params)
        req_nics_str = requirements.get("roce_nics_per_node", "0").format(**user_params)
        req_gpus = int(req_gpus_str) if req_gpus_str.isdigit() else 0
        req_nics = int(req_nics_str) if req_nics_str.isdigit() else 0
        chosen_node = allocate_node(req_gpus, req_nics)
        logging.info(
            f"Task '{task.get('name')}' allocated to Node {chosen_node} (GPUs needed: {req_gpus})"
        )
        allocated_task = {
            "name": task.get("name"),
            "template": template_name,
            "node": int(chosen_node),
            "params": user_params,
        }
        allocated_task["params"]["num_gpus"] = req_gpus
        if req_nics > 0:
            allocated_task["params"]["num_roce_nics"] = req_nics
        allocated_tasks.append(allocated_task)
    computej_body = {
        "apiVersion": "coldpress.io/v1",
        "kind": "ComputeJ",
        "metadata": {
            "name": f"{name}-allocated",
            "namespace": namespace,
            "ownerReferences": [
                {
                    "apiVersion": "coldpress.io/v1",
                    "kind": "ColdpressResourceAllocator",
                    "name": name,
                    "uid": body["metadata"]["uid"],
                    "controller": True,
                }
            ],
        },
        "spec": {"storage": spec.get("storage", {}), "tasks": allocated_tasks},
    }
    try:
        api.create_namespaced_custom_object(
            group="coldpress.io",
            version="v1",
            namespace=namespace,
            plural="compute-jobs",
            body=computej_body,
        )
        return {"status": "Allocated", "compute_job": computej_body["metadata"]["name"]}
    except Exception as e:
        raise kopf.PermanentError(f"Failed to create ComputeJ: {e}")
