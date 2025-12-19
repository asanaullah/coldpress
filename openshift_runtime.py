import os
import time
import shutil
import subprocess
from pathlib import Path
from kubernetes import client, config
from kubernetes.stream import stream
from urllib.parse import urlparse


def get_current_namespace_from_serviceAccount() -> str:
    namespace_path: Path = Path(
        "/var/run/secrets/kubernetes.io/serviceaccount/namespace"
    )

    if namespace_path.is_file():
        with namespace_path.open("r") as f:
            return f.read().strip()

    # Fallback if the file isn't present (e.g., service account not mounted)
    return "default"


def get_current_namespace_from_kubeconfig() -> str:
    try:
        contexts, active_context = config.list_kube_config_contexts()
    except config.ConfigException:
        return "default"

    namespace = active_context["context"].get("namespace")

    if not namespace:
        namespace = "default"

    return namespace


# Initialize Kubernetes client
try:
    config.load_incluster_config()
    current_namespace = get_current_namespace_from_serviceAccount()
except Exception:
    config.load_kube_config()
    current_namespace = get_current_namespace_from_kubeconfig()

# Create API clients
v1 = client.CoreV1Api()

# Get namespace from environment variable, default to "default"
NAMESPACE = os.getenv("COLDPRESS_NAMESPACE", current_namespace or "default")


def copy_from_pod(pod_name, namespace, source_path, dest_path):
    """Copy files from a pod using Kubernetes stream API instead of kubectl exec"""
    exec_command = ["tar", "cf", "-", "-C", source_path, ".", "--exclude", "lost+found"]

    print(f"Streaming tar data from pod {pod_name}...")
    resp = stream(
        v1.connect_get_namespaced_pod_exec,
        pod_name,
        namespace,
        command=exec_command,
        stderr=True,
        stdin=False,
        stdout=True,
        tty=False,
        _preload_content=False,
    )

    # Start tar extraction process
    tar_process = subprocess.Popen(
        ["tar", "xf", "-", "-C", dest_path],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    try:
        # Stream data from pod to tar process
        while resp.is_open():
            resp.update(timeout=1)
            if resp.peek_stdout():
                stdout_data = resp.read_stdout()
                if isinstance(stdout_data, str):
                    stdout_data = stdout_data.encode("utf-8")
                tar_process.stdin.write(stdout_data)
            if resp.peek_stderr():
                stderr_data = resp.read_stderr()
                print(f"Pod stderr: {stderr_data}")
    finally:
        resp.close()
        tar_process.stdin.close()
        tar_process.wait()

    if tar_process.returncode != 0:
        stderr_output = tar_process.stderr.read().decode()
        raise Exception(f"tar extraction failed: {stderr_output}")


def _copy_results_(params, pvc_dirs, pod_log):
    run_params = params["run_params"]
    node_name = params["node_name"]
    tag = params["tag"]
    pod_name = tag + "-" + node_name + "-" + run_params.get("label", "unlabeled_pod")
    target_dir = params["target_dir"]
    os.makedirs(f"{target_dir}", exist_ok=True)
    if pod_log:
        with open(target_dir + "/" + pod_name + ".log", "w") as f:
            f.write(pod_log)
    for i in range(len(pvc_dirs)):
        for filename in run_params["files_to_copy"]:
            if os.path.exists(f"{pvc_dirs[i]}/{filename}"):
                shutil.copy(f"{pvc_dirs[i]}/{filename}", f"{target_dir}/{filename}")
        for foldername in run_params["folders_to_copy"]:
            if os.path.isdir(f"{pvc_dirs[i]}/{foldername}"):
                if os.path.isdir(f"{target_dir}/{foldername}"):
                    shutil.rmtree(f"{target_dir}/{foldername}")
                shutil.copytree(
                    f"{pvc_dirs[i]}/{foldername}", f"{target_dir}/{foldername}"
                )


def openshift_run(params):
    run_params = params["run_params"]
    node_name = params["node_name"]
    tmpdir = params["tmpdir"]
    tag = params["tag"]
    blocking_params = run_params.get("blocking", {"type": "completion"})
    blocking_type = blocking_params.get("type", "completion")
    image = run_params["image"]
    pod_name = tag + "-" + node_name + "-" + run_params.get("label", "unlabeled_pod")
    pod_volumes = []
    container_volume_mounts = []
    created_pvcs = []
    pod_log = ""
    os.makedirs(f"{params['tmpdir']}", exist_ok=True)
    try:
        for i, mount_info in enumerate(run_params.get("ephemeral_mounts", [])):
            mount_path = mount_info["target"]
            storage_size = mount_info.get("size", "1Gi")  # Default to 1Gi if not set
            volume_name = f"oneshot-pvc-mount-{i}"
            pvc_name = f"{pod_name}-{volume_name}"
            pvc_spec = client.V1PersistentVolumeClaim(
                metadata=client.V1ObjectMeta(name=pvc_name),
                spec=client.V1PersistentVolumeClaimSpec(
                    access_modes=["ReadWriteOnce"],
                    resources=client.V1ResourceRequirements(
                        requests={"storage": storage_size}
                    ),
                ),
            )
            print(f"Creating PVC: {pvc_name} for mount {mount_path}")
            v1.create_namespaced_persistent_volume_claim(
                namespace=NAMESPACE, body=pvc_spec
            )
            created_pvcs.append({"name": pvc_name, "mount_path": mount_path})
            pod_volumes.append(
                {"name": volume_name, "persistentVolumeClaim": {"claimName": pvc_name}}
            )
            container_volume_mounts.append(
                {"name": volume_name, "mountPath": mount_path, "readOnly": False}
            )
        for i, mount_info in enumerate(run_params.get("sys_mounts", [])):
            volume_name = f"oneshot-hostpath-mount-{i}"
            pod_volumes.append(
                {
                    "name": volume_name,
                    "hostPath": {"path": mount_info["source"], "type": "Directory"},
                }
            )
            container_volume_mounts.append(
                {
                    "name": volume_name,
                    "mountPath": mount_info["target"],
                    "readOnly": mount_info.get("read_only", False),
                }
            )
        pod_spec = {
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {"name": pod_name, "labels": {"job": pod_name}},
            "spec": {
                "nodeName": node_name,
                "hostNetwork": True
                if run_params.get("network_mode") == "host"
                else False,
                "restartPolicy": "Never",
                "volumes": pod_volumes,  # <-- Uses the combined list of PVCs and hostPaths
                "containers": [
                    {
                        "name": "main",
                        "image": image,
                        "volumeMounts": container_volume_mounts,
                        "env": run_params["env"],
                        "args": run_params["args"],
                        "resources": run_params.get("resources", {}),
                    }
                ],
            },
        }
        if run_params["command"]:
            pod_spec["spec"]["containers"][0]["command"] = run_params["command"]
        if blocking_type == "endpoint":
            address = blocking_params.get("address", "http://127.0.0.1:8000")
            initialDelaySeconds = blocking_params.get("initialDelaySeconds", 30)
            periodSeconds = blocking_params.get("periodSeconds", 30)
            failureThreshold = blocking_params.get("failureThreshold", 10)
            if not address:
                raise ValueError("'endpoint' blocking type requires an 'address' key")
            parsed_url = urlparse(address)
            port = parsed_url.port
            scheme = parsed_url.scheme.upper()
            path = parsed_url.path or "/"
            if not port:
                raise ValueError(
                    f"Could not determine port from endpoint address: {address}"
                )
            pod_spec["spec"]["containers"][0]["readinessProbe"] = {
                "httpGet": {"path": path, "port": port, "scheme": scheme},
                "initialDelaySeconds": initialDelaySeconds,
                "periodSeconds": periodSeconds,
                "failureThreshold": failureThreshold,
            }
        print(f"Creating pod: {pod_name}")
        v1.create_namespaced_pod(namespace=NAMESPACE, body=pod_spec)
        print(f"Waiting for pod {pod_name} based on condition: {blocking_type}")
        while True:
            try:
                pod = v1.read_namespaced_pod(name=pod_name, namespace=NAMESPACE)
            except client.exceptions.ApiException as e:
                if e.status == 404:
                    print(
                        f"Pod {pod_name} not found. Assuming creation failed or it was deleted."
                    )
                    break
                raise
            phase = pod.status.phase
            if blocking_type == "completion":
                if phase in ["Succeeded", "Failed"]:
                    print(f"Pod completed with phase: {phase}")
                    pod_log = (
                        v1.read_namespaced_pod_log(
                            name=pod_name, namespace=NAMESPACE, container="main"
                        )
                        if run_params.get("log", False)
                        else ""
                    )
                    break
            elif blocking_type == "endpoint":
                if phase == "Failed":
                    print(f"Pod {pod_name} failed before becoming ready.")
                    pod_log = (
                        v1.read_namespaced_pod_log(
                            name=pod_name, namespace=NAMESPACE, container="main"
                        )
                        if run_params.get("log", False)
                        else ""
                    )
                    break
                if pod.status.conditions:
                    ready_condition = next(
                        (c for c in pod.status.conditions if c.type == "Ready"), None
                    )
                    if ready_condition and ready_condition.status == "True":
                        print(f"Pod {pod_name} is Ready at endpoint.")
                        break
            elif blocking_type == "delay":
                if phase == "Failed" or phase == "Succeeded":
                    print(f"Pod {pod_name} {phase} before delay could complete.")
                    pod_log = (
                        v1.read_namespaced_pod_log(
                            name=pod_name, namespace=NAMESPACE, container="main"
                        )
                        if run_params.get("log", False)
                        else ""
                    )
                    break
                if phase == "Running":
                    delay_seconds = blocking_params.get("delay", 10)
                    print(
                        f"Pod {pod_name} is Running. Waiting for {delay_seconds} seconds..."
                    )
                    time.sleep(delay_seconds)
                    print("Delay complete.")
                    break
            time.sleep(2)
    finally:
        if blocking_type == "completion":
            print("Starting resource cleanup...")
            try:
                print(f"Deleting pod: {pod_name}")
                v1.delete_namespaced_pod(name=pod_name, namespace=NAMESPACE)
            except Exception as e:
                print(f"Warning: Failed to delete pod {pod_name}. Error: {e}")

    if blocking_type in ["endpoint", "delay"]:
        resources = {"pod_name": pod_name, "pvc": []}
        for pvc_info in created_pvcs:
            pvc_name = pvc_info["name"]
            resources["pvc"].append(pvc_name)
        return resources
    else:
        dest_base_dir_list = []
        helper_pods_created = []
        try:
            print("Pod completed, copying data...")
            for pvc_info in created_pvcs:
                pvc_name = pvc_info["name"]
                dest_base_dir = os.path.join(tmpdir, f"{pod_name}_{pvc_name}")
                dest_base_dir_list.append(dest_base_dir)
                if os.path.exists(dest_base_dir):
                    print(f"Removing existing directory: {dest_base_dir}")
                    shutil.rmtree(dest_base_dir)
                os.makedirs(dest_base_dir, exist_ok=True)
                helper_pod_name = f"{pvc_name}-extractor"
                helper_mount_point = "/data"
                print(f"Creating helper pod: {helper_pod_name}")
                v1.create_namespaced_pod(
                    namespace=NAMESPACE,
                    body={
                        "apiVersion": "v1",
                        "kind": "Pod",
                        "metadata": {"name": helper_pod_name},
                        "spec": {
                            "nodeName": node_name,
                            "restartPolicy": "Never",
                            "containers": [
                                {
                                    "name": "h",
                                    "image": "alpine:latest",
                                    "command": ["sleep", "3600"],
                                    "volumeMounts": [
                                        {"name": "v", "mountPath": helper_mount_point}
                                    ],
                                }
                            ],
                            "volumes": [
                                {
                                    "name": "v",
                                    "persistentVolumeClaim": {"claimName": pvc_name},
                                }
                            ],
                        },
                    },
                )
                helper_pods_created.append(helper_pod_name)
                print(f"Waiting for {helper_pod_name} to be Running...")
                while True:
                    try:
                        pod = v1.read_namespaced_pod(
                            name=helper_pod_name, namespace=NAMESPACE
                        )
                        if pod.status.phase == "Running":
                            print("Helper pod is Running.")
                            break
                        if pod.status.phase in ["Failed", "Succeeded"]:
                            raise Exception(
                                f"Helper pod {helper_pod_name} failed to start."
                            )
                    except client.exceptions.ApiException as e:
                        if e.status != 404:
                            raise
                    time.sleep(1)
                print(
                    f"Copying data from {helper_pod_name}:{helper_mount_point} to {dest_base_dir}"
                )
                copy_from_pod(
                    helper_pod_name, NAMESPACE, helper_mount_point, dest_base_dir
                )
        except Exception as e:
            print(f"Warning: Failed to copy data {pod_name}. Error: {e}")
        finally:
            for helper_pod_name in helper_pods_created:
                try:
                    print(f"Deleting helper pod: {helper_pod_name}")
                    v1.delete_namespaced_pod(name=helper_pod_name, namespace=NAMESPACE)
                except Exception as e:
                    print(
                        f"Warning: Failed to delete helper pod {helper_pod_name}. Error: {e}"
                    )
            for pvc_info in created_pvcs:
                pvc_name = pvc_info["name"]
                try:
                    print(f"Deleting PVC: {pvc_name}")
                    v1.delete_namespaced_persistent_volume_claim(
                        name=pvc_name, namespace=NAMESPACE
                    )
                except Exception as e:
                    print(f"Warning: Failed to delete PVC {pvc_name}. Error: {e}")
        _copy_results_(params, dest_base_dir_list, pod_log)
        shutil.rmtree(params["tmpdir"], ignore_errors=True)
        return None


def openshift_cleanup(params, resources):
    run_params = params["run_params"]
    node_name = params["node_name"]
    tmpdir = params["tmpdir"]
    tag = params["tag"]
    pod_name = resources.get("pod_name")
    pod_log = ""
    if pod_name:
        pod_log = (
            v1.read_namespaced_pod_log(
                name=pod_name, namespace=NAMESPACE, container="main"
            )
            if run_params.get("log", False)
            else ""
        )
        print(f"Deleting pod: {pod_name}")
        v1.delete_namespaced_pod(name=pod_name, namespace=NAMESPACE)
    else:
        pod_name = (
            tag + "-" + node_name + "-" + run_params.get("label", "unlabeled_pod")
        )
    print("Copying data for pvcs...")
    dest_base_dir_list = []
    for pvc_name in resources.get("pvc", []):
        try:
            dest_base_dir = os.path.join(tmpdir, f"{pod_name}_{pvc_name}")
            dest_base_dir_list.append(dest_base_dir)
            if os.path.exists(dest_base_dir):
                print(f"Removing existing directory: {dest_base_dir}")
                shutil.rmtree(dest_base_dir)
            os.makedirs(dest_base_dir, exist_ok=True)
            helper_pod_name = f"{pvc_name}-extractor"
            helper_mount_point = "/data"
            print(f"Creating helper pod: {helper_pod_name}")
            v1.create_namespaced_pod(
                namespace=NAMESPACE,
                body={
                    "apiVersion": "v1",
                    "kind": "Pod",
                    "metadata": {"name": helper_pod_name},
                    "spec": {
                        "nodeName": node_name,
                        "restartPolicy": "Never",
                        "containers": [
                            {
                                "name": "h",
                                "image": "alpine:latest",
                                "command": ["sleep", "3600"],
                                "volumeMounts": [
                                    {"name": "v", "mountPath": helper_mount_point}
                                ],
                            }
                        ],
                        "volumes": [
                            {
                                "name": "v",
                                "persistentVolumeClaim": {"claimName": pvc_name},
                            }
                        ],
                    },
                },
            )
            print(f"Waiting for {helper_pod_name} to be Running...")
            while True:
                try:
                    pod = v1.read_namespaced_pod(
                        name=helper_pod_name, namespace=NAMESPACE
                    )
                    if pod.status.phase == "Running":
                        print("Helper pod is Running.")
                        break
                    if pod.status.phase in ["Failed", "Succeeded"]:
                        raise Exception(
                            f"Helper pod {helper_pod_name} failed to start."
                        )
                except client.exceptions.ApiException as e:
                    if e.status != 404:
                        raise
                time.sleep(1)
            print(
                f"Copying data from {helper_pod_name}:{helper_mount_point} to {dest_base_dir}"
            )
            copy_from_pod(helper_pod_name, NAMESPACE, helper_mount_point, dest_base_dir)
            print(f"Deleting helper pod: {helper_pod_name}")
            v1.delete_namespaced_pod(name=helper_pod_name, namespace=NAMESPACE)
        except Exception as e:
            print(f"Error: {e}")
        finally:
            try:
                print(f"Deleting PVC: {pvc_name}")
                v1.delete_namespaced_persistent_volume_claim(
                    name=pvc_name, namespace=NAMESPACE
                )
            except Exception as e:
                print(f"Warning: Failed to delete PVC {pvc_name}. Error: {e}")
    _copy_results_(params, dest_base_dir_list, pod_log)
    shutil.rmtree(params["tmpdir"], ignore_errors=True)
    return
