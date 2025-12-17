import os
import time
import shutil
import tempfile
import subprocess
from kubernetes import client, config
from kubernetes.client.rest import ApiException
from kubernetes.stream import stream
from urllib.parse import urlparse

# Initialize kubernetes client
try:
    config.load_incluster_config()
except config.ConfigException:
    config.load_kube_config()

v1 = client.CoreV1Api()

def _copy_from_pod(pod_name, source_path, dest_dir, namespace="default"):
    """
    Copy data from a pod using the Kubernetes API instead of kubectl.
    Executes tar in the pod and extracts locally.
    """
    # Create tar command to execute in the pod
    exec_command = [
        'tar', 'czf', '-',
        '-C', source_path,
        '.',
        '--exclude', 'lost+found'
    ]

    try:
        # Execute command in pod and capture stdout
        resp = stream(
            v1.connect_get_namespaced_pod_exec,
            pod_name,
            namespace,
            command=exec_command,
            stderr=True,
            stdin=False,
            stdout=True,
            tty=False,
            _preload_content=False
        )

        # Create subprocess to extract tar locally
        tar_extract = subprocess.Popen(
            ['tar', 'xzf', '-', '-C', dest_dir],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )

        # Stream data from pod to local tar extraction
        while resp.is_open():
            resp.update(timeout=1)
            if resp.peek_stdout():
                tar_extract.stdin.write(resp.read_stdout().encode() if isinstance(resp.read_stdout(), str) else resp.read_stdout())
            if resp.peek_stderr():
                stderr_output = resp.read_stderr()
                if stderr_output:
                    print(f"stderr from pod: {stderr_output}")

        # Close stdin and wait for tar to finish
        tar_extract.stdin.close()
        tar_extract.wait()
        resp.close()

        if tar_extract.returncode != 0:
            stderr_output = tar_extract.stderr.read().decode() if tar_extract.stderr else ""
            print(f"Warning: tar extraction failed with code {tar_extract.returncode}: {stderr_output}")
            return False

        return True

    except ApiException as e:
        print(f"Error executing command in pod {pod_name}: {e}")
        return False
    except Exception as e:
        print(f"Error copying data from pod {pod_name}: {e}")
        return False

def _copy_results_(params, pvc_dirs, pod_log):
    run_params = params["run_params"]
    node_name = params["node_name"]
    tag = params["tag"]
    pod_name = tag + "-" + node_name + "-" + run_params.get("label", "unlabeled_pod")
    target_dir = params["target_dir"]
    os.makedirs(f"{target_dir}", exist_ok=True)
    if pod_log:
        with open(target_dir + "/" + pod_name + ".log", 'w') as f:
            f.write(pod_log)
    for i in range(len(pvc_dirs)):
        for filename in run_params["files_to_copy"]:
            if os.path.exists(f"{pvc_dirs[i]}/{filename}"):
                shutil.copy(f"{pvc_dirs[i]}/{filename}", f"{target_dir}/{filename}")
        for foldername in run_params["folders_to_copy"]:
            if os.path.isdir(f"{pvc_dirs[i]}/{foldername}"):
                if os.path.isdir(f"{target_dir}/{foldername}"):
                    shutil.rmtree(f"{target_dir}/{foldername}")
                shutil.copytree(f"{pvc_dirs[i]}/{foldername}", f"{target_dir}/{foldername}")     


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
    pod_log = ''
    os.makedirs(f"{params["tmpdir"]}", exist_ok=True)
    try:
        for i, mount_info in enumerate(run_params.get("ephemeral_mounts", [])):
            mount_path = mount_info["target"]
            storage_size = mount_info.get("size", "1Gi") # Default to 1Gi if not set
            volume_name = f"oneshot-pvc-mount-{i}"
            pvc_name = f"{pod_name}-{volume_name}" 
            pvc = client.V1PersistentVolumeClaim(
                metadata=client.V1ObjectMeta(name=pvc_name),
                spec=client.V1PersistentVolumeClaimSpec(
                    access_modes=["ReadWriteOnce"],
                    resources=client.V1ResourceRequirements(
                        requests={"storage": storage_size}
                    )
                )
            )
            print(f"Creating PVC: {pvc_name} for mount {mount_path}")
            v1.create_namespaced_persistent_volume_claim(namespace="default", body=pvc)
            created_pvcs.append({"name": pvc_name, "mount_path": mount_path})
            pod_volumes.append({
                "name": volume_name,
                "persistentVolumeClaim": {
                    "claimName": pvc_name
                }
            })
            container_volume_mounts.append({
                "name": volume_name,
                "mountPath": mount_path,
                "readOnly": False
            })
        for i, mount_info in enumerate(run_params.get("sys_mounts", [])):
            volume_name = f"oneshot-hostpath-mount-{i}"
            pod_volumes.append({
                "name": volume_name,
                "hostPath": {
                    "path": mount_info["source"], 
                    "type": "Directory" 
                }
            })
            container_volume_mounts.append({
                "name": volume_name,
                "mountPath": mount_info["target"], 
                "readOnly": mount_info.get("read_only", False)
            })
        # Build container spec
        container = client.V1Container(
            name="main",
            image=image,
            volume_mounts=[
                client.V1VolumeMount(
                    name=vm["name"],
                    mount_path=vm["mountPath"],
                    read_only=vm.get("readOnly", False)
                ) for vm in container_volume_mounts
            ],
            env=[
                client.V1EnvVar(name=e["name"], value=str(e["value"]))
                for e in run_params["env"]
            ],
            args=run_params["args"],
            resources=client.V1ResourceRequirements(
                **run_params.get("resources", {})
            ) if run_params.get("resources") else None
        )
        if run_params["command"]:
            container.command = run_params["command"]

        # Build pod spec
        pod_spec = client.V1PodSpec(
            node_name=node_name,
            host_network=True if run_params.get("network_mode") == "host" else False,
            restart_policy="Never",
            volumes=[
                client.V1Volume(
                    name=v["name"],
                    persistent_volume_claim=client.V1PersistentVolumeClaimVolumeSource(
                        claim_name=v["persistentVolumeClaim"]["claimName"]
                    ) if "persistentVolumeClaim" in v else None,
                    host_path=client.V1HostPathVolumeSource(
                        path=v["hostPath"]["path"],
                        type=v["hostPath"]["type"]
                    ) if "hostPath" in v else None
                ) for v in pod_volumes
            ],
            containers=[container]
        )
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
                raise ValueError(f"Could not determine port from endpoint address: {address}")
            container.readiness_probe = client.V1Probe(
                http_get=client.V1HTTPGetAction(
                    path=path,
                    port=port,
                    scheme=scheme
                ),
                initial_delay_seconds=initialDelaySeconds,
                period_seconds=periodSeconds,
                failure_threshold=failureThreshold
            )

        # Create pod object
        pod = client.V1Pod(
            metadata=client.V1ObjectMeta(
                name=pod_name,
                labels={"job": pod_name}
            ),
            spec=pod_spec
        )

        print(f"Creating pod: {pod_name}")
        v1.create_namespaced_pod(namespace="default", body=pod)
        print(f"Waiting for pod {pod_name} based on condition: {blocking_type}")
        while True:
            try:
                pod_status = v1.read_namespaced_pod(name=pod_name, namespace="default")
            except ApiException as e:
                if e.status == 404:
                    print(f"Pod {pod_name} not found. Assuming creation failed or it was deleted.")
                    break
                raise

            phase = pod_status.status.phase
            if blocking_type == "completion":
                if phase in ["Succeeded", "Failed"]:
                    print(f"Pod completed with phase: {phase}")
                    if run_params.get("log", False):
                        pod_log = v1.read_namespaced_pod_log(name=pod_name, namespace="default", container="main")
                    break
            elif blocking_type == "endpoint":
                if phase == "Failed":
                    print(f"Pod {pod_name} failed before becoming ready.")
                    if run_params.get("log", False):
                        pod_log = v1.read_namespaced_pod_log(name=pod_name, namespace="default", container="main")
                    break
                if pod_status.status.conditions:
                    ready_condition = next((c for c in pod_status.status.conditions if c.type == "Ready"), None)
                    if ready_condition and ready_condition.status == "True":
                        print(f"Pod {pod_name} is Ready at endpoint.")
                        break
            elif blocking_type == "delay":
                if phase == "Failed" or phase == "Succeeded":
                    print(f"Pod {pod_name} {phase} before delay could complete.")
                    if run_params.get("log", False):
                        pod_log = v1.read_namespaced_pod_log(name=pod_name, namespace="default", container="main")
                    break
                if phase == "Running":
                    delay_seconds = blocking_params.get("delay", 10)
                    print(f"Pod {pod_name} is Running. Waiting for {delay_seconds} seconds...")
                    time.sleep(delay_seconds)
                    print(f"Delay complete.")
                    break
            time.sleep(2)
    finally:
        if blocking_type == "completion":
            print("Starting resource cleanup...")
            try:
                print(f"Deleting pod: {pod_name}")
                v1.delete_namespaced_pod(name=pod_name, namespace="default")
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
                helper_pod = client.V1Pod(
                    metadata=client.V1ObjectMeta(name=helper_pod_name),
                    spec=client.V1PodSpec(
                        node_name=node_name,
                        restart_policy="Never",
                        containers=[
                            client.V1Container(
                                name="h",
                                image="alpine:latest",
                                command=["sleep", "3600"],
                                volume_mounts=[
                                    client.V1VolumeMount(name="v", mount_path=helper_mount_point)
                                ]
                            )
                        ],
                        volumes=[
                            client.V1Volume(
                                name="v",
                                persistent_volume_claim=client.V1PersistentVolumeClaimVolumeSource(
                                    claim_name=pvc_name
                                )
                            )
                        ]
                    )
                )
                v1.create_namespaced_pod(namespace="default", body=helper_pod)
                print(f"Waiting for {helper_pod_name} to be Running...")
                while True:
                    try:
                        pod_status = v1.read_namespaced_pod(name=helper_pod_name, namespace="default")
                        if pod_status.status.phase == "Running":
                            print("Helper pod is Running.")
                            break
                        if pod_status.status.phase in ["Failed", "Succeeded"]:
                            raise Exception(f"Helper pod {helper_pod_name} failed to start.")
                    except ApiException as e:
                        if e.status == 404:
                            raise Exception(f"Helper pod {helper_pod_name} not found.")
                        raise
                    time.sleep(1)
                print(f"Copying data from {helper_pod_name}:{helper_mount_point} to {dest_base_dir}")
                if not _copy_from_pod(helper_pod_name, helper_mount_point, dest_base_dir):
                    print(f"Warning: Failed to copy data from {helper_pod_name}:{helper_mount_point}")
        except Exception as e:
                print(f"Warning: Failed to copy data {pod_name}. Error: {e}")
        finally:
            print(f"Deleting helper pod: {helper_pod_name}")
            try:
                v1.delete_namespaced_pod(name=helper_pod_name, namespace="default")
            except Exception as e:
                print(f"Warning: Failed to delete helper pod {helper_pod_name}. Error: {e}")
            for pvc_info in created_pvcs:
                pvc_name = pvc_info["name"]
                try:
                    print(f"Deleting PVC: {pvc_name}")
                    v1.delete_namespaced_persistent_volume_claim(name=pvc_name, namespace="default")
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
    pod_log = ''
    if pod_name:
        if run_params.get("log", False):
            pod_log = v1.read_namespaced_pod_log(name=pod_name, namespace="default", container="main")
        print(f"Deleting pod: {pod_name}")
        v1.delete_namespaced_pod(name=pod_name, namespace="default")
    else:
        pod_name =  tag + "-" + node_name + "-" + run_params.get("label", "unlabeled_pod")
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
            helper_pod = client.V1Pod(
                metadata=client.V1ObjectMeta(name=helper_pod_name),
                spec=client.V1PodSpec(
                    node_name=node_name,
                    restart_policy="Never",
                    containers=[
                        client.V1Container(
                            name="h",
                            image="alpine:latest",
                            command=["sleep", "3600"],
                            volume_mounts=[
                                client.V1VolumeMount(name="v", mount_path=helper_mount_point)
                            ]
                        )
                    ],
                    volumes=[
                        client.V1Volume(
                            name="v",
                            persistent_volume_claim=client.V1PersistentVolumeClaimVolumeSource(
                                claim_name=pvc_name
                            )
                        )
                    ]
                )
            )
            v1.create_namespaced_pod(namespace="default", body=helper_pod)
            print(f"Waiting for {helper_pod_name} to be Running...")
            while True:
                try:
                    pod_status = v1.read_namespaced_pod(name=helper_pod_name, namespace="default")
                    if pod_status.status.phase == "Running":
                        print("Helper pod is Running.")
                        break
                    if pod_status.status.phase in ["Failed", "Succeeded"]:
                        raise Exception(f"Helper pod {helper_pod_name} failed to start.")
                except ApiException as e:
                    if e.status == 404:
                        raise Exception(f"Helper pod {helper_pod_name} not found.")
                    raise
                time.sleep(1)
            print(f"Copying data from {helper_pod_name}:{helper_mount_point} to {dest_base_dir}")
            if not _copy_from_pod(helper_pod_name, helper_mount_point, dest_base_dir):
                print(f"Warning: Failed to copy data from {helper_pod_name}:{helper_mount_point}")
            print(f"Deleting helper pod: {helper_pod_name}")
            try:
                v1.delete_namespaced_pod(name=helper_pod_name, namespace="default")
            except Exception as e:
                print(f"Warning: Failed to delete helper pod {helper_pod_name}. Error: {e}")
        except Exception as e:
            print(f"Error: {e}")
        finally:
            try:
                print(f"Deleting PVC: {pvc_name}")
                v1.delete_namespaced_persistent_volume_claim(name=pvc_name, namespace="default")
            except Exception as e:
                print(f"Warning: Failed to delete PVC {pvc_name}. Error: {e}")
    _copy_results_(params, dest_base_dir_list, pod_log)
    shutil.rmtree(params["tmpdir"], ignore_errors=True)
    return 
