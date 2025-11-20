import os
import time
import shutil
import tempfile
import openshift_client as oc
from urllib.parse import urlparse

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
            pvc_spec = {
                "apiVersion": "v1",
                "kind": "PersistentVolumeClaim",
                "metadata": {
                    "name": pvc_name
                },
                "spec": {
                    "accessModes": ["ReadWriteOnce"],
                    "resources": {
                        "requests": {
                            "storage": storage_size 
                        }
                    }
                }
            }
            print(f"Creating PVC: {pvc_name} for mount {mount_path}")
            oc.create(pvc_spec)
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
        pod_spec = {
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "name": pod_name,
                "labels": {
                    "job": pod_name
                }
            },
            "spec": {
                "nodeName": node_name,
                "hostNetwork": True if run_params.get("network_mode") == "host" else False,
                "restartPolicy": "Never",
                "volumes": pod_volumes, # <-- Uses the combined list of PVCs and hostPaths
                "containers": [
                    {
                        "name": "main",
                        "image": image,
                        "volumeMounts": container_volume_mounts,
                        "env": run_params["env"],
                        "args": run_params["args"],
                        "resources": run_params.get("resources", {})
                    }
                ]
            }
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
                raise ValueError(f"Could not determine port from endpoint address: {address}")
            pod_spec["spec"]["containers"][0]["readinessProbe"] = {
                "httpGet": {
                    "path": path,
                    "port": port,
                    "scheme": scheme
                },
                "initialDelaySeconds": initialDelaySeconds, 
                "periodSeconds": periodSeconds,
                "failureThreshold": failureThreshold
            }
        print(f"Creating pod: {pod_name}")
        oc.create(pod_spec)
        print(f"Waiting for pod {pod_name} based on condition: {blocking_type}")
        while True:
            pod = oc.selector(f'pod/{pod_name}').object()
            if not pod:
                print(f"Pod {pod_name} not found. Assuming creation failed or it was deleted.")
                break 
            phase = pod.model.status.phase
            if blocking_type == "completion":
                if phase in ["Succeeded", "Failed"]:
                    print(f"Pod completed with phase: {phase}")
                    pod_log = oc.invoke("logs", pod_name, "-c", "main").out() if run_params.get("log", False) else ''
                    break
            elif blocking_type == "endpoint":
                if phase == "Failed":
                    print(f"Pod {pod_name} failed before becoming ready.")
                    pod_log = oc.invoke("logs", pod_name, "-c", "main").out() if run_params.get("log", False) else ''
                    break
                if pod.model.status.conditions:
                    ready_condition = next((c for c in pod.model.status.conditions if c.type == "Ready"), None)
                    if ready_condition and ready_condition.status == "True":
                        print(f"Pod {pod_name} is Ready at endpoint.")
                        break 
            elif blocking_type == "delay":
                if phase == "Failed" or phase == "Succeeded":
                    print(f"Pod {pod_name} {phase} before delay could complete.")
                    pod_log = oc.invoke("logs", pod_name, "-c", "main").out() if run_params.get("log", False) else ''
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
                oc.invoke("delete", f"pod/{pod_name}")
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
                oc.create({
                    "apiVersion": "v1", "kind": "Pod", "metadata": {"name": helper_pod_name},
                    "spec": {
                        "nodeName": node_name,
                        "restartPolicy": "Never",
                        "containers": [{"name": "h", "image": "alpine:latest", "command": ["sleep", "3600"],
                                        "volumeMounts": [{"name": "v", "mountPath": helper_mount_point}]}],
                        "volumes": [{"name": "v", "persistentVolumeClaim": {"claimName": pvc_name}}]
                    }
                })
                print(f"Waiting for {helper_pod_name} to be Running...")
                while True:
                    pod = oc.selector(f'pod/{helper_pod_name}').object()
                    if pod and pod.model.status.phase == "Running":
                        print("Helper pod is Running.")
                        break
                    if not pod or pod.model.status.phase in ["Failed", "Succeeded"]:
                        raise Exception(f"Helper pod {helper_pod_name} failed to start.")
                    time.sleep(1) 
                source_spec = f"{helper_pod_name}:{helper_mount_point}/."
                copy_cmd = f"oc exec {helper_pod_name} -- tar czf - -C {helper_mount_point} . --exclude lost+found | tar xzf - -C {dest_base_dir}"
                print(f"Executing: {copy_cmd}")
                if os.system(copy_cmd) != 0:
                    print(f"Warning: 'oc cp' command failed for {source_spec}")
        except Exception as e:
                print(f"Warning: Failed to copy data {pod_name}. Error: {e}")
        finally:
            print(f"Deleting helper pod: {helper_pod_name}")
            oc.invoke("delete", f"pod/{helper_pod_name}")
            for pvc_info in created_pvcs:
                pvc_name = pvc_info["name"]
                try:
                    print(f"Deleting PVC: {pvc_name}")
                    oc.invoke("delete", f"pvc/{pvc_name}")
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
        pod_log = oc.invoke("logs", pod_name, "-c", "main").out() if run_params.get("log", False) else ''
        print(f"Deleting pod: {pod_name}")
        oc.invoke("delete", f"pod/{pod_name}")
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
            oc.create({
                "apiVersion": "v1", "kind": "Pod", "metadata": {"name": helper_pod_name},
                "spec": {
                    "nodeName": node_name,
                    "restartPolicy": "Never",
                    "containers": [{"name": "h", "image": "alpine:latest", "command": ["sleep", "3600"],
                                    "volumeMounts": [{"name": "v", "mountPath": helper_mount_point}]}],
                    "volumes": [{"name": "v", "persistentVolumeClaim": {"claimName": pvc_name}}]
                }
            })
            print(f"Waiting for {helper_pod_name} to be Running...")
            while True:
                pod = oc.selector(f'pod/{helper_pod_name}').object()
                if pod and pod.model.status.phase == "Running":
                    print("Helper pod is Running.")
                    break
                if not pod or pod.model.status.phase in ["Failed", "Succeeded"]:
                    raise Exception(f"Helper pod {helper_pod_name} failed to start.")
                time.sleep(1) 
            source_spec = f"{helper_pod_name}:{helper_mount_point}/."
            copy_cmd = f"oc exec {helper_pod_name} -- tar czf - -C {helper_mount_point} . --exclude lost+found | tar xzf - -C {dest_base_dir}"
            print(f"Executing: {copy_cmd}")
            if os.system(copy_cmd) != 0:
                print(f"Warning: 'oc cp' command failed for {source_spec}")
            print(f"Deleting helper pod: {helper_pod_name}")
            oc.invoke("delete", f"pod/{helper_pod_name}")
        except Exception as e:
            print(f"Error: {e}")
        finally:
            try:
                print(f"Deleting PVC: {pvc_name}")
                oc.invoke("delete", f"pvc/{pvc_name}")
            except Exception as e:
                print(f"Warning: Failed to delete PVC {pvc_name}. Error: {e}")
    _copy_results_(params, dest_base_dir_list, pod_log)
    shutil.rmtree(params["tmpdir"], ignore_errors=True)
    return 
