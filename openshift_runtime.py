# Assisted by: Gemini 3
import os
import time
import shutil
import subprocess
from pathlib import Path
from kubernetes import client, config
from kubernetes.stream import stream
from urllib.parse import urlparse

class runtime:
    def get_current_namespace_from_serviceAccount(self) -> str:
        namespace_path: Path = Path(
            "/var/run/secrets/kubernetes.io/serviceaccount/namespace"
        )
        if namespace_path.is_file():
            with namespace_path.open("r") as f:
                return f.read().strip()
        # Fallback if the file isn't present (e.g., service account not mounted)
        return "default"

    def get_current_namespace_from_kubeconfig(self) -> str:
        try:
            contexts, active_context = config.list_kube_config_contexts()
        except config.ConfigException:
            return "default"
        namespace = active_context["context"].get("namespace")
        if not namespace:
            namespace = "default"
        return namespace

    def __init__(self):
        # Initialize Kubernetes client
        try:
            config.load_incluster_config()
            self.current_namespace = self.get_current_namespace_from_serviceAccount()
        except Exception:
            config.load_kube_config()
            self.current_namespace = self.get_current_namespace_from_kubeconfig()
        # Create API clients
        self.v1 = client.CoreV1Api()
        self.batch_v1 = client.BatchV1Api()
        # Get namespace from environment variable, default to "default"
        self.NAMESPACE = os.getenv("COLDPRESS_NAMESPACE", self.current_namespace or "default")
        self.KUEUE_QUEUE_NAME = os.getenv("COLDPRESS_KUEUE_QUEUE", "local-queue-coldpress")


    def get_nodes(self):
        try:
            config.load_incluster_config()
        except Exception:
            config.load_kube_config()
        nodes = self.v1.list_node().items
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
                gpu_availability_map = {}
                for i in range(gpu_count):
                    gpu_id = str(i)
                    gpu_availability_map[gpu_id] = False
                node_data[str(nodeid)] = {
                    "name": node.metadata.name,
                    "gpus": gpu_availability_map,
                }
        return node_data

    def copy_from_pod(self, pod_name, namespace, source_path, dest_path):
        """Copy files from a pod using Kubernetes stream API instead of kubectl exec"""
        exec_command = ["tar", "cf", "-", "-C", source_path, ".", "--exclude", "lost+found"]
        print(f"Streaming tar data from pod {pod_name}...")
        resp = stream(
            self.v1.connect_get_namespaced_pod_exec,
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


    def _copy_results_(self, params, pvc_dirs, pod_log):
        run_params = params["run_params"]
        node_name = params["node_name"]
        tag = params["tag"]
        log_name = f"{tag}-{node_name}-{run_params.get('label', 'job')}"
        target_dir = params["target_dir"]
        os.makedirs(f"{target_dir}", exist_ok=True)
        if pod_log:
            with open(target_dir + "/" + log_name + ".log", "w") as f:
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

    def get_job_pod_name(self, job_name, namespace):
        label_selector = f"job-name={job_name}"
        pods = self.v1.list_namespaced_pod(namespace=namespace, label_selector=label_selector)
        if pods.items:
            # Sort by creation timestamp, descending
            sorted_pods = sorted(
                pods.items, 
                key=lambda x: x.metadata.creation_timestamp, 
                reverse=True
            )
            return sorted_pods[0].metadata.name
        return None

    def _extract_data_from_pvc_(self, pvc_name, dest_base_dir, flavor_name):
        if os.path.exists(dest_base_dir):
            shutil.rmtree(dest_base_dir)
        os.makedirs(dest_base_dir, exist_ok=True)
        helper_job_name = f"{pvc_name}-extractor"
        helper_job_name = helper_job_name.lower().replace("_", "-")[:63]
        helper_mount_point = "/data"
        print(f"Creating helper job: {helper_job_name} (Flavor: {flavor_name})")
        
        pod_template = client.V1PodTemplateSpec(
            metadata=client.V1ObjectMeta(labels={"app": helper_job_name}),
            spec=client.V1PodSpec(
                restart_policy="Never",
                volumes=[
                    client.V1Volume(
                        name="v",
                        persistent_volume_claim=client.V1PersistentVolumeClaimVolumeSource(claim_name=pvc_name)
                    )
                ],
                containers=[
                    client.V1Container(
                        name="h",
                        image="alpine:latest",
                        command=["sleep", "3600"], 
                        volume_mounts=[
                            client.V1VolumeMount(name="v", mount_path=helper_mount_point)
                        ],
                        resources=client.V1ResourceRequirements(
                            requests={"cpu": "10m", "memory": "10Mi"}
                        )
                    )
                ],
            ),
        )
        job_body = client.V1Job(
            api_version="batch/v1",
            kind="Job",
            metadata=client.V1ObjectMeta(
                name=helper_job_name,
                labels={
                    "kueue.x-k8s.io/queue-name": self.KUEUE_QUEUE_NAME, 
                    "kueue.x-k8s.io/resource-flavor": flavor_name
                } 
            ),
            spec=client.V1JobSpec(
                # FIX: Explicit suspend helps Kueue adopt the job cleanly
                suspend=True,
                template=pod_template,
                backoff_limit=0,
                completions=1,
                parallelism=1
            )
        )
        try:
            self.batch_v1.create_namespaced_job(namespace=self.NAMESPACE, body=job_body)
            helper_pod_name = None
            print(f"Waiting for Helper Job {helper_job_name} to start...")
            
            # FIX: Added timeout to prevent infinite loops
            timeout_counter = 0
            max_timeout = 120 # 2 minutes
            
            while timeout_counter < max_timeout:
                try:
                    current_job = self.batch_v1.read_namespaced_job(name=helper_job_name, namespace=self.NAMESPACE)
                    if not current_job.spec.suspend:
                        if not helper_pod_name:
                            helper_pod_name = self.get_job_pod_name(helper_job_name, self.NAMESPACE)
                    
                    if helper_pod_name:
                        pod = self.v1.read_namespaced_pod(name=helper_pod_name, namespace=self.NAMESPACE)
                        if pod.status.phase == "Running":
                            print("Helper pod is Running.")
                            break
                        if pod.status.phase in ["Failed", "Succeeded"]:
                            raise Exception(f"Helper pod {helper_pod_name} finished prematurely ({pod.status.phase}).")
                except client.exceptions.ApiException as e:
                    if e.status != 404:
                        print(f"Warning: Error checking helper pod: {e}")
                time.sleep(1)
                timeout_counter += 1
            
            if timeout_counter >= max_timeout:
                raise Exception(f"Timeout waiting for Helper Job {helper_job_name} to start.")

            print(f"Copying data from {helper_pod_name}...")
            self.copy_from_pod(helper_pod_name, self.NAMESPACE, helper_mount_point, dest_base_dir)

        except Exception as e:
            print(f"Error extracting PVC {pvc_name}: {e}")
        finally:
            try:
                print(f"Deleting Helper Job: {helper_job_name}")
                self.batch_v1.delete_namespaced_job(
                    name=helper_job_name, 
                    namespace=self.NAMESPACE, 
                    body=client.V1DeleteOptions(propagation_policy="Foreground") 
                )
                time.sleep(2)
            except Exception:
                pass

    def _wait_for_pvc_bound_(self, pvc_name, timeout=120):
        """Polls the PVC status until it is Bound or timeout occurs."""
        print(f"Waiting for PVC {pvc_name} to bind...")
        start_time = time.time()
        while time.time() - start_time < timeout:
            try:
                pvc = self.v1.read_namespaced_persistent_volume_claim(
                    name=pvc_name, 
                    namespace=self.NAMESPACE
                )
                if pvc.status.phase == "Bound":
                    print(f"PVC {pvc_name} is Bound.")
                    return True
                # Optional: Check for specific events/errors if needed
            except client.exceptions.ApiException as e:
                print(f"Warning: Error reading PVC {pvc_name}: {e}")
            
            time.sleep(2)
            
        raise Exception(f"Timeout waiting for PVC {pvc_name} to bind after {timeout} seconds.")

    def delete_job(self, job_name):
        try:
            self.batch_v1.delete_namespaced_job(
                name=job_name,
                namespace=self.NAMESPACE,
                body=client.V1DeleteOptions(propagation_policy="Foreground")
            )
        except Exception as e:
            print(f"Error force deleting job {job_name}: {e}")

    def run(self, params):
        run_params = params["run_params"]
        node_name = params["node_name"]
        tmpdir = params["tmpdir"]
        tag = params["tag"]
        blocking_params = run_params.get("blocking", {"type": "completion"})
        blocking_type = blocking_params.get("type", "completion")
        image = run_params["image"]
        job_name = tag + "-" + node_name + "-" + run_params.get("label", "unlabeled-job")
        job_name = job_name.lower().replace("_", "-")
        flavor_name = f"node{params['node_id']}"
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
                pvc_name = f"{job_name}-{volume_name}"
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
                self.v1.create_namespaced_persistent_volume_claim(
                    namespace=self.NAMESPACE, body=pvc_spec
                )
                self._wait_for_pvc_bound_(pvc_name)
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
            init_containers = []
            chmod_targets = []
            # Identify mounts that need permission fixing (ephemeral PVCs)
            for mount_info in run_params.get("ephemeral_mounts", []):
                chmod_targets.append(mount_info["target"])
            # Only add the init container if there are targets
            if chmod_targets:
                # Build a command string like: "chmod 777 /path1 && chmod 777 /path2"
                chmod_cmd = " && ".join([f"chmod -R 777 {t}" for t in chmod_targets])
                init_containers.append(
                    client.V1Container(
                        name="fix-permissions",
                        image="alpine:latest",
                        # We use shell to execute the chmod command
                        command=["sh", "-c", chmod_cmd],
                        volume_mounts=container_volume_mounts,
                        resources=client.V1ResourceRequirements(
                            requests={"cpu": "10m", "memory": "10Mi"}
                        ),
                        security_context=client.V1SecurityContext(
                            run_as_user=0  # Attempt to run as root to fix perms
                        )
                    )
                )
            # For now, we ignore specific GPU requests since this is not supported in k8s
            # will need to implement a workaround using uuids from a gpu discovery tool
            # Assume max of one gpu requested per job
            if "nvidia.com/gpu" in run_params.get("resources", {}).get("limits", {}):
                run_params["resources"]["limits"]["nvidia.com/gpu"] = 1
            pod_template = client.V1PodTemplateSpec(
                metadata=client.V1ObjectMeta(labels={"app": job_name}),
                spec=client.V1PodSpec(
                    node_selector={"coldpress.node": params['node_id']},
                    init_containers=init_containers,
                    host_network=True if run_params.get("network_mode") == "host" else False,
                    restart_policy="Never",
                    volumes=pod_volumes,
                    containers=[
                        client.V1Container(
                            name="main",
                            image=image,
                            volume_mounts=container_volume_mounts,
                            env=[client.V1EnvVar(name=k, value=str(v)) for k, v in run_params.get("env", {}).items()] 
                                if isinstance(run_params.get("env"), dict) else run_params.get("env"),
                            args=run_params["args"],
                            command=run_params["command"],
                            resources=client.V1ResourceRequirements(
                                requests=run_params.get("resources", {}).get("limits", {}),
                                limits=run_params.get("resources", {}).get("limits", {})
                            ) if run_params.get("resources") else None,
                        )
                    ],
                ),
            )
            #if run_params["command"]:
            #    pod_spec["spec"]["containers"][0]["command"] = run_params["command"]
            if blocking_type == "endpoint":
                address = blocking_params.get("address", "http://127.0.0.1:8000")
                initialDelaySeconds = blocking_params.get("initialDelaySeconds", 30)
                periodSeconds = blocking_params.get("periodSeconds", 30)
                failureThreshold = blocking_params.get("failureThreshold", 10)
                parsed_url = urlparse(address)
                port = parsed_url.port
                scheme = parsed_url.scheme.upper()
                path = parsed_url.path or "/"
                if not port:
                    raise ValueError(
                        f"Could not determine port from endpoint address: {address}"
                    )
                pod_template.spec.containers[0].readiness_probe = client.V1Probe(
                    http_get=client.V1HTTPGetAction(path=path, port=port, scheme=scheme),
                    initial_delay_seconds=initialDelaySeconds,
                    period_seconds=periodSeconds,
                    failure_threshold=failureThreshold,
                )
            job_spec = client.V1JobSpec(
                suspend=True,
                template=pod_template,
                backoff_limit=0,
                completions=1,
                parallelism=1
            )
            job_body = client.V1Job(
                api_version="batch/v1",
                kind="Job",
                metadata=client.V1ObjectMeta(
                    name=job_name,
                    labels={
                        "kueue.x-k8s.io/queue-name": self.KUEUE_QUEUE_NAME, # Select Queue
                        "kueue.x-k8s.io/resource-flavor": flavor_name  # Select Flavor explicitly
                    } 
                ),
                spec=job_spec
            )
            print(f"Creating Job: {job_name} in queue {self.KUEUE_QUEUE_NAME} with flavor {flavor_name}")
            self.batch_v1.create_namespaced_job(namespace=self.NAMESPACE, body=job_body)
            # 3. Wait Logic
            print(f"Waiting for Job {job_name} based on condition: {blocking_type}")
            pod_name = None
            while True:
                try:
                    job = self.batch_v1.read_namespaced_job(name=job_name, namespace=self.NAMESPACE)
                    if job.spec.suspend:
                        print(f"Job {job_name} is queued (suspended). Waiting for admission...")
                        time.sleep(5) 
                        continue
                    # Check for Job Failure/Success
                    if job.status.succeeded == 1:
                        print(f"Job {job_name} Succeeded.")
                        # Get pod for logs
                        pod_name = self.get_job_pod_name(job_name, self.NAMESPACE)
                        if pod_name and run_params.get("log", False):
                            try:
                                pod_log = self.v1.read_namespaced_pod_log(...)
                            except Exception:
                                pod_log = "Pod logs unavailable (Pod deleted or GC'd)."
                        break
                    if job.status.failed is not None and job.status.failed > 0:
                        print(f"Job {job_name} Failed.")
                        pod_name = self.get_job_pod_name(job_name, self.NAMESPACE)
                        if pod_name and run_params.get("log", False):
                             pod_log = self.v1.read_namespaced_pod_log(name=pod_name, namespace=self.NAMESPACE, container="main")
                        break
                    # For endpoint/delay, we need to inspect the underlying Pod
                    if blocking_type in ["endpoint", "delay"]:
                        if not pod_name:
                            pod_name = self.get_job_pod_name(job_name, self.NAMESPACE)
                        if pod_name:
                            pod = self.v1.read_namespaced_pod(name=pod_name, namespace=self.NAMESPACE)
                            phase = pod.status.phase
                            if blocking_type == "endpoint":
                                if pod.status.conditions:
                                    ready_condition = next(
                                        (c for c in pod.status.conditions if c.type == "Ready"), None
                                    )
                                    if ready_condition and ready_condition.status == "True":
                                        print(f"Job Pod {pod_name} is Ready at endpoint.")
                                        break
                            elif blocking_type == "delay":
                                if phase == "Running":
                                    delay_seconds = blocking_params.get("delay", 10)
                                    print(f"Job Pod {pod_name} is Running. Waiting {delay_seconds}s...")
                                    time.sleep(delay_seconds)
                                    break
                        else:
                            # Pod doesn't exist yet (but Job is unsuspended), wait a bit
                            print("Waiting for pod creation...")
                            time.sleep(2)
                except client.exceptions.ApiException as e:
                    if e.status == 404:
                        print(f"Job {job_name} not found.")
                        break
                    raise
                time.sleep(2)
        finally:
            # 4. Cleanup Logic
            if blocking_type == "completion":
                print("Starting resource cleanup...")
                try:
                    print(f"Deleting Job: {job_name}")
                    self.batch_v1.delete_namespaced_job(
                        name=job_name, 
                        namespace=self.NAMESPACE, 
                        body=client.V1DeleteOptions(propagation_policy="Foreground")
                    )
                except Exception as e:
                    print(f"Warning: Failed to delete job {job_name}. Error: {e}")
        if blocking_type in ["endpoint", "delay"]:
            resources = {"job_name": job_name, "pod_name": pod_name, "flavor_name": flavor_name, "pvc": []}
            for pvc_info in created_pvcs:
                resources["pvc"].append(pvc_info["name"])
            return resources
        else:
            dest_base_dir_list = []
            try:
                print("Job completed, copying data...")
                for pvc_info in created_pvcs:
                    pvc_name = pvc_info["name"]
                    dest_base_dir = os.path.join(tmpdir, f"{job_name}_{pvc_name}")
                    dest_base_dir_list.append(dest_base_dir)
                    
                    # Call shared extraction helper
                    self._extract_data_from_pvc_(pvc_name, dest_base_dir, flavor_name)

            finally:
                for pvc_info in created_pvcs:
                    try:
                        self.v1.delete_namespaced_persistent_volume_claim(name=pvc_info["name"], namespace=self.NAMESPACE)
                    except Exception:
                        pass
            self._copy_results_(params, dest_base_dir_list, pod_log)
            shutil.rmtree(params["tmpdir"], ignore_errors=True)
            return None


    def cleanup(self, params, resources):
        run_params = params["run_params"]
        node_name = params["node_name"]
        tmpdir = params["tmpdir"]
        tag = params["tag"]
        job_name = resources.get("job_name")
        pod_name = resources.get("pod_name")
        flavor_name = resources.get("flavor_name")
        pod_log = ""
        if job_name:
            if not pod_name:
                pod_name = self.get_job_pod_name(job_name, self.NAMESPACE)
            if pod_name and run_params.get("log", False):
                try:
                    pod_log = self.v1.read_namespaced_pod_log(name=pod_name, namespace=self.NAMESPACE, container="main")
                except Exception:
                    pass
            print(f"Deleting Job: {job_name}")
            try:
                self.batch_v1.delete_namespaced_job(
                    name=job_name, 
                    namespace=self.NAMESPACE, 
                    body=client.V1DeleteOptions(propagation_policy="Foreground")
                )
            except client.exceptions.ApiException as e:
                if e.status != 404:
                    print(f"Warning: Failed to delete job {job_name}: {e}")
        
        print("Copying data for pvcs...")
        dest_base_dir_list = []
        prefix = job_name if job_name else f"{params['tag']}-{params['node_name']}"
        for pvc_name in resources.get("pvc", []):
            try:
                dest_base_dir = os.path.join(tmpdir, f"{prefix}_{pvc_name}")
                dest_base_dir_list.append(dest_base_dir)
                # Call shared extraction helper
                self._extract_data_from_pvc_(pvc_name, dest_base_dir, flavor_name)
            except Exception as e:
                print(f"Error extracting PVC {pvc_name}: {e}")
            finally:
                try:
                    self.v1.delete_namespaced_persistent_volume_claim(name=pvc_name, namespace=self.NAMESPACE)
                except Exception:
                    pass

        self._copy_results_(params, dest_base_dir_list, pod_log)
        shutil.rmtree(params["tmpdir"], ignore_errors=True)
        return
