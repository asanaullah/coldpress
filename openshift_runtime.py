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
    def __init__(self):
        # Initialize Kubernetes client
        try:
            config.load_incluster_config()
        except Exception:
            config.load_kube_config()
        self.v1 = client.CoreV1Api()
        self.batch_v1 = client.BatchV1Api()
        self.custom_api = client.CustomObjectsApi()
        # Namespace resolution
        try:
            with open("/var/run/secrets/kubernetes.io/serviceaccount/namespace", "r") as f:
                self.current_namespace = f.read().strip()
        except:
            _, active_context = config.list_kube_config_contexts()
            self.current_namespace = active_context["context"].get("namespace", "default")
        self.NAMESPACE = os.getenv("COLDPRESS_NAMESPACE", self.current_namespace)
        self.KUEUE_QUEUE_NAME = os.getenv("COLDPRESS_KUEUE_QUEUE", "local-queue-coldpress")
        self.jobs = {}

    def get_nodes(self):
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

    def wait_for_pvc_bound(self, pvc_name, timeout=120):
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
            except client.exceptions.ApiException as e:
                print(f"Warning: Error reading PVC {pvc_name}: {e}")
            time.sleep(2)
        raise Exception(f"Timeout waiting for PVC {pvc_name} to bind after {timeout} seconds.")

    def create_pvc_if_not_exists(self, name, size):
        try:
            self.v1.read_namespaced_persistent_volume_claim(name, self.NAMESPACE)
        except client.exceptions.ApiException as e:
            if e.status == 404:
                pvc = client.V1PersistentVolumeClaim(
                    metadata=client.V1ObjectMeta(name=name),
                    spec=client.V1PersistentVolumeClaimSpec(
                        access_modes=["ReadWriteOnce"],
                        resources=client.V1ResourceRequirements(requests={"storage": size})
                    )
                )
                self.v1.create_namespaced_persistent_volume_claim(self.NAMESPACE, pvc)
                self.wait_for_pvc_bound(name)

    def extract_data_from_pvc(self, pvc_name, dest_base_dir, node_id):
        if os.path.exists(dest_base_dir):
            shutil.rmtree(dest_base_dir)
        os.makedirs(dest_base_dir, exist_ok=True)
        helper_job_name = f"{pvc_name}-extractor"
        helper_job_name = helper_job_name.lower().replace("_", "-")[:63]
        helper_mount_point = "/data"
        print(f"Creating helper job: {helper_job_name}")
        pod_template = client.V1PodTemplateSpec(
            metadata=client.V1ObjectMeta(labels={"app": helper_job_name}),
            spec=client.V1PodSpec(
                restart_policy="Never",
                node_selector={"coldpress.node": node_id},
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
                    "kueue.x-k8s.io/queue-name": self.KUEUE_QUEUE_NAME
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

    def finalize_results(self, params, source_pvc_dir, target_dir):
        """Replicates original logic for moving specific files/folders from temp PVC path to final local path"""
        run_params = params["run_params"]
        # Copy individual files
        for filename in run_params.get("files_to_copy", []):
            src_file = os.path.join(source_pvc_dir, filename)
            if os.path.exists(src_file):
                shutil.copy(src_file, os.path.join(target_dir, filename))
        # Copy folders
        for foldername in run_params.get("folders_to_copy", []):
            src_folder = os.path.join(source_pvc_dir, foldername)
            dst_folder = os.path.join(target_dir, foldername)
            if os.path.isdir(src_folder):
                if os.path.isdir(dst_folder):
                    shutil.rmtree(dst_folder)
                shutil.copytree(src_folder, dst_folder)


    def status(self, job_id):
        name = f"coldpress-job-{job_id}"
        try:
            # Check JobSet Status
            jobset = self.custom_api.get_namespaced_custom_object(
                group="jobset.x-k8s.io", version="v1alpha2",
                namespace=self.NAMESPACE, plural="jobsets", name=name
            )
            if jobset.get("spec", {}).get("suspend", False) is True:
                return {"state": "Pending (Suspended)"}
            conditions = jobset.get("status", {}).get("conditions", [])
            for c in conditions:
                if c["type"] == "Completed" and c["status"] == "True":
                    return {"state": "Completed"}
                if c["type"] == "Failed" and c["status"] == "True":
                    return {"state": "Failed", "reason": c.get("message", "Unknown")}
            # Check for Endpoint Blocking (Ready State)
            # If JobSet is not complete, check if pods are Ready (for blocking="endpoint")
            replicated_jobs_status = jobset.get("status", {}).get("replicatedJobsStatus", [])
            ready_count = 0
            total_count = 0
            for rjs in replicated_jobs_status:
                ready_count += rjs.get("ready", 0)
                total_count += rjs.get("succeeded", 0) + rjs.get("active", 0) + rjs.get("failed", 0)
            # Here we check if active pods are ready.
            if ready_count > 0:
                return {"state": "Ready"} # Maps to Endpoint ready logic
            return {"state": "Running"}
        except client.exceptions.ApiException as e:
            if e.status == 404:
                return {"state": "Unknown"} # Maybe deleted
            raise e

    def collect_logs(self, job_id):
        print(f"Collecting logs for Job {job_id}...")
        task_list = self.jobs.get(str(job_id), [])
        jobset_name = f"coldpress-job-{job_id}"
        # Find all pods for this jobset to extract logs
        try:
            pods = self.v1.list_namespaced_pod(
                self.NAMESPACE, 
                label_selector=f"jobset.x-k8s.io/jobset-name={jobset_name}"
            ).items
        except:
            pods = []
        for task in task_list:
            task_id = task["task_id"]
            params = task["params"]
            target_dir = params["target_dir"]
            os.makedirs(target_dir, exist_ok=True)
            # 1. Collect Logs
            target_pod = None
            for pod in pods:
                # JobSet naming convention involves the ReplicatedJob name
                if f"task-{task_id}-" in pod.metadata.name:
                    target_pod = pod
                    break
            if target_pod and params["run_params"].get("log", False):
                try:
                    logs = self.v1.read_namespaced_pod_log(target_pod.metadata.name, self.NAMESPACE)
                    with open(f"{target_dir}/{params['tag']}-{params['node_name']}-job.log", "w") as f:
                        f.write(logs)
                except Exception as e:
                    print(f"Warning: Failed to get logs for task {task_id}: {e}")

    def collect_results(self, job_id):
        print(f"Collecting results for Job {job_id}...")
        task_list = self.jobs.get(str(job_id), [])
        jobset_name = f"coldpress-job-{job_id}"
        print("Waiting for task pods to terminate to release PVC locks...")
        while True:
            pods = self.v1.list_namespaced_pod(
                self.NAMESPACE, 
                label_selector=f"jobset.x-k8s.io/jobset-name={jobset_name}"
            ).items
            if not pods:
                break
            time.sleep(2)
        # 2. Extract and Cleanup PVCs
        for task in task_list:
            task_id = task["task_id"]
            params = task["params"]
            target_dir = params["target_dir"]
            os.makedirs(target_dir, exist_ok=True)
            ephemeral_mounts = params["run_params"].get("ephemeral_mounts", [])
            for i, _ in enumerate(ephemeral_mounts):
                pvc_name = f"{jobset_name}-{task_id}-pvc-{i}"
                temp_extract_dir = os.path.join(params["tmpdir"], f"{pvc_name}_extract")
                try:
                    self.extract_data_from_pvc(pvc_name, temp_extract_dir, params['node_id'])
                    self.finalize_results(params, temp_extract_dir, target_dir)
                except Exception as e:
                    print(f"Error handling results for PVC {pvc_name}: {e}")
                finally:
                    # Cleanup Temp Local
                    if os.path.exists(temp_extract_dir):
                        shutil.rmtree(temp_extract_dir)
                    # Cleanup K8s PVC
                    try:
                        self.v1.delete_namespaced_persistent_volume_claim(pvc_name, self.NAMESPACE)
                    except:
                        pass
        if task_list:
             shutil.rmtree(task_list[0]["params"]["tmpdir"], ignore_errors=True)

    def delete(self, job_id):
        name = f"coldpress-job-{job_id}"
        try:
            self.collect_logs(job_id)
            self.custom_api.delete_namespaced_custom_object(
                group="jobset.x-k8s.io", version="v1alpha2",
                namespace=self.NAMESPACE, plural="jobsets", name=name
            )
            self.collect_results(job_id)
        except Exception:
            pass

    def run(self, job_id, task_list):
        jobset_name = f"coldpress-job-{job_id}"
        self.jobs[str(job_id)] = task_list
        replicated_jobs = []
        api_client = client.ApiClient()
        print(f"Preparing JobSet {jobset_name} with {len(task_list)} tasks...")
        # 1. Create ReplicatedJobs specs
        previous_job_name = None
        previous_job_blocking = None
        for task in task_list:
            task_id = task["task_id"]
            params = task["params"]
            run_params = params["run_params"]
            blocking_params = run_params.get("blocking", {"type": "completion"})
            # Map params to Pod Template
            # Pre-create PVCs (JobSet doesn't handle ephemeral PVC creation logic for us natively like StatefulSet might)
            # We maintain the existing logic of creating PVCs for ephemeral mounts manually
            volumes = []
            volume_mounts = []
            # Handle Ephemeral Mounts (Create PVCs immediately)
            for i, mount in enumerate(run_params.get("ephemeral_mounts", [])):
                pvc_name = f"{jobset_name}-{task_id}-pvc-{i}"
                self.create_pvc_if_not_exists(pvc_name, mount.get("size", "1Gi"))
                volumes.append({
                    "name": f"oneshot-pvc-mount-{i}",
                    "persistentVolumeClaim": {"claimName": pvc_name}
                })
                volume_mounts.append({
                    "name": f"oneshot-pvc-mount-{i}",
                    "mountPath": mount["target"]
                })
            # Handle Sys Mounts
            for i, mount in enumerate(run_params.get("sys_mounts", [])):
                volumes.append({
                    "name": f"sys-vol-{i}",
                    "hostPath": {"path": mount["source"], "type": "Directory"}
                })
                volume_mounts.append({
                    "name": f"sys-vol-{i}",
                    "mountPath": mount["target"],
                    "readOnly": mount.get("read_only", False)
                })
            # Identify mounts that need permission fixing (ephemeral PVCs)
            init_containers = []
            chmod_targets = []
            for mount in run_params.get("ephemeral_mounts", []):
                chmod_targets.append(mount["target"])
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
                        volume_mounts=volume_mounts,
                        resources=client.V1ResourceRequirements(
                            requests={"cpu": "10m", "memory": "10Mi"}
                        ),
                        security_context=client.V1SecurityContext(
                            run_as_user=0  # Attempt to run as root to fix perms
                        )
                    )
                )
            if blocking_params.get("type") == "delay":
                delay_seconds = blocking_params.get("delay", 10)
                init_containers.append(
                    client.V1Container(
                        name="blocking-delay",
                        image="alpine:latest",
                        command=["sleep", str(delay_seconds)],
                        resources=client.V1ResourceRequirements(requests={"cpu": "10m", "memory": "10Mi"})
                    )
                )
            # For now, we ignore specific GPU requests since this is not supported in k8s
            # will need to implement a workaround using uuids from a gpu discovery tool
            # Assume max of one gpu requested per job
            if "nvidia.com/gpu" in run_params.get("resources", {}).get("limits", {}):
                run_params["resources"]["limits"]["nvidia.com/gpu"] = 1
            pod_template = client.V1PodTemplateSpec(
                metadata=client.V1ObjectMeta(labels={"app": f"task-{task_id}"}),
                spec=client.V1PodSpec(
                    node_selector={"coldpress.node": params['node_id']},
                    init_containers=init_containers,
                    host_network=True if run_params.get("network_mode") == "host" else False,
                    restart_policy="Never",
                    volumes=volumes,
                    containers=[
                        client.V1Container(
                            name="main",
                            image=run_params["image"],
                            volume_mounts=volume_mounts,
                            env=[client.V1EnvVar(name=k, value=str(v)) for k, v in run_params.get("env", {}).items()] 
                                if isinstance(run_params.get("env"), dict) else run_params.get("env"),
                            args=run_params.get("args"),
                            command=run_params.get("command"),
                            resources=client.V1ResourceRequirements(
                                requests=run_params.get("resources", {}).get("limits", {}),
                                limits=run_params.get("resources", {}).get("limits", {})
                            ) if run_params.get("resources") else None,
                        )
                    ],
                ),
            )
            # BLOCKING TYPE: ENDPOINT
            # Handled via Readiness Probe + Status Polling
            if blocking_params.get("type") == "endpoint":
                address = blocking_params.get("address", "http://127.0.0.1:8000")
                try:
                    parsed_url = urlparse(address)
                    port = parsed_url.port
                    scheme = parsed_url.scheme.upper()
                    path = parsed_url.path or "/"
                    if not port:
                        raise ValueError("No port found")
                    
                    print(f"Task {task_id}: configuring readiness probe for {address}")
                    pod_template.spec.containers[0].readiness_probe = client.V1Probe(
                        http_get=client.V1HTTPGetAction(path=path, port=port, scheme=scheme),
                        initial_delay_seconds=blocking_params.get("initialDelaySeconds", 30),
                        period_seconds=blocking_params.get("periodSeconds", 30),
                        failure_threshold=blocking_params.get("failureThreshold", 10),
                    )
                except Exception as e:
                    print(f"Warning: Failed to configure readiness probe for task {task_id}: {e}")
            # --- Create ReplicatedJob Spec --- 
            pod_template_dict = api_client.sanitize_for_serialization(pod_template)
            replicated_job = {
                "name": f"task-{task_id}-c",
                "replicas": 1,
                "template": {
                    "metadata": {
                        "labels": {
                            
                        }
                    },
                    "spec": {
                        "parallelism": 1,
                        "completions": 1,
                        "backoffLimit": 0,
                        "template": pod_template_dict
                    }
                },
            }
            if previous_job_name:
                replicated_job["dependsOn"] = [{
                    "name": previous_job_name,
                    "status": "Completed" if previous_job_blocking == "completion" else "Ready"
                }]
            replicated_jobs.append(replicated_job)
            previous_job_name = replicated_job.get("name", f"task-{task_id}-c")
            previous_job_blocking = blocking_params.get("type")
        # Create JobSet Object
        driver_job_names = []
        for task in task_list:
            if task["params"]["run_params"].get("blocking", {}).get("type") == "completion":
                driver_job_names.append(f"task-{task['task_id']}-c")
        if not driver_job_names and any(t["params"]["run_params"]["blocking"]["type"] == "endpoint" for t in task_list):
             print(f"Warning: Job {job_id} has no completion-blocking tasks. It may never finish.")
        jobset_spec = {
            "suspend": True,
            "replicatedJobs": replicated_jobs,
        }
        if driver_job_names:
             jobset_spec["successPolicy"] = {
                 "operator": "All", 
                 "targetReplicatedJobs": driver_job_names
             }
        jobset_body = {
            "apiVersion": "jobset.x-k8s.io/v1alpha2",
            "kind": "JobSet",
            "metadata": {
                "name": jobset_name,
                "namespace": self.NAMESPACE,
                "labels": {
                    "kueue.x-k8s.io/queue-name": self.KUEUE_QUEUE_NAME
                }
            },
            "spec": jobset_spec
        }
        try:
            self.custom_api.create_namespaced_custom_object(
                group="jobset.x-k8s.io",
                version="v1alpha2",
                namespace=self.NAMESPACE,
                plural="jobsets",
                body=jobset_body
            )
            print(f"JobSet {jobset_name} created successfully.")
        except client.exceptions.ApiException as e:
            print(f"Error creating JobSet: {e}")
            raise e

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