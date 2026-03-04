# Assisted by: Gemini 3
import time
from kubernetes import client, config
from urllib.parse import urlparse


class runtime:
    def __init__(self):
        try:
            config.load_incluster_config()
        except Exception:
            config.load_kube_config()
        self.v1 = client.CoreV1Api()
        self.batch_v1 = client.BatchV1Api()
        self.custom_api = client.CustomObjectsApi()

    def get_queue_name(self, namespace):
        return f"local-queue-{namespace}"

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

    def wait_for_pvc_bound(self, pvc_name, namespace, timeout=120):
        print(f"Waiting for PVC {pvc_name} to bind...")
        start_time = time.time()
        while time.time() - start_time < timeout:
            try:
                pvc = self.v1.read_namespaced_persistent_volume_claim(
                    name=pvc_name, namespace=namespace
                )
                if pvc.status.phase == "Bound":
                    print(f"PVC {pvc_name} is Bound.")
                    return True
            except client.exceptions.ApiException:
                pass
            time.sleep(2)
        return False

    def create_pvc_if_not_exists(self, name, namespace, size):
        try:
            self.v1.read_namespaced_persistent_volume_claim(name, namespace)
        except client.exceptions.ApiException as e:
            if e.status == 404:
                pvc = client.V1PersistentVolumeClaim(
                    metadata=client.V1ObjectMeta(name=name),
                    spec=client.V1PersistentVolumeClaimSpec(
                        storage_class_name="nfs-csi",
                        access_modes=["ReadWriteMany"],
                        resources=client.V1ResourceRequirements(
                            requests={"storage": size}
                        ),
                    ),
                )
                self.v1.create_namespaced_persistent_volume_claim(namespace, pvc)
                self.wait_for_pvc_bound(name, namespace)

    def get_namespace_fs_group(self, namespace):
        try:
            ns = self.v1.read_namespace(namespace)
            ann = ns.metadata.annotations or {}
            group_range = ann.get("openshift.io/sa.scc.supplemental-groups") or ann.get(
                "openshift.io/sa.scc.uid-range"
            )
            if group_range:
                return int(group_range.split("/")[0])
        except Exception as e:
            print(
                f"Warning: Could not determine fsGroup from namespace annotations: {e}"
            )
        return None

    def status(self, job_id, namespace):
        name = f"cpj-{job_id}"
        try:
            jobset = self.custom_api.get_namespaced_custom_object(
                group="jobset.x-k8s.io",
                version="v1alpha2",
                namespace=namespace,
                plural="jobsets",
                name=name,
            )
            if jobset.get("spec", {}).get("suspend", False) is True:
                return {"state": "Pending (Suspended)"}
            conditions = jobset.get("status", {}).get("conditions", [])
            for c in conditions:
                if c["type"] == "Completed" and c["status"] == "True":
                    return {"state": "Completed"}
                if c["type"] == "Failed" and c["status"] == "True":
                    return {"state": "Failed", "reason": c.get("message", "Unknown")}
            replicated_jobs_status = jobset.get("status", {}).get(
                "replicatedJobsStatus", []
            )
            ready_count = sum(rjs.get("ready", 0) for rjs in replicated_jobs_status)
            if ready_count > 0:
                return {"state": "Ready"}
            return {"state": "Running"}
        except client.exceptions.ApiException as e:
            if e.status == 404:
                return {"state": "Unknown"}
            raise e

    def wait_for_job_completion(self, name, namespace, timeout=120):
        start_time = time.time()
        while time.time() - start_time < timeout:
            try:
                job = self.batch_v1.read_namespaced_job_status(name, namespace)
                if job.status.succeeded and job.status.succeeded > 0:
                    return True
                if job.status.failed and job.status.failed > 0:
                    print(f"Helper Job {name} failed.")
                    return False
            except client.exceptions.ApiException:
                pass
            time.sleep(2)
        print(f"Helper Job {name} timed out.")
        return False

    def collect_logs_to_pvc(self, job_id, namespace):
        """
        To Do
        """
        pass

    def delete(self, job_id, namespace):
        name = f"cpj-{job_id}"
        try:
            self.custom_api.delete_namespaced_custom_object(
                group="jobset.x-k8s.io",
                version="v1alpha2",
                namespace=namespace,
                plural="jobsets",
                name=name,
                body=client.V1DeleteOptions(propagation_policy="Foreground"),
            )
        except Exception:
            pass
        try:
            services = self.v1.list_namespaced_service(
                namespace, label_selector=f"coldpress/gid={job_id}"
            )
            for svc in services.items:
                self.v1.delete_namespaced_service(svc.metadata.name, namespace)
                print(f"Deleted Service {svc.metadata.name}")
        except Exception:
            pass

    def run(
        self,
        job_id,
        task_list,
        namespace,
        data_pvc_name,
        model_pvc_name="coldpress-model-storage",
    ):
        jobset_name = f"cpj-{job_id}"
        queue_name = self.get_queue_name(namespace)
        replicated_jobs = []
        api_client = client.ApiClient()
        previous_job_name = None
        previous_job_blocking = None
        for task in task_list:
            task_id = task["task_id"]
            params = task["params"]
            run_params = params["run_params"]
            blocking_params = run_params.get("blocking", {"type": "completion"})
            volumes = []
            volume_mounts = []
            volumes.append(
                {
                    "name": "coldpress-data",
                    "persistentVolumeClaim": {"claimName": data_pvc_name},
                }
            )
            volume_mounts.append(
                {"name": "coldpress-data", "mountPath": "/mnt/coldpress-data"}
            )
            for i, mount in enumerate(run_params.get("ephemeral_mounts", [])):
                volume_mounts.append(
                    {
                        "name": "coldpress-data",
                        "mountPath": mount["target"],
                        "subPath": params["result_path"],
                    }
                )
            for i, mount in enumerate(run_params.get("sys_mounts", [])):
                volumes.append(
                    {
                        "name": f"sys-{i}",
                        "hostPath": {"path": mount["source"], "type": "Directory"},
                    }
                )
                volume_mounts.append(
                    {
                        "name": f"sys-{i}",
                        "mountPath": mount["target"],
                        "readOnly": mount.get("read_only", False),
                    }
                )
            init_containers = []
            if blocking_params.get("type") == "delay":
                init_containers.append(
                    client.V1Container(
                        name="delay",
                        image="alpine:latest",
                        command=["sleep", str(blocking_params.get("delay", 10))],
                    )
                )
            resources = run_params.get("resources", {})
            if "limits" not in resources:
                resources["limits"] = {}
            if "nvidia.com/gpu" not in resources["limits"]:
                resources["limits"]["nvidia.com/gpu"] = "0"
            pod_annotations = run_params.get("annotations", {})
            container_security_ctx = None
            if run_params.get("privileged"):
                container_security_ctx = client.V1SecurityContext(privileged=True)
            pod_template = client.V1PodTemplateSpec(
                metadata=client.V1ObjectMeta(
                    labels={"app": f"task-{task_id}", "coldpress/gid": str(job_id)},
                    annotations=pod_annotations,
                ),
                spec=client.V1PodSpec(
                    security_context=None,
                    node_selector={"coldpress.node": params["node_id"]},
                    init_containers=init_containers,
                    host_network=True
                    if run_params.get("network_mode") == "host"
                    else False,
                    restart_policy="Never",
                    volumes=volumes,
                    tolerations=[client.V1Toleration(operator="Exists")]
                    if run_params.get("tolerate_all")
                    else None,
                    containers=[
                        client.V1Container(
                            name="main",
                            image=run_params["image"],
                            security_context=container_security_ctx,
                            volume_mounts=volume_mounts,
                            env=[
                                client.V1EnvVar(name=k, value=str(v))
                                for k, v in run_params.get("env", {}).items()
                            ]
                            if isinstance(run_params.get("env"), dict)
                            else run_params.get("env"),
                            args=run_params.get("args"),
                            command=run_params.get("command"),
                            resources=client.V1ResourceRequirements(
                                requests=resources.get("limits", {}),
                                limits=resources.get("limits", {}),
                            )
                            if resources
                            else None,
                        )
                    ],
                ),
            )
            if blocking_params.get("type") == "endpoint":
                try:
                    address = blocking_params.get("address", "")
                    parsed = urlparse(address)
                    if parsed.port:
                        svc_name = f"s-{job_id}-{task_id}"
                        svc_body = client.V1Service(
                            metadata=client.V1ObjectMeta(
                                name=svc_name,
                                namespace=namespace,
                                labels={"coldpress/gid": str(job_id)},
                            ),
                            spec=client.V1ServiceSpec(
                                selector={
                                    "app": f"task-{task_id}",
                                    "coldpress/gid": str(job_id),
                                },
                                ports=[
                                    client.V1ServicePort(
                                        port=parsed.port, target_port=parsed.port
                                    )
                                ],
                                type="ClusterIP",
                            ),
                        )
                        try:
                            self.v1.create_namespaced_service(namespace, svc_body)
                        except client.exceptions.ApiException as e:
                            if e.status != 409:
                                raise
                except Exception as e:
                    print(f"Failed to create service for task {task_id}: {e}")
            if blocking_params.get("type") == "endpoint":
                try:
                    address = blocking_params.get("address", "http://127.0.0.1:8000")
                    parsed = urlparse(address)
                    pod_template.spec.containers[0].readiness_probe = client.V1Probe(
                        http_get=client.V1HTTPGetAction(
                            path=parsed.path or "/",
                            port=parsed.port,
                            scheme=parsed.scheme.upper(),
                        ),
                        initial_delay_seconds=30,
                        period_seconds=30,
                        failure_threshold=10,
                    )
                except Exception:
                    pass
            replicated_job = {
                "name": f"task-{task_id}",
                "replicas": 1,
                "template": {
                    "spec": {
                        "parallelism": 1,
                        "completions": 1,
                        "backoffLimit": 0,
                        "template": api_client.sanitize_for_serialization(pod_template),
                    }
                },
            }
            if previous_job_name:
                replicated_job["dependsOn"] = [
                    {
                        "name": previous_job_name,
                        "status": "Complete"
                        if previous_job_blocking == "completion"
                        else "Ready",
                    }
                ]
            replicated_jobs.append(replicated_job)
            previous_job_name = replicated_job["name"]
            previous_job_blocking = blocking_params.get("type")
        driver_jobs = [
            f"task-{t['task_id']}"
            for t in task_list
            if t["params"]["run_params"].get("blocking", {}).get("type") == "completion"
        ]
        jobset_spec = {
            "suspend": True,
            "replicatedJobs": replicated_jobs,
        }
        if driver_jobs:
            jobset_spec["successPolicy"] = {
                "operator": "All",
                "targetReplicatedJobs": driver_jobs,
            }

        jobset_body = {
            "apiVersion": "jobset.x-k8s.io/v1alpha2",
            "kind": "JobSet",
            "metadata": {
                "name": jobset_name,
                "namespace": namespace,
                "labels": {"kueue.x-k8s.io/queue-name": queue_name},
            },
            "spec": jobset_spec,
        }
        try:
            self.custom_api.create_namespaced_custom_object(
                "jobset.x-k8s.io", "v1alpha2", namespace, "jobsets", jobset_body
            )
        except client.exceptions.ApiException as e:
            print(f"Error creating JobSet: {e}")
            raise e
