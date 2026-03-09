# Assisted by: Gemini 3, Claude Sonnet 4.5
import pytest
from unittest.mock import MagicMock, patch

# Import the module directly
from openshift_runtime import runtime


# This fixture automatically runs before every test,
# keeping the config functions mocked while the tests execute.
@pytest.fixture(autouse=True)
def mock_k8s_config():
    with (
        patch("kubernetes.config.load_incluster_config"),
        patch("kubernetes.config.load_kube_config"),
    ):
        yield


class TestOpenshiftRuntime:
    def test_get_queue_name(self):
        rt = runtime()
        assert rt.get_queue_name("researcher-a") == "local-queue-researcher-a"

    @patch("kubernetes.client.CoreV1Api")
    def test_get_nodes(self, mock_core_v1):
        rt = runtime()
        # Mock node setup
        mock_node = MagicMock()
        mock_node.metadata.name = "worker-0"
        mock_node.metadata.labels = {"coldpress.node": "0"}
        mock_node.status.allocatable = {"nvidia.com/gpu": "2"}

        rt.v1.list_node.return_value.items = [mock_node]

        nodes = rt.get_nodes()
        assert "0" in nodes
        assert nodes["0"]["name"] == "worker-0"
        assert len(nodes["0"]["gpus"]) == 2  # Should create 2 gpu slots

    @patch("kubernetes.client.CustomObjectsApi")
    def test_status_running(self, mock_custom_api):
        rt = runtime()

        # Mock running jobset
        mock_jobset = {
            "status": {"conditions": [], "replicatedJobsStatus": [{"ready": 0}]}
        }
        rt.custom_api.get_namespaced_custom_object.return_value = mock_jobset

        status = rt.status("test-job", "default")
        assert status["state"] == "Running"

    @patch("kubernetes.client.CustomObjectsApi")
    def test_status_completed(self, mock_custom_api):
        rt = runtime()

        # Mock completed jobset
        mock_jobset = {
            "status": {"conditions": [{"type": "Completed", "status": "True"}]}
        }
        rt.custom_api.get_namespaced_custom_object.return_value = mock_jobset

        status = rt.status("test-job", "default")
        assert status["state"] == "Completed"

    @patch("kubernetes.client.CustomObjectsApi")
    def test_status_suspended(self, mock_custom_api):
        rt = runtime()

        # Mock suspended jobset (waiting for Kueue)
        mock_jobset = {"spec": {"suspend": True}, "status": {}}
        rt.custom_api.get_namespaced_custom_object.return_value = mock_jobset

        status = rt.status("test-job", "default")
        assert status["state"] == "Pending (Suspended)"


class TestJobSetCreation:
    """Tests for JobSet creation and configuration."""

    @patch("kubernetes.client.CustomObjectsApi")
    def test_jobset_created_suspended(self, mock_custom_api_cls):
        """Verifies JobSet starts with suspend: True for Kueue admission."""
        rt = runtime()
        mock_api = MagicMock()
        rt.custom_api = mock_api

        task_list = [
            {
                "task_id": 0,
                "label": "Task 0",
                "params": {
                    "run_params": {
                        "label": "test-task",
                        "image": "test:latest",
                        "args": [],
                        "env": [],
                        "blocking": {"type": "completion"},
                        "annotations": {},
                        "resources": {"limits": {"nvidia.com/gpu": "1"}},
                        "ephemeral_mounts": [],
                        "sys_mounts": [],
                        "files_to_copy": [],
                    },
                    "node_id": "0",
                    "node_name": "worker-0",
                    "tmpdir": "/tmp/test",
                    "result_path": "results/0",
                    "tag": "test-0",
                },
            }
        ]

        rt.run("test-job", task_list, "default", "test-pvc")

        # Verify JobSet was created with suspend: True
        call_args = mock_api.create_namespaced_custom_object.call_args
        # jobset_body is the 5th positional argument (index 4)
        jobset_body = call_args[0][4]
        assert jobset_body["spec"]["suspend"] is True

    @patch("kubernetes.client.CustomObjectsApi")
    def test_task_dependency_ordering(self, mock_custom_api_cls):
        """Verifies tasks have correct dependsOn fields."""
        rt = runtime()
        mock_api = MagicMock()
        rt.custom_api = mock_api

        # 3-task workflow
        task_list = []
        for i in range(3):
            task_list.append(
                {
                    "task_id": i,
                    "label": f"Task {i}",
                    "params": {
                        "run_params": {
                            "label": f"task-{i}",
                            "image": "test:latest",
                            "args": [],
                            "env": [],
                            "blocking": {"type": "completion"},
                            "annotations": {},
                            "resources": {"limits": {"nvidia.com/gpu": "0"}},
                            "ephemeral_mounts": [],
                            "sys_mounts": [],
                            "files_to_copy": [],
                        },
                        "node_id": "0",
                        "node_name": "worker-0",
                        "tmpdir": f"/tmp/test/{i}",
                        "result_path": f"results/{i}",
                        "tag": f"test-{i}",
                    },
                }
            )

        rt.run("test-job", task_list, "default", "test-pvc")

        # Verify dependency chain
        call_args = mock_api.create_namespaced_custom_object.call_args
        jobset_body = call_args[0][4]
        replicated_jobs = jobset_body["spec"]["replicatedJobs"]

        # Task 0 should have no dependencies
        assert "dependsOn" not in replicated_jobs[0]

        # Task 1 should depend on task-0
        assert replicated_jobs[1]["dependsOn"][0]["name"] == "task-0"
        assert replicated_jobs[1]["dependsOn"][0]["status"] == "Complete"

        # Task 2 should depend on task-1
        assert replicated_jobs[2]["dependsOn"][0]["name"] == "task-1"
        assert replicated_jobs[2]["dependsOn"][0]["status"] == "Complete"

    @patch("kubernetes.client.CustomObjectsApi")
    def test_volume_mounts_for_pvc(self, mock_custom_api_cls):
        """Verifies data PVC is mounted at /mnt/coldpress-data."""
        rt = runtime()
        mock_api = MagicMock()
        rt.custom_api = mock_api

        task_list = [
            {
                "task_id": 0,
                "label": "Task 0",
                "params": {
                    "run_params": {
                        "label": "test-task",
                        "image": "test:latest",
                        "args": [],
                        "env": [],
                        "blocking": {"type": "completion"},
                        "annotations": {},
                        "resources": {"limits": {"nvidia.com/gpu": "0"}},
                        "ephemeral_mounts": [],
                        "sys_mounts": [],
                        "files_to_copy": [],
                    },
                    "node_id": "0",
                    "node_name": "worker-0",
                    "tmpdir": "/tmp/test",
                    "result_path": "results/0",
                    "tag": "test-0",
                },
            }
        ]

        rt.run("test-job", task_list, "default", "my-data-pvc")

        # Verify volume and volumeMount
        call_args = mock_api.create_namespaced_custom_object.call_args
        jobset_body = call_args[0][4]
        pod_spec = jobset_body["spec"]["replicatedJobs"][0]["template"]["spec"][
            "template"
        ]["spec"]

        # Check volume
        volumes = pod_spec["volumes"]
        data_volume = next((v for v in volumes if v["name"] == "coldpress-data"), None)
        assert data_volume is not None
        assert data_volume["persistentVolumeClaim"]["claimName"] == "my-data-pvc"

        # Check volumeMount
        volume_mounts = pod_spec["containers"][0]["volumeMounts"]
        data_mount = next(
            (m for m in volume_mounts if m["name"] == "coldpress-data"), None
        )
        assert data_mount is not None
        assert data_mount["mountPath"] == "/mnt/coldpress-data"

    @patch("kubernetes.client.CustomObjectsApi")
    def test_ephemeral_mount_paths(self, mock_custom_api_cls):
        """Verifies ephemeral mounts use subPath for result isolation."""
        rt = runtime()
        mock_api = MagicMock()
        rt.custom_api = mock_api

        task_list = [
            {
                "task_id": 0,
                "label": "Task 0",
                "params": {
                    "run_params": {
                        "label": "test-task",
                        "image": "test:latest",
                        "args": [],
                        "env": [],
                        "blocking": {"type": "completion"},
                        "annotations": {},
                        "resources": {"limits": {"nvidia.com/gpu": "0"}},
                        "ephemeral_mounts": [{"target": "/tmp/result"}],
                        "sys_mounts": [],
                        "files_to_copy": [],
                    },
                    "node_id": "0",
                    "node_name": "worker-0",
                    "tmpdir": "/tmp/test",
                    "result_path": "results/task-0",
                    "tag": "test-0",
                },
            }
        ]

        rt.run("test-job", task_list, "default", "test-pvc")

        # Verify ephemeral mount uses subPath
        call_args = mock_api.create_namespaced_custom_object.call_args
        jobset_body = call_args[0][4]
        volume_mounts = jobset_body["spec"]["replicatedJobs"][0]["template"]["spec"][
            "template"
        ]["spec"]["containers"][0]["volumeMounts"]

        # Find the ephemeral mount (after the base coldpress-data mount)
        ephemeral_mount = next(
            (m for m in volume_mounts if m.get("mountPath") == "/tmp/result"), None
        )
        assert ephemeral_mount is not None
        assert ephemeral_mount["subPath"] == "results/task-0"

    @patch("kubernetes.client.CustomObjectsApi")
    def test_gpu_resource_limits(self, mock_custom_api_cls):
        """Verifies GPU limits are set from template resources."""
        rt = runtime()
        mock_api = MagicMock()
        rt.custom_api = mock_api

        task_list = [
            {
                "task_id": 0,
                "label": "Task 0",
                "params": {
                    "run_params": {
                        "label": "test-task",
                        "image": "test:latest",
                        "args": [],
                        "env": [],
                        "blocking": {"type": "completion"},
                        "annotations": {},
                        "resources": {"limits": {"nvidia.com/gpu": "2"}},
                        "ephemeral_mounts": [],
                        "sys_mounts": [],
                        "files_to_copy": [],
                    },
                    "node_id": "0",
                    "node_name": "worker-0",
                    "tmpdir": "/tmp/test",
                    "result_path": "results/0",
                    "tag": "test-0",
                },
            }
        ]

        rt.run("test-job", task_list, "default", "test-pvc")

        # Verify GPU resource limits
        call_args = mock_api.create_namespaced_custom_object.call_args
        jobset_body = call_args[0][4]
        resources = jobset_body["spec"]["replicatedJobs"][0]["template"]["spec"][
            "template"
        ]["spec"]["containers"][0]["resources"]

        assert resources["limits"]["nvidia.com/gpu"] == "2"
        assert resources["requests"]["nvidia.com/gpu"] == "2"


class TestBlockingMechanisms:
    """Tests for task blocking mechanisms."""

    @patch("kubernetes.client.CoreV1Api")
    @patch("kubernetes.client.CustomObjectsApi")
    def test_endpoint_blocking_creates_service(
        self, mock_custom_api_cls, mock_core_v1_cls
    ):
        """Verifies endpoint blocking creates ClusterIP Service."""
        rt = runtime()
        mock_custom_api = MagicMock()
        rt.custom_api = mock_custom_api
        mock_v1 = MagicMock()
        rt.v1 = mock_v1

        task_list = [
            {
                "task_id": 0,
                "label": "Task 0",
                "params": {
                    "run_params": {
                        "label": "test-task",
                        "image": "test:latest",
                        "args": [],
                        "env": [],
                        "blocking": {
                            "type": "endpoint",
                            "address": "http://127.0.0.1:8000/health",
                        },
                        "annotations": {},
                        "resources": {"limits": {"nvidia.com/gpu": "0"}},
                        "ephemeral_mounts": [],
                        "sys_mounts": [],
                        "files_to_copy": [],
                    },
                    "node_id": "0",
                    "node_name": "worker-0",
                    "tmpdir": "/tmp/test",
                    "result_path": "results/0",
                    "tag": "test-0",
                },
            }
        ]

        rt.run("test-job", task_list, "default", "test-pvc")

        # Verify service was created
        service_call = mock_v1.create_namespaced_service.call_args
        service_body = service_call[0][1]

        assert service_body.metadata.name == "s-test-job-0"
        assert service_body.spec.type == "ClusterIP"
        assert service_body.spec.ports[0].port == 8000

    @patch("kubernetes.client.CustomObjectsApi")
    def test_endpoint_blocking_adds_readiness_probe(self, mock_custom_api_cls):
        """Verifies endpoint blocking adds HTTP readinessProbe."""
        rt = runtime()
        mock_api = MagicMock()
        rt.custom_api = mock_api

        task_list = [
            {
                "task_id": 0,
                "label": "Task 0",
                "params": {
                    "run_params": {
                        "label": "test-task",
                        "image": "test:latest",
                        "args": [],
                        "env": [],
                        "blocking": {
                            "type": "endpoint",
                            "address": "http://127.0.0.1:8000/health",
                        },
                        "annotations": {},
                        "resources": {"limits": {"nvidia.com/gpu": "0"}},
                        "ephemeral_mounts": [],
                        "sys_mounts": [],
                        "files_to_copy": [],
                    },
                    "node_id": "0",
                    "node_name": "worker-0",
                    "tmpdir": "/tmp/test",
                    "result_path": "results/0",
                    "tag": "test-0",
                },
            }
        ]

        rt.run("test-job", task_list, "default", "test-pvc")

        # Verify readinessProbe was added
        call_args = mock_api.create_namespaced_custom_object.call_args
        jobset_body = call_args[0][4]
        container = jobset_body["spec"]["replicatedJobs"][0]["template"]["spec"][
            "template"
        ]["spec"]["containers"][0]

        assert container["readinessProbe"] is not None
        assert container["readinessProbe"]["httpGet"]["path"] == "/health"
        assert container["readinessProbe"]["httpGet"]["port"] == 8000

    @patch("kubernetes.client.CustomObjectsApi")
    def test_delay_blocking_creates_init_container(self, mock_custom_api_cls):
        """Verifies delay blocking adds init container with sleep."""
        rt = runtime()
        mock_api = MagicMock()
        rt.custom_api = mock_api

        task_list = [
            {
                "task_id": 0,
                "label": "Task 0",
                "params": {
                    "run_params": {
                        "label": "test-task",
                        "image": "test:latest",
                        "args": [],
                        "env": [],
                        "blocking": {"type": "delay", "delay": 30},
                        "annotations": {},
                        "resources": {"limits": {"nvidia.com/gpu": "0"}},
                        "ephemeral_mounts": [],
                        "sys_mounts": [],
                        "files_to_copy": [],
                    },
                    "node_id": "0",
                    "node_name": "worker-0",
                    "tmpdir": "/tmp/test",
                    "result_path": "results/0",
                    "tag": "test-0",
                },
            }
        ]

        rt.run("test-job", task_list, "default", "test-pvc")

        # Verify init container with sleep
        call_args = mock_api.create_namespaced_custom_object.call_args
        jobset_body = call_args[0][4]
        pod_spec = jobset_body["spec"]["replicatedJobs"][0]["template"]["spec"][
            "template"
        ]["spec"]

        assert len(pod_spec["initContainers"]) == 1
        assert pod_spec["initContainers"][0]["name"] == "delay"
        assert pod_spec["initContainers"][0]["command"] == ["sleep", "30"]

    @patch("kubernetes.client.CustomObjectsApi")
    def test_completion_blocking_dependency_status(self, mock_custom_api_cls):
        """Verifies completion blocking sets dependsOn.status: Complete."""
        rt = runtime()
        mock_api = MagicMock()
        rt.custom_api = mock_api

        # Two tasks: first with completion blocking, second depends on it
        task_list = [
            {
                "task_id": 0,
                "label": "Task 0",
                "params": {
                    "run_params": {
                        "label": "task-0",
                        "image": "test:latest",
                        "args": [],
                        "env": [],
                        "blocking": {"type": "completion"},
                        "annotations": {},
                        "resources": {"limits": {"nvidia.com/gpu": "0"}},
                        "ephemeral_mounts": [],
                        "sys_mounts": [],
                        "files_to_copy": [],
                    },
                    "node_id": "0",
                    "node_name": "worker-0",
                    "tmpdir": "/tmp/test/0",
                    "result_path": "results/0",
                    "tag": "test-0",
                },
            },
            {
                "task_id": 1,
                "label": "Task 1",
                "params": {
                    "run_params": {
                        "label": "task-1",
                        "image": "test:latest",
                        "args": [],
                        "env": [],
                        "blocking": {
                            "type": "endpoint",
                            "address": "http://127.0.0.1:8000",
                        },
                        "annotations": {},
                        "resources": {"limits": {"nvidia.com/gpu": "0"}},
                        "ephemeral_mounts": [],
                        "sys_mounts": [],
                        "files_to_copy": [],
                    },
                    "node_id": "0",
                    "node_name": "worker-0",
                    "tmpdir": "/tmp/test/1",
                    "result_path": "results/1",
                    "tag": "test-1",
                },
            },
        ]

        rt.run("test-job", task_list, "default", "test-pvc")

        # Verify task-1 depends on task-0 with status "Complete"
        call_args = mock_api.create_namespaced_custom_object.call_args
        jobset_body = call_args[0][4]
        task1_deps = jobset_body["spec"]["replicatedJobs"][1]["dependsOn"]

        assert task1_deps[0]["name"] == "task-0"
        assert task1_deps[0]["status"] == "Complete"


class TestEdgeCases:
    """Tests for edge cases and error handling."""

    @patch("kubernetes.client.CoreV1Api")
    def test_get_nodes_zero_gpus(self, mock_core_v1_cls):
        """Verifies nodes without GPUs are handled gracefully."""
        rt = runtime()

        # Mock node with 0 GPUs
        mock_node = MagicMock()
        mock_node.metadata.name = "cpu-worker"
        mock_node.metadata.labels = {"coldpress.node": "0"}
        mock_node.status.allocatable = {"nvidia.com/gpu": "0"}

        rt.v1.list_node.return_value.items = [mock_node]

        nodes = rt.get_nodes()
        assert "0" in nodes
        assert nodes["0"]["name"] == "cpu-worker"
        assert len(nodes["0"]["gpus"]) == 0  # Empty GPU map

    @patch("kubernetes.client.CustomObjectsApi")
    def test_status_unknown_on_404(self, mock_custom_api_cls):
        """Verifies status returns 'Unknown' when JobSet missing."""
        rt = runtime()
        mock_api = MagicMock()
        rt.custom_api = mock_api

        # Simulate 404 error
        from kubernetes import client as k8s_client

        mock_api.get_namespaced_custom_object.side_effect = (
            k8s_client.exceptions.ApiException(status=404)
        )

        status = rt.status("missing-job", "default")
        assert status["state"] == "Unknown"

    @patch("kubernetes.client.CoreV1Api")
    @patch("kubernetes.client.CustomObjectsApi")
    def test_delete_handles_missing_resources(
        self, mock_custom_api_cls, mock_core_v1_cls
    ):
        """Verifies delete doesn't crash on missing JobSet/Services."""
        rt = runtime()
        mock_custom_api = MagicMock()
        mock_v1 = MagicMock()
        rt.custom_api = mock_custom_api
        rt.v1 = mock_v1

        # Simulate 404 errors
        from kubernetes import client as k8s_client

        mock_custom_api.delete_namespaced_custom_object.side_effect = (
            k8s_client.exceptions.ApiException(status=404)
        )
        mock_v1.list_namespaced_service.return_value.items = []

        # Should complete without exceptions
        rt.delete("nonexistent-job", "default")
