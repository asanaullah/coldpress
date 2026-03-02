# Assisted by: Gemini 3
import pytest
from unittest.mock import MagicMock, patch

with (
    patch("kubernetes.config.load_incluster_config"),
    patch("kubernetes.config.load_kube_config"),
):
    # Import the module to test
    from openshift_runtime import runtime

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
            "status": {
                "conditions": [],
                "replicatedJobsStatus": [{"ready": 0}]
            }
        }
        rt.custom_api.get_namespaced_custom_object.return_value = mock_jobset
        
        status = rt.status("test-job", "default")
        assert status["state"] == "Running"

    @patch("kubernetes.client.CustomObjectsApi")
    def test_status_completed(self, mock_custom_api):
        rt = runtime()
        
        # Mock completed jobset
        mock_jobset = {
            "status": {
                "conditions": [
                    {"type": "Completed", "status": "True"}
                ]
            }
        }
        rt.custom_api.get_namespaced_custom_object.return_value = mock_jobset
        
        status = rt.status("test-job", "default")
        assert status["state"] == "Completed"

    @patch("kubernetes.client.CustomObjectsApi")
    def test_status_suspended(self, mock_custom_api):
        rt = runtime()
        
        # Mock suspended jobset (waiting for Kueue)
        mock_jobset = {
            "spec": {"suspend": True},
            "status": {}
        }
        rt.custom_api.get_namespaced_custom_object.return_value = mock_jobset
        
        status = rt.status("test-job", "default")
        assert status["state"] == "Pending (Suspended)"