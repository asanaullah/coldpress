import os
import tempfile
from unittest import mock
from unittest.mock import MagicMock, Mock, patch, mock_open

import pytest
from kubernetes import config

# Mock the config loading before importing the module to prevent
# load_incluster_config() from failing in CI environments
with (
    patch("kubernetes.config.load_incluster_config"),
    patch("kubernetes.config.load_kube_config"),
):
    # Import the module to test
    import openshift_runtime


class TestGetCurrentNamespaceFromServiceAccount:
    """Tests for get_current_namespace_from_serviceAccount function"""

    def test_reads_namespace_from_file_when_exists(self):
        """Test reading namespace when service account file exists"""
        with patch("openshift_runtime.Path") as mock_path_class:
            mock_path = Mock()
            mock_path.is_file.return_value = True

            # Use mock_open to properly mock the file reading
            m_open = mock_open(read_data="my-namespace\n")
            mock_path.open = m_open
            mock_path_class.return_value = mock_path

            result = openshift_runtime.get_current_namespace_from_serviceAccount()

            assert result == "my-namespace"
            mock_path.is_file.assert_called_once()
            m_open.assert_called_once_with("r")

    def test_returns_default_when_file_not_exists(self):
        """Test returns 'default' when service account file doesn't exist"""
        with patch("openshift_runtime.Path") as mock_path_class:
            mock_path = Mock()
            mock_path.is_file.return_value = False
            mock_path_class.return_value = mock_path

            result = openshift_runtime.get_current_namespace_from_serviceAccount()

            assert result == "default"
            mock_path.is_file.assert_called_once()


class TestGetCurrentNamespaceFromKubeconfig:
    """Tests for get_current_namespace_from_kubeconfig function"""

    @patch("openshift_runtime.config.list_kube_config_contexts")
    def test_returns_namespace_from_active_context(self, mock_list_contexts):
        """Test reading namespace from active context"""
        mock_list_contexts.return_value = (
            [],
            {"context": {"namespace": "custom-namespace"}},
        )

        result = openshift_runtime.get_current_namespace_from_kubeconfig()

        assert result == "custom-namespace"

    @patch("openshift_runtime.config.list_kube_config_contexts")
    def test_returns_default_when_namespace_not_set(self, mock_list_contexts):
        """Test returns 'default' when namespace not set in context"""
        mock_list_contexts.return_value = ([], {"context": {}})

        result = openshift_runtime.get_current_namespace_from_kubeconfig()

        assert result == "default"

    @patch("openshift_runtime.config.list_kube_config_contexts")
    def test_returns_default_when_namespace_is_none(self, mock_list_contexts):
        """Test returns 'default' when namespace is None"""
        mock_list_contexts.return_value = ([], {"context": {"namespace": None}})

        result = openshift_runtime.get_current_namespace_from_kubeconfig()

        assert result == "default"

    @patch("openshift_runtime.config.list_kube_config_contexts")
    def test_returns_default_on_config_exception(self, mock_list_contexts):
        """Test returns 'default' when ConfigException is raised"""
        mock_list_contexts.side_effect = config.ConfigException()

        result = openshift_runtime.get_current_namespace_from_kubeconfig()

        assert result == "default"


class TestCopyFromPod:
    """Tests for copy_from_pod function"""

    @patch("openshift_runtime.subprocess.Popen")
    @patch("openshift_runtime.stream")
    @patch("openshift_runtime.v1")
    def test_successful_copy(self, mock_v1, mock_stream, mock_popen):
        """Test successful file copy from pod"""
        # Setup mock stream
        mock_resp = MagicMock()
        mock_resp.is_open.side_effect = [True, True, False]
        mock_resp.peek_stdout.side_effect = [True, False, False]
        mock_resp.read_stdout.return_value = b"test data"
        mock_resp.peek_stderr.return_value = False
        mock_stream.return_value = mock_resp

        # Setup mock tar process
        mock_tar = MagicMock()
        mock_tar.returncode = 0
        mock_tar.stdin = MagicMock()
        mock_popen.return_value = mock_tar

        openshift_runtime.copy_from_pod(
            "test-pod", "test-namespace", "/source", "/dest"
        )

        # Verify stream was called correctly
        mock_stream.assert_called_once()
        call_args = mock_stream.call_args
        # The first positional argument is the API method
        assert call_args[0][1] == "test-pod"  # pod_name
        assert call_args[0][2] == "test-namespace"  # namespace

        # Verify tar process was created
        mock_popen.assert_called_once_with(
            ["tar", "xf", "-", "-C", "/dest"],
            stdin=mock.ANY,
            stdout=mock.ANY,
            stderr=mock.ANY,
        )

    @patch("openshift_runtime.subprocess.Popen")
    @patch("openshift_runtime.stream")
    @patch("openshift_runtime.v1")
    def test_tar_extraction_failure(self, mock_v1, mock_stream, mock_popen):
        """Test handling of tar extraction failure"""
        # Setup mock stream
        mock_resp = MagicMock()
        mock_resp.is_open.side_effect = [False]
        mock_resp.peek_stdout.return_value = False
        mock_resp.peek_stderr.return_value = False
        mock_stream.return_value = mock_resp

        # Setup mock tar process with failure
        mock_tar = MagicMock()
        mock_tar.returncode = 1
        mock_tar.stdin = MagicMock()
        mock_tar.stderr.read.return_value = b"tar error"
        mock_popen.return_value = mock_tar

        with pytest.raises(Exception, match="tar extraction failed: tar error"):
            openshift_runtime.copy_from_pod(
                "test-pod", "test-namespace", "/source", "/dest"
            )


class TestCopyResults:
    """Tests for _copy_results_ function"""

    def test_copy_files_and_folders(self):
        """Test copying files and folders to target directory"""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Setup test environment
            target_dir = os.path.join(tmpdir, "target")
            pvc_dir = os.path.join(tmpdir, "pvc")
            os.makedirs(pvc_dir)

            # Create test files and folders
            test_file = os.path.join(pvc_dir, "test.txt")
            test_folder = os.path.join(pvc_dir, "testfolder")
            os.makedirs(test_folder)

            with open(test_file, "w") as f:
                f.write("test content")
            with open(os.path.join(test_folder, "nested.txt"), "w") as f:
                f.write("nested content")

            params = {
                "run_params": {
                    "label": "test-label",
                    "files_to_copy": ["test.txt"],
                    "folders_to_copy": ["testfolder"],
                },
                "node_name": "node1",
                "tag": "tag1",
                "target_dir": target_dir,
            }

            openshift_runtime._copy_results_(params, [pvc_dir], "test log content")

            # Verify files were copied
            assert os.path.exists(os.path.join(target_dir, "test.txt"))
            assert os.path.exists(os.path.join(target_dir, "testfolder", "nested.txt"))

            # Verify log was written
            log_file = os.path.join(target_dir, "tag1-node1-test-label.log")
            assert os.path.exists(log_file)
            with open(log_file, "r") as f:
                assert f.read() == "test log content"

    def test_copy_with_folder_replacement(self):
        """Test that existing folders are replaced when copying"""
        with tempfile.TemporaryDirectory() as tmpdir:
            target_dir = os.path.join(tmpdir, "target")
            pvc_dir = os.path.join(tmpdir, "pvc")
            os.makedirs(target_dir)
            os.makedirs(pvc_dir)

            # Create existing folder in target
            existing_folder = os.path.join(target_dir, "testfolder")
            os.makedirs(existing_folder)
            with open(os.path.join(existing_folder, "old.txt"), "w") as f:
                f.write("old content")

            # Create new folder in source
            source_folder = os.path.join(pvc_dir, "testfolder")
            os.makedirs(source_folder)
            with open(os.path.join(source_folder, "new.txt"), "w") as f:
                f.write("new content")

            params = {
                "run_params": {
                    "label": "test",
                    "files_to_copy": [],
                    "folders_to_copy": ["testfolder"],
                },
                "node_name": "node1",
                "tag": "tag1",
                "target_dir": target_dir,
            }

            openshift_runtime._copy_results_(params, [pvc_dir], "")

            # Verify old folder was replaced
            assert not os.path.exists(os.path.join(target_dir, "testfolder", "old.txt"))
            assert os.path.exists(os.path.join(target_dir, "testfolder", "new.txt"))


class TestOpenshiftRun:
    """Tests for openshift_run function"""

    @patch("openshift_runtime.shutil.rmtree")
    @patch("openshift_runtime._copy_results_")
    @patch("openshift_runtime.copy_from_pod")
    @patch("openshift_runtime.os.makedirs")
    @patch("openshift_runtime.os.path.exists")
    @patch("openshift_runtime.v1")
    def test_run_with_completion_blocking(
        self,
        mock_v1,
        mock_exists,
        mock_makedirs,
        mock_copy_from_pod,
        mock_copy_results,
        mock_rmtree,
    ):
        """Test pod run with completion blocking type"""
        mock_exists.return_value = False

        # Setup pod status mock
        mock_pod = MagicMock()
        mock_pod.status.phase = "Succeeded"
        mock_v1.read_namespaced_pod.return_value = mock_pod
        mock_v1.read_namespaced_pod_log.return_value = "pod log output"

        params = {
            "run_params": {
                "label": "test-pod",
                "image": "test-image:latest",
                "command": ["python", "test.py"],
                "args": ["--arg1", "value1"],
                "env": [{"name": "TEST_ENV", "value": "test"}],
                "ephemeral_mounts": [],
                "sys_mounts": [],
                "log": True,
                "files_to_copy": [],
                "folders_to_copy": [],
            },
            "node_name": "test-node",
            "tmpdir": "/tmp/test",
            "tag": "test-tag",
            "target_dir": "/tmp/target",
        }

        result = openshift_runtime.openshift_run(params)

        # Verify pod was created
        mock_v1.create_namespaced_pod.assert_called_once()
        create_call_args = mock_v1.create_namespaced_pod.call_args
        pod_spec = create_call_args[1]["body"]
        assert pod_spec["spec"]["containers"][0]["image"] == "test-image:latest"
        assert pod_spec["spec"]["nodeName"] == "test-node"

        # Verify pod was deleted after completion
        mock_v1.delete_namespaced_pod.assert_called()

        # Verify result is None for completion blocking
        assert result is None

    @patch("openshift_runtime.os.makedirs")
    @patch("openshift_runtime.v1")
    def test_run_with_endpoint_blocking(self, mock_v1, mock_makedirs):
        """Test pod run with endpoint blocking type"""
        # Setup pod status mock - pod becomes ready
        mock_pod = MagicMock()
        mock_pod.status.phase = "Running"
        ready_condition = MagicMock()
        ready_condition.type = "Ready"
        ready_condition.status = "True"
        mock_pod.status.conditions = [ready_condition]
        mock_v1.read_namespaced_pod.return_value = mock_pod

        params = {
            "run_params": {
                "label": "test-pod",
                "image": "test-image:latest",
                "command": None,
                "args": [],
                "env": [],
                "ephemeral_mounts": [],
                "sys_mounts": [],
                "blocking": {
                    "type": "endpoint",
                    "address": "http://127.0.0.1:8000/health",
                },
            },
            "node_name": "test-node",
            "tmpdir": "/tmp/test",
            "tag": "test-tag",
        }

        result = openshift_runtime.openshift_run(params)

        # Verify pod was created with readiness probe
        create_call_args = mock_v1.create_namespaced_pod.call_args
        pod_spec = create_call_args[1]["body"]
        assert "readinessProbe" in pod_spec["spec"]["containers"][0]
        readiness_probe = pod_spec["spec"]["containers"][0]["readinessProbe"]
        assert readiness_probe["httpGet"]["port"] == 8000
        assert readiness_probe["httpGet"]["path"] == "/health"

        # Verify resources dict is returned
        assert isinstance(result, dict)
        assert "pod_name" in result
        assert "pvc" in result

    @patch("openshift_runtime.time.sleep")
    @patch("openshift_runtime.os.makedirs")
    @patch("openshift_runtime.v1")
    def test_run_with_delay_blocking(self, mock_v1, mock_makedirs, mock_sleep):
        """Test pod run with delay blocking type"""
        # Setup pod status mock
        mock_pod = MagicMock()
        mock_pod.status.phase = "Running"
        mock_v1.read_namespaced_pod.return_value = mock_pod

        params = {
            "run_params": {
                "label": "test-pod",
                "image": "test-image:latest",
                "command": None,
                "args": [],
                "env": [],
                "ephemeral_mounts": [],
                "sys_mounts": [],
                "blocking": {"type": "delay", "delay": 5},
            },
            "node_name": "test-node",
            "tmpdir": "/tmp/test",
            "tag": "test-tag",
        }

        result = openshift_runtime.openshift_run(params)

        # Verify delay was called
        mock_sleep.assert_any_call(5)

        # Verify resources dict is returned
        assert isinstance(result, dict)
        assert "pod_name" in result

    @patch("openshift_runtime.os.makedirs")
    @patch("openshift_runtime.v1")
    def test_run_with_ephemeral_mounts(self, mock_v1, mock_makedirs):
        """Test pod run with ephemeral PVC mounts"""
        mock_pod = MagicMock()
        mock_pod.status.phase = "Succeeded"
        mock_v1.read_namespaced_pod.return_value = mock_pod

        params = {
            "run_params": {
                "label": "test-pod",
                "image": "test-image:latest",
                "command": None,
                "args": [],
                "env": [],
                "ephemeral_mounts": [
                    {"target": "/data", "size": "5Gi"},
                    {"target": "/output", "size": "2Gi"},
                ],
                "sys_mounts": [],
                "log": False,
                "files_to_copy": [],
                "folders_to_copy": [],
            },
            "node_name": "test-node",
            "tmpdir": "/tmp/test",
            "tag": "test-tag",
            "target_dir": "/tmp/target",
        }

        with (
            patch("openshift_runtime._copy_results_"),
            patch("openshift_runtime.copy_from_pod"),
            patch("openshift_runtime.shutil.rmtree"),
            patch("openshift_runtime.os.path.exists", return_value=False),
        ):
            openshift_runtime.openshift_run(params)

        # Verify PVCs were created
        assert mock_v1.create_namespaced_persistent_volume_claim.call_count == 2
        pvc_calls = mock_v1.create_namespaced_persistent_volume_claim.call_args_list

        # Check first PVC
        first_pvc = pvc_calls[0][1]["body"]
        assert first_pvc.spec.resources.requests["storage"] == "5Gi"

    @patch("openshift_runtime.os.makedirs")
    @patch("openshift_runtime.v1")
    def test_run_with_sys_mounts(self, mock_v1, mock_makedirs):
        """Test pod run with system hostPath mounts"""
        mock_pod = MagicMock()
        mock_pod.status.phase = "Succeeded"
        mock_v1.read_namespaced_pod.return_value = mock_pod

        params = {
            "run_params": {
                "label": "test-pod",
                "image": "test-image:latest",
                "command": None,
                "args": [],
                "env": [],
                "ephemeral_mounts": [],
                "sys_mounts": [
                    {"source": "/host/data", "target": "/data", "read_only": True},
                    {"source": "/host/config", "target": "/config"},
                ],
                "log": False,
                "files_to_copy": [],
                "folders_to_copy": [],
            },
            "node_name": "test-node",
            "tmpdir": "/tmp/test",
            "tag": "test-tag",
            "target_dir": "/tmp/target",
        }

        with (
            patch("openshift_runtime._copy_results_"),
            patch("openshift_runtime.shutil.rmtree"),
        ):
            openshift_runtime.openshift_run(params)

        # Verify pod was created with hostPath volumes
        create_call_args = mock_v1.create_namespaced_pod.call_args
        pod_spec = create_call_args[1]["body"]
        volumes = pod_spec["spec"]["volumes"]

        # Should have 2 hostPath volumes
        hostpath_volumes = [v for v in volumes if "hostPath" in v]
        assert len(hostpath_volumes) == 2

    @patch("openshift_runtime.os.makedirs")
    @patch("openshift_runtime.v1")
    def test_run_with_host_network(self, mock_v1, mock_makedirs):
        """Test pod run with host network mode"""
        mock_pod = MagicMock()
        mock_pod.status.phase = "Succeeded"
        mock_v1.read_namespaced_pod.return_value = mock_pod

        params = {
            "run_params": {
                "label": "test-pod",
                "image": "test-image:latest",
                "command": None,
                "args": [],
                "env": [],
                "ephemeral_mounts": [],
                "sys_mounts": [],
                "network_mode": "host",
                "log": False,
                "files_to_copy": [],
                "folders_to_copy": [],
            },
            "node_name": "test-node",
            "tmpdir": "/tmp/test",
            "tag": "test-tag",
            "target_dir": "/tmp/target",
        }

        with (
            patch("openshift_runtime._copy_results_"),
            patch("openshift_runtime.shutil.rmtree"),
        ):
            openshift_runtime.openshift_run(params)

        # Verify pod was created with hostNetwork
        create_call_args = mock_v1.create_namespaced_pod.call_args
        pod_spec = create_call_args[1]["body"]
        assert pod_spec["spec"]["hostNetwork"] is True


class TestOpenshiftCleanup:
    """Tests for openshift_cleanup function"""

    @patch("openshift_runtime.shutil.rmtree")
    @patch("openshift_runtime._copy_results_")
    @patch("openshift_runtime.copy_from_pod")
    @patch("openshift_runtime.os.makedirs")
    @patch("openshift_runtime.os.path.exists")
    @patch("openshift_runtime.v1")
    def test_cleanup_with_pod_and_pvcs(
        self,
        mock_v1,
        mock_exists,
        mock_makedirs,
        mock_copy_from_pod,
        mock_copy_results,
        mock_rmtree,
    ):
        """Test cleanup with pod and PVCs"""
        mock_exists.return_value = False
        mock_v1.read_namespaced_pod_log.return_value = "cleanup log"

        # Setup helper pod to become running
        mock_pod = MagicMock()
        mock_pod.status.phase = "Running"
        mock_v1.read_namespaced_pod.return_value = mock_pod

        params = {
            "run_params": {
                "label": "test-pod",
                "log": True,
                "files_to_copy": [],
                "folders_to_copy": [],
            },
            "node_name": "test-node",
            "tmpdir": "/tmp/test",
            "tag": "test-tag",
            "target_dir": "/tmp/target",
        }

        resources = {
            "pod_name": "test-tag-test-node-test-pod",
            "pvc": ["pvc-1", "pvc-2"],
        }

        openshift_runtime.openshift_cleanup(params, resources)

        # Verify pod was deleted
        mock_v1.delete_namespaced_pod.assert_called()

        # Verify log was fetched
        mock_v1.read_namespaced_pod_log.assert_called_once()

        # Verify PVCs were deleted
        delete_pvc_calls = (
            mock_v1.delete_namespaced_persistent_volume_claim.call_args_list
        )
        assert len(delete_pvc_calls) == 2

    @patch("openshift_runtime.shutil.rmtree")
    @patch("openshift_runtime._copy_results_")
    @patch("openshift_runtime.copy_from_pod")
    @patch("openshift_runtime.os.makedirs")
    @patch("openshift_runtime.os.path.exists")
    @patch("openshift_runtime.v1")
    def test_cleanup_without_pod_name(
        self,
        mock_v1,
        mock_exists,
        mock_makedirs,
        mock_copy_from_pod,
        mock_copy_results,
        mock_rmtree,
    ):
        """Test cleanup when pod_name is not in resources"""
        mock_exists.return_value = False

        # Setup helper pod to become running
        mock_pod = MagicMock()
        mock_pod.status.phase = "Running"
        mock_v1.read_namespaced_pod.return_value = mock_pod

        params = {
            "run_params": {
                "label": "test-pod",
                "log": False,
                "files_to_copy": [],
                "folders_to_copy": [],
            },
            "node_name": "test-node",
            "tmpdir": "/tmp/test",
            "tag": "test-tag",
            "target_dir": "/tmp/target",
        }

        resources = {"pvc": ["pvc-1"]}

        openshift_runtime.openshift_cleanup(params, resources)

        # Verify pod log was not fetched
        mock_v1.read_namespaced_pod_log.assert_not_called()

        # Verify cleanup still proceeded
        mock_copy_results.assert_called_once()

    @patch("openshift_runtime.shutil.rmtree")
    @patch("openshift_runtime._copy_results_")
    @patch("openshift_runtime.copy_from_pod")
    @patch("openshift_runtime.os.makedirs")
    @patch("openshift_runtime.os.path.exists")
    @patch("openshift_runtime.v1")
    def test_cleanup_handles_pvc_deletion_errors(
        self,
        mock_v1,
        mock_exists,
        mock_makedirs,
        mock_copy_from_pod,
        mock_copy_results,
        mock_rmtree,
    ):
        """Test cleanup handles PVC deletion errors gracefully"""
        mock_exists.return_value = False

        # Setup helper pod to become running
        mock_pod = MagicMock()
        mock_pod.status.phase = "Running"
        mock_v1.read_namespaced_pod.return_value = mock_pod

        # Make PVC deletion fail
        mock_v1.delete_namespaced_persistent_volume_claim.side_effect = Exception(
            "Delete failed"
        )

        params = {
            "run_params": {
                "label": "test-pod",
                "log": False,
                "files_to_copy": [],
                "folders_to_copy": [],
            },
            "node_name": "test-node",
            "tmpdir": "/tmp/test",
            "tag": "test-tag",
            "target_dir": "/tmp/target",
        }

        resources = {"pvc": ["pvc-1"]}

        # Should not raise exception
        openshift_runtime.openshift_cleanup(params, resources)

        # Verify cleanup was attempted
        mock_v1.delete_namespaced_persistent_volume_claim.assert_called_once()
