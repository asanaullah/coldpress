"""
Demo helper functions for Coldpress vs Traditional Kubernetes comparison.

This module provides clean high-level interfaces for both traditional Kubernetes
and Coldpress workflows, hiding implementation details.
"""

import subprocess
import time
import json
import re
from datetime import datetime
import matplotlib.pyplot as plt
import numpy as np


######################################
# TRADITIONAL KUBERNETES (researcher-b)
######################################

def create_pod(pod_name, image, gpus, tolerations, namespace="researcher-b"):
    """Create a pod with specified configuration."""
    # Build tolerations YAML
    tolerations_yaml = ""
    for tol in tolerations:
        tolerations_yaml += f"  - key: {tol['key']}\n"
        tolerations_yaml += f"    operator: {tol['operator']}\n"
        if tol.get('value'):
            tolerations_yaml += f"    value: {tol['value']}\n"
        tolerations_yaml += f"    effect: {tol['effect']}\n"

    pod_yaml = f"""
apiVersion: v1
kind: Pod
metadata:
  name: {pod_name}
  namespace: {namespace}
spec:
  restartPolicy: Never
  nodeSelector:
    nvidia.com/gpu.present: "true"
  tolerations:
{tolerations_yaml}  containers:
  - name: pytorch
    image: {image}
    env:
    - name: HF_HOME
      value: "/tmp/.cache"
    - name: TRANSFORMERS_CACHE
      value: "/tmp/.cache"
    - name: TRITON_CACHE_DIR
      value: "/tmp/.triton"
    resources:
      requests:
        nvidia.com/gpu: {gpus}
        memory: 32Gi
        cpu: 16
      limits:
        nvidia.com/gpu: {gpus}
        memory: 64Gi
        cpu: 32
    command: ["sleep", "infinity"]
    workingDir: /tmp
"""

    with open(f'/tmp/{pod_name}.yaml', 'w') as f:
        f.write(pod_yaml)

    result = subprocess.run(
        ["oc", "apply", "-f", f"/tmp/{pod_name}.yaml", "-n", namespace],
        capture_output=True,
        text=True
    )

    if result.returncode == 0:
        print(f"[OK] Pod '{pod_name}' created")
        return True
    else:
        print(f"✗ Failed to create pod: {result.stderr}")
        return False


def wait_for_pod_ready(pod_name, namespace="researcher-b", timeout=120):
    """Wait for pod to reach Running state."""
    print(f"[WAIT] Waiting for pod '{pod_name}' to be ready...")

    start_time = time.time()
    while time.time() - start_time < timeout:
        result = subprocess.run(
            ["oc", "get", "pod", pod_name, "-n", namespace, "-o", "jsonpath={.status.phase}"],
            capture_output=True,
            text=True
        )

        if result.stdout.strip() == "Running":
            print(f"[OK] Pod is ready")
            return True

        time.sleep(5)

    print(f"✗ Timeout waiting for pod")
    return False


def run_script(script_path, pod_name, namespace="researcher-b", timeout=600):
    """Copy script to pod and execute it."""
    import os

    # Copy script to pod
    print(f"[INFO] Copying script to pod...")
    result = subprocess.run(
        ["oc", "cp", script_path, f"{namespace}/{pod_name}:/tmp/workload.py"],
        capture_output=True,
        text=True
    )

    if result.returncode != 0:
        print(f"✗ Failed to copy script: {result.stderr}")
        return None

    # Execute script
    print(f"[RUN] Executing script...")
    result = subprocess.run(
        ["oc", "exec", pod_name, "-n", namespace, "--",
         "bash", "-c", "cd /tmp && python workload.py 2>&1 | tee /tmp/output.log"],
        capture_output=True,
        text=True,
        timeout=timeout
    )

    if result.returncode == 0:
        print(f"[OK] Script completed successfully\n")
        print("=" * 80)
        print("MODEL OUTPUT")
        print("=" * 80)
        # Show last 20 lines of output
        output_lines = result.stdout.strip().split('\n')
        print('\n'.join(output_lines[-20:]))
        print("=" * 80)
        return result.stdout
    else:
        print(f"✗ Script failed: {result.stderr}")
        return None


def collect_provenance(pod_name, user_requirements, namespace="researcher-b"):
    """Collect complete provenance data from pod."""
    print("[INFO] Collecting provenance...\n")

    provenance_data = {
        "timestamp": datetime.now().isoformat(),
        "user_requirements": user_requirements,
    }

    # Get pod details
    result = subprocess.run(
        ["oc", "get", "pod", pod_name, "-n", namespace, "-o", "json"],
        capture_output=True,
        text=True
    )

    if result.returncode == 0:
        pod_data = json.loads(result.stdout)

        provenance_data["pod"] = {
            "name": pod_data.get('metadata', {}).get('name'),
            "namespace": pod_data.get('metadata', {}).get('namespace'),
            "created": pod_data.get('metadata', {}).get('creationTimestamp'),
            "spec": pod_data.get('spec', {}),
            "status": {
                "phase": pod_data.get('status', {}).get('phase'),
                "startTime": pod_data.get('status', {}).get('startTime'),
            }
        }

        # Get node details
        node_name = pod_data.get('spec', {}).get('nodeName')
        if node_name:
            node_result = subprocess.run(
                ["oc", "get", "node", node_name, "-o", "json"],
                capture_output=True,
                text=True
            )
            if node_result.returncode == 0:
                node_data = json.loads(node_result.stdout)
                provenance_data["node"] = {
                    "name": node_name,
                    "labels": node_data.get('metadata', {}).get('labels', {}),
                    "allocatable": node_data.get('status', {}).get('allocatable', {}),
                }

        # Get logs
        log_result = subprocess.run(
            ["oc", "logs", pod_name, "-n", namespace],
            capture_output=True,
            text=True
        )
        if log_result.returncode == 0:
            provenance_data["logs"] = log_result.stdout

        # Save to file
        provenance_file = f"/tmp/provenance_{pod_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(provenance_file, 'w') as f:
            json.dump(provenance_data, f, indent=2)

        # Display provenance summary
        print("=" * 80)
        print("PROVENANCE SUMMARY")
        print("=" * 80)
        print(f"\nSaved to: {provenance_file}")
        print(f"[WARNING] /tmp is ephemeral - will be lost on reboot\n")

        # User Configuration
        print("-" * 80)
        print("USER CONFIGURATION")
        print("-" * 80)
        print(f"  model: {user_requirements.get('model', 'N/A')}")
        print(f"  prompt: {user_requirements.get('prompt', 'N/A')}")
        print(f"  max_tokens: {user_requirements.get('max_tokens', 'N/A')}")
        print(f"  gpus_needed: {user_requirements.get('gpus_needed', 'N/A')}")

        # Execution Timeline
        print("\n" + "-" * 80)
        print("EXECUTION TIMELINE")
        print("-" * 80)
        created = pod_data.get('metadata', {}).get('creationTimestamp', 'N/A')
        started = pod_data.get('status', {}).get('startTime', 'N/A')
        phase = pod_data.get('status', {}).get('phase', 'N/A')

        print(f"\n  {'Event':<30} {'Timestamp':<35}")
        print("  " + "-" * 65)
        print(f"  {'Pod Created':<30} {created:<35}")
        if started != 'N/A':
            print(f"  {'Pod Started':<30} {started:<35}")
        print(f"  {'Provenance Collected':<30} {provenance_data['timestamp']:<35}")
        print(f"  {'Phase':<30} {phase:<35}")

        # Calculate duration if possible
        if created != 'N/A' and started != 'N/A':
            try:
                created_dt = datetime.fromisoformat(created.replace('Z', '+00:00'))
                started_dt = datetime.fromisoformat(started.replace('Z', '+00:00'))
                now_dt = datetime.fromisoformat(provenance_data['timestamp'].replace('Z', '+00:00'))
                startup_time = int((started_dt - created_dt).total_seconds())
                runtime = int((now_dt - started_dt).total_seconds())
                print(f"  {'Startup Time':<30} {startup_time} seconds")
                print(f"  {'Runtime':<30} {runtime} seconds")
            except:
                pass

        # Node info
        if provenance_data.get("node"):
            node = provenance_data["node"]
            print("\n" + "-" * 80)
            print("EXECUTION NODE")
            print("-" * 80)
            print(f"  Node: {node['name']}")
            print(f"  GPU: {node['labels'].get('nvidia.com/gpu.product', 'N/A')}")
            print(f"  GPU Count: {node['allocatable'].get('nvidia.com/gpu', 'N/A')}")

        print("\n" + "=" * 80)
        return provenance_data

    print("✗ Failed to collect provenance")
    return None


def cleanup_pod(pod_name, namespace="researcher-b"):
    """Delete pod."""
    result = subprocess.run(
        ["oc", "delete", "pod", pod_name, "-n", namespace],
        capture_output=True,
        text=True
    )

    if result.returncode == 0:
        print(f"[OK] Pod '{pod_name}' deleted")
        return True
    else:
        print(f"✗ Failed to delete pod: {result.stderr}")
        return False


######################################
# COLDPRESS (researcher-c)
######################################

def submit_job(job_name, template, params, namespace="researcher-c", storage="researcher-c-storage"):
    """Submit a Coldpress job with given parameters."""
    allocator_yaml = f"""apiVersion: coldpress.io/v1
kind: ColdpressResourceAllocator
metadata:
  name: {job_name}
  namespace: {namespace}
spec:
  storage:
    results: {storage}
  tasks:
  - name: {job_name}-task
    template: {template}
    params:
"""
    for key, value in params.items():
        allocator_yaml += f'      {key}: "{value}"\n'

    with open(f'/tmp/{job_name}.yaml', 'w') as f:
        f.write(allocator_yaml)

    result = subprocess.run(
        ["oc", "apply", "-f", f"/tmp/{job_name}.yaml"],
        capture_output=True,
        text=True
    )

    if result.returncode == 0:
        print(f"[OK] Job '{job_name}' submitted")
        return True
    else:
        print(f"✗ Failed to submit job: {result.stderr}")
        return False


def wait_for_completion(job_name, namespace="researcher-c", timeout=600):
    """Wait for job to complete (CR is auto-deleted on completion)."""
    print(f"[WAIT] Waiting for job '{job_name}' to complete...")

    start_time = time.time()
    while time.time() - start_time < timeout:
        result = subprocess.run(
            ["oc", "get", "coldpressresourceallocator", job_name, "-n", namespace],
            capture_output=True,
            text=True
        )

        if result.returncode != 0:
            elapsed = int(time.time() - start_time)
            print(f"[OK] Job completed in {elapsed} seconds (CR auto-deleted)")
            return True

        time.sleep(10)

    print(f"✗ Timeout after {timeout} seconds")
    return False


def get_provenance(job_name, namespace="researcher-c"):
    """Display provenance summary from PVC."""

    # Find latest results directory
    result = subprocess.run(
        ["oc", "exec", "researcher-c-data-explorer", "-n", namespace, "--",
         "sh", "-c", f"ls -dt /data/coldpress*/{job_name}-* 2>/dev/null | head -1"],
        capture_output=True,
        text=True
    )

    if result.returncode != 0 or not result.stdout.strip():
        print("✗ No results found")
        return None

    result_dir = result.stdout.strip()

    print("=" * 80)
    print("PROVENANCE SUMMARY")
    print("=" * 80)
    print(f"\nResults: {result_dir}")
    print("[OK] Persisted in PVC forever\n")

    # 1. User Configuration
    print("-" * 80)
    print("USER CONFIGURATION")
    print("-" * 80)

    result = subprocess.run(
        ["oc", "exec", "researcher-c-data-explorer", "-n", namespace, "--",
         "sh", "-c", f"grep -E '(model_name|prompt|max_new_tokens|num_gpus):' {result_dir}/provenance.yaml | grep -v 'f:' | head -4"],
        capture_output=True,
        text=True
    )

    for line in result.stdout.strip().split('\n'):
        cleaned = line.strip().replace("'", "")
        print(f"  {cleaned}")

    # 2. Executed Script (first 15 lines)
    print("\n" + "-" * 80)
    print("EXECUTED SCRIPT (first 15 lines)")
    print("-" * 80)

    result = subprocess.run(
        ["oc", "exec", "researcher-c-data-explorer", "-n", namespace, "--",
         "sh", "-c", f"head -15 {result_dir}/0/workload.py 2>/dev/null"],
        capture_output=True,
        text=True
    )

    if result.stdout:
        for line in result.stdout.strip().split('\n'):
            print(f"  {line}")
        print("  ...")
    else:
        print("  Script not found")

    # 3. Execution Timeline
    print("\n" + "-" * 80)
    print("EXECUTION TIMELINE")
    print("-" * 80)

    result = subprocess.run(
        ["oc", "exec", "researcher-c-data-explorer", "-n", namespace, "--",
         "sh", "-c", f"cat {result_dir}/provenance.yaml | grep -E '(creationTimestamp|start_time|started_at|finished_at|timestamp|exit_code|phase):' | grep -v 'f:' | grep -v 'lastTransitionTime'"],
        capture_output=True,
        text=True
    )

    lines = result.stdout.strip().split('\n')
    timeline = []
    exit_code = None
    phase = None

    for line in lines:
        if 'creationTimestamp' in line and not any('Job' in t for t, _ in timeline):
            match = re.search(r"'([0-9T:Z-]+)'", line)
            if match:
                timeline.append(("Job Created", match.group(1)))
        elif 'start_time' in line:
            match = re.search(r"'([0-9T:+-]+)'", line)
            if match:
                timeline.append(("Pod Started", match.group(1)))
        elif 'started_at' in line:
            match = re.search(r'datetime\.datetime\((\d+), (\d+), (\d+), (\d+), (\d+), (\d+)', line)
            if match:
                y, m, d, h, mi, s = [int(x) for x in match.groups()]
                timeline.append(("Container Started", f"{y}-{m:02d}-{d:02d}T{h:02d}:{mi:02d}:{s:02d}Z"))
        elif 'finished_at' in line:
            match = re.search(r'datetime\.datetime\((\d+), (\d+), (\d+), (\d+), (\d+), (\d+)', line)
            if match:
                y, m, d, h, mi, s = [int(x) for x in match.groups()]
                timeline.append(("Container Finished", f"{y}-{m:02d}-{d:02d}T{h:02d}:{mi:02d}:{s:02d}Z"))
        elif line.strip().startswith('timestamp:'):
            match = re.search(r"'([0-9T:+.-]+)'", line)
            if match:
                timeline.append(("Provenance Saved", match.group(1)))
        elif 'exit_code' in line:
            match = re.search(r"'exit_code': (\d+)", line)
            if match:
                exit_code = match.group(1)
        elif 'phase:' in line:
            match = re.search(r'phase: (\w+)', line)
            if match and not phase:
                phase = match.group(1)

    print(f"\n  {'Event':<30} {'Timestamp':<35}")
    print("  " + "-" * 65)
    for event, ts in timeline:
        print(f"  {event:<30} {ts:<35}")
    if exit_code:
        print(f"  {'Exit Code':<30} {exit_code:<35}")
    if phase:
        print(f"  {'Phase':<30} {phase:<35}")

    container_events = {e: t for e, t in timeline}
    if "Container Started" in container_events and "Container Finished" in container_events:
        try:
            start = datetime.fromisoformat(container_events["Container Started"].replace('Z', '+00:00'))
            end = datetime.fromisoformat(container_events["Container Finished"].replace('Z', '+00:00'))
            duration = int((end - start).total_seconds())
            print(f"  {'Duration':<30} {duration} seconds")
        except:
            pass

    print("\n" + "=" * 80)
    return result_dir


def get_results(job_name, namespace="researcher-c"):
    """Display job results from PVC."""

    # Find latest results directory
    result = subprocess.run(
        ["oc", "exec", "researcher-c-data-explorer", "-n", namespace, "--",
         "sh", "-c", f"ls -dt /data/coldpress*/{job_name}-* 2>/dev/null | head -1"],
        capture_output=True,
        text=True
    )

    if result.returncode != 0 or not result.stdout.strip():
        print("✗ No results found")
        return None

    result_dir = result.stdout.strip()

    print("=" * 80)
    print("MODEL OUTPUT")
    print("=" * 80)

    result = subprocess.run(
        ["oc", "exec", "researcher-c-data-explorer", "-n", namespace, "--",
         "sh", "-c", f"tail -20 {result_dir}/0/output.log 2>/dev/null"],
        capture_output=True,
        text=True
    )

    if result.stdout:
        print(result.stdout)
    else:
        print("No output found")

    print("=" * 80)
    return result_dir


######################################
# SETUP & DISCOVERY FUNCTIONS
######################################

def setup_namespaces():
    """Setup both researcher-b and researcher-c namespaces with all required infrastructure."""
    import subprocess
    import time

    print("Setting up namespaces and infrastructure...\n")

    # researcher-c (Coldpress user)
    result = subprocess.run(
        ["oc", "get", "namespace", "researcher-c"],
        capture_output=True,
        text=True
    )

    if result.returncode != 0:
        researcher_c_yaml = """
apiVersion: v1
kind: Namespace
metadata:
  name: researcher-c
  labels:
    kueue.openshift.io/managed: "true"
  annotations:
    coldpress.io/allowed-allocator-parsers: "deepti-parser"
    coldpress.io/allowed-compute-parsers: ""
"""
        with open('/tmp/researcher-c-ns.yaml', 'w') as f:
            f.write(researcher_c_yaml)
        subprocess.run(["oc", "apply", "-f", "/tmp/researcher-c-ns.yaml", "--as", "system:admin"], capture_output=True)
        print("[OK] Created researcher-c namespace")
    else:
        print("[OK] researcher-c namespace exists")

    # LocalQueue
    localqueue_yaml = """
apiVersion: kueue.x-k8s.io/v1beta1
kind: LocalQueue
metadata:
  namespace: researcher-c
  name: local-queue-researcher-c
spec:
  clusterQueue: cluster-queue-test
"""
    with open('/tmp/researcher-c-localqueue.yaml', 'w') as f:
        f.write(localqueue_yaml)
    subprocess.run(["oc", "apply", "-f", "/tmp/researcher-c-localqueue.yaml", "--as", "system:admin"], capture_output=True)
    print("[OK] LocalQueue configured")

    # Get current user
    current_user = subprocess.run(
        ["oc", "whoami"],
        capture_output=True,
        text=True
    ).stdout.strip()

    # RBAC
    rbac_yaml = f"""
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: coldpress-user-role
  namespace: researcher-c
rules:
  - apiGroups: ["coldpress.io"]
    resources: ["coldpressresourceallocators", "compute-jobs"]
    verbs: ["create", "get", "list", "watch", "update", "patch", "delete"]
  - apiGroups: ["jobset.x-k8s.io"]
    resources: ["jobsets"]
    verbs: ["get", "list", "watch"]
  - apiGroups: ["batch"]
    resources: ["jobs"]
    verbs: ["get", "list", "watch"]
  - apiGroups: [""]
    resources: ["pods", "pods/log"]
    verbs: ["get", "list", "watch"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: coldpress-user-binding
  namespace: researcher-c
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: Role
  name: coldpress-user-role
subjects:
  - kind: User
    name: {current_user}
    apiGroup: rbac.authorization.k8s.io
"""
    with open('/tmp/researcher-c-rbac.yaml', 'w') as f:
        f.write(rbac_yaml)
    subprocess.run(["oc", "apply", "-f", "/tmp/researcher-c-rbac.yaml", "--as", "system:admin"], capture_output=True)
    print("[OK] RBAC configured")

    # PVC
    result = subprocess.run(["oc", "get", "pvc", "researcher-c-storage", "-n", "researcher-c"], capture_output=True)
    if result.returncode != 0:
        pvc_yaml = """
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: researcher-c-storage
  namespace: researcher-c
spec:
  storageClassName: nfs-csi
  accessModes: [ "ReadWriteMany" ]
  resources: { requests: { storage: 500Gi } }
"""
        with open('/tmp/researcher-c-pvc.yaml', 'w') as f:
            f.write(pvc_yaml)
        subprocess.run(["oc", "apply", "-f", "/tmp/researcher-c-pvc.yaml", "--as", "system:admin"], capture_output=True)
        print("[OK] PVC created")
    else:
        print("[OK] PVC exists")

    # Helper pod
    result = subprocess.run(["oc", "get", "pod", "researcher-c-data-explorer", "-n", "researcher-c"], capture_output=True)
    if result.returncode != 0:
        helper_pod_yaml = """
apiVersion: v1
kind: Pod
metadata:
  name: researcher-c-data-explorer
  namespace: researcher-c
spec:
  restartPolicy: Never
  containers:
    - name: explorer
      image: alpine
      command: ["sleep", "infinity"]
      volumeMounts:
        - mountPath: "/data"
          name: myvol
  volumes:
    - name: myvol
      persistentVolumeClaim:
        claimName: researcher-c-storage
"""
        with open('/tmp/helper-pod-c.yaml', 'w') as f:
            f.write(helper_pod_yaml)
        subprocess.run(["oc", "apply", "-f", "/tmp/helper-pod-c.yaml", "--as", "system:admin"], capture_output=True)

        # Wait for pod
        start_time = time.time()
        while time.time() - start_time < 60:
            result = subprocess.run(
                ["oc", "get", "pod", "researcher-c-data-explorer", "-n", "researcher-c", "-o", "jsonpath={.status.phase}"],
                capture_output=True, text=True
            )
            if result.stdout.strip() == "Running":
                break
            time.sleep(2)
        print("[OK] Helper pod created")
    else:
        print("[OK] Helper pod exists")

    # researcher-b (Traditional K8s)
    result = subprocess.run(["oc", "get", "namespace", "researcher-b"], capture_output=True)
    if result.returncode != 0:
        subprocess.run(["oc", "create", "namespace", "researcher-b", "--as", "system:admin"], capture_output=True)
        subprocess.run(["oc", "adm", "policy", "add-role-to-user", "edit", current_user, "-n", "researcher-b", "--as", "system:admin"], capture_output=True)
        print("[OK] Created researcher-b namespace")
    else:
        print("[OK] researcher-b namespace exists")

    print("\n[OK] Setup complete")


def discover_gpu_nodes():
    """Discover GPU nodes and return node configurations."""
    import subprocess
    import json

    result = subprocess.run(
        ["oc", "get", "nodes", "-l", "coldpress.node", "-o", "json"],
        capture_output=True,
        text=True
    )

    if result.returncode != 0:
        print("Failed to get nodes")
        return {}

    nodes_data = json.loads(result.stdout)
    node_configs = {}

    print("=" * 80)
    print("DISCOVERED GPU NODES")
    print("=" * 80)

    for node in nodes_data['items']:
        node_name = node['metadata']['name']
        labels = node['metadata']['labels']
        taints = node['spec'].get('taints', [])
        allocatable = node['status']['allocatable']

        coldpress_id = labels.get('coldpress.node', 'N/A')
        gpu_count = allocatable.get('nvidia.com/gpu', '0')
        gpu_product = labels.get('nvidia.com/gpu.product', 'N/A')
        gpu_family = labels.get('nvidia.com/gpu.family', 'N/A')
        gpu_memory = labels.get('nvidia.com/gpu.memory', 'N/A')

        gpu_taints = [t for t in taints if 'gpu' in t.get('key', '').lower()]

        node_configs[coldpress_id] = {
            'node_name': node_name,
            'gpu_count': int(gpu_count),
            'gpu_product': gpu_product,
            'gpu_family': gpu_family,
            'gpu_memory': gpu_memory,
            'gpu_taints': gpu_taints
        }

        print(f"\n{node_name}:")
        print(f"  GPU: {gpu_product} ({gpu_family})")
        print(f"  Count: {gpu_count} x {gpu_memory} MB")

    print("\n" + "=" * 80)
    return node_configs


def create_template_from_file(template_file):
    """Create WorkloadTemplate from YAML file."""
    import subprocess

    result = subprocess.run(
        ["oc", "apply", "-f", template_file, "--as", "system:admin"],
        capture_output=True,
        text=True
    )

    if result.returncode == 0:
        print(f"[OK] WorkloadTemplate created from {template_file}")
        return True
    else:
        print(f"✗ Failed to create template: {result.stderr}")
        return False


######################################
# UTILITY FUNCTIONS
######################################

def get_gpu_tolerations(node_configs):
    """Collect GPU tolerations from node configurations."""
    all_tolerations = [
        {
            'key': 'nvidia.com/gpu',
            'operator': 'Exists',
            'effect': 'NoSchedule'
        }
    ]

    seen_products = set()
    for node_id, config in node_configs.items():
        gpu_product = config['gpu_product']
        if gpu_product not in seen_products and gpu_product != 'N/A':
            seen_products.add(gpu_product)
            for taint in config['gpu_taints']:
                all_tolerations.append({
                    'key': taint['key'],
                    'operator': taint.get('operator', 'Equal'),
                    'value': taint.get('value', ''),
                    'effect': taint['effect']
                })

    return all_tolerations


######################################
# VISUALIZATION
######################################

def plot_complexity_comparison():
    """Plot orchestration vs operational complexity for different platforms."""
    fig, ax = plt.subplots(figsize=(10, 6))
    x = np.linspace(0, 10, 100)

    # Slurm: starts low, stays flat, gentle increase when beyond native capabilities
    # Stays excellent for pure batch workloads. Complexity increases when you need things
    # beyond batch: running services alongside jobs, specific K8s-style primitives, etc.
    # But doesn't exceed Trad K8s - just shows upward tick.
    slurm = np.piecewise(x, [x < 8.0, x >= 8.0],
        [lambda x: 1.5 + 0.05*x, lambda x: 1.5 + 0.05*8.0 + 0.4*(x-8.0)**1.1])

    # Traditional K8s: starts medium-high, increases steadily
    trad_k8s = 4.5 + 0.35*x + 0.02*x**2

    # Coldpress: flat at 5.5 throughout
    # Higher operational complexity upfront, but stays constant - automation handles everything
    coldpress = np.full_like(x, 5.5)

    ax.plot(x, slurm, 'b-', linewidth=2.5, label='Slurm')
    ax.plot(x, trad_k8s, 'r-', linewidth=2.5, label='Traditional K8s')
    ax.plot(x, coldpress, 'g-', linewidth=2.5, label='Coldpress')

    # Mark crossover point
    crossover_idx = np.argmin(np.abs(coldpress - trad_k8s))
    ax.plot(x[crossover_idx], coldpress[crossover_idx], 'ko', markersize=8, zorder=5)
    ax.axvline(x[crossover_idx], color='gray', linestyle='--', alpha=0.3)

    # Annotations
    ax.annotate('Run workload\nbash script (x=0)', xy=(0.3, 0.8), fontsize=10,
                ha='left', va='bottom', color='black', fontweight='bold')
    ax.annotate('Slurm: excellent for\npure batch workloads', xy=(7.0, slurm[70]), fontsize=9,
                ha='right', va='bottom', color='blue')
    ax.annotate('Complexity increases\nbeyond native capabilities',
                xy=(8.8, slurm[88]), fontsize=9, ha='left', va='bottom', color='blue',
                bbox=dict(boxstyle='round,pad=0.5', facecolor='lightblue', alpha=0.7))
    ax.annotate('Coldpress becomes\nsimpler to use', xy=(x[crossover_idx], coldpress[crossover_idx]),
                xytext=(x[crossover_idx]-1.5, coldpress[crossover_idx]+1), fontsize=10, ha='center',
                arrowprops=dict(arrowstyle='->', color='black', lw=1.5))

    ax.set_xlabel('Orchestration Complexity (# of tasks needed to run AI workload)', fontsize=13, fontweight='bold')
    ax.set_ylabel('Operational Complexity\n(Researcher effort needed to run AI workload)', fontsize=13, fontweight='bold')
    ax.set_title('Research Computing Platforms: Orchestration vs Operational Complexity',
                 fontsize=14, fontweight='bold', pad=20)
    ax.legend(loc='upper left', fontsize=11, framealpha=0.9)
    ax.grid(True, alpha=0.2)
    ax.set_xlim(0, 10)
    ax.set_ylim(0, 11)
    ax.set_xticks([])
    ax.set_yticks([])

    plt.tight_layout()
    plt.show()
