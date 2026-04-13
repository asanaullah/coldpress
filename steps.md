<!-- Assisted by: Claude Sonnet 4.5 -->
# Coldpress Full Test - PyTorch DDP Training Example

## Overview

This document walks through a complete end-to-end test of Coldpress, demonstrating all components from initial setup to running a PyTorch Distributed Data Parallel (DDP) training job with 2 GPUs.

**What this test covers:**
- Development environment setup
- Cluster-wide configuration (admin task, one-time)
- Project and user configuration (admin task, one-time)
- User workflow for running AI/HPC workloads (repeatable)

**The test workload:**
- PyTorch DDP training on MNIST dataset
- 2 GPUs, 50 epochs
- Results saved to persistent storage
- Hardware discovery and benchmarking

---

# Part A: Setup (One-Time)

This section covers the initial development environment setup. This only needs to be done once on your local machine.

## A1: Install Coldpress CLI Tools

**What:** Install the `coldpress` and `coldpress-setup` command-line tools.

**Why:** These tools generate job manifests and configure cluster resources.

### Option 1: Automated setup (recommended)

```bash
cd /path/to/coldpress
./setup-env.sh
source .venv/bin/activate
```

The script automatically installs `uv` (fast Python package installer) if not present.

### Option 2: Manual setup

```bash
cd /path/to/coldpress

# Install uv if not already installed
python3 -m pip install --user uv

# Create virtual environment with uv
uv venv

# Activate the virtual environment
source .venv/bin/activate

# Install Coldpress CLI tools
uv pip install -e .
```

### Verification

```bash
coldpress --version
# Output: coldpress, version 2.0.0

coldpress-setup --version
# Output: coldpress-setup, version 2.0.0
```

### For subsequent sessions

After the initial setup, you only need to activate the virtual environment:

```bash
source .venv/bin/activate
```

**Result:** Your virtual environment is now set up with both CLI tools installed.

---

# Part B: Admin - Cluster Configuration (One-Time)

This section covers cluster-wide configuration tasks that require admin privileges. These are typically done once by a cluster administrator.

**Prerequisites:**
- Admin access to Kubernetes/OpenShift cluster
- Kueue operator installed
- JobSet operator installed
- **`kubectl` or `oc` CLI installed and configured** (required for all Coldpress operations)
- **Optional:** Set `COLDPRESS_OC_FLAGS` environment variable for additional kubectl/oc flags (e.g., `export COLDPRESS_OC_FLAGS="--as system:admin"`)

## B1: Apply Cluster Configuration

**What:** Configure cluster-wide resources (ResourceFlavors, ClusterQueue).

**Why:** Sets up the Kueue queueing system for GPU allocation across the cluster.

**Command:**
```bash
coldpress-setup apply cluster ocp-test-nerc-mghpcc.yaml
```

**This creates:**
- Kueue operator configuration
- JobSet operator configuration
- ResourceFlavors (GPU node pools: node0, node1)
- ClusterQueue (cluster-queue-coldpress)

**Verification:**
```bash
oc get clusterqueues
oc get resourceflavors
```

**You should see:**
```
NAME                      AGE
cluster-queue-coldpress   5s

NAME    AGE
node0   5s
node1   5s
```

---

## B2: Apply Project Configuration

**What:** Create a project namespace with storage and queueing resources.

**Why:** Provides isolated workspace for a research group or project team.

**Command:**
```bash
coldpress-setup apply project researcher-a.yaml
```

**This creates:**
- Namespace: `researcher-a`
- LocalQueue: `local-queue-researcher-a` (connects to ClusterQueue)
- PersistentVolumeClaim: `researcher-a-storage` (500Gi)
- RBAC: ServiceAccount, Role, RoleBinding for job execution

**Verification:**
```bash
oc get namespace researcher-a
oc get pvc -n researcher-a
oc get localqueues -n researcher-a
```

**You should see:**
```
NAME           STATUS   AGE
researcher-a   Active   5s

NAME                    STATUS   VOLUME   CAPACITY   ACCESS MODES   AGE
researcher-a-storage    Bound    pvc-...  500Gi      RWX            5s

NAME                        CLUSTERQUEUE              AGE
local-queue-researcher-a    cluster-queue-coldpress   5s
```

---

## B3: Apply User RBAC

**What:** Grant an existing cluster user permission to submit jobs to the project namespace.

**Why:** Allows regular users to create and manage JobSets without admin privileges.

**Prerequisites:**
- User must already exist in the cluster's authentication system (OpenShift OAuth, LDAP, etc.)
- Project configuration must be applied first (creates the Role that this RoleBinding references)

**Command:**
```bash
coldpress-setup apply user coldpress-user.yaml
```

**User config example** (`users/coldpress-user.yaml`):
```yaml
username: coldpress-user
namespaces:
  - researcher-a
```

**This creates:**
- RoleBinding: `coldpress-user-coldpress-user` in namespace `researcher-a`
- Binds existing user to existing Role: `coldpress-user-role` (created by project setup)
- Grants permissions: create/manage JobSets, view Jobs/Pods/Services

**Important:** This does NOT create a user account. Users must already exist in your cluster's authentication system.

**Verification:**
```bash
oc get rolebindings -n researcher-a | grep coldpress-user
```

**Expected output:**
```
coldpress-user-coldpress-user   5s
```

**Result:** Cluster, project, and user configuration is now complete. Regular users can submit jobs.

---

# Part C: User Workflow - Running a Workload

This section shows the typical workflow for a regular user submitting and managing an AI/HPC workload. These steps are repeatable for each job.

**Prerequisites:**
- Coldpress CLI tools installed (Part A)
- **`kubectl` or `oc` CLI installed and configured** (required)
- Admin has configured cluster, project, and user access (Part B)
- User has access to the target namespace

---

## C1: Generate Job Manifests

**What:** Generate JobSet manifests and helper scripts from a job specification.

**Why:** Creates all Kubernetes resources needed to run your workload.

**Command:**
```bash
source .venv/bin/activate
coldpress generate --config examples/pytorch_ddp_training/config.yaml
```

**Input files:**
- `examples/pytorch_ddp_training/config.yaml` - Coldpress job configuration
- `examples/pytorch_ddp_training/job-spec.yaml` - Workload specification
- `examples/pytorch_ddp_training/train.py` - Training script
- `examples/pytorch_ddp_training/model_config.json` - Model configuration

**This generates** (`output/ddp-training-job/`):
```
ddp-training-job/
├── jobset.yaml      # JobSet manifest with all tasks
├── metadata.json    # Job metadata and node assignments
├── run.sh           # Apply JobSet and wait for completion
├── cleanup.sh       # Delete JobSet and services
├── monitor.sh       # Watch job status
├── logs.sh          # Capture and save logs to PVC
└── explore.sh       # Interactive shell to browse results
```

**You'll see output like:**
```
Generating JobSet for: ddp-training
Project: researcher-a
Namespace: researcher-a
Tasks: 1

  Task 0 (ddp-training) → Node 0 (GPUs: 2)

Generated: ddp-training-job/jobset.yaml
Generated: ddp-training-job/metadata.json
Generated: ddp-training-job/run.sh
Generated: ddp-training-job/cleanup.sh
Generated: ddp-training-job/monitor.sh
Generated: ddp-training-job/logs.sh
Generated: ddp-training-job/explore.sh
```

**Result:** Job manifests and helper scripts are now ready to use.

---

## C2: Inspect Generated Manifests (Optional)

**What:** Review the generated JobSet manifest before applying.

**Why:** Understand what resources will be created and verify configuration.

**File:** `output/ddp-training-job/jobset.yaml`

**JobSet structure:**
```yaml
apiVersion: jobset.x-k8s.io/v1alpha2
kind: JobSet
metadata:
  name: ddp-training
  namespace: researcher-a
  labels:
    kueue.x-k8s.io/queue-name: local-queue-researcher-a
spec:
  replicatedJobs:
  - name: mkdir       # Job 1: Create results directory
  - name: discovery   # Job 2: Hardware snapshot and benchmarking
  - name: task-0      # Job 3: PyTorch DDP training (2 GPUs)
```

**Key configuration for task-0 (training job):**
- Image: `pytorch/pytorch:2.2.0-cuda12.1-cudnn8-runtime`
- Node selector: `coldpress.node: '0'`
- Resources: 2 GPUs, 16Gi memory, 8 CPU cores
- Command: `python -m torch.distributed.run --nproc_per_node=2 train.py`
- Volumes: PVC for results, emptyDir for shared memory
- Dependencies: Waits for discovery job to complete

---

## C3: Run the Job

**What:** Apply the JobSet to the cluster and wait for completion.

**Why:** Submits your workload to Kueue for scheduling and execution.

**Commands:**
```bash
cd output/ddp-training-job
./run.sh
```

**What happens:**
1. JobSet is created in the cluster
2. Kueue queues the job and waits for resources
3. When GPUs are available, Kueue unsuspends the JobSet
4. Jobs execute in order: mkdir → discovery → training
5. Script waits for all jobs to complete

**You'll see:**
```
Applying JobSet...
jobset.jobset.x-k8s.io/ddp-training created

Waiting for JobSet to complete...
Job status: Running
Job status: Running
...
Job status: Complete

JobSet completed successfully!
```

**Execution timeline (typical):**
- mkdir: 5-10 seconds
- discovery: 5-10 seconds  
- training: 2-3 minutes (depends on workload)

**Result:** Your job is now submitted and will execute when resources are available.

---

## C4: Monitor Job Progress

**What:** Watch the job status in real-time.

**Why:** Track progress and identify issues quickly.

**Command:**
```bash
./monitor.sh
```

**Output:**
```
Monitoring JobSet: ddp-training

NAMESPACE      NAME                              READY   AGE
researcher-a   ddp-training-mkdir-0              1/1     15s
researcher-a   ddp-training-discovery-0          1/1     20s
researcher-a   ddp-training-task-0-0             0/1     25s

Pods:
NAME                              READY   STATUS    RESTARTS   AGE
ddp-training-mkdir-0-0-abc123     0/1     Completed 0          15s
ddp-training-discovery-0-0-def456 0/1     Completed 0          20s
ddp-training-task-0-0-ghi789      1/1     Running   0          25s
```

**Tip:** Press Ctrl+C to exit monitoring.

---

## C5: View and Save Logs

**What:** Capture pod logs and save them to persistent storage.

**Why:** Preserve training output, metrics, and debugging information.

**Command:**
```bash
./logs.sh
```

**Output:**
```
Capturing logs for job: ddp-training
Fetching logs from pod: ddp-training-task-0-0-ghi789

Logs saved to PVC:
  /data/researcher-a/coldpress_results/ddp-training-9bdbf55a-20260409_072200/logs/
  ├── ddp-training-task-0-0-ghi789.log
  └── combined.log

Log capture complete!
```

**What's in the logs:**
- Dataset download progress
- NCCL initialization (GPU communication)
- Training progress (epochs, loss, accuracy)
- Model save confirmation

**Example log snippet:**
```
Epoch 10/50 - Loss: 0.234 - Accuracy: 85.2%
Epoch 20/50 - Loss: 0.156 - Accuracy: 90.8%
Epoch 30/50 - Loss: 0.128 - Accuracy: 92.5%
Epoch 40/50 - Loss: 0.115 - Accuracy: 93.8%
Epoch 50/50 - Loss: 0.111 - Accuracy: 94.55%
Saving model to /results/checkpoints/model_weights.pth
Training complete!
```

**Result:** Logs are now captured and saved to your PVC.

---

## C6: Explore Results

**What:** Browse the results directory in persistent storage.

**Why:** Inspect training outputs, model weights, and metrics.

### Option 1: Using explore.sh (recommended)

```bash
./explore.sh
```

**What happens:**
- Creates a temporary interactive pod
- Mounts the PVC with your results
- Opens a shell at the results directory
- Auto-cleans up the pod when you exit

**Inside the shell:**
```bash
# You're now in the results directory
ls -lh

# Output:
# discovery_user_snapshot.json
# checkpoints/
# logs/

# Check training stats
cat checkpoints/training_stats.json

# View model file
ls -lh checkpoints/model_weights.pth
# -rw-r--r-- 1 nobody nobody 77M Apr 9 07:24 model_weights.pth

# Exit the shell
exit
```

### Option 2: Quick check with oc

```bash
oc run check-results --rm -i --restart=Never \
  --image=ubi9/ubi-minimal -n researcher-a \
  --overrides='{"spec":{"volumes":[{"name":"data","persistentVolumeClaim":{"claimName":"researcher-a-storage"}}],"containers":[{"name":"check","image":"ubi9/ubi-minimal","command":["ls","-lR","/data/researcher-a/coldpress_results"],"volumeMounts":[{"name":"data","mountPath":"/data"}]}]}}'
```

### Results directory structure

```
/data/researcher-a/coldpress_results/ddp-training-{uid}-{timestamp}/
├── discovery_user_snapshot.json    # Hardware/benchmark data (2.7KB)
├── checkpoints/
│   ├── model_weights.pth          # Trained model (77MB)
│   └── training_stats.json        # Training metrics (303 bytes)
└── logs/
    ├── ddp-training-task-0-0-ghi789.log  # Individual pod log (8.7KB)
    └── combined.log                      # Combined logs (8.8KB)
```

### Training statistics example

**File:** `checkpoints/training_stats.json`
```json
{
  "dataset": "mnist",
  "epochs": 50,
  "batch_size": 128,
  "hidden_size": 4096,
  "train_test_split": 0.8,
  "num_gpus": 2,
  "time_seconds": 116.05,
  "final_loss": 0.111,
  "accuracy": 94.55,
  "model_params": 20037642,
  "input_dim": 784,
  "output_dim": 10
}
```

**Result:** When training completes, you should see results like 94.55% accuracy achieved in ~2 minutes.

---

## C7: Cleanup Resources

**What:** Delete the JobSet and associated Kubernetes resources.

**Why:** Free up cluster resources while preserving results in persistent storage.

**Command:**
```bash
./cleanup.sh
```

**This will delete:**
- JobSet: `ddp-training`
- All Jobs and Pods (cascading delete)
- ConfigMap: `ddp-training-files` (injected files)
- Services (if any were created)

**This will preserve:**
- All results in PVC (`researcher-a-storage`)
- Discovery snapshots
- Model weights and checkpoints
- Training logs and statistics

**Output:**
```
Cleaning up resources for job: ddp-training
Deleting JobSet...
jobset.jobset.x-k8s.io "ddp-training" deleted
Deleting Services...
No resources found
Deleting ConfigMap...
configmap "ddp-training-files" deleted
Cleanup complete!
```

**Verification:**
```bash
oc get jobset,job,pod,configmap -n researcher-a | grep ddp-training
# Output: No resources found (all cleaned up)

oc get pvc -n researcher-a
# Output: researcher-a-storage still exists with all results intact
```

**Result:** Cluster resources are now cleaned up, with all results preserved in your PVC.

---

# Summary

## Complete Workflow Summary

By following this guide, you will have:

### Part A: Setup
- Created a virtual environment with uv
- Installed Coldpress CLI tools

### Part B: Admin Configuration
- Configured cluster-wide Kueue resources
- Set up project namespace with storage and queueing
- Configured user RBAC for job submission

### Part C: User Workflow
1. Generated job manifests from specification
2. Submitted JobSet to cluster
3. Monitored job progress
4. Captured and saved logs
5. Explored results in persistent storage
6. Cleaned up Kubernetes resources

## Final Results

**Training job:**
- Dataset: MNIST
- Configuration: 2 GPUs, 50 epochs, batch size 128
- Performance: 94.55% accuracy in 116 seconds
- Model size: 77MB (20M parameters)

**Persistent storage structure:**
```
/data/researcher-a/coldpress_results/ddp-training-{uid}-{timestamp}/
├── discovery_user_snapshot.json    # Hardware/benchmark data
├── checkpoints/
│   ├── model_weights.pth          # Trained model (77MB)
│   └── training_stats.json        # Training metrics
└── logs/
    ├── {pod-name}.log             # Individual pod logs
    └── combined.log               # Combined logs
```

## Key Features Validated

- Cluster setup with Kueue queueing
- GPU allocation and node selection
- Job dependencies (mkdir → discovery → training)
- File injection via ConfigMap (train.py, model_config.json)
- Hardware discovery and benchmarking
- Persistent result storage
- Log capture to PVC
- Clean resource cleanup
- User RBAC for regular users

---

## Next Steps

**For new workloads:**
1. Create a job specification in `examples/your-workload/`
2. Run `coldpress generate --config examples/your-workload/config.yaml`
3. Follow the user workflow (Part C) to run your job

**For additional users:**
1. Create a user config in `users/username.yaml`
2. Apply with `coldpress-setup apply user username.yaml`

**For additional projects:**
1. Create a project config in `projects/project-name.yaml`
2. Apply with `coldpress-setup apply project project-name.yaml`
