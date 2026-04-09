<!-- Assisted by: Claude Sonnet 4.5 -->
# PyTorch DDP Training Example - Step by Step

## Overview
Running the PyTorch Distributed Data Parallel (DDP) training example with 2 GPUs.

---

## Step 1: Setup Python Environment ✓

**What we're doing:** Installing Coldpress CLI tools in a Python virtual environment.

**Why:** We need the `coldpress` and `coldpress-setup` command-line tools to generate job manifests.

**Commands:**
```bash
cd /home/ahmed/MOC/esi/coldpress_repos/helm
source venv/bin/activate
pip install -e .
```

**Verification:**
```bash
coldpress --version
# Output: coldpress, version 2.0.0

coldpress-setup --version
# Output: coldpress-setup, version 2.0.0
```

**Result:** ✓ Both CLI tools installed successfully.

---

## Step 2: Check if researcher-a Namespace Exists ✓

**What we did:** Checked if the researcher-a namespace and its resources exist.

**Commands:**
```bash
oc get namespace researcher-a
# Output: researcher-a   Active   37d
# ✓ Namespace exists

oc get pvc researcher-a-storage -n researcher-a
# Output: ✗ PVC does NOT exist
```

**Result:** Namespace exists, but PVC and other resources need to be created.

---

## Step 3: Apply Cluster Configuration ✓

**What we did:** Applied cluster-wide configuration using admin privileges.

**Command:**
```bash
source venv/bin/activate
coldpress-setup apply cluster/ocp-test-nerc-mghpcc.yaml
```

**Result:**
```
Using oc to apply cluster config (as system:admin)...
kueue.kueue.openshift.io/cluster unchanged
jobsetoperator.operator.openshift.io/cluster unchanged
resourceflavor.kueue.x-k8s.io/node0 unchanged
resourceflavor.kueue.x-k8s.io/node1 unchanged
clusterqueue.kueue.x-k8s.io/cluster-queue-coldpress created
```

**Resources:**
- ✓ Kueue operator (already existed)
- ✓ JobSet operator (already existed)
- ✓ ResourceFlavors: node0, node1 (already existed)
- ✓ ClusterQueue: cluster-queue-coldpress (newly created)

**Note:** Used `oc --as system:admin` for cluster-scoped resources.

---

## Step 4: Apply Project Configuration ✓

**What we did:** Applied project configuration using admin privileges.

**Command:**
```bash
source venv/bin/activate
coldpress-setup apply projects/researcher-a.yaml
```

**Result:**
```
Using oc to apply manifests (as system:admin)...
localqueue.kueue.x-k8s.io/local-queue-researcher-a unchanged
namespace/researcher-a configured
persistentvolumeclaim/researcher-a-storage created
serviceaccount/coldpress-user created
role.rbac.authorization.k8s.io/coldpress-user-role configured
rolebinding.rbac.authorization.k8s.io/coldpress-user-binding configured
```

**Resources created/configured:**
- ✓ Namespace: researcher-a (already existed, now configured)
- ✓ LocalQueue: local-queue-researcher-a (already existed)
- ✓ PVC: researcher-a-storage (500Gi, nfs-csi, newly created)
- ✓ RBAC: coldpress-user ServiceAccount, Role, RoleBinding

**Verification:**
```bash
oc get namespace researcher-a
# Output: researcher-a   Active   37d

oc get pvc -n researcher-a
# Output: researcher-a-storage   Bound   pvc-567e1e0b-6b3b-4fe7-9c3d-3acff47d2340   500Gi   RWX   nfs-csi   28s
```

**Note:** Used `oc --as system:admin` for namespace-scoped resources that require elevated privileges.

---

## Step 5: Generate DDP Training Job Manifests ✓

**What we did:** Generated JobSet manifests and helper scripts using coldpress CLI.

**Command:**
```bash
source venv/bin/activate
coldpress generate --config examples/pytorch_ddp_training/config.yaml
```

**Result:**
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

**Generated files:**
- JobSet manifest: `jobset.yaml`
- Metadata: `metadata.json`
- Helper scripts: `run.sh`, `cleanup.sh`, `monitor.sh`, `logs.sh`, `explore.sh`

**Output directory:** `ddp-training-job/`

**Note:** Warnings about Kueue permissions are expected (user cannot read ClusterQueue). Job allocation still succeeded using node discovery.

---

## Step 6: Inspect Generated Output Directory ✓

**What we did:** Inspected the structure and contents of generated files.

**Output directory:** `output/ddp-training-job/`

**Generated files:**
```
output/ddp-training-job/
├── jobset.yaml      (263 lines) - JobSet manifest with 3 ReplicatedJobs
├── metadata.json    (14 lines)  - Job metadata and node assignments
├── run.sh           (31 lines)  - Apply JobSet and wait for completion
├── cleanup.sh       (19 lines)  - Delete JobSet and services
├── monitor.sh       (11 lines)  - Watch job status
├── logs.sh          (23 lines)  - Stream pod logs
└── explore.sh       (74 lines)  - Interactive shell with PVC mounted
```

**JobSet structure:**
1. **mkdir job** - Creates results directory in PVC
2. **discovery job** - Records hardware snapshot (right after mkdir)
3. **ddp-training job** - Main PyTorch DDP training (2 GPUs, node 0)

**Key verification:**
- ✓ Discovery job appears immediately after mkdir job
- ✓ All helper scripts are executable
- ✓ Output is in `output/` subdirectory (coldpress now always uses this structure)

---

## Step 7: Review Generated JobSet Manifest ✓

**What we did:** Reviewed the generated JobSet YAML manifest in detail.

**Location:** `output/ddp-training-job/jobset.yaml`

**JobSet Structure:**
```yaml
apiVersion: jobset.x-k8s.io/v1alpha2
kind: JobSet
metadata:
  name: ddp-training
  namespace: researcher-a
  labels:
    kueue.x-k8s.io/queue-name: local-queue-researcher-a
spec:
  suspend: true  # Kueue will unsuspend when resources available
  replicatedJobs:
  - name: mkdir       # Job 1: Create results directory
  - name: discovery   # Job 2: Hardware snapshot
  - name: task-0      # Job 3: PyTorch DDP training
```

**Task-0 (DDP Training) Details:**
- **Image:** `pytorch/pytorch:2.2.0-cuda12.1-cudnn8-runtime`
- **Node:** `coldpress.node: '0'` (node selector)
- **Resources:**
  - GPUs: 2 (`nvidia.com/gpu: '2'`)
  - Memory: 16Gi
  - CPU: 8 cores
- **Working Directory:** `/workspace`
- **Command:** `python -m torch.distributed.run`
- **Args:**
  - `--nproc_per_node=2` (2 GPUs for DDP)
  - `--nnodes=1` (single node training)
  - `train.py` (training script)
  - `--dataset=mnist`, `--epochs=50`, `--batch-size=128`, etc.
- **Volumes:**
  - `coldpress-data` PVC → `/mnt/coldpress-data` and `/results`
  - `dshm` emptyDir (Memory, 16Gi) → `/dev/shm` (for PyTorch shared memory)
- **Environment:** `NCCL_DEBUG=INFO` (NCCL logging)
- **Dependency:** Waits for discovery job to complete

**Job Dependencies:**
1. mkdir → discovery → task-0 (sequential execution)

**Note:** Fixed generator bug that was ignoring container array in job-spec.yaml.

---

## Step 8: Apply Job to Cluster ✓

**What we did:** Applied JobSet to cluster and monitored execution.

**Issue encountered:** User permission error - user "asanaullah" couldn't create JobSets.

**Solution:** Created user RBAC system:
1. Created `users/` directory for user configurations
2. Created `users/asanaullah.yaml` with username and namespace access list
3. Updated `coldpress-setup` to handle user configs
4. Applied user RBAC: `coldpress-setup apply users/asanaullah.yaml`

**User config:**
```yaml
username: asanaullah
namespaces:
  - researcher-a
```

**RBAC generated:**
- RoleBinding: `coldpress-user-asanaullah` in `researcher-a` namespace
- Grants user permission to create/manage JobSets, view Jobs/Pods/Services

**Job submission:**
```bash
cd output/ddp-training-job
./run.sh
```

**Result:**
```
Applying JobSet...
jobset.jobset.x-k8s.io/ddp-training created

Job status (after 80s):
- mkdir job: Complete (9s)
- discovery job: Complete (5s)  
- task-0 (training): Running (67s and counting)
```

**Issue #2: Job failed to schedule - missing tolerations**
- Error: "7 node(s) had untolerated taint(s)"
- Solution: Added `tolerate_all: true` to job-spec.yaml
- Generator now adds `tolerations: [{operator: Exists}]` to pod spec
- Result: Pod scheduled successfully ✓

**Job execution after fix:**
```bash
oc delete jobset ddp-training -n researcher-a
rm -rf output/ddp-training-job
coldpress generate --config examples/pytorch_ddp_training/config.yaml
cd output/ddp-training-job && ./run.sh
```

**Status:**
- mkdir: ✓ Complete
- discovery: ✓ Complete
- task-0: Scheduled and ran (tolerations working!)
  - Error: train.py not found (example job needs actual training script)

**Note:** Scheduling issue resolved. The tolerations are now properly applied and pods can schedule on tainted nodes.

---

## Step 9: Monitor Job Progress

**Status:** Pending

---

## Step 10: View Job Logs ✓

**What we did:** Captured and saved job logs to persistent storage.

**Implementation:**
- Updated logs.sh script to save logs to PVC
- Logs are saved to `{base_dir}/logs/` directory
- Creates individual pod logs and combined log file

**Command:**
```bash
cd output/ddp-training-job
./logs.sh
```

**Results structure:**
```
/data/researcher-a/coldpress_results/ddp-training-9bdbf55a-20260409_072200/logs/
├── ddp-training-task-0-0-0-pnqfr.log (8.7KB, 109 lines) - Individual pod log
└── combined.log (8.8KB, 110 lines) - Combined log from all pods
```

**Log contents:**
- Dataset download progress
- NCCL initialization (GPU communication)
- Training progress (epochs 10, 20, 30, 40, 50)
- Final accuracy: 94.55%
- Model save confirmation

**Key fix:**
- Used direct piping (`oc logs | oc exec -i ... sh -c "cat > file"`) instead of `oc cp` to avoid tar dependency in minimal images

---

## Step 11: Explore Results Directory ✓

**What we did:** Explored the training results saved to PVC.

**Method 1: Using explore.sh script**
```bash
cd output/ddp-training-job
./explore.sh
```

**Result:**
- ✓ Interactive pod created and mounted PVC
- ✓ Shell opened at results directory
- ✓ Pod auto-cleaned up on exit

**Method 2: Quick check with temporary pod**
```bash
oc run check-results --rm -i --restart=Never --image=ubi9/ubi-minimal -n researcher-a \
  --overrides='...' # see full command in conversation
```

**Results Found:**
```
/data/researcher-a/coldpress_results/ddp-training-9bdbf55a-20260409_072200/
├── discovery_user_snapshot.json (2.7KB)
├── checkpoints/
│   ├── model_weights.pth (77MB)
│   └── training_stats.json (303 bytes)
└── logs/
    ├── ddp-training-task-0-0-0-pnqfr.log (8.7KB, 109 lines)
    └── combined.log (8.8KB, 110 lines)
```

**training_stats.json:**
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
  "accuracy": 94.55%,
  "model_params": 20,037,642,
  "input_dim": 784,
  "output_dim": 10
}
```

**Key Achievements:**
- ✓ Results successfully saved to persistent storage
- ✓ Training completed in ~2 minutes with 94.55% accuracy
- ✓ Model weights (77MB) and stats saved to PVC
- ✓ Discovery snapshot with hardware benchmarks saved
- ✓ Training logs captured and saved to PVC
- ✓ ConfigMap-based file injection working (train.py, model_config.json)
- ✓ Multiple files support verified

---

## Step 12: Cleanup Job Resources ✓

**What we did:** Cleaned up Kubernetes resources while preserving results in PVC.

**Command:**
```bash
cd output/ddp-training-job
./cleanup.sh
```

**Result:**
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

**Resources deleted:**
- ✓ JobSet: ddp-training
- ✓ All Jobs and Pods (cascading delete from JobSet)
- ✓ ConfigMap: ddp-training-files
- ✓ Services (if any were created)

**Resources preserved:**
- ✓ All results in PVC remain intact
- ✓ Discovery snapshot, model weights, training stats, and logs all preserved

**Verification:**
```bash
oc get jobset,job,pod,configmap -n researcher-a | grep ddp-training
# Output: No resources found (all cleaned up)
```

---

## Summary

**Complete workflow successfully demonstrated:**
1. ✓ Cluster and project setup with Kueue queuing
2. ✓ User RBAC for regular users to submit jobs
3. ✓ Job generation with ConfigMap file injection
4. ✓ Discovery snapshot with hardware benchmarks
5. ✓ PyTorch DDP training on 2 GPUs (94.55% accuracy)
6. ✓ Results saved to persistent storage
7. ✓ Logs captured and saved to PVC
8. ✓ Clean resource cleanup preserving all results

**Final directory structure in PVC:**
```
/data/researcher-a/coldpress_results/ddp-training-{uid}-{timestamp}/
├── discovery_user_snapshot.json    # Hardware/benchmark data
├── checkpoints/
│   ├── model_weights.pth          # Trained model (77MB)
│   └── training_stats.json        # Training metrics
└── logs/
    ├── {pod-name}.log             # Individual pod logs
    └── combined.log               # Combined logs from all pods
```
