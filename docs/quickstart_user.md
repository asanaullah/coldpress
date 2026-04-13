<!-- Assisted by: Claude Sonnet 4.5 -->
# Coldpress User Quickstart

This guide shows the typical workflow for a regular user submitting and managing an AI/HPC workload. These steps are repeatable for each job.

**Prerequisites:**
- **Admin must have completed the [Admin Quickstart Guide](quickstart_admin.md) first** (cluster, project, and user configuration)
- Coldpress CLI tools installed (see main [README.md](../README.md))
- `kubectl` or `oc` CLI installed and configured
- User has access to the target namespace

---

## Step 1: Generate Job Manifests

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
├── explore.sh       # Interactive shell to browse results
└── cp.sh            # Copy results from PVC to local directory
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
Generated: ddp-training-job/cp.sh
```

**Result:** Job manifests and helper scripts are now ready to use.

---

## Step 2: Inspect Generated Manifests (Optional)

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
  - name: mkdir       # Job 1: Create results directory and task subdirectories
  - name: task-0      # Job 2: PyTorch DDP training (2 GPUs, with discovery init container)
```

**Key configuration for task-0 (training job):**
- Image: `pytorch/pytorch:2.2.0-cuda12.1-cudnn8-runtime`
- Node selector: `coldpress.node: '0'`
- Resources: 2 GPUs, 16Gi memory, 8 CPU cores
- Command: `python -m torch.distributed.run --nproc_per_node=2 train.py`
- Volumes: PVC for results, emptyDir for shared memory
- Dependencies: Waits for mkdir job to complete
- Init container: Runs discovery to capture node hardware before training starts

---

## Step 3: Run the Job

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
4. Jobs execute in order: mkdir → training (with discovery init container)
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
- mkdir: 5-10 seconds (creates base dir + task subdirectories)
- task-0 init container (discovery): 5-10 seconds (runs before main container)
- task-0 main container (training): 2-3 minutes (depends on workload)

**Result:** Your job is now submitted and will execute when resources are available.

---

## Step 4: Monitor Job Progress

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
researcher-a   ddp-training-task-0-0             0/1     25s

Pods:
NAME                              READY   STATUS       RESTARTS   AGE
ddp-training-mkdir-0-0-abc123     0/1     Completed    0          15s
ddp-training-task-0-0-ghi789      0/1     Init:0/1     0          10s  # Discovery running
ddp-training-task-0-0-ghi789      1/1     Running      0          25s  # Training started
```

**Tip:** Press Ctrl+C to exit monitoring.

---

## Step 5: View and Save Logs

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

## Step 6: Explore Results

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
# task-0/
# logs/

# Navigate to task-0 directory
cd task-0
ls -lh

# Output:
# discovery_user_snapshot.json
# checkpoints/

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
├── task-0/
│   ├── discovery_user_snapshot.json    # Hardware/benchmark data (2.7KB)
│   └── checkpoints/
│       ├── model_weights.pth          # Trained model (77MB)
│       └── training_stats.json        # Training metrics (303 bytes)
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

## Step 7: Copy Results to Local Machine

**What:** Copy results from the PVC to your local machine.

**Why:** Download results for local analysis, backup, or sharing.

**Command:**
```bash
# Copy to default location (./results)
./cp.sh

# Copy to specific directory
./cp.sh /path/to/destination
```

**Output:**
```
Copying results from PVC to local directory...
Creating temporary pod...
Copying files...
Cleaning up temporary pod...

===== Copy Complete =====
Results copied to: ./results
==========================
```

**Result:** Results are now available on your local machine.

---

## Step 8: Cleanup Resources

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

## Summary

By following this guide, you have:

1. ✓ Generated job manifests from specification
2. ✓ Submitted JobSet to cluster
3. ✓ Monitored job progress
4. ✓ Captured and saved logs
5. ✓ Explored results in persistent storage
6. ✓ Copied results to local machine
7. ✓ Cleaned up Kubernetes resources

**Final results:**
- Dataset: MNIST
- Configuration: 2 GPUs, 50 epochs, batch size 128
- Performance: 94.55% accuracy in 116 seconds
- Model size: 77MB (20M parameters)

---

## Next Steps

**For new workloads:**
1. Create a job specification in `examples/your-workload/`
2. Run `coldpress generate --config examples/your-workload/config.yaml`
3. Follow the steps in this guide to run your job

**Advanced features:**
- See [examples/README.md](../examples/README.md) for more example workloads
- See [VALIDATION.md](VALIDATION.md) for YAML validation system documentation
- See main [README.md](../README.md) for advanced features like node scheduling
