# Coldpress

[![Tests](https://github.com/asanaullah/coldpress/workflows/Tests/badge.svg?branch=v0.2)](https://github.com/asanaullah/coldpress/actions/workflows/tests.yml)

Coldpress is a prescriptive manifest generator that reduces the effort and expertise needed to deploy complex AI/HPC workloads on Kubernetes clusters.

**Two-piece architecture:**

1. **Admin** (`coldpress-setup`) - Generates cluster setup manifests (node labels, queues, namespaces, RBAC)
2. **User** (`coldpress`) - Transforms vanilla Kubernetes Jobs into orchestrated resources (JobSet, Kubeflow PyTorchJob, KubeRay RayJob, KServe InferenceService)

## Table of Contents

- [How It Works](#how-it-works)
- [Installation](#installation)
- [Quick Start](#quick-start)
  - [For Administrators](#for-administrators)
  - [For Users](#for-users)
- [Core Concepts](#core-concepts)
  - [Intent Files](#intent-files)
  - [Macros](#macros)
- [Examples](#examples)
- [Testing](#testing)
- [Repository Structure](#repository-structure)
- [Requirements](#requirements)
- [Environment Variables](#environment-variables)
- [License](#license)

## How It Works

### Overall Flow

```
┌─────────────────────────────────────────────────────────────────┐
│ Prerequisites: Operators must be installed (see Requirements)  │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│ Phase 1: Admin Setup (One-time)                                 │
├─────────────────────────────────────────────────────────────────┤
│ 1. coldpress-setup generate cluster → manifests/cluster-*.yaml │
│                                     → manifests/label-nodes-*.sh │
│                                                                 │
│ 2. ./manifests/label-nodes-*.sh (labels nodes for scheduling)  │
│                                                                 │
│ 3. oc apply -f manifests/cluster-*.yaml                         │
│                                                                 │
│ 4. coldpress-setup generate project → manifests/project-*.yaml │
│                                                                 │
│ 5. oc apply -f manifests/project-*.yaml                         │
│                                                                 │
│ 6. coldpress-setup generate user    → manifests/user-*.yaml    │
│                                                                 │
│ 7. oc apply -f manifests/user-*.yaml                            │
│                                                                 │
│ Creates: Node labels, ClusterQueue, ResourceFlavors,           │
│          Namespaces, PVCs, RBAC                                 │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│ Phase 2: User Workflow (Repeatable)                             │
├─────────────────────────────────────────────────────────────────┤
│ 1. Create job-spec.yaml (vanilla Kubernetes Jobs)               │
│    Create intent.yaml (specify target: jobset|kubeflow|kuberay) │
│                                                                 │
│ 2. coldpress generate --intent intent.yaml → output/job-name/  │
│    - Generated manifest (jobset.yaml, pytorchjob.yaml, etc.)    │
│    - run.sh, monitor.sh, logs.sh, explore.sh, cp.sh, cleanup.sh │
│                                                                 │
│ 3. User reviews generated manifest                              │
│                                                                 │
│ 4. ./run.sh applies manifest to cluster                         │
│                                                                 │
│ 5. Kueue schedules job when resources available                │
│                                                                 │
│ 6. Jobs execute: mkdir → task-0 → task-1 → ...                 │
│    - Init containers capture hardware discovery                │
│    - Main containers run workload                               │
│    - Results saved to PVC in task-specific directories         │
│                                                                 │
│ 7. ./logs.sh captures logs to PVC                               │
│                                                                 │
│ 8. ./explore.sh opens interactive shell to browse results      │
│                                                                 │
│ 9. ./cp.sh copies results from PVC (optional)                  │
│                                                                 │
│ 10. ./cleanup.sh deletes resources (preserves results in PVC)  │
└─────────────────────────────────────────────────────────────────┘
```

### What Coldpress Does

**For Administrators (`coldpress-setup`):**
- Generates cluster-wide Kueue configuration (ClusterQueue, ResourceFlavors, LocalQueues)
- Generates project namespaces with PersistentVolumeClaims
- Generates user RBAC (RoleBindings) for job submission
- Generates node labeling scripts
- Outputs timestamped manifests for GitOps workflows

**For Users (`coldpress`):**
- Transforms vanilla Kubernetes Jobs into multiple backend formats:
  - **JobSet** - Multi-task workflows with dependencies
  - **Kubeflow PyTorchJob** - Distributed PyTorch training
  - **KubeRay RayJob** - Ray-based distributed computing
  - **KServe InferenceService** - Model serving infrastructure
- Configures task dependencies (endpoint blocking, completion blocking)
- Configures node affinity rules
- Injects discovery init containers for hardware profiling
- Generates helper scripts for job lifecycle management
- Validates YAML schemas before generation

## Installation

Choose the installation method that fits your use case:

| Use Case | Command | Activation Needed? | Best For |
|----------|---------|-------------------|----------|
| **Running jobs** | `./setup-env.sh --pipx` | ❌ No | End users, cluster users |
| **Running jobs + dev** | `./setup-env.sh --pipx-editable` | ❌ No | Users who also contribute |
| **Development** | `./setup-env.sh --uv` | ✅ Yes | Contributors, testing changes |

### Quick Start

```bash
# For end users (global install, no activation needed)
./setup-env.sh --pipx
coldpress --version

# For developers (local venv, requires activation)
./setup-env.sh --uv
source .venv/bin/activate
coldpress --version
```

### For End Users (pipx)

**pipx** installs Coldpress in an isolated environment with global CLI access - no activation needed.

```bash
# Install with pipx (recommended for end users)
./setup-env.sh --pipx

# Commands work globally, from any directory
coldpress --help
coldpress-setup --help
```

**Install in editable mode** (get updates as you pull from git):
```bash
./setup-env.sh --pipx-editable
```

**Manage installation:**
```bash
pipx upgrade coldpress    # Upgrade to latest version
pipx reinstall coldpress  # Reinstall
pipx uninstall coldpress  # Remove completely
pipx list                 # Show installed packages
```

### For Developers (uv)

**uv** provides fast, reproducible virtual environments for development work.

```bash
# One-time setup (installs uv, creates venv, installs coldpress)
./setup-env.sh --uv
# or just:
./setup-env.sh  # --uv is the default

# Activate the environment
source .venv/bin/activate

# Use coldpress (while venv is active)
coldpress --help

# Or use without activation
.venv/bin/coldpress --help
```

**For subsequent sessions:**
```bash
source .venv/bin/activate
```

## Quick Start

### For Administrators

If you are setting up Coldpress for the first time on a cluster:

1. **Generate cluster configuration:**
   ```bash
   coldpress-setup generate cluster cluster/ocp-test-nerc-mghpcc.yaml
   ```
   This creates:
   - `manifests/cluster-*.yaml` - ClusterQueue, ResourceFlavors
   - `manifests/label-nodes-*.sh` - Node labeling script

2. **Label nodes and apply cluster config:**
   ```bash
   ./manifests/label-nodes-*.sh
   oc apply -f manifests/cluster-*.yaml
   ```

3. **Generate project configuration:**
   ```bash
   coldpress-setup generate project projects/coldpress-project.yaml
   ```
   This creates:
   - `manifests/project-*.yaml` - Namespace, LocalQueue, PVCs

4. **Apply project config:**
   ```bash
   oc apply -f manifests/project-*.yaml
   ```

5. **Generate user RBAC:**
   ```bash
   coldpress-setup generate user users/myuser.yaml
   ```
   This creates:
   - `manifests/user-*.yaml` - RoleBindings for job submission

6. **Apply user config:**
   ```bash
   oc apply -f manifests/user-*.yaml
   ```

### For Users

Once the admin has completed the cluster setup:

1. **Create your workload specification:**
   ```bash
   cd my-workflow/
   # Create job-spec.yaml with vanilla Kubernetes Jobs
   # Create intent.yaml specifying target backend and transformations
   ```

2. **Generate manifest for your chosen backend:**
   ```bash
   coldpress generate --intent intent.yaml
   ```
   This creates (based on `target` in intent.yaml):
   - `output/job-name/jobset.yaml` (target: jobset)
   - `output/job-name/pytorchjob.yaml` (target: kubeflow)
   - `output/job-name/rayjob.yaml` (target: kuberay)
   - `output/job-name/inferenceservice.yaml` (target: kserve)
   - Helper scripts: `run.sh`, `monitor.sh`, `logs.sh`, `explore.sh`, `cleanup.sh`

3. **Review and apply:**
   ```bash
   cd output/job-name/
   cat *.yaml       # Review generated manifest
   ./run.sh         # Apply to cluster
   ```

4. **Monitor job progress:**
   ```bash
   ./monitor.sh
   ```

5. **Capture logs:**
   ```bash
   ./logs.sh
   ```

6. **Explore results:**
   ```bash
   ./explore.sh  # Opens interactive shell in PVC
   ```

7. **Copy results locally (optional):**
   ```bash
   ./cp.sh
   ```

8. **Clean up:**
   ```bash
   ./cleanup.sh  # Deletes JobSet, preserves results in PVC
   ```

## Core Concepts

### Intent Files

The **intent.yaml** file specifies how Coldpress transforms vanilla Kubernetes Jobs into orchestrated JobSet or Kubeflow resources.

#### Structure

```yaml
# Required
project: <namespace>
output: <output-directory-name>
target: jobset | kubeflow | kuberay | kserve  # Default: jobset

# Optional - if omitted, no files mounted
files:
  - <file1>
  - <file2>

# Optional - if omitted, no discovery
discovery:
  template: <template-name>
  tasks: all | [task1, task2]

# Required - must have at least one task
tasks:
  - name: <job-name>  # Must match a Job name in job-spec.yaml
    replicas: <count>
    nodes: [<node-ids>]  # Optional
    args:
      <key>: <value-or-macro>
    env:  # Optional - additional environment variables
      <key>: <value>
    depends_on:  # Optional (JobSet only)
      task: <other-task-name>
      wait_for: ready | completion
```

#### Fields

**Top-Level:**
- `project` (required): Namespace for deployment
- `output` (required): Output directory name
- `target` (optional): Backend to generate (default: `jobset`)
  - `jobset` - Multi-task workflows with dependencies
  - `kubeflow` - PyTorchJob for distributed PyTorch training
  - `kuberay` - RayJob for Ray-based distributed computing
  - `kserve` - InferenceService for model serving
- `files` (optional): List of files to mount as ConfigMap
- `discovery` (optional): Discovery configuration
- `tasks` (required): List of task definitions

**Task Fields:**
- `name` (required): Must match a Job name in job-spec.yaml exactly
- `replicas` (optional): Number of replicas (default: 1)
- `nodes` (optional): Node IDs for pinning
- `args` (optional): Key-value pairs for argument replacement
- `env` (optional): Additional environment variables to inject
- `depends_on` (optional, JobSet only): Dependency specification

**Dependency Fields:**
- `task` (required): Name of task to depend on
- `wait_for` (required): `ready` (wait for readinessProbe) or `completion` (wait for Job completion)

#### Example: Multi-Task with Dependencies

```yaml
project: coldpress-project
output: vllm-benchmark
target: jobset

tasks:
  - name: inference-server
    replicas: 1
  
  - name: benchmark-client
    replicas: 1
    depends_on:
      task: inference-server
      wait_for: ready
    args:
      target: "http://${inference-server}:8000"
```

### Macros

Macros are placeholders in your job-spec.yaml that Coldpress automatically fills in when generating manifests.

#### Available Macros

**Task-Local Macros** (current task):

| Macro | Description | Example Value |
|-------|-------------|---------------|
| `${INDEX}` | Replica index within current task | `0`, `1`, `2` |
| `${REPLICAS}` | Total replicas in current task | `2` |
| `${TASK_NAME}` | Name of current task | `ddp-training` |
| `${NODE_ID}` | Physical node ID (if specified) | `1` |
| `${REPLICA_0}` | Pod DNS of replica 0 (current task) | `coldpress-...-task-0-0-0....svc.cluster.local` |
| `${REPLICA_1}` | Pod DNS of replica 1 (current task) | `coldpress-...-task-1-0-0....svc.cluster.local` |

**Cross-Task Macros** (reference other tasks):

| Macro | Description | Example |
|-------|-------------|---------|
| `${REPLICA_<taskname>_0}` | Pod DNS of replica 0 of named task | `${REPLICA_inference-server_0}` |
| `${REPLICA_<taskname>_1}` | Pod DNS of replica 1 of named task | `${REPLICA_ddp-training_1}` |
| `${SERVICE_<taskname>}` | Service DNS for named task (if task has ports) | `${SERVICE_inference-server}` |

#### Usage Examples

**Single-Task DDP Training:**
```yaml
tasks:
  - name: ddp-training
    replicas: 2
    env:
      NNODES: "${REPLICAS}"
      MASTER_ADDR: "${REPLICA_ddp-training_0}"
      RANK: "${INDEX}"
```

**Multi-Task Client-Server:**
```yaml
# job-spec.yaml
---
apiVersion: batch/v1
kind: Job
metadata:
  name: inference-server
spec:
  template:
    spec:
      containers:
        - name: server
          ports:
            - containerPort: 8000
          env:
            - name: VLLM_PORT
              value: "8000"

---
apiVersion: batch/v1
kind: Job
metadata:
  name: benchmark-client
spec:
  template:
    spec:
      containers:
        - name: client
          env:
            - name: GUIDELLM_TARGET
              value: "${SERVICE_inference-server}"  # Resolved automatically
```

## Examples

Coldpress supports multiple backend targets. Each example demonstrates a different use case and backend.

### PyTorch DDP Training (JobSet)

Distributed PyTorch training with 2 workers across 2 GPUs using JobSet.

**Run:**
```bash
coldpress generate --intent examples/pytorch_ddp_training/intent_jobset.yaml
cd output/ddp-training-job/
./run.sh
```

**What it demonstrates:**
- Multi-replica DDP training with automatic DNS coordination
- ConfigMap mounting for training script
- Hardware discovery via init containers
- Persistent storage for checkpoints

### PyTorch DDP Training (Kubeflow)

Same training workload using Kubeflow's PyTorchJob operator.

**Run:**
```bash
coldpress generate --intent examples/pytorch_ddp_training/intent_kubeflow.yaml
cd output/ddp-training-job/
./run.sh
```

**What it demonstrates:**
- PyTorchJob CRD for native PyTorch distributed training
- Automatic MASTER_ADDR, MASTER_PORT, RANK injection by Kubeflow
- Single vanilla job-spec.yaml works across both JobSet and Kubeflow targets

### Ray Distributed Training (KubeRay)

Ray-based distributed training using KubeRay operator.

**Run:**
```bash
coldpress generate --intent examples/pytorch_ray_training/intent_kuberay.yaml
cd output/ray-training-job/
./run.sh
```

**What it demonstrates:**
- RayJob CRD for Ray-based workloads
- Automatic Ray cluster setup (head + worker nodes)
- Resource scaling via replicas (2 pods → 4 GPUs total)

### vLLM + GuideLLM Benchmark (JobSet)

Multi-task client-server workflow with dependency management.

**Run:**
```bash
coldpress generate --intent examples/vllm_guidellm_benchmark/intent_jobset.yaml
cd output/vllm-benchmark-job/
./run.sh
```

**What it demonstrates:**
- Task dependencies (`wait_for: ready`)
- Service discovery via `${SERVICE_*}` macros
- Automatic service creation for tasks with ports
- Client waits for server readiness before starting

**Comparison with manual approach:**
- Manual: 100+ lines of bash for orchestration, polling, error handling
- Coldpress: 14 lines of YAML (intent file)

### vLLM Inference (KServe)

Model serving using KServe InferenceService.

**Run:**
```bash
coldpress generate --intent examples/vllm_guidellm_benchmark/intent_kserve.yaml
cd output/vllm-kserve-inference/
./run.sh
```

**What it demonstrates:**
- KServe InferenceService for production model serving
- Single job-spec.yaml reused across JobSet and KServe targets
- Automatic scaling and traffic management via KServe

## Testing

Comprehensive test suite validates:
- Pydantic model validation (`test_validation.py`)
- Standard Kubernetes labels (`test_labels.py`)
- Security and input validation (`test_security.py`)
- Error handling (`test_error_handling.py`)
- Exit codes (`test_exit_codes.sh`)

**Run all tests:**
```bash
./tests/run_all_tests.sh
```

**Run individual tests:**
```bash
python tests/test_validation.py
python tests/test_labels.py
bash tests/test_exit_codes.sh
```

Tests run automatically via GitHub Actions on every push.

## Repository Structure

```
coldpress/
├── coldpress/              # CLI: Job manifest generator
├── coldpress_setup/        # CLI: Cluster setup and configuration
├── coldpress_common/       # Shared validation models (Pydantic)
├── tests/                  # Comprehensive test suite
├── discovery/              # Hardware discovery pod templates
├── projects/               # Example project configs (namespace, storage)
├── examples/               # Example workloads (intent.yaml + job-spec.yaml)
├── cluster/                # Example cluster-wide configurations
├── users/                  # Example user RBAC configurations
├── docs/                   # Documentation (CHANGELOG, quickstart guides)
├── pyproject.toml          # Package configuration (modern Python packaging)
├── setup.py                # Package setup (legacy, for backward compatibility)
└── setup-env.sh            # Environment setup script
```

## Requirements

**Cluster:**
- Kubernetes cluster (tested on OpenShift 4.21.5, Kubernetes v1.34.4)
- Kueue operator (tested with v0.11.6, API v1beta1) - required for all targets
- **Additional operators** (depending on target backend):
  - JobSet operator (v1.0.0+, API v1alpha2) - for `target: jobset`
  - Kubeflow Training Operator (v1.8+) - for `target: kubeflow`
  - KubeRay operator (v1.0+) - for `target: kuberay`
  - KServe (v0.11+) - for `target: kserve`

**Local development:**
- Python 3.10+ (tested on Python 3.14)

**Cluster tools:**
- `oc` CLI (tested with oc 4.17.0)

## Environment Variables

Customize directory locations with environment variables:

**For `coldpress` (job generation):**
- `COLDPRESS_DISCOVERY_DIR` - Discovery templates directory (default: `discovery`)
- `COLDPRESS_PROJECT_DIR` - Project configs directory (default: `projects`)
- `COLDPRESS_OUTPUT_DIR` - Default output directory (default: `output`)

**For `coldpress-setup` (manifest generation):**
- `COLDPRESS_MANIFESTS_DIR` - Manifest output directory (default: `manifests`)
- `COLDPRESS_CLUSTER_DIR` - Cluster configs directory (default: `cluster`)
- `COLDPRESS_USER_DIR` - User configs directory (default: `users`)

**Example:**
```bash
export COLDPRESS_OUTPUT_DIR=jobs
coldpress generate --intent examples/pytorch_ddp_training/intent_jobset.yaml
# Outputs to: jobs/ddp-training-job/ instead of output/ddp-training-job/

export COLDPRESS_MANIFESTS_DIR=gitops/manifests
coldpress-setup generate project coldpress-project.yaml
# Outputs to: gitops/manifests/project-coldpress-project-*.yaml
```

## Resource Labels

All generated resources include standard Kubernetes labels for easy querying and management:

- `app.kubernetes.io/managed-by: coldpress` - Identifies all Coldpress-managed resources
- `app.kubernetes.io/version: 0.2.1` - Tracks Coldpress version
- `coldpress.io/job-id: {job_name}` - Job-specific identifier for compute resources

**Query all Coldpress resources:**
```bash
oc get all -A -l app.kubernetes.io/managed-by=coldpress
```

**Delete resources by job:**
```bash
oc delete all -n namespace -l coldpress.io/job-id=job-name
```

## License

See LICENSE file.
