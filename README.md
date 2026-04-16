<!-- Assisted by: Claude Sonnet 4.5 -->
# Coldpress

[![Tests](https://github.com/asanaullah/coldpress/workflows/Tests/badge.svg?branch=v0.2)](https://github.com/asanaullah/coldpress/actions/workflows/tests.yml)

Coldpress is a prescriptive manifest generator that reduces the effort and expertise needed to deploy complex AI/HPC workloads on Kubernetes clusters.

**Two-piece architecture:**

1. **Admin** (`coldpress-setup`) - Generates cluster setup manifests (node labels, queues, namespaces, RBAC)
2. **User** (`coldpress`) - Generates job manifests from job specifications, creates JobSet YAML + helper scripts

## How Does It Work?

### Overall Flow

```
┌─────────────────────────────────────────────────────────────────┐
│ Prerequisites: Kueue and JobSet operators must be installed    │
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
│ 1. coldpress generate --config job.yaml → output/job-name/     │
│    - jobset.yaml (Kubernetes manifest)                          │
│    - run.sh, monitor.sh, logs.sh, explore.sh, cp.sh, cleanup.sh │
│                                                                 │
│ 2. User reviews jobset.yaml                                     │
│                                                                 │
│ 3. ./run.sh applies JobSet to cluster                           │
│                                                                 │
│ 4. Kueue schedules job when resources available                │
│                                                                 │
│ 5. Jobs execute: mkdir → task-0 → task-1 → ...                 │
│    - Init containers capture hardware discovery                │
│    - Main containers run workload                               │
│    - Results saved to PVC in task-specific directories         │
│                                                                 │
│ 6. ./logs.sh captures logs to PVC                               │
│                                                                 │
│ 7. ./explore.sh opens interactive shell to browse results      │
│                                                                 │
│ 8. ./cp.sh copies results from PVC (optional)                  │
│                                                                 │
│ 9. ./cleanup.sh deletes JobSet (preserves results in PVC)      │
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
- Generates JobSet manifests from simple job specifications
- Configures task dependencies (endpoint blocking, completion blocking)
- Configures node affinity rules
- Configures volume mounts and hardware discovery init containers
- Creates helper scripts for job lifecycle management
- Validates YAML schemas before generation

**Prerequisites:**
- Kueue and JobSet operators must be installed on the cluster
- Users must exist in cluster authentication system

## Getting Started

### Installation

Choose the installation method that fits your use case:

| Use Case | Command | Activation Needed? | Best For |
|----------|---------|-------------------|----------|
| **Running jobs** | `./setup-env.sh --pipx` | ❌ No | End users, cluster users |
| **Running jobs + dev** | `./setup-env.sh --pipx-editable` | ❌ No | Users who also contribute |
| **Development** | `./setup-env.sh --uv` | ✅ Yes | Contributors, testing changes |

#### Quick Start

```bash
# For end users (global install, no activation needed)
./setup-env.sh --pipx
coldpress --version

# For developers (local venv, requires activation)
./setup-env.sh --uv
source .venv/bin/activate
coldpress --version
```

#### For End Users (pipx)

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

#### For Developers (uv)

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

**Why uv for development?**
- Fast dependency resolution and installation
- Reproducible builds
- Editable install by default (changes reflect immediately)
- Isolated from system Python

### For Administrators

If you are setting up Coldpress for the first time on a cluster, follow the **[Admin Quickstart Guide](docs/quickstart_admin.md)** to:

1. Generate and apply cluster-wide configuration (ClusterQueue, ResourceFlavors)
2. Generate and apply project configuration (namespaces, storage, queues)
3. Generate and apply user RBAC (permissions for job submission)

This is a one-time setup process that configures the cluster infrastructure for all users.

### For Users

Once the admin has completed the cluster setup, follow the **[User Quickstart Guide](docs/quickstart_user.md)** to:

1. Generate job manifests from your workload specification
2. Review and apply the JobSet to the cluster
3. Monitor job progress and capture logs
4. Explore results in persistent storage
5. Clean up cluster resources (preserves results)

This workflow is repeatable for each job you want to run.

## Documentation

- **[Resource Labels](docs/LABELS.md)** - Query and manage Coldpress resources using standard Kubernetes labels
  - Find all Coldpress-managed resources: `kubectl get all -A -l app.kubernetes.io/managed-by=coldpress`
  - Delete resources by job: `kubectl delete all -n namespace -l coldpress.io/job-id=job-name`
- **[Error Handling](docs/ERROR_HANDLING.md)** - Exit codes, exception handling, and debugging
  - Exit code 0: Success, 1: Application errors, 2: Usage errors
  - Specific exception types with clear error messages
  - Pydantic validation catches configuration errors early
- **Security** - Input validation and injection prevention
  - Kubernetes naming rules enforced (lowercase alphanumeric, dashes, dots)
  - JSON constructed safely with `json.dumps()` (no f-string injection)
  - No hardcoded temp files (timestamped outputs instead)

## Quickstart Guides

- **[Admin Quickstart](docs/quickstart_admin.md)** - Cluster setup for administrators (one-time)
- **[User Quickstart](docs/quickstart_user.md)** - Running workloads for users (repeatable)


## Repository Structure

```
coldpress/
├── coldpress/              # CLI: Job manifest generator
├── coldpress_setup/        # CLI: Cluster setup and configuration
├── coldpress_common/       # Shared validation models (Pydantic)
├── tests/                  # Comprehensive test suite
│   ├── test_validation.py  # Pydantic model validation tests
│   ├── test_labels.py      # Resource labeling tests
│   ├── test_security.py    # Security and input validation tests
│   ├── test_error_handling.py  # Error handling tests
│   ├── test_roce_disabled.py   # RoCE NIC disabled tests
│   ├── test_exit_codes.sh  # Shell exit code tests
│   └── run_all_tests.sh    # Run full test suite
├── discovery/              # Hardware discovery pod templates
├── projects/               # Example project configs (namespace, storage)
├── examples/               # Example workloads (config.yaml + job-spec.yaml)
├── cluster/                # Example cluster-wide configurations
├── users/                  # Example user RBAC configurations
├── docs/                   # Documentation
├── pyproject.toml          # Package configuration (modern Python packaging)
├── setup.py                # Package setup (legacy, for backward compatibility)
└── setup-env.sh            # Environment setup script
```

## Requirements

**Cluster:**
- Kubernetes cluster (tested on OpenShift 4.21.5, Kubernetes v1.34.4)
- Kueue operator (tested with v0.11.6, API v1beta1)
- JobSet operator (tested with v1.0.0, API v1alpha2)

**Local development:**
- Python 3.9+ (tested on Python 3.14)

**Cluster tools:**
- `kubectl` or `oc` CLI (tested with oc 4.17.0)

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
coldpress generate --config examples/pytorch_ddp_training/config.yaml
# Outputs to: jobs/ddp-training-job/ instead of output/ddp-training-job/

export COLDPRESS_MANIFESTS_DIR=gitops/manifests
coldpress-setup generate project coldpress-project.yaml
# Outputs to: gitops/manifests/project-coldpress-project-*.yaml
```

## Example: PyTorch DDP Training

**Job spec** (examples/pytorch_ddp_training/job-spec.yaml):

Note: This generates a JobSet named `coldpress-ddp-training` in the cluster.

```yaml
name: ddp-training
tolerate_all: true

containers:
  - name: training
    image: pytorch/pytorch:2.2.0-cuda12.1-cudnn8-runtime
    workingDir: /workspace
    command: ["python", "-m", "torch.distributed.run"]
    args:
      - --nproc_per_node=2
      - --nnodes=1
      - train.py
      - --dataset=mnist
      - --train-test-split=0.8
      - --epochs=50
      - --batch-size=128
      - --hidden-size=4096
      - --lr=0.01
      - --output-dir=/results/checkpoints
    resources:
      requests:
        nvidia.com/gpu: "2"
        memory: "16Gi"
        cpu: "8"
      limits:
        nvidia.com/gpu: "2"
        memory: "16Gi"
    env:
      - name: NCCL_DEBUG
        value: "INFO"

volumes:
  - name: results
    mount: /results
  - name: dshm
    type: emptyDir
    medium: Memory
    sizeLimit: 16Gi
    mount: /dev/shm
```

**Config** (examples/pytorch_ddp_training/config.yaml):
```yaml
project: coldpress-project

# Discovery - runs as init container per task to capture actual node hardware
discovery: user_snapshot  # Simple format
# Or use detailed format:
# discovery:
#   template: user_snapshot
#   tasks: all  # or [0, 1] for specific tasks

output: ddp-training-job

# Files to mount into container (creates ConfigMap)
files:
  - train.py
  - model_config.json
```

**Generate and run:**
```bash
coldpress generate --config examples/pytorch_ddp_training/config.yaml
cd output/ddp-training-job/
./run.sh
```

**Results structure in PVC:**
```
/data/coldpress-project/coldpress_results/ddp-training-{uid}-{timestamp}/
├── task-0/
│   ├── discovery_user_snapshot.json    # Hardware/benchmark data for task 0
│   ├── checkpoints/
│   │   ├── model_weights.pth          # Trained model
│   │   └── training_stats.json        # Training metrics
└── logs/
    ├── {pod-name}.log                  # Individual pod logs
    └── combined.log                    # Combined logs
```

## Example: vLLM + GuideLLM Benchmark

Multi-task workflow with endpoint blocking:

```bash
coldpress generate --config examples/vllm_guidellm_benchmark/config.yaml
cd output/vllm-benchmark-job/
./run.sh
```

The job-spec defines:
- **Task 1**: vLLM inference server with readinessProbe (endpoint blocking)
- **Task 2**: GuideLLM benchmark client that waits for server readiness

See [examples/README.md](examples/README.md) for more details.

## License

See LICENSE file.
