<!-- Assisted by: Claude Sonnet 4.5 -->
# Coldpress

Kubernetes-native job orchestration for AI/HPC workloads with GPU allocation and multi-task dependencies.

## Overview

Coldpress simplifies running complex multi-task jobs on Kubernetes clusters with GPU resources. Generate JobSet manifests locally, inspect them, and run with standard `oc` commands.

**Two-piece architecture:**

1. **Setup** (`coldpress-setup`) - One-time cluster setup (node labels, queues, namespaces)
2. **Generate** (`coldpress`) - Local CLI generates JobSet YAML + bash scripts, then run with `oc apply`

## Quick Start

### 1. Setup Environment

```bash
./setup-env.sh
source .venv/bin/activate
```

The setup script uses `uv` (fast Python package installer) and will install it automatically if not present.

**Why uv?**
- 10-100x faster than pip for package installation
- Better dependency resolution
- Built-in virtual environment management
- Backward compatible with pip workflows

### 2. Configure Cluster (Admin)

Apply cluster-wide configuration:
```bash
coldpress-setup apply cluster cluster/ocp-test-nerc-mghpcc.yaml
```

Apply project configuration:
```bash
coldpress-setup apply project projects/researcher-a.yaml
```

Grant user access (user must already exist in cluster auth system):
```bash
coldpress-setup apply user users/coldpress-user.yaml
```

**Note:** 
- Config files can be stored anywhere - the subcommand (`cluster`, `project`, `user`) specifies the type
- User must already exist in the cluster's authentication system (OAuth, LDAP, etc.)
- Project must be configured first (creates the Role that user RBAC references)

This creates:
- Node labels (`coldpress.node=0`, `coldpress.node=1`)
- Kueue ResourceFlavors and ClusterQueue
- Namespace with LocalQueue and PVC (500Gi)
- RoleBindings granting user permissions to submit JobSets in specified namespaces

### 3. Generate Job

```bash
coldpress generate --config examples/pytorch_ddp_training/config.yaml
```

Creates `output/ddp-training-job/` with:
- `jobset.yaml` - JobSet manifest
- `run.sh` - Apply and wait for completion
- `cleanup.sh` - Delete resources
- `monitor.sh` - Watch job status
- `logs.sh` - Capture and save logs to PVC
- `explore.sh` - Interactive shell to browse results

### 4. Run Job

```bash
cd output/ddp-training-job/
./run.sh
```

Monitor progress:
```bash
./monitor.sh
```

View and save logs:
```bash
./logs.sh
```

Explore results:
```bash
./explore.sh
```

Cleanup (preserves results in PVC):
```bash
./cleanup.sh
```

## Features

- **GPU Allocation** - Automatic node selection based on GPU availability
- **Task Dependencies** - Endpoint and completion blocking for multi-task workflows
- **Result Collection** - Organized storage in user PVCs with discovery snapshots
- **Log Capture** - Save pod logs to persistent storage
- **File Injection** - Mount local files via ConfigMaps
- **YAML Validation** - Pydantic-based schema validation catches errors before cluster operations
- **Transparent Workflow** - All manifests and scripts generated locally for inspection

## Complete Tutorial

See [steps.md](steps.md) for a detailed step-by-step walkthrough of running the PyTorch DDP training example, including:
- Cluster and project setup
- User RBAC configuration
- Job generation and execution
- Results exploration and log capture
- Resource cleanup

## Documentation

- [steps.md](steps.md) - Complete PyTorch DDP training walkthrough
- [docs/README.md](docs/README.md) - Architecture and detailed documentation
- [docs/VALIDATION.md](docs/VALIDATION.md) - YAML validation system documentation
- [examples/README.md](examples/README.md) - Example workloads and templates
- [TODO.md](TODO.md) - Known limitations and planned features

## Repository Structure

```
coldpress/
├── coldpress/              # CLI: Job manifest generator
├── coldpress_setup/        # CLI: Cluster setup and configuration
├── coldpress_common/       # Shared validation models (Pydantic)
├── discovery/              # Hardware discovery pod templates
├── projects/               # Example project configs (namespace, storage)
├── examples/               # Example workloads (config.yaml + job-spec.yaml)
├── cluster/                # Example cluster-wide configurations
├── users/                  # Example user RBAC configurations
├── docs/                   # Documentation
├── pyproject.toml          # Package configuration (modern Python packaging)
├── setup.py                # Package setup (legacy, for backward compatibility)
├── setup-env.sh            # Environment setup script (uses uv)
└── steps.md                # Complete tutorial walkthrough
```

## Requirements

**Cluster:**
- Kubernetes cluster (tested on OpenShift 4.21.5, Kubernetes v1.34.4)
- Kueue operator (tested with v0.11.6, API v1beta1)
- JobSet operator (tested with v1.0.0, API v1alpha2)

**Local development:**
- Python 3.9+ (tested on Python 3.14)
- `uv` (fast Python package installer - auto-installed by setup-env.sh)
- **`kubectl` or `oc` CLI** (tested with oc 4.17.0, required for all cluster operations - must be installed separately)

## Environment Variables

Customize directory locations with environment variables:

- `COLDPRESS_DISCOVERY_DIR` - Discovery templates directory (default: `discovery`)
- `COLDPRESS_PROJECT_DIR` - Project configs directory (default: `projects`)
- `COLDPRESS_OUTPUT_DIR` - Default output directory (default: `output`)

**Example:**
```bash
export COLDPRESS_OUTPUT_DIR=jobs
coldpress generate --config examples/pytorch_ddp_training/config.yaml
# Outputs to: jobs/ddp-training-job/ instead of output/ddp-training-job/
```

## Example: PyTorch DDP Training

**Job spec** (examples/pytorch_ddp_training/job-spec.yaml):
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
      - --epochs=50
      - --batch-size=128
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
project: researcher-a
discovery: user_snapshot
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
/data/researcher-a/coldpress_results/ddp-training-{uid}-{timestamp}/
├── discovery_user_snapshot.json    # Hardware/benchmark data
├── checkpoints/
│   ├── model_weights.pth          # Trained model
│   └── training_stats.json        # Training metrics
└── logs/
    ├── {pod-name}.log             # Individual pod logs
    └── combined.log               # Combined logs
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
