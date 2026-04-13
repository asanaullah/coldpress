# PyTorch DDP Training - Explicit Node Assignment

This example demonstrates running a PyTorch Distributed Data Parallel (DDP) training job with **explicit node assignment** specified in the config file.

## Difference from Base Example

**Base example (`pytorch_ddp_training`):**
- No `nodes` field in config.yaml
- Kubernetes scheduler picks any node with `coldpress.node` label
- Uses `nodeAffinity` with `Exists` operator

**This example (`pytorch_ddp_training_node1`):**
- Has `nodes: [1]` in config.yaml
- Task is pinned to node 1 (label: `coldpress.node=1`)
- Uses `nodeSelector` with specific node ID

## Config File

```yaml
# config.yaml
project: researcher-a

# Per-task discovery - runs on the actual node where task executes
discovery:
  template: user_snapshot
  tasks: all  # Run discovery for all tasks

output: ddp-training-job-node1

files:
  - train.py
  - model_config.json

# Explicit node assignment - pin task to node 1
nodes:
  - 1
```

## Per-Task Discovery

This example uses **per-task discovery**, which runs hardware discovery as an init container for each task. This ensures the discovery snapshot reflects the **actual hardware** where the task executes, not a different node.

**Discovery configuration:**
- `template: user_snapshot` - Discovery template to use
- `tasks: all` - Run discovery for all tasks (can also be `[0, 1, 2]` for specific tasks)

**Results structure:**
```
/data/researcher-a/coldpress_results/ddp-training-job-{uid}-{timestamp}/
├── task-0/
│   └── discovery_user_snapshot.json    # Hardware info for task 0
└── logs/
    └── task-0.log
```

**Backward compatibility:** Simple string format still works:
```yaml
discovery: user_snapshot  # Equivalent to {template: user_snapshot, tasks: all}
```

## Usage

```bash
# Generate manifests (node assignment is in config.yaml)
coldpress generate --config examples/pytorch_ddp_training_node1/config.yaml

# Output directory
cd output/ddp-training-job-node1/

# Apply to cluster
./run.sh

# Monitor
./monitor.sh
```

## Generated Manifest Difference

**With `--node 1`:**
```yaml
spec:
  nodeSelector:
    coldpress.node: "1"
```

**Without `--node` (base example):**
```yaml
spec:
  affinity:
    nodeAffinity:
      requiredDuringSchedulingIgnoredDuringExecution:
        nodeSelectorTerms:
        - matchExpressions:
          - key: coldpress.node
            operator: Exists
```

## When to Use Explicit Node Assignment

Use `--node` when you need:
- **Specific hardware**: Task requires GPU model only available on certain nodes
- **Reproducibility**: Pin experiments to same hardware for consistent results
- **Resource isolation**: Dedicate specific nodes to specific teams/projects
- **Multi-task workflows**: Each task has different hardware requirements

Otherwise, let the Kubernetes scheduler handle allocation for better cluster utilization.

## Multi-Task Example

For workflows with multiple tasks on different nodes, you can specify in config.yaml:

```yaml
# config.yaml for multi-task workflow
project: researcher-a
output: multi-task-job

nodes:
  - 0  # Task 0 → node 0
  - 1  # Task 1 → node 1
```

Or override via CLI:
```bash
coldpress generate --config job.yaml --node 0 --node 1
```

**Priority:** CLI `--node` flag > `nodes` in config.yaml > Kubernetes scheduler

## Files

Same as base example:
- `config.yaml` - Coldpress configuration
- `job-spec.yaml` - Task specification
- `train.py` - PyTorch training script
- `model_config.json` - Model hyperparameters
