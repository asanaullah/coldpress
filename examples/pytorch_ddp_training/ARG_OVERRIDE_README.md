# Argument Override Pattern for Coldpress

This example demonstrates the new argument override pattern for Coldpress intent files.

## Problem

The previous approach used environment variables with complex bash conditionals in the job-spec, making YAMLs unreadable and difficult to maintain. Single-node job-specs couldn't be directly run with `kubectl apply`.

## Solution

Use clean, directly-runnable job-specs with argument substitution from intent.yaml.

## Key Features

### 1. Clean Single-Node Job-Spec

The `job-spec.yaml` is now a standard Kubernetes Job that can be directly applied:

```yaml
command:
  - python
  - -m
  - torch.distributed.run
  - --nproc_per_node=2
  - train.py
  - --dataset=mnist
  - --epochs=50
  - --batch-size=128
```

### 2. Intent-Based Argument Overrides

Two patterns are supported in `intent.yaml`:

#### Simple Replacement (existing args)
```yaml
args:
  epochs: "100"           # Replace existing --epochs value
  batch-size: "256"       # Replace existing --batch-size value
```

#### Positioned Insertion (new args)
```yaml
args:
  nnodes:
    value: "${REPLICAS}"
    insert_after: nproc_per_node
  
  master_addr:
    value: "${REPLICA_ddp-training_0}"
    insert_after: node_rank
```

## Generated Output

The JobSet generator produces:

```yaml
command:
  - python
  - -m
  - torch.distributed.run
  - --nproc_per_node=2
  - --nnodes=2                    # Inserted after nproc_per_node
  - --node_rank=0                 # Inserted after nnodes
  - --master_addr=coldpress-...   # Inserted after node_rank
  - --master_port=29500           # Inserted after master_addr
  - train.py
  - --dataset=mnist
  - --epochs=100                  # Replaced from 50
  - --batch-size=256              # Replaced from 128
```

## Macro Substitution

The following macros are available:
- `${REPLICAS}` - Total replica count
- `${INDEX}` - Current replica index (0-based)
- `${REPLICA_<taskname>_N}` - DNS name of replica N
- `${NODE_ID}` - Assigned node ID (if using node pinning)

## Implementation

### Model Changes

`coldpress_common/model.py`:
- Added `ArgOverride` Pydantic model
- Updated `TaskIntent.args` to support `Union[str, ArgOverride]`

### Generator Changes

Both `jobset_generator.py` and `kubeflow_generator.py`:
- Added `parse_arg()` function to parse CLI flags
- Enhanced `replace_or_add_arg()` with `insert_after`/`insert_before` support
- Updated arg processing to handle both `command` and `args` fields
- Handle both string values and ArgOverride objects

## Example Files

- `job-spec.yaml` - Clean single-node Job specification
- `intent_jobset.yaml` - JobSet intent with arg overrides
- `intent_kubeflow.yaml` - PyTorchJob intent (simpler, uses auto-injection)

## Testing

Generate manifests:
```bash
coldpress generate --intent intent_jobset.yaml
coldpress generate --intent intent_kubeflow.yaml
```

Verify args in output:
```bash
grep -A 20 "command:" output/ddp-training-job/jobset.yaml
grep -A 20 "command:" output/ddp-training-job/pytorchjob.yaml
```
