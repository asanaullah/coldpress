# Argument Override Pattern for Ray Training Example

This example demonstrates the argument override pattern for Ray Train jobs using KubeRay, matching the approach used in the PyTorch DDP example.

## Updates Made

### 1. Clean Single-Node Job-Spec

The `job-spec.yaml` now uses clean command/args (no bash conditionals):

```yaml
command:
  - python
  - train.py
  - --dataset=mnist
  - --epochs=50
  - --batch-size=128
  - --hidden-size=4096
  - --lr=0.01
  - --num-workers=1        # Single-node default
  - --gpus-per-worker=2
  - --cpus-per-worker=4
```

**Before:** Used bash script with env var conditionals (complex, hard to read)  
**After:** Direct command with arguments (clean, directly runnable)

### 2. Intent-Based Argument Overrides

The `intent_kuberay.yaml` now uses `args` instead of `env`:

```yaml
tasks:
  - name: ray-training
    replicas: 2  # 1 head + 1 worker = 4 GPUs total

    args:
      # Scale Ray Train workers to match pod count
      num-workers: "${REPLICAS}"  # Overrides from 1 to 2
      # gpus-per-worker and cpus-per-worker use defaults from job-spec
```

**Before:** Used `env` with environment variable overrides  
**After:** Uses `args` with macro substitution (consistent with DDP/Kubeflow patterns)

### 3. Generator Updates

Updated `kuberay_generator.py` to:
- Import `parse_arg` and `replace_or_add_arg` from `jobset_generator`
- Apply arg overrides before building the RayJob entrypoint
- Handle both simple string values and `ArgOverride` objects with positioning
- Process args for both head and worker containers

## Generated Output

The RayJob manifest now has a clean entrypoint:

```yaml
entrypoint: cd /results/.../workspace && python train.py 
  --dataset=mnist 
  --train-test-split=0.8 
  --epochs=50 
  --batch-size=128 
  --hidden-size=4096 
  --lr=0.01 
  --output-dir=/results/checkpoints 
  --num-workers=2          # Overridden from 1
  --gpus-per-worker=2      # Default preserved
  --cpus-per-worker=4      # Default preserved
```

## Macro Substitution

Available macros for RayJob:
- `${REPLICAS}` - Total replica count (head + workers)
- `${INDEX}` - Container index (0 for head, 1+ for workers)
- `${TASK_NAME}` - Task name from intent

## Consistency with DDP Example

Both examples now follow the same pattern:

| Aspect | DDP Example | Ray Example |
|--------|-------------|-------------|
| job-spec | Clean command/args | Clean command/args |
| intent | Args with positioning | Args with macros |
| Generator | `jobset_generator.py` | `kuberay_generator.py` |
| Override function | `replace_or_add_arg()` | `replace_or_add_arg()` |

## Testing

Generate and verify:
```bash
coldpress generate --intent examples/pytorch_ray_training/intent_kuberay.yaml
grep "num-workers" output/ray-training-job/rayjob.yaml
# Should show: --num-workers=2 (overridden from 1)
```

## Benefits

1. **Readable job-specs**: No bash scripts, directly runnable
2. **Consistent pattern**: Same approach across JobSet, Kubeflow, KubeRay
3. **Type-safe**: Pydantic validation of arg overrides
4. **Flexible**: Supports both simple replacement and positioned insertion
5. **Maintainable**: Clear separation between single-node defaults and multi-node scaling
