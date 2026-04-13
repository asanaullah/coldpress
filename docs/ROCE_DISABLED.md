# RoCE NIC Support - Temporarily Disabled

## Overview

RoCE (RDMA over Converged Ethernet) NIC support has been temporarily disabled in Coldpress to simplify cluster configuration. The `roce_nics` field is preserved in cluster config files for reference and potential future re-enablement.

## What Changed

### Disabled Features

1. **ClusterQueue RDMA Resources** - No longer generates resources like:
   - `openshift.io/eno5np0rdma`
   - `openshift.io/eno6np0rdma`
   - etc.

2. **NetworkAttachmentDefinitions** - No longer generates SRIOV network attachments for RDMA

3. **Network Category in Manifests** - The `network` manifest category is no longer used

### What's Preserved

1. **`roce_nics` Field in Config** - The field still exists in:
   - `coldpress_common.model.NodeConfig`
   - `coldpress_common.model.TaskSpec`
   - Cluster YAML config files

2. **Generator Functions** - Functions are preserved but disabled:
   - `generate_sriov_network_attachments()` - Marked as disabled in docstring

## Configuration Files

### Cluster Config (cluster/*.yaml)

You can still include `roce_nics` in node specifications:

```yaml
nodes:
  - hostname: node1
    gpus: 2
    roce_nics: 2  # ← Still accepted, but ignored during manifest generation
  - hostname: node2
    gpus: 4
    roce_nics: 1  # ← Still accepted, but ignored during manifest generation
```

The field is validated and stored but **not used** when generating manifests.

### Generated Resources

**Before (with RoCE enabled):**
```yaml
apiVersion: kueue.x-k8s.io/v1beta1
kind: ClusterQueue
spec:
  resourceGroups:
    - coveredResources:
        - cpu
        - memory
        - nvidia.com/gpu
        - openshift.io/eno5np0rdma  # ← RoCE resources
        - openshift.io/eno6np0rdma  # ← RoCE resources
```

**After (RoCE disabled):**
```yaml
apiVersion: kueue.x-k8s.io/v1beta1
kind: ClusterQueue
spec:
  resourceGroups:
    - coveredResources:
        - cpu
        - memory
        - nvidia.com/gpu  # ← Only GPU resources
```

## Benefits of Disabling

1. **Simpler Setup** - Cluster admins don't need to configure SRIOV or RDMA
2. **Fewer Dependencies** - No need for SR-IOV CNI or NetworkAttachmentDefinition CRDs
3. **Cleaner Manifests** - Fewer resources to review and apply
4. **Easier Debugging** - Less complexity when troubleshooting networking issues

## Re-enabling RoCE Support

If you need RoCE support in the future, you can re-enable it by:

1. Uncommenting the code in `coldpress_setup/generator.py`:
   - Line ~412: `max_roce_nics = max([node.get("roce_nics", 0) for node in nodes], default=0)`
   - Line ~419: `"network": []` in manifests dict
   - Line ~458-461: NetworkAttachmentDefinitions generation
   - Lines in `generate_cluster_queue()`: RoCE resource addition

2. Update `manifests_to_yaml()` to include `"network"` in the category list

3. Remove the "disabled" notes from function docstrings

4. Run tests to verify: `python test_roce_disabled.py`

## Code Locations

**Disabled Code:**
- `coldpress_setup/generator.py:generate_cluster_queue()` - Lines ~35-65
- `coldpress_setup/generator.py:generate_all_manifests()` - Lines ~411-461
- `coldpress_setup/generator.py:generate_sriov_network_attachments()` - Entire function

**Preserved Config:**
- `coldpress_common/model.py:NodeConfig.roce_nics` - Line ~203
- `coldpress_common/model.py:TaskSpec.roce_nics` - Line ~158

## Testing

Run the RoCE disabled test suite:

```bash
python test_roce_disabled.py
```

This verifies:
- ✅ No RoCE resources in ClusterQueue
- ✅ No NetworkAttachmentDefinitions generated
- ✅ `roce_nics` field still validates in config
- ✅ Existing tests still pass

## Related

- [Security](../README.md#security) - Input validation improvements
- [Labels](LABELS.md) - Resource labeling for queries
