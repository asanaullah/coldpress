<!-- Assisted by: Claude Sonnet 4.5 -->
# TODO

## Known Limitations

### Discovery
- **GPU info not in user snapshot**: The discovery pod may be scheduled on a different GPU than the actual workload, so capturing GPU-specific information (model, memory, utilization) in `discovery/user_snapshot.yaml` would be misleading. Need to implement per-task GPU discovery that runs on the same GPU allocation as the workload.

## Active Work

### Result Directory Management
- [x] **mkdir job**: Create base directory before any tasks run
  - Uses init job in JobSet
  - Ensures directory exists with correct permissions

### Discovery Integration
- [x] **Discovery jobs in JobSet**: Stitch discovery pod templates into JobSet as pre-tasks
  - Output to: `/data/{base_dir}/discovery_{template_name}.json`
  - Run before main workload tasks
  - Use completion blocking

### Directory Structure
- [x] **Base directory format**: `{namespace}/coldpress_results/{job_name}_{uid}_{timestamp}`
  - uid: 8-char hex from job metadata
  - timestamp: YYYYMMDD_HHMMSS format
- [x] **Discovery files in base_dir**: `discovery_{template_name}.json`
- [x] **Job metadata**: `metadata.json` in output directory

## Planned Features

### Core Functionality
- [ ] Support multiple discovery templates in job spec
- [ ] Add service auto-creation from container ports with readinessProbe
- [ ] Implement RoCE NIC allocation and network attachment
- [ ] Support ephemeral volume extraction to PVC

### CLI Improvements
- [ ] Add `--dry-run` flag to show generated YAML without writing files
- [ ] Add `--validate` flag to validate job-spec before generation
- [ ] Add template validation for discovery pod specs
- [ ] Better error messages for missing project configs

### Cluster Config
- [ ] Auto-detect cluster capabilities (storage classes, network plugins)
- [ ] Support for heterogeneous nodes (mixed GPU types)
- [ ] Validation of cluster state before applying config

### Documentation
- [ ] Complete QUICKSTART.md walkthrough with real example
- [ ] Add discovery usage examples
- [ ] Document node allocation algorithm in detail
- [ ] Add troubleshooting guide
- [ ] Document SRIOV/RDMA network setup

### Testing
- [ ] Add tests for discovery template parsing
- [ ] Add tests for discovery pod stitching into JobSet
- [ ] Add integration tests with real cluster
- [ ] Add performance benchmarks for node allocation
- [ ] Test with different Kubernetes distributions (vanilla k8s, OpenShift, etc.)

## Future Enhancements

### Advanced Discovery
- [ ] Per-task GPU discovery (runs on same allocated GPU)
- [ ] RDMA/network topology discovery (privileged)
- [ ] PCIe topology discovery (privileged)
- [ ] Discovery result caching and comparison

### Scheduling
- [ ] Support for GPU topology hints (NVLink, PCIe affinity)
- [ ] Multi-node job allocation
- [ ] Gang scheduling for distributed jobs
- [ ] Preemption and checkpointing support

### Storage
- [ ] Multiple PVC support per task
- [ ] Shared ephemeral storage between tasks
- [ ] Result streaming to object storage (S3, MinIO)
- [ ] Automatic result archival

### Monitoring
- [ ] Prometheus metrics export
- [ ] Grafana dashboard templates
- [ ] Job progress tracking
- [ ] Resource usage analytics

## Bugs

(None reported yet)

## Notes

- Keep v2.0 focused on simplicity and transparency
- All "provenance" is just the generated YAML files in the output directory
- Maintain compatibility with standard Kubernetes tooling
- Discovery output format: `discovery_{template_name}.json`
- Cluster-agnostic: no hardcoded cluster-specific values
