# Changelog

All notable changes to Coldpress will be documented in this file.

## [Unreleased]

### Added
- **Per-task hardware discovery** - Discovery now runs as init container for each task instead of a separate job
  - Captures hardware info on the actual node where each task executes (not a different node)
  - Discovery results placed in task-specific directories: `{base_dir}/task-{task_id}/discovery_{template}.json`
  - New discovery config format supports selecting which tasks run discovery:
    - Simple format: `discovery: user_snapshot` (all tasks, backward compatible)
    - Detailed format: `discovery: {template: user_snapshot, tasks: all}` or `tasks: [0, 1]`
  - Solves the dynamic scheduling problem: discovery reflects actual hardware used by each task
  - Mkdir job now creates task subdirectories: `task-0/`, `task-1/`, etc.

### Changed
- **`coldpress-setup` now generates manifests instead of applying directly**
  - Commands now write timestamped manifest files to `manifests/` directory (configurable with `--output-dir`)
  - Admin is responsible for reviewing and applying manifests with `oc apply -f`
  - Manifest filenames include subcommand, config name, and timestamp (e.g., `project-researcher-a-20260413-152928.yaml`)
  - All RBAC permissions and resources are now visible in generated manifests before application
  - Enables GitOps workflows and better audit trails
  - Removed `--dry-run` flag (no longer needed)
  - Removed `--output` flag (replaced with `--output-dir`)
  - Removed `COLDPRESS_OC_FLAGS` environment variable (CLI no longer interacts with cluster)
  - Added `COLDPRESS_MANIFESTS_DIR` environment variable (default: `manifests/`)

- **`coldpress` no longer requires kubectl/oc for manifest generation**
  - Removed automatic cluster-based node allocation
  - Default behavior: Kubernetes scheduler selects any node with `coldpress.node` label
  - Node assignment priority: CLI `--node` flag > `nodes` in config.yaml > Kubernetes scheduler
  - Can specify nodes in config.yaml: `nodes: [0, 1]` or via CLI: `--node 0 --node 1`
  - Generated manifests use `nodeAffinity` (scheduler picks node) or `nodeSelector` (pinned to specific node)
  - Manifest generation now works completely offline without cluster access

### Removed
- **`coldpress-setup verify` command** - Admins can verify resources using standard kubectl/oc commands
  - Use `oc get namespaces`, `oc get pvc -n <namespace>`, etc. instead
  - No longer requires kubectl/oc to be installed for manifest generation

- **`coldpress/allocator.py` module** - Removed automatic node allocation
  - Previous behavior queried cluster to find least-loaded node
  - New behavior: let Kubernetes scheduler handle allocation (simpler, no cluster access needed)
  - Users can still pin tasks to specific nodes with `--node` flag

## [0.2.0] - 2026-04-13

### Changed
- **Version downgrade from 2.0.0 to 0.2.0** - Reset version to reflect early development stage
- **Switched to `uv` package manager**
  - `setup-env.sh` now uses `uv` for virtual environment and package installation
  - Virtual environment renamed from `venv/` to `.venv/`
- **Removed `kubernetes` Python dependency** - Replaced with direct `oc`/`kubectl` subprocess calls
  - Better compatibility across different cluster configurations
  - Applies to `coldpress/allocator.py` and `coldpress_setup/cli.py`
- **Refactored `coldpress-setup` CLI** - Changed from single command to subcommand structure:
  - `coldpress-setup apply <file>` → `coldpress-setup generate cluster|project|user <file>`
  - Auto-resolves config files from standard directories (cluster/, projects/, users/)
  - Commands work with just filename: `coldpress-setup generate cluster ocp-test.yaml`

### Added
- **Shared validation module** (`coldpress_common/model.py`) - Config files are now validated at load time by both tools
  - Validates config.yaml, job-spec.yaml, project configs, and user configs
  - Clear error messages when YAML structure is invalid
  - Both `coldpress` and `coldpress-setup` validate before operations
  - New `UserConfig` model for validating users/*.yaml files
- **`pyproject.toml`** - Modern Python packaging configuration
- **`docs/VALIDATION.md`** - Documentation for config validation
- **`test_validation.py`** - Test script for validating Pydantic models
- **kubectl/oc requirement check** - `setup-env.sh` now verifies kubectl or oc is installed

### Improved
- **`coldpress` CLI refactoring** - Extracted helper functions for better code organization:
  - `_load_and_validate_config()` - Config loading with validation
  - `_load_task_specs()` - Task spec loading with validation
  - `_allocate_nodes_for_tasks()` - Node allocation logic
  - `_prepare_configmap_files()` - ConfigMap preparation
  - `_write_output_files()` - Output file generation
- **`coldpress-setup` CLI refactoring** - Modular functions for each operation:
  - `_get_kubectl_cmd()` - Auto-detect kubectl or oc
  - `_get_kubectl_flags()` - Load flags from environment variable
  - `_apply_yaml_to_cluster()` - Apply YAML via stdin
  - `_verify_project()` - Verify project resources exist
- **`coldpress/generator.py` refactoring** - Extracted helper functions:
  - `_infer_blocking_type_and_health_check()` - Infer task blocking behavior
  - `_substitute_dns_in_args()` - DNS placeholder substitution
  - `_build_init_jobs()` - Build initialization jobs
- **Documentation updates**:
  - `steps.md` - Comprehensive end-to-end validation guide
  - `README.md` - Updated commands to reflect new CLI structure

### Removed
- **`TODO.md`** - Removed temporary task tracking file

### Fixed
- Permission issues when running without admin privileges - Now properly handled via environment variable
- Config file path resolution - Automatically checks standard directories

## [Previous Versions]

See git history for changes prior to 0.2.0
