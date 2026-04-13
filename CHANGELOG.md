# Changelog

All notable changes to Coldpress will be documented in this file.

## [0.2.0] - 2026-04-13

### Changed
- **Version downgrade from 2.0.0 to 0.2.0** - Reset version to reflect early development stage
- **Switched to `uv` package manager** - Faster and more reliable than pip
  - `setup-env.sh` now uses `uv` for virtual environment and package installation
  - Virtual environment renamed from `venv/` to `.venv/`
- **Removed `kubernetes` Python dependency** - Replaced with direct `oc`/`kubectl` subprocess calls
  - Better compatibility across different cluster configurations
  - Applies to `coldpress/allocator.py` and `coldpress_setup/cli.py`
- **Refactored `coldpress-setup` CLI** - Changed from single command to subcommand structure:
  - `coldpress-setup apply <file>` → `coldpress-setup apply cluster|project|user <file>`
  - Auto-resolves config files from standard directories (cluster/, projects/, users/)
  - Commands now work without path prefixes: `coldpress-setup apply cluster ocp-test.yaml`
- **Removed hardcoded `--as system:admin` flag** - Now uses `COLDPRESS_OC_FLAGS` environment variable
  - Example: `export COLDPRESS_OC_FLAGS="--as system:admin"`
  - Provides flexibility for different cluster authentication methods
  - Users without admin privileges get clear error messages

### Added
- **Pydantic validation models** (`coldpress/model.py`) - Config files are now validated at load time
  - Validates config.yaml, job-spec.yaml, and project configs
  - Clear error messages when YAML structure is invalid
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
