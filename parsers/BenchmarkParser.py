#!/usr/bin/env python3

class BenchmarkParser:
    def __init__(self):
        self.image_map = {"guidellm" : "ghcr.io/vllm-project/guidellm:nightly"}


    def create_params_guidellm(self, config):
        port = config.port
        target_nodeip = config.target_nodeip or "127.0.0.1"
        command_args = f"guidellm benchmark --target \"http://{target_nodeip}:{port}\" --output-path \"/tmp/result/benchmarks.json\" --disable-progress"
        for field_name in config.args.__class__.model_fields:
            value = getattr(config.args, field_name)
            command_args += f" --{field_name.replace('_', '-')} {value}"
        run_params = {
            "label": "guidellm-benchmark",
            "image": self.image_map["guidellm"],
            "network_mode": "host",
            "ephemeral_mounts": [{"target": "/tmp/result", "size": "100Mi"}],
            "sys_mounts": [],
            "blocking": {"type": "completion"},
            "files_to_copy": ["benchmarks.json"],
            "folders_to_copy": [],
            "log": config.log,
            "env": [{"name": "HOME", "value": "/tmp"}],
            "args": [command_args],
            "command": ["bash", "-c"]
        }
        return run_params

    def parse(self, benchmark='guidellm', config={}):
        run_params = getattr(self, f"create_params_{benchmark}")(config)
        return run_params