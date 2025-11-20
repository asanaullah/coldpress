class vLLMParser:
    def __init__(self):
        self.image = "docker.io/vllm/vllm-openai:latest"

    def parse(self, config):
        server_config = config["server_config"]
        model_config = config["model_config"]
        parsed_config = {}
        parsed_config["env"] = server_config.get("framework", {}).get("env", {})
        parsed_config["gpu"] = server_config.get("hardware", {}).get("gpu", 0)
        parsed_config["log"] = server_config.get("log", False)
        parsed_config["args"] = server_config.get("framework", {}).get("args", {})
        parsed_config["args"]["model"] = model_config.get("name", 'ibm-granite/granite-3.3-8b-instruct')
        parsed_config["args"]["max_model_len"] = model_config.get("max_model_len", 10000)
        port = parsed_config["args"].get("port")
        if not port:
            return {}
        endpoint_address = f"http://127.0.0.1:{port}/health"
        env_variables = [{"name": "HOME", "value": "/tmp"}]
        for key, value in parsed_config["env"].items():
            env_variables.append({"name":key, "value": str(value)})
        command_args = "python3 -m vllm.entrypoints.openai.api_server "
        for key, value in parsed_config["args"].items():
            command_args += f" --{key.replace('_', '-')} {value}"
        run_params = {
            "label": "vllm-inference-server",
            "image": self.image,
            "network_mode": "host",
            "ephemeral_mounts": [],
            "sys_mounts": [],
            "blocking": {"type": "endpoint", "address": endpoint_address},
            "files_to_copy": [],
            "folders_to_copy": [],
            "log": parsed_config["log"],
            "env": env_variables,
            "args": [command_args],
            "command": ["bash", "-c"],
            "resources": {
                "limits": {
                    "nvidia.com/gpu": parsed_config["gpu"]
                }
            }
        }
        return run_params
