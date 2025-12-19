#!/usr/bin/env python3
class DiscoveryParser:
    def __init__(self):
        self.image_map = {
            "network": "quay.io/asanaullah/coldpress-network-discovery:latest"
        }

    def create_params_network(self, config):
        run_params = {
            "label": "network-discovery",
            "image": self.image_map["network"],
            "network_mode": "host",
            "ephemeral_mounts": [{"target": "/tmp/output", "size": "10Mi"}],
            "sys_mounts": [
                {"target": "/sys", "source": "/sys", "type": "bind", "read_only": True}
            ],
            "blocking": {"type": "completion"},
            "files_to_copy": ["network_discovery.yaml"],
            "folders_to_copy": [],
            "log": True,
            "env": [],
            "args": [],
            "command": None,
        }
        return run_params

    def parse(self, discovery_type="network", config={}):
        run_params = getattr(self, f"create_params_{discovery_type}")(config)
        return run_params
