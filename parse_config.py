import argparse
import os

from pathlib import Path

import yaml

import models


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("config_dir", type=Path, nargs="?", default="examples")
    return p.parse_args()


def main():
    args = parse_args()
    for dirpath, _, filenames in args.config_dir.walk():
        for fn in filenames:
            if fn == "config.yaml":
                path = dirpath / fn
                print(f"checking file {path}")
                with path.open("r") as fd:
                    cfg = models.ConfigFile.model_validate(yaml.safe_load(fd))
                    for server in cfg.model_server:
                        print(f"  framework: {server.framework.name}")
                        print(
                            f"  hardware: node={server.hardware.node}, gpu={server.hardware.gpu}"
                        )


if __name__ == "__main__":
    main()
