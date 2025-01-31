from pathlib import Path
import os
import yaml

def read_config():
    config_path = os.path.join(
        str(Path(__file__).parents[0]),
        'config', 'default_config.yaml'
    )
    with open(config_path, "r") as conf:
        config = yaml.safe_load(conf)
    return config

if __name__ == '__main__':
    config = read_config()
