
import json
import os
from pathlib import Path

def load_config(path: str = None) -> dict:
    
    config_path = Path(path or os.getenv("CONFIG_PATH", "config.json"))
    with open(config_path, 'r') as f:
        return json.load(f)
