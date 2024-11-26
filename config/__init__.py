"""Read and expose keys."""
from config.models import Config
from pathlib import Path
import yaml


config_path = (Path(__file__).parent.parent / "config.yaml").absolute()

if not config_path.exists():
    raise FileNotFoundError("config.yaml file not found.")

with open(str(config_path), "r", encoding="utf-8") as f:
    RAW_CONFIG = yaml.safe_load(f)


# Validate the keys and expose KEYS
CONFIG = Config(**RAW_CONFIG)
