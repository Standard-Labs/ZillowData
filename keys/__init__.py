"""Read and expose keys."""
from keys.models import Keys
from pathlib import Path
import yaml


keys_path = (Path(__file__).parent.parent / "keys.yaml").absolute()

if not keys_path.exists():
    raise FileNotFoundError("keys.yaml file not found.")

with open(str(keys_path), "r", encoding="utf-8") as f:
    RAW_KEYS = yaml.safe_load(f)


# Validate the keys and expose KEYS
KEYS = Keys(**RAW_KEYS)
