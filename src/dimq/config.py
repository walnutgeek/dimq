from pathlib import Path

import yaml

from dimq.models import DimqConfig


def load_config(path: Path) -> DimqConfig:
    with open(path) as f:
        data = yaml.safe_load(f) or {}
    return DimqConfig(**data)
