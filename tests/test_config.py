import tempfile
from pathlib import Path

import yaml

from dimq.config import load_config


def test_load_config_from_yaml(tmp_path: Path):
    config_data = {
        "endpoint": "tcp://0.0.0.0:9999",
        "heartbeat_interval_seconds": 10,
        "heartbeat_timeout_missed": 5,
        "tasks": [
            {"name": "my_mod:my_func", "max_retries": 2, "timeout_seconds": 15},
        ],
    }
    config_file = tmp_path / "config.yaml"
    config_file.write_text(yaml.dump(config_data))

    cfg = load_config(config_file)
    assert cfg.endpoint == "tcp://0.0.0.0:9999"
    assert cfg.heartbeat_interval_seconds == 10
    assert len(cfg.tasks) == 1
    assert cfg.tasks[0].name == "my_mod:my_func"
    assert cfg.tasks[0].timeout_seconds == 15


def test_load_config_defaults(tmp_path: Path):
    config_file = tmp_path / "config.yaml"
    config_file.write_text(yaml.dump({}))

    cfg = load_config(config_file)
    assert cfg.endpoint == "tcp://0.0.0.0:5555"
    assert cfg.heartbeat_interval_seconds == 5
    assert cfg.tasks == []
