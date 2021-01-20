import os
from typing import Dict
from pathlib import Path
from types import SimpleNamespace

import yaml
from yaml.parser import ParserError


def load_config() -> SimpleNamespace:
    """ Loads and returns CFSI config """

    def dict_to_namespace(d: Dict) -> SimpleNamespace:
        """ Recursively convert a dict into a SimpleNamespace """
        if not isinstance(d, dict):
            raise ValueError("Can only convert dicts to SimpleNamespace")
        for k, v in d.items():
            if isinstance(v, dict):
                d[k] = dict_to_namespace(v)
        return SimpleNamespace(**d)

    try:
        cfsi_base_dir = os.environ["CFSI_BASE_DIR"]
    except KeyError:
        cfsi_base_dir = Path().cwd()
    config_path = Path(cfsi_base_dir / "cfsi-config.yaml")
    config_data = load_config_file(config_path)
    return dict_to_namespace(config_data)


def load_config_file(config_path: Path):
    """ Loads and parses a configuration file from given path """
    with open(config_path) as config_file:
        try:
            config_data = yaml.safe_load(config_file)
        except ParserError:  # TODO: handle errors in cfg
            raise
        return config_data
