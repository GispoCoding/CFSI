import os
from typing import Dict
from pathlib import Path
from types import SimpleNamespace

import yaml
from yaml.parser import ParserError

from cfsi.utils.logger import create_logger

LOGGER = create_logger()


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
        cfsi_base_dir = Path(os.environ["CFSI_BASE_DIR"])
        if not cfsi_base_dir.exists():
            LOGGER.critical("Directory specified in environment variable"
                            "CFSI_BASE_DIR does not exist")
            raise ValueError("Invalid CFSI_BASE_DIR value")  # TODO: custom exception
    except KeyError:
        LOGGER.warning("Environment variable CFSI_BASE_DIR not set, "
                       f"trying to load configuration from {Path().cwd()}")
        cfsi_base_dir = Path().cwd()  # TODO: remove comment
    config_path = Path(cfsi_base_dir / "cfsi_config.yaml")
    config_data = load_config_file(config_path)
    return dict_to_namespace(config_data)


def load_config_file(config_path: Path):
    """ Loads and parses a configuration file from given path """
    try:
        with open(config_path) as config_file:
            try:
                config_data = yaml.safe_load(config_file)
            except ParserError:  # TODO: handle errors in cfg
                raise
            return config_data

    except FileNotFoundError as err:
        LOGGER.critical("Error loading configuration: " 
                        "CFSI configuration file not found, " 
                        f"ensure configuration file exists: {err}")
        raise
