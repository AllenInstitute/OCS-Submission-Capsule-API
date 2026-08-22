"""Load the JSONC workflow configuration."""

from __future__ import annotations

import json
import os
import re

CONFIG_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "config.jsonc")


def load_jsonc_config(config_path: str) -> dict:
    """
    Load a JSONC config file into a dictionary.

    Comments are stripped and pipe-delimited organism keys are expanded.

    Parameters:
    config_path: The path to the JSONC config file to load.

    Returns:
    A dictionary containing the parsed configuration with expanded ``references`` keys.
    """
    with open(config_path, "r") as file:
        jsonc_text = file.read()

    json_text = re.sub(r"/\*.*?\*/", "", jsonc_text, flags=re.DOTALL)
    json_text = re.sub(r"^\s*//.*$", "", json_text, flags=re.MULTILINE)
    config = json.loads(json_text)

    config["references"] = {
        organism.strip(): reference
        for organisms, reference in config["references"].items()
        for organism in organisms.split("|")
    }
    return config
