import json
import logging
from typing import Dict, Any

JsonDict = Dict[str, Any]


def load_json(filename: str) -> list[JsonDict]:
    """Load a dumped json file"""
    logging.debug(f"Attempting to read data from {filename}")
    with open(filename) as f:
        data = json.load(f)
        logging.debug(f"Loaded {len(data)} entries")
        return data


def save_json(data: list[Dict[str, Any]], filename: str):
    """Save data to a given path"""
    logging.debug(f"Attempting to save data to {filename}")
    with open(filename, "w") as f:
        json.dump(data, f, indent=4)
