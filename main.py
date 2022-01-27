import argparse
import json
import logging
import sys
from typing import Dict, Any

import process_lsl
import process_mdg

JsonDict = Dict[str, Any]


def load_json(filename: str) -> list[JsonDict]:
    """Load a dumped json file"""
    logging.debug(f"(load_json) Attempting to read data from {filename}")
    with open(filename) as f:
        data = json.load(f)
        logging.debug(f"(load_json) Loaded {len(data)} entries")
        return data


def save_json(data: list[Dict[str, Any]], filename: str):
    """Save data to a given path"""
    logging.debug(f"(save_json) Attempting to save data to {filename}")
    with open(filename, "w") as f:
        json.dump(data, f, indent=4)


def main(filename: str, type: str, plot: bool, process: bool, output_path=""):
    # get a json
    if type == "lsl":
        data = process_lsl.lsl_to_json(filename)
    elif type == "mdg":
        data = process_mdg.mdg_to_json(filename)
    elif type == "json":
        data = load_json(filename)
    else:
        logging.critical("Please provide correct type of input file. Supported: mdg or lsl")
        sys.exit()

    # process the json

    # save the json
    if output_path is not None:
        save_json(data, output_path)


if __name__ == "__main__":
    # setup logger
    logging.basicConfig(format='%(asctime)s %(levelname)s: %(message)s', level=logging.DEBUG)
    logging.debug(f"Arguments: {sys.argv}")
    # parse arguments
    parser = argparse.ArgumentParser(description="Process maven jar-artifact information")
    parser.add_argument('input', metavar='path', type=str, help='path to input file')
    parser.add_argument('type', type=str, help='mdg or lsl')
    parser.add_argument('--process', action='store_true', help='If given, process a file')
    parser.add_argument('--plot', action='store_true', help='If given, plot things')
    parser.add_argument('-o', '--output', metavar='path', action='store', help='Path to save results to')
    args = parser.parse_args(sys.argv[1:])

    # let's go
    logging.debug(f"Trying to process {args.input}")
    main(args.input, type=args.type, plot=args.plot, process=args.process, output_path=args.output)
