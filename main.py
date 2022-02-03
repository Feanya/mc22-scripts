import argparse
import logging
import sys

import analyze_database
import jsonutils
import process_lsl
import process_mdg


def main(filename: str, type: str, shrink: bool, strip: bool,
         database_path="", output_path=""):
    # get a json from lsl file
    if type == "lsl":
        data = process_lsl.lsl_to_json(filename)
        # process the json
        if strip:
            data = process_lsl.strip_docs_sources_tests(data)
        # save the json
        if output_path is not None:
            jsonutils.save_json(data, output_path)
    # load lsl to sqlite database and build index
    elif type == "lsl-db":
        process_lsl.lsl_to_database(filename, database_path, shrink)
        process_lsl.build_indices(database_path)
    # get a json from mdg file
    elif type == "mdg":
        data = process_mdg.mdg_to_json(filename)
    # load a previously stored json file
    elif type == "json":
        data = jsonutils.load_json(filename)
    # analyze a database table
    elif type == "db":
        analyze_database.analyze_data(database_path)
    else:
        logging.critical("Please provide correct type of action")
        sys.exit()


if __name__ == "__main__":
    # setup logger
    logging.basicConfig(format='%(asctime)s %(levelname)s: (%(funcName)s) %(message)s', level=logging.DEBUG)
    logging.debug(f"Arguments: {sys.argv}")
    # parse arguments
    parser = argparse.ArgumentParser(description="Process maven jar-artifact information")
    parser.add_argument('input', metavar='path', type=str, help='path to input file')
    parser.add_argument('type', type=str, help='mdg or lsl')
    parser.add_argument('--process', action='store_true', help='If given, process a file')
    parser.add_argument('--shrink', action='store_true', help='If given, load only j-type and drop all others')
    parser.add_argument('--strip', action='store_true', help='If given, cleanup lsl file')
    parser.add_argument('-db', '--database', metavar='path', action='store', help='Filename of database')
    parser.add_argument('-o', '--output', metavar='path', action='store', help='Path to save results to')
    args = parser.parse_args(sys.argv[1:])

    # let's go
    logging.debug(f"Trying to process {args.input}")
    main(args.input, type=args.type, shrink=args.shrink, strip=args.strip,
         database_path=args.database, output_path=args.output)
