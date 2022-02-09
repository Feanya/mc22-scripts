import argparse
import logging
import sys

import analyze_database
import process_lsl


def main(filename: str, type: str, shrink: bool, database_path=""):
    # load lsl to sqlite database and build index
    if type == "lsl-db":
        process_lsl.import_lsl_to_database(filename, database_path, shrink)
        process_lsl.build_indices(database_path)
    # import mdg file to database
    elif type == "mdg-db":
        pass
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
    parser.add_argument('-db', '--database', metavar='path', action='store', help='Filename of database')
    args = parser.parse_args(sys.argv[1:])

    # let's go
    logging.debug(f"Trying to process {args.input}")
    main(args.input, type=args.type, shrink=args.shrink, database_path=args.database)
