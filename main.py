import argparse
import logging
import sys

import process_lsl
from postgres_utils.connect import get_connection


def main(filename: str, type: str, test=False):
    # load lsl to sqlite database and build index
    if test:
        con = get_connection("postgres_test.ini")
    else:
        con = get_connection("postgres.ini")
    if type == "lsl-db":
        process_lsl.import_lsl_to_database(filename, con)
        process_lsl.build_indices(con)
    # import mdg file to database
    elif type == "mdg-db":
        pass
    # analyze a database table
    elif type == "db":
        pass
    else:
        logging.critical("Please provide correct type of action")
    con.commit()
    con.close()


if __name__ == "__main__":
    # setup logger
    logging.basicConfig(format='%(asctime)s %(levelname)s: (%(funcName)s) %(message)s', level=logging.DEBUG,
                        filemode='w', filename='log/mc22-script.log')
    logging.debug(f"Arguments: {sys.argv}")
    # parse arguments
    parser = argparse.ArgumentParser(description="Process maven jar-artifact information")
    parser.add_argument('input', metavar='input_path', type=str, help='path to input file')
    parser.add_argument('type', type=str, help='mdg or lsl')
    parser.add_argument('--shrink', action='store_true', help='If given, load only primary artifacts')
    parser.add_argument('--test', action='store_true', help='Use test database instead of data22')
    args = parser.parse_args(sys.argv[1:])

    # let's go
    logging.debug(f"Trying to process: {args.input}")
    main(args.input, type=args.type, test=args.test)
