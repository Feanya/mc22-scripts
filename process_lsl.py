"""
Tool for parsing the output of rclones (https://github.com/rclone/rclone) `lsl` command.

@date Jan. 2022
"""
import csv
import logging
import sqlite3

from dateutil import parser

# Types
from utils import determine_versionscheme_raemaekers


def import_lsl_to_database(filename: str, database_file: str, shrink: bool):
    """Read an lsl file to a given sqlite-database, table 'data'.
    @param shrink: if true, store only j-types, if true store all types of jars (approx. 3x as many)"""
    con = sqlite3.connect(database_file)
    con.execute('''DROP TABLE IF EXISTS data''')
    con.execute('''CREATE TABLE IF NOT EXISTS data
         (id            INTEGER  PRIMARY KEY     AUTOINCREMENT,
         groupid        TEXT,
         artifactname   TEXT,
         version        TEXT,
         versionscheme  INTEGER,
         TYPE           CHAR(1),
         size           INTEGER,
         isodate      TEXT);''')

    cursor = con.cursor()

    with open(filename) as f:
        reader = csv.DictReader(f, delimiter=' ', fieldnames=['size', 'date', 'time', 'path'],
                                skipinitialspace=True)
        sql = '''INSERT INTO data (groupid, artifactname, version, versionscheme, type, size, isodate) 
        VALUES (?,?,?,?,?,?,?)'''
        cursor.executemany(sql, process_data(reader, shrink))
    con.commit()
    con.close()


def build_indices(database_file: str):
    con = sqlite3.connect(database_file)
    con.execute("CREATE INDEX groups ON data(groupid)")
    logging.debug("Created index on groupid")
    con.execute("CREATE INDEX years ON data(SUBSTRING(isodate, 1,4))")
    logging.debug("Created index on years")
    con.commit()
    con.close()


def process_data(data: csv.DictReader, shrink=False) -> tuple:
    """Generator function for lazy processing of lsl files.
    :yields one GAV at a time as tuple """
    i = 0
    errorcount = 0
    for line in data:
        try:
            # stitch together timestamp
            isodate: str = parser.parse(f"{line['date']} {line['time']}").isoformat()

            # split the path
            groupid, artifactname, version, jarname = line['path'].rsplit('/', 3)

            # determine type
            type_slice = jarname[-10:]
            if type_slice == "-tests.jar":
                jar_type = 't'
            elif type_slice == "avadoc.jar":
                jar_type = 'd'
            elif type_slice == "ources.jar":
                jar_type = 's'
            else:
                jar_type = 'j'

            # drop docs, sources and tests
            if jar_type != 'j' and shrink is True:
                continue

            # determine version scheme
            scheme = determine_versionscheme_raemaekers(version)

            # convert groupid
            groupid_clean = groupid.replace('/', '.')

            i += 1
            if i % 1000000 == 0:
                logging.debug(f"Lines processed: {i}")
            entry = (groupid_clean, artifactname, version, scheme, jar_type, line['size'], isodate)
            yield entry
        except ValueError as err:
            errorcount += 1
            logging.error(f"ValueError {errorcount}: {err}\n {line}")
