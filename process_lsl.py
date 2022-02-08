"""
Tool for parsing the output of rclones (https://github.com/rclone/rclone) `lsl` command.

@date Jan. 2022
"""
import csv
import datetime
import logging
import sqlite3
from typing import Dict, Any

from dateutil import parser

# Types
from utils import determine_versionscheme_raemaekers

JsonDict = Dict[str, Any]


def lsl_to_json(filename: str) -> list[JsonDict]:
    """Read a lsl-file and convert it to json"""
    result: list = []
    try:
        with open(filename) as lsl_file:
            reader = csv.DictReader(lsl_file, delimiter=' ', fieldnames=['size', 'date', 'time', 'path'],
                                    skipinitialspace=True)

            count = 0
            for line in reader:
                try:
                    # stitch together timestamp
                    timestamp: datetime = parser.parse(f"{line['date']} {line['time']}").isoformat()

                    # split the path
                    groupid, artifactid, version, jarname = line['path'].rsplit('/', 3)

                    # convert groupid
                    groupid_clean = groupid.replace('/', '.')

                    temp_dict = dict(groupid=groupid_clean,
                                     artifactid=artifactid,
                                     version=version,
                                     jarname=jarname,
                                     timestamp=timestamp)
                    result.append(temp_dict)
                    count += 1
                    if count % 1000000 == 0:
                        logging.debug(f"Lines processed: {count}")
                except ValueError as err:
                    logging.error(f"ValueError: {err}\n {line}")
            logging.info(f"Done! Lines processed: {count}")
    except IOError:
        logging.critical(f"File {filename} not readable!")
    return result


def strip_docs_sources_tests(artifact_list: list[JsonDict]) -> list[JsonDict]:
    """Remove entries from a list of artifacts that contain -javadoc, -sources or -tests"""
    logging.debug(f"Try to cleanup a list of {len(artifact_list)} jars")
    result_list: list[JsonDict] = []

    # filter javadocs, sources and tests
    javadoc_count = 0
    sources_count = 0
    tests_count = 0
    for artifact in artifact_list:
        try:
            critical_part = artifact['jarname'][-10:-4]
            if critical_part == "avadoc":
                javadoc_count += 1
            elif critical_part == "ources":
                sources_count += 1
            elif critical_part == "-tests":
                tests_count += 1
            else:
                result_list.append(artifact)
        except ValueError as err:
            logging.critical(f"ValueError: {err}\n {artifact}")
    logging.info(f"Stripped {javadoc_count} javadoc.jars")
    logging.info(f"Stripped {sources_count} sources.jars")
    logging.info(f"Stripped {tests_count} -tests.jars")
    logging.info(f"Remaining jars: {len(result_list)}")
    return result_list


def lsl_to_database(filename: str, database_file: str, shrink: bool):
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
