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
                    logging.critical(f"ValueError: {err}\n {line}")
            logging.debug(f"Done! Lines processed: {count}")
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


def lsl_to_database(filename: str, database: str):
    con = sqlite3.connect(database)
    con.execute('''DROP TABLE IF EXISTS data''')
    con.execute('''CREATE TABLE IF NOT EXISTS data
         (id    INTEGER  PRIMARY KEY     AUTOINCREMENT,
         groupid        TEXT,
         artifactname   TEXT,
         version        TEXT,
         jarname        TEXT,
         size           INTEGER    NOT NULL,
         timestamp      INTEGER);''')

    cursor = con.cursor()

    with open(filename) as f:
        reader = csv.DictReader(f, delimiter=' ', fieldnames=['size', 'date', 'time', 'path'],
                                skipinitialspace=True)
        for row in reader:
            try:
                # stitch together timestamp
                timestamp: int = int(parser.parse(f"{row['date']} {row['time']}").timestamp())

                # split the path
                groupid, artifactname, version, jarname = row['path'].rsplit('/', 3)

                # convert groupid
                groupid_clean = groupid.replace('/', '.')

                sql = '''INSERT INTO data (groupid, artifactname, version, jarname, size, timestamp)
                 VALUES (?, ?, ?, ?, ?, ?)'''
                cursor.execute(sql, (groupid_clean, artifactname, version, jarname, row['size'], timestamp))
            except ValueError as err:
                logging.critical(f"ValueError: {err}\n {row}")

    logging.debug(f"Inserted until row {cursor.lastrowid}")

    cursor = con.execute("SELECT * FROM data WHERE ID <30 ")
    for row in cursor:
        logging.debug(row)


def strip_dates():
    pass
