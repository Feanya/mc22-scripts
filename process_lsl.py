"""
Tool for parsing the output of rclones (https://github.com/rclone/rclone) `lsl` command.

@date Jan. 2022
"""
import csv
import datetime
import logging
import re
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


def lsl_to_database(filename: str, database_file: str):
    con = sqlite3.connect(database_file)
    con.execute('''DROP TABLE IF EXISTS data2''')
    con.execute('''CREATE TABLE IF NOT EXISTS data2
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
        sql = '''INSERT INTO data2 (groupid, artifactname, version, versionscheme, type, size, isodate) VALUES (?,?,?,?,?,?,?)'''
        cursor.executemany(sql, process_data(reader))
    con.commit()

    # get type counts
    cursor = con.execute("SELECT MAX(id) FROM data2")
    rowcount: int = cursor.fetchone()[0]
    logging.info(f"Evaluating {rowcount} jars by type")
    cursor = con.execute("SELECT type,COUNT(*) FROM data2 GROUP BY type ")
    for row in cursor:
        logging.debug(row)

    # get scheme counts
    logging.info(f"Evaluating {rowcount} jars by version scheme")
    cursor = con.execute("SELECT versionscheme,COUNT(*) FROM data2 GROUP BY versionscheme ")
    for row in cursor:
        logging.debug(row)

    # get examples
    logging.info(f"*** Just printing some examples ***")
    cursor = con.execute(
        '''SELECT * FROM data2 
        WHERE id BETWEEN 200 AND 220
        ORDER BY groupid''')
    for row in cursor:
        logging.debug(row)

    # get artifact counts
    logging.info(f"Evaluating {rowcount} jars by artifactname, sorted ***")
    cursor = con.execute(
        '''SELECT *, COUNT(*), (groupid || ':' || artifactname) AS ga FROM data2 
        GROUP BY ga, type
        ORDER BY COUNT(*)
        DESC
        LIMIT 10
        ''')
    for row in cursor:
        logging.debug(row)

    # get most versions
    logging.info(f"Looking at the package with the most versions")
    cursor = con.execute(
        '''
        SELECT (groupid || '/' || artifactname) AS ga, groupid, artifactname, COUNT(*) FROM data2 
        WHERE type == 'j'
        GROUP BY ga, type
        ORDER BY COUNT(*)
        DESC
        LIMIT 1
        ''')
    for row in cursor:
        logging.debug(row)
        g = row[1]
        a = row[2]
        logging.info(f"GA with most versions: {g}:{a}")
        sql = '''SELECT version
              FROM data2
              WHERE groupid==(?) AND artifactname==(?)
              GROUP BY version'''
        fetch_cursor = con.execute(sql, (g, a))
        for result in fetch_cursor:
            logging.debug(result)

    con.close()


def determine_versionscheme(version: str) -> int:
    # match SEMVER
    if re.fullmatch(pattern=
                    "^(0|[1-9]\d*)\.(0|[1-9]\d*)$",
                    string=version) is not None:
        return 1
    if re.fullmatch(pattern=
                    "^(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)$",
                    string=version) is not None:
        return 2
    # todo: #3
    if re.fullmatch(pattern=
                    "^(0|[1-9a-zA-Z]*)\.(0|[1-9a-zA-Z]*)(\.(0|[1-9a-zA-Z]\d*))?$",
                    string=version) is not None:
        return 3
    if re.fullmatch(pattern=
                    "^(0|[1-9]\d*)\.(0|[1-9]\d*)(?:-((?:0|[1-9]\d*|\d*[a-zA-Z-][0-9a-zA-Z-]*)"
                    "(?:\.(?:0|[1-9]\d*|\d*[a-zA-Z-][0-9a-zA-Z-]*))*))?(?:\+([0-9a-zA-Z-]+(?:\.[0-9a-zA-Z-]+)*))?$",
                    string=version) is not None:
        return 4
    if re.fullmatch(pattern=
                    "^(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)(?:-((?:0|[1-9]\d*|\d*[a-zA-Z-][0-9a-zA-Z-]*)"
                    "(?:\.(?:0|[1-9]\d*|\d*[a-zA-Z-][0-9a-zA-Z-]*))*))?(?:\+([0-9a-zA-Z-]+(?:\.[0-9a-zA-Z-]+)*))?$",
                    string=version) is not None:
        return 5
    else:
        return 6


def process_data(data: csv.DictReader):
    """Generator function for lazy processing of lsl files"""
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

            # determine version scheme
            scheme = determine_versionscheme(version)
            if scheme == 3:
                print(line)

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


def strip_dates():
    pass
