"""
Tool for parsing the output of rclones (https://github.com/rclone/rclone) `lsl` command.

@date Jan. 2022
"""
import csv
import datetime
import logging
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


def strip_dates():
    pass
