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
            logging.debug(f"Lines processed: {count}")
    except IOError:
        logging.critical(f"File {filename} not readable!")
    except ValueError as err:
        logging.critical(f"ValueError {err}")
    return result


def strip_docs_sources_tests(data: list[JsonDict]):
    # filter javadocs, sources and tests
    pass


def strip_dates():
    pass
