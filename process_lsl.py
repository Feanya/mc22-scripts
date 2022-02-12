"""
Tool for parsing the output of rclones (https://github.com/rclone/rclone) `lsl` command.

@date Jan. 2022
"""
import csv
import logging
import re

from psycopg2.extras import execute_values

# Types
from utils import determine_versionscheme_raemaekers


def import_lsl_to_database(filename: str, con):
    """Read an lsl file to a given sqlite-database, table 'data'.
    :param filename: lsl file to import
    :param con: psycopg2 connection object"""
    cursor = con.cursor()
    logging.debug("Remove old data table…")
    cursor.execute('''DROP TABLE IF EXISTS data''')
    logging.debug("Create new data table…")
    cursor.execute('''CREATE TABLE IF NOT EXISTS data
         (id                serial PRIMARY KEY, 
          groupid           varchar NOT NULL,         
          artifactname      varchar NOT NULL,
          path           varchar,
          version           varchar,
          versionscheme     integer,
          classifier        varchar,
          ref_tests         int REFERENCES data,
          ref_javadocs      int REFERENCES data,
          ref_sources       int REFERENCES data,
          size              integer,
          timestamp         timestamp,
          previous_version_l  int REFERENCES data,
          previous_version_t  int REFERENCES data
        );''')
    con.commit()

    logging.debug(f"Import data from: {filename}")
    with open(filename) as f:
        reader = csv.DictReader(f, delimiter=' ', fieldnames=['size', 'date', 'time', 'path'],
                                skipinitialspace=True)
        execute_values(cursor,
                       '''INSERT INTO data (groupid, artifactname, path, version, versionscheme, classifier, size, timestamp) 
                       VALUES %s''', process_data(reader, False), page_size=500)
    logging.debug("Done importing!")
    con.commit()


def fill_jar_type_flags(con):
    """If there is another artifact with same groupid, artifactname and version, fill info on tests, javadoc etc
    into the j-type row"""
    cursor = con.cursor()
    # todo, wip
    # '''SELECT CONCAT(groupid, artifactname, version) as gav FROM data d
    # JOIN(
    # SELECT CONCAT(groupid, artifactname, version) as gav, COUNT(*) as c
    # FROM data d2
    # GROUP BY gav
    # HAVING c > 1
    # ON d.groupid = d2.groupid) as gc
    # )'''

    pass


def fill_previous_versions(con):
    """For each groupid:artifact, order versions and fill the previous_version columns"""
    # todo
    pass


def get_test_tuples() -> tuple:
    return (("groupid", "artifactname", "version", 0, 'a', 42, "2000-01-01 10:00:00.000000000"),
            ("groupid", "artifactname", "version", 0, 'j', 10, "2000-02-01 11:00:30.000000000"))


def build_indices(con):
    """Build indices on the data table
    1. groupid
    2. artifactname
    3. timestamp"""
    cursor = con.cursor()
    logging.debug("Create index on groupid")
    cursor.execute("CREATE INDEX index_groupid ON data(groupid)")
    con.commit()
    logging.debug("Create index on artifactname")
    cursor.execute("CREATE INDEX index_artifactname ON data(artifactname)")
    con.commit()
    logging.debug("Create index on timestamp")
    cursor.execute("CREATE INDEX index_timestamp ON data(timestamp)")
    con.commit()
    logging.debug("Done!")


def process_data(data: csv.DictReader, shrink=True) -> tuple:
    """Generator function for lazy processing of lsl files.
    :yields one GAV at a time as tuple """
    i = 0
    errorcount = 0
    for line in data:
        try:
            # stitch together timestamp
            isodate = f"{line['date']} {line['time']}"

            # split the path
            result = re.match(r"^(?P<group>.*)/(?P<artifact>[^/]+?)(?:_(?P<artifactsuffix>[\-_\.\d]+))?/"
                              r"(?P<version>[^/]+)/(?P=artifact).?(?P=version)(?:-(?P<classifier>.*?))?\.jar$",
                              line['path'])

            if not result:
                logging.error("Pathname not parseable: %s %s", line['path'], isodate)
                continue

            groupid = result.group("group")
            artifactname = result.group("artifact")
            version = result.group("version")
            classifier = result.group("classifier")

            # drop docs, sources and tests
            # if jar_type != 'j' and shrink is True:
            #    continue

            # determine version scheme
            scheme = determine_versionscheme_raemaekers(version)

            # convert groupid
            groupid_clean = groupid.replace('/', '.')

            i += 1
            if i % 1000000 == 0:
                logging.debug(f"Lines processed: {i}")
            entry = (groupid_clean, artifactname, line['path'], version, scheme, classifier, line['size'], isodate)
            yield entry

        except ValueError as err:
            errorcount += 1
            logging.error(f"ValueError {errorcount}: {err}\n {line}")
