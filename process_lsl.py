"""
Tool for parsing the output of rclones (https://github.com/rclone/rclone) `lsl` command.

@date Jan. 2022
"""
import csv
import logging

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
          version           varchar,
          versionscheme     integer,
          type              char(1) NOT NULL,
          has_tests         boolean,
          has_javadocs      boolean,
          has_sources       boolean,
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
                       '''INSERT INTO data (groupid, artifactname, version, versionscheme, type, size, timestamp) 
                       VALUES %s''', process_data(reader, False), page_size=500)
    logging.debug("Done importing!")
    con.commit()


def fill_jar_type_flags(con):
    """If there is another artifact with same groupid, artifactname and version, fill info on tests, javadoc etc
    into the j-type row"""
    cursor = con.cursor()

    pass


def fill_previous_versions(con):
    """For each groupid:artifact, order versions and fill the previous_version columns"""
    # todo
    pass


def get_test_tuples() -> tuple:
    return (("groupid", "artifactname", "version", 0, 'a', 42, "2000-01-01 10:00:00.000000000"),
            ("groupid", "artifactname", "version", 0, 'j', 10, "2000-02-01 11:00:30.000000000"))


def build_indices(con):
    """Build indices on the database
    1. groupid
    2. artifactname
    3. timestamp ?"""
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


def process_data(data: csv.DictReader, shrink=False) -> tuple:
    """Generator function for lazy processing of lsl files.
    :yields one GAV at a time as tuple """
    i = 0
    errorcount = 0
    for line in data:
        try:
            # stitch together timestamp
            isodate = f"{line['date']} {line['time']}"

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
