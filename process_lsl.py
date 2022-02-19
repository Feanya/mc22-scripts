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

# logger for import errors
err_logger = logging.getLogger('import_err')
## new handler to only log import errors into error log
err_logger.handlers.clear()
err_logger.propagate = False
error_log_path = 'log/import_errors.log'
err_logger.addHandler(logging.FileHandler(error_log_path, mode='w'))


def import_lsl_to_database(filename: str, con, shrink=False):
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

    logging.info(f"Import data from: {filename}")
    with open(filename) as f:
        reader = csv.DictReader(f, delimiter=' ', fieldnames=['size', 'date', 'time', 'path'],
                                skipinitialspace=True)
        execute_values(cursor,
                       '''INSERT INTO data (groupid, artifactname, path, version, versionscheme, classifier, size, timestamp) 
                       VALUES %s''', process_data(reader, shrink), page_size=500)
    con.commit()
    cursor.execute('''SELECT COUNT(*) FROM data''')
    count = cursor.fetchone()[0]
    logging.info("✅ Done importing %d rows!", count)
    logging.info("%d errors occured, see %s", len(open(error_log_path).readlines()), error_log_path)


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
    logging.debug("Create index on classifier")
    cursor.execute("CREATE INDEX index_classifier ON data(classifier)")
    con.commit()
    logging.info("Indices created 🔧")


def process_data(data: csv.DictReader, shrink=False) -> tuple:
    """Generator function for lazy processing of lsl files.
    :yields one GAV at a time as tuple """
    i = 0
    for line in data:
        try:
            # stitch together timestamp
            isodate = f"{line['date']} {line['time']}"

            # split the path
            result = re.match(r"^(?P<group>.*)/(?P<artifact>[^/]+?)(?:_(?P<artifactsuffix>[\-_\.\d]+))?/"
                              r"(?P<version>[^/]+)/(?P=artifact).?(?P=version)(?:-(?P<classifier>.*?))?\.jar$",
                              line['path'])

            if not result:
                err_logger.error("%s, %s", line['path'], isodate)
                continue

            groupid = result.group("group")
            artifactname = result.group("artifact")
            version = result.group("version")
            classifier = result.group("classifier")

            # drop docs, sources and tests
            if classifier is not None and shrink is True:
                continue

            # determine version scheme
            scheme = determine_versionscheme_raemaekers(version)

            # convert groupid
            groupid_clean = groupid.replace('/', '.')

            i += 1
            if i % 1000000 == 0:
                logging.debug("Lines processed: %d", i)
            entry = (groupid_clean, artifactname, line['path'], version, scheme, classifier, line['size'], isodate)
            yield entry

        except ValueError as err:
            errorcount += 1
            logging.error(f"ValueError {errorcount}: {err}\n {line}")
