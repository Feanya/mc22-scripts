"""
Tool for parsing the output of rclones (https://github.com/rclone/rclone) `lsl` command.

@date Jan. 2022
"""
import csv
import datetime
import logging
import re
import sqlite3
import sys
from typing import Dict, Any

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
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


def lsl_to_database(filename: str, database_file: str, shrink: bool):
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
    con.commit()
    con.close()


def analyze_data(database_file: str):
    logging.getLogger().setLevel(logging.INFO)
    con = sqlite3.connect(database_file)

    # get rowcount
    cursor = con.execute("SELECT MAX(id) FROM data")
    rowcount: int = cursor.fetchone()[0]

    # get type counts
    logging.info(f"Evaluating {rowcount} jars by type")
    cursor = con.execute("SELECT type,COUNT(*) FROM data GROUP BY type ")
    df_type = pd.DataFrame(cursor, columns=['type', 'jars'])
    logging.debug(df_type)
    df_type.plot(kind='bar', x='type', y='jars',
                 title='Jartypen')
    plt.show()

    # get type counts
    logging.info(f"Evaluating {rowcount} jars by year and type")
    cursor = con.execute('''SELECT SUBSTRING(isodate, 1,4) AS year,type,COUNT(*) FROM data 
                             GROUP BY type,year''')
    df = pd.DataFrame(cursor, columns=['year', 'type', 'count'])
    df_cross = pd.crosstab(index=df['year'], columns=df['type'], values=df['count'], aggfunc=np.sum, dropna=False)
    logging.info(df)
    logging.info(df_cross)
    df_cross.plot.bar(stacked=True)
    plt.show()

    df_cross_n = pd.crosstab(index=df['year'], columns=df['type'], values=df['count'],
                             aggfunc=np.sum, dropna=False, normalize='index')

    df_cross_n.plot.bar(stacked=True)
    logging.info(df_cross_n)
    plt.show()

    sys.exit()
    # logging.info(f"Evaluating {rowcount} jars by year and type")
    # cursor = con.execute('''SELECT SUBSTRING(isodate, 1,4) AS year,type,COUNT(*) FROM data
    #                     WHERE type=='j'
    #                     GROUP BY type,year''')
    # df_j = pd.DataFrame(cursor, columns=['year', 'type', 'count'])
    # logging.info(df_j)
    # cursor = con.execute('''SELECT SUBSTRING(isodate, 1,4) AS year,type,COUNT(*) FROM data
    #                     WHERE type=='t'
    #                     GROUP BY type,year''')
    # df_t = pd.DataFrame(cursor, columns=['year', 'type', 'count'])
    # logging.info(df_t)
    # df_typeyear.plot(kind='bar', x='type', y='jars',
    #                 title='Jartypen')
    # plt.show()

    # Stacked bargraph
    # fig, ax = plt.subplots()
    # ax.bar(df_j['year'], df_j['count'], 0.35, label='J-Typ')
    # ax.bar(df_j['year'], df_t['count'], 0.35, label='T-Typ')

    # ax.set_ylabel('Scores')
    # ax.set_title('Scores by group and gender')
    # ax.legend()

    plt.show()

    # get scheme counts
    logging.info(f"Evaluating {rowcount} jars by version scheme")
    cursor = con.execute("SELECT versionscheme,COUNT(*) FROM data GROUP BY versionscheme ")

    logging.info(f"Evaluating {rowcount} jars by version scheme since 2020")
    cursor_2020 = con.execute(
        '''SELECT versionscheme,COUNT(*), version, isodate FROM data 
        WHERE isodate > 2020 
        GROUP BY versionscheme 
        ''')

    fig, (ax1, ax2) = plt.subplots(1, 2, sharey=True)
    df_year = pd.DataFrame(cursor, columns=['scheme', 'jars'], index=['M.M', 'M.M.P', '3',
                                                                      'M.M-p', 'M.M.P-p', 'other'])
    logging.debug(df_year)
    df_year.plot(kind='bar', x='scheme', y='jars',
                 title='Versionsschemata gesamt', ax=ax1)

    df_2020 = pd.DataFrame(cursor_2020, columns=['scheme', 'jars', '', ''], index=['M.M', 'M.M.P', '3',
                                                                                   'M.M-p', 'M.M.P-p', 'other'])
    logging.debug(df_2020)
    df_2020.plot(kind='bar', x='scheme', y='jars',
                 title='Versionsschemata seit 2020', ax=ax2)
    plt.show()

    fig, (ax1, ax2) = plt.subplots(1, 2)
    df_year.plot(kind='pie', y='jars', title='Versionsschemata gesamt', ax=ax1)
    logging.info(df_year)
    df_2020.plot(kind='pie', y='jars', title='Versionsschemata seit 2020', ax=ax2)
    logging.info(df_2020)
    plt.show()

    # get year counts
    logging.info(f"Evaluating {rowcount} jars by year")
    cursor = con.execute(
        '''SELECT SUBSTRING(isodate, 1,4) AS year, COUNT(*) FROM data 
        GROUP BY year''')
    df = pd.DataFrame(cursor, columns=['year', 'jars'])
    logging.debug(df)
    df.plot(kind='bar', x='year', y='jars', title='Jars pro Jahr')
    plt.show()

    # get examples
    logging.info(f"*** Just printing some examples ***")
    cursor = con.execute(
        '''SELECT * FROM data 
        WHERE id BETWEEN 200 AND 220
        ORDER BY groupid''')
    for row in cursor:
        logging.debug(row)

    # get artifact counts
    logging.info(f"Evaluating {rowcount} jars by artifactname, sorted ***")
    cursor = con.execute(
        '''SELECT *, COUNT(*), (groupid || ':' || artifactname) AS ga FROM data 
        GROUP BY ga, type
        ORDER BY COUNT(*)
        DESC
        LIMIT 10
        ''')
    for row in cursor:
        logging.debug(row)

    # get most versions
    logging.info(f"Looking at the package with the most versions, j-type only")
    cursor = con.execute(
        '''
        SELECT (groupid || ':' || artifactname) AS ga, groupid, artifactname, COUNT(*) FROM data 
        WHERE type == 'j'
        GROUP BY ga, type
        ORDER BY COUNT(*)
        DESC
        LIMIT 3
        ''')
    for row in cursor:
        logging.debug(row)
        g = row[1]
        a = row[2]
        logging.info(f"GA with most versions: {g}:{a}")
        sql = '''SELECT version
              FROM data
              WHERE groupid==(?) AND artifactname==(?)
              GROUP BY version'''
        fetch_cursor = con.execute(sql, (g, a))
        for result in fetch_cursor:
            # logging.debug(result)
            pass
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


def process_data(data: csv.DictReader, shrink=False):
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

            # drop docs, sources and tests
            if jar_type != 'j' and shrink is True:
                continue

            # determine version scheme
            scheme = determine_versionscheme(version)

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
