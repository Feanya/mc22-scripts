import logging
import sqlite3
from sqlite3 import Connection

import numpy as np
import pandas as pd
from matplotlib import pyplot as plt


def analyze_data(database_file: str):
    logging.getLogger().setLevel(logging.INFO)
    con = sqlite3.connect(database_file)

    # total jars, jars per year
    get_basic_counts(con)

    # get types
    analyze_types(con)
    analyze_types_by_year(con)

    # get scheme counts
    analyze_version_schemes_raemakers(con)

    # get examples
    logging.info(f"*** Just printing some examples ***")
    cursor = con.execute(
        '''SELECT * FROM data 
        WHERE id BETWEEN 200 AND 220
        ORDER BY groupid''')
    for row in cursor:
        logging.debug(row)

    # get most versions
    find_packages_with_most_versions(con, 5)

    # close
    con.close()


def get_basic_counts(con: Connection):
    cursor = con.execute("SELECT MAX(id) FROM data")
    logging.info(f"Evaluating {cursor.fetchone()[0]} jars…")

    # get year counts
    logging.info(f"Evaluating jars by year")
    cursor = con.execute(
        '''SELECT SUBSTRING(isodate, 1,4) AS year, COUNT(*) FROM data 
        GROUP BY year''')
    df = pd.DataFrame(cursor, columns=['year', 'jars'])
    logging.debug(df)
    df.plot(kind='bar', x='year', y='jars', title='Jars pro Jahr')
    plt.show()


def analyze_version_schemes_raemakers(con: Connection):
    # total jars per version scheme
    logging.info(f"Evaluating jars by version scheme")
    cursor = con.execute("SELECT versionscheme,COUNT(*) FROM data GROUP BY versionscheme ")
    # only 2020
    logging.info(f"Evaluating jars by version scheme in 2020")
    cursor_2020 = con.execute(
        '''SELECT versionscheme,COUNT(*),isodate FROM data 
        WHERE isodate BETWEEN 2020 AND 2021 
        GROUP BY versionscheme 
        ''')
    labels = ['M.M', 'M.M.P', '3', 'M.M-p', 'M.M.P-p', 'other']

    # plot it
    fig, (ax1, ax2) = plt.subplots(1, 2, sharey='all')
    df_year = pd.DataFrame(cursor, columns=['scheme', 'jars'])
    logging.debug(df_year)
    df_year.plot(kind='bar', x='scheme', y='jars',
                 title='Versionsschemata gesamt', ax=ax1)
    df_2020 = pd.DataFrame(cursor_2020, columns=['scheme', 'jars', ''])
    logging.info(df_2020)
    df_2020.plot(kind='bar', x='scheme', y='jars',
                 title='Versionsschemata in 2020', ax=ax2)
    plt.show()

    # absolut pro Jahr
    logging.info(f"Evaluating jars by version scheme and year")
    cursor_cross = con.execute(
        '''SELECT SUBSTRING(isodate, 1,4) AS year, versionscheme,COUNT(*) FROM data
        GROUP BY versionscheme, year
        ''')
    df = pd.DataFrame(cursor_cross, columns=['year', 'scheme', 'count'])
    df_cross = pd.crosstab(index=df['year'], columns=df['scheme'], values=df['count'],
                           aggfunc=np.sum, dropna=False)
    # logging.info(df)
    logging.info(df_cross)
    df_cross.plot.bar(stacked=True)
    plt.title('Anzahl Jars nach Versionsschema pro Jahr')
    plt.show()

    # Anteile pro Jahr als Areaplot
    df_cross_n = pd.crosstab(index=df['year'], columns=df['scheme'], values=df['count'],
                             aggfunc=np.sum, dropna=False, normalize='index')
    df_cross_n.plot.area(stacked=True)
    logging.debug(df_cross_n)
    plt.legend(loc='center left', bbox_to_anchor=(1.0, 0.5))
    plt.title('Anteile der Versionsschemata nach Jahr')
    plt.show()

    # Anteile pro Jahr als Lineplot
    df_cross_n.plot.line()
    plt.title('Anteile der Versionsschemata nach Jahr')
    plt.legend(loc='center left', bbox_to_anchor=(1.0, 0.5))
    plt.show()


def analyze_version_schemes_per_artifact(con: Connection):
    """ 1. Welche Versionsschema-Kombinationen kommen in einer Library/GA vor?
        2. Was passiert mit den M.M-Libraries?
        3. Wie sieht die Verteilung der Versionsschemata pro Library aus?"""

    # schemes per library
    logging.info(f"Evaluating jars by type")
    cursor = con.execute('''SELECT (groupid || artifactname) AS ga, versionscheme,COUNT(*) AS c FROM data 
                        GROUP BY ga, versionscheme
                        ORDER BY c DESC''')
    df = pd.DataFrame(cursor, columns=['type', 'jars'])
    pass


def analyze_version_schemes_strict_semver():
    pass


def find_packages_with_most_versions(con: Connection, n: int):
    logging.info(f"Looking at the package with the most versions, j-type only")
    cursor = con.execute(
        '''
        SELECT (groupid || artifactname) AS ga, groupid, artifactname, COUNT(*) FROM data 
        WHERE type == 'j'
        GROUP BY ga
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


def analyze_types(con: Connection):
    """Wie viele Jars welchen Typs (Javadoc (d), Sources (d), Tests(t), Paket(j) sind in der Datenbank?"""
    logging.info(f"Evaluating jars by type")
    cursor = con.execute("SELECT type,COUNT(*) FROM data GROUP BY type ")
    df_type = pd.DataFrame(cursor, columns=['type', 'jars'])
    logging.debug(df_type)
    df_type.plot(kind='bar', x='type', y='jars',
                 title='Jartypen')
    plt.show()


def analyze_types_by_year(con: Connection):
    """Wie verteilen sich die Jartypen auf die Jahre?"""
    # get type counts
    logging.info(f"Evaluating jars by year and type")
    cursor = con.execute('''SELECT SUBSTRING(isodate, 1,4) AS year,type,COUNT(*) FROM data 
                             GROUP BY type,year''')
    df = pd.DataFrame(cursor, columns=['year', 'type', 'count'])
    logging.debug(df)

    # absolut
    df_cross = pd.crosstab(index=df['year'], columns=df['type'], values=df['count'],
                           aggfunc=np.sum, dropna=False)
    logging.debug(df_cross)
    df_cross.plot.bar(stacked=True)
    plt.show()

    # normalized per rows/years
    df_cross_n = pd.crosstab(index=df['year'], columns=df['type'], values=df['count'],
                             aggfunc=np.sum, dropna=False, normalize='index')
    logging.debug(df_cross_n)
    df_cross_n.plot.bar(stacked=True)
    plt.show()
