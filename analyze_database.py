import logging
import os
from sqlite3 import Connection

import numpy as np
import pandas as pd
from matplotlib import pyplot as plt
from pandas import DataFrame


def analyze_data(con: Connection):
    logging.getLogger().setLevel(logging.INFO)

    # create folder for result csvs
    os.makedirs('results/', exist_ok=True)

    # total jars, jars per year
    # get_basic_counts(con)
    one_year_per_month(con, 2016)
    one_year_per_month(con, 2017)
    one_year_per_month(con, 2018)
    one_year_per_month(con, 2019)
    one_year_per_month(con, 2020)
    one_year_per_month(con, 2021)
    one_year_per_month(con, 2022)

    # get types
    # analyze_types(con)
    # analyze_types_by_year(con)

    # get scheme counts
    # analyze_version_schemes_raemakers_sidebyside(con)
    # version_schemes_raemakers(con, '2002-01-01 00:00:00', '2005-01-01 00:00:00', '2002-2004')
    # version_schemes_raemakers(con, '2005-01-01 00:00:00', '2022-01-01 00:00:00', '2005-2021')
    # version_schemes_raemakers(con, '2022-01-01 00:00:00', '2023-01-01 00:00:00', '2022')
    # version_schemes_raemakers(con, '2005-01-01 00:00:00', '2023-01-01 00:00:00', '2002-2022')

    # get examples
    logging.info(f"*** Just printing some examples ***")
    cursor = con.cursor()
    cursor.execute(
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
    cursor = con.cursor()
    cursor.execute("SELECT MAX(id) FROM data")
    logging.info(f"Evaluating {cursor.fetchone()[0]} jars…")

    # get year counts
    logging.info(f"Evaluating jars by year")
    cursor.execute(
        '''SELECT EXTRACT(YEAR FROM timestamp) AS year, COUNT(*) FROM data 
        GROUP BY year
        ORDER BY year ''')
    df = pd.DataFrame(cursor, columns=['year', 'jars'])
    logging.debug(df)
    df.to_csv('results/jars_per_year.csv')
    version_schemes_raemakers(con, '2005-01-01 00:00:00', '2023-01-01 00:00:00', '2005-2022')
    df.plot(kind='bar', x='year', y='jars', title='Jars pro Jahr (alle)')
    plt.show()

    # get year counts
    logging.info(f"Evaluating libs by year")
    cursor.execute(
        '''SELECT year, COUNT(*) FROM (
                         SELECT DISTINCT(groupid, artifactname) AS ga, COUNT(*), EXTRACT(YEAR from timestamp) AS year
                         FROM data
                         GROUP BY year, ga
                     )AS libs
            GROUP BY year
            ORDER BY year  ''')
    df = pd.DataFrame(cursor, columns=['year', 'jars'])
    logging.debug(df)
    df.to_csv('results/libs_per_year.csv')
    version_schemes_raemakers(con, '2005-01-01 00:00:00', '2023-01-01 00:00:00', '2005-2022')
    df.plot(kind='bar', x='year', y='jars', title='Artefakte/Libraries mit min. einer Version pro Jahr (alle)')
    plt.show()

    # get year counts, only primary artifacts
    logging.info(f"Evaluating jars by year")
    cursor.execute(
        '''SELECT EXTRACT(YEAR FROM timestamp) AS year, COUNT(*) FROM data 
        WHERE classifier IS NULL
        GROUP BY year
        ORDER BY year ''')
    df = pd.DataFrame(cursor, columns=['year', 'jars'])
    logging.debug(df)
    df.plot(kind='bar', x='year', y='jars', title='Jars pro Jahr (nur primäre)')
    plt.show()


def one_year_per_month(con, year: int):
    cursor = con.cursor()
    # get year counts
    logging.info(f"Evaluating jars by year")
    cursor.execute(
        '''
         SELECT COUNT(*), EXTRACT(MONTH from timestamp) AS month
         FROM data
         WHERE timestamp BETWEEN %s::timestamp AND %s::timestamp 
         AND versionscheme=2 
         AND version>'1.0.0'
         GROUP BY month
         ORDER BY month''', (f"{year}-01-01 00:00:00", f"{year + 1}-01-01 00:00:00"))
    df = pd.DataFrame(cursor, columns=['jars', 'month'])
    logging.debug(df)
    df.to_csv(f'results/jars_{year}.csv')
    title = f'Artefakte/Versionen > 1.0.0 nach Monat({year})'
    df.plot(kind='bar', x='month', y='jars', title=title)
    plt.show()

    # get year counts
    logging.info(f"Evaluating libs by year")
    cursor.execute(
        '''SELECT month, COUNT(*) FROM (
                         SELECT DISTINCT(groupid, artifactname) AS ga, COUNT(*), EXTRACT(MONTH from timestamp) AS month
                         FROM data
                         WHERE timestamp BETWEEN %s::timestamp AND %s::timestamp
                         GROUP BY month, ga
                     )AS libs
        GROUP BY month
        ORDER BY month  ''', (f"{year}-01-01 00:00:00", f"{year + 1}-01-01 00:00:00"))
    df = pd.DataFrame(cursor, columns=['month', 'libs'])
    logging.debug(df)
    # df.to_csv('results/libs_per_year.csv')
    title = f'Artefakte/Libraries mit min. einer Version nach Monat ({year})'
    df.plot(kind='bar', x='month', y='libs', title=title)
    plt.show()


def analyze_version_schemes_raemakers_sidebyside(con: Connection):
    # total jars per version scheme
    logging.info(f"Evaluating jars by version scheme")
    cursor = con.cursor()
    cursor.execute('''SELECT versionscheme, COUNT(*) FROM data 
    GROUP BY versionscheme ORDER BY versionscheme''')
    # only 2020
    logging.info(f"Evaluating jars by version scheme in 2022")
    cursor_2020 = con.cursor()
    cursor_2020.execute(
        '''SELECT versionscheme, COUNT(*) FROM data 
        WHERE timestamp BETWEEN '2020-01-01 00:00:00'::timestamp AND '2020-12-31 23:59:59'::timestamp 
        GROUP BY versionscheme 
        ORDER BY versionscheme 
        ''')
    labels = ['M.M', 'M.M.P', '3', 'M.M-p', 'M.M.P-p', 'other']

    # plot it
    fig, (ax1, ax2) = plt.subplots(1, 2, sharey='all')
    df_year = pd.DataFrame(cursor, columns=['scheme', 'jars'])
    logging.debug(df_year)
    df_year.plot(kind='bar', x='scheme', y='jars',
                 title='Versionsschemata gesamt', ax=ax1)
    df_2020 = pd.DataFrame(cursor_2020, columns=['scheme', 'jars'])
    logging.debug(df_2020)
    df_2020.plot(kind='bar', x='scheme', y='jars',
                 title='Versionsschemata in 2020', ax=ax2)
    plt.show()


def version_schemes_raemakers(con: Connection, from_date: str, to_date: str, title: str):
    labels = ['M.M', 'M.M.P', '3', 'M.M-p', 'M.M.P-p', 'other']
    # absolut pro Jahr
    logging.info(f"Evaluating jars by version scheme and year")
    cursor_cross = con.cursor()
    cursor_cross.execute(
        '''SELECT EXTRACT(YEAR FROM timestamp) AS year, versionscheme, COUNT(*) FROM data
        WHERE timestamp BETWEEN %s::timestamp AND %s::timestamp
        GROUP BY versionscheme, year
        ''', (from_date, to_date))

    df = pd.DataFrame(cursor_cross, columns=['year', 'scheme', 'count'])
    df_cross = pd.crosstab(index=df['year'], columns=df['scheme'], values=df['count'],
                           aggfunc=np.sum, dropna=False)
    # logging.info(df)
    logging.debug(df_cross)
    df_cross.plot.bar(stacked=True)

    plt.legend(labels=labels)
    plt.title(f'Anzahl Jars nach Versionsschema pro Jahr ({title})')
    plt.show()

    # Anteile pro Jahr als Areaplot
    df_cross_n = pd.crosstab(index=df['year'], columns=df['scheme'], values=df['count'],
                             aggfunc=np.sum, dropna=False, normalize='index')
    df_cross_n.plot.area(stacked=True)
    logging.debug(df_cross_n)
    plt.legend(loc='center left', bbox_to_anchor=(1.0, 0.5), labels=labels)
    plt.title(f'Anteile der Versionsschemata nach Jahr ({title})')
    plt.show()
    # Anteile pro Jahr als Lineplot
    df_cross_n.plot.line()
    plt.title(f'Anteile der Versionsschemata nach Jahr ({title})')
    plt.legend(loc='center left', bbox_to_anchor=(1.0, 0.5))
    plt.show()


def analyze_version_schemes_per_artifact(con: Connection):
    """ 1. Welche Versionsschema-Kombinationen kommen in einer Library/GA vor?
        2. Was passiert mit den M.M-Libraries?
        3. Wie sieht die Verteilung der Versionsschemata pro Library aus?"""

    # schemes per library
    logging.info(f"Evaluating jars by type")
    cursor = con.cursor()
    cursor.execute('''SELECT (groupid || artifactname) AS ga, versionscheme, COUNT(*) AS c FROM data 
                        GROUP BY ga, versionscheme
                        ORDER BY c DESC''')
    df = pd.DataFrame(cursor, columns=['type', 'jars'])
    pass


def analyze_version_schemes_strict_semver():
    pass


def find_packages_with_most_versions(con: Connection, n: int):
    logging.info(f"Looking at the package with the most versions, j-type only")
    cursor = con.cursor()
    cursor.execute(
        '''
        SELECT groupid, artifactname, COUNT(*) FROM data 
        WHERE classifier IS NULL
        GROUP BY groupid, artifactname
        ORDER BY COUNT(*)
        DESC
        LIMIT 3
        ''')
    for row in cursor:
        logging.info(row)
        g = row[0]
        a = row[1]
        logging.info(f"GA with most versions: {g}:{a}")
        sql = '''SELECT version
              FROM data
              WHERE groupid=%s AND artifactname=%s
              GROUP BY version'''
        fetch_cursor = con.cursor()
        fetch_cursor.execute(sql, (g, a))
        for result in fetch_cursor:
            # logging.debug(result)
            pass


def analyze_types(con: Connection):
    """Wie viele Jars welchen Typs (Javadoc (d), Sources (d), Tests(t), Paket(j) sind in der Datenbank?"""
    logging.info(f"Evaluating jars by type")
    cursor = con.cursor()
    cursor.execute('''SELECT classifier, COUNT(*) FROM data
                                     GROUP BY classifier ORDER BY COUNT(*) DESC''')
    df_type = pd.DataFrame(cursor, columns=['type', 'jars'])
    logging.debug(df_type)
    df_type.plot(kind='bar', x='type', y='jars', title='Jartypen')
    plt.show()


def analyze_types_by_year(con: Connection):
    """Wie verteilen sich die Jartypen auf die Jahre?"""
    # get type counts
    logging.info(f"Evaluating jars by year and type")
    cursor = con.cursor()
    cursor.execute('''SELECT EXTRACT(YEAR FROM timestamp) AS year,classifier,COUNT(*) FROM data 
                             GROUP BY classifier,year
                             ORDER BY COUNT(*) DESC LIMIT 5''')
    df = pd.DataFrame(cursor, columns=['year', 'type', 'count'])
    logging.info(df)

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


def export_dataframe(df: DataFrame, name: str):
    df.to_csv('results/%s', name)
