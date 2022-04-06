import logging
import os

import numpy as np
import pandas as pd
from matplotlib import pyplot as plt
from psycopg2._psycopg import connection

prefix = 'aar'


def analyze_data(con: connection):
    logging.getLogger().setLevel(logging.INFO)

    # create folder for result tsvs
    os.makedirs('results/', exist_ok=True)

    # total jars, jars per year
    # get_basic_counts(con)

    # monthly
    for year in range(2015, 2022):
        one_year_per_month(con, year)

    # get types
    # analyze_types(con)
    # analyze_types_by_year(con)

    # get scheme counts
    # analyze_version_schemes_raemakers_sidebyside(con)
    # version_schemes_raemakers(con, '2002-01-01 00:00:00', '2005-01-01 00:00:00', '2002-2004')
    # version_schemes_raemakers(con, '2005-01-01 00:00:00', '2022-01-01 00:00:00', '2005-2021')
    # version_schemes_raemakers(con, '2022-01-01 00:00:00', '2023-01-01 00:00:00', '2022')
    # version_schemes_raemakers(con, '2005-01-01 00:00:00', '2023-01-01 00:00:00', '2002-2022')
    version_scheme_changes(con)

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
    # find_packages_with_most_versions(con, 5)

    # close
    con.close()


def get_basic_counts(con: connection):
    cursor = con.cursor()
    cursor.execute("SELECT MAX(id) FROM data")
    logging.info(f"Evaluating {cursor.fetchone()[0]} jars…")

    # get year counts
    logging.info(f"Evaluating {prefix}s (GAV) by year")
    cursor.execute(
        '''SELECT EXTRACT(YEAR FROM timestamp) AS year, COUNT(*) FROM data 
        GROUP BY year
        ORDER BY year ''')
    df = pd.DataFrame(cursor, columns=['year', 'jars'])
    logging.debug(df)
    df.to_csv(f"results/{prefix}_per_year.tsv", sep='\t')
    df.plot(kind='bar', x='year', y='jars', title='Jars pro Jahr (alle)')
    plt.show()

    # get year counts
    logging.info(f"Evaluating libs (GA) by year")
    cursor.execute(
        '''SELECT year, COUNT(*) FROM (
                         SELECT DISTINCT(groupid, artifactname) AS ga, COUNT(*), EXTRACT(YEAR from timestamp) AS year
                         FROM data
                         GROUP BY year, ga
                     ) AS libs
            GROUP BY year
            ORDER BY year  ''')
    df = pd.DataFrame(cursor, columns=['year', f"{prefix}s"])
    logging.debug(df)
    df.to_tsv(f'results/{prefix}_libs_per_year.tsv', sep='\t')
    df.plot(kind='bar', x='year', y=f"{prefix}s",
            title='Artefakte/Libraries mit min. einer Version pro Jahr ({prefix}s)')
    plt.show()

    # get year counts, only primary artifacts
    logging.info(f"Evaluating {prefix}s by year")
    cursor.execute(
        '''SELECT EXTRACT(YEAR FROM timestamp) AS year, COUNT(*) FROM data 
        WHERE classifier IS NULL
        GROUP BY year
        ORDER BY year ''')
    df = pd.DataFrame(cursor, columns=['year', 'jars'])
    logging.debug(df)
    df.plot(kind='bar', x='year', y='jars', title='Jars pro Jahr (nur primäre)')
    plt.show()


def one_year_per_month(con: connection, year: int):
    cursor = con.cursor()
    # get year counts
    logging.info(f"Evaluating {prefix}s by month for a fixed year")
    cursor.execute(
        '''
         SELECT COUNT(*), EXTRACT(MONTH from timestamp) AS month
         FROM data
         WHERE timestamp BETWEEN %s::timestamp AND %s::timestamp 
         AND versionscheme=2 
         AND version>'1.0.0'
         GROUP BY month
         ORDER BY month''', (f"{year}-01-01 00:00:00", f"{year + 1}-01-01 00:00:00"))
    df = pd.DataFrame(cursor, columns=[f"{prefix}s", 'month'])
    logging.debug(df)
    df.to_csv(f'results/{prefix}s_{year}.tsv', sep='\t')
    title = f'{prefix}-GAV > 1.0.0 nach Monat({year})'
    df.plot(kind='bar', x='month', y=f"{prefix}s", title=title)
    plt.show()

    # get year counts
    logging.info(f"Evaluating libs (GA) by year")
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
    # df.to_csv('results/libs_per_year.tsv', sep='\t')
    title = f'{prefix}-(GA) mit min. einer Version nach Monat ({year})'
    df.plot(kind='bar', x='month', y='libs', title=title)
    plt.show()


def analyze_version_schemes_raemakers_sidebyside(con: connection):
    # total jars per version scheme
    logging.info(f"Evaluating {prefix}s by version scheme")
    cursor = con.cursor()
    cursor.execute('''SELECT versionscheme, COUNT(*) FROM data 
    GROUP BY versionscheme ORDER BY versionscheme''')
    # only 2020
    logging.info(f"Evaluating {prefix}s by version scheme in 2022")
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
    df_year = pd.DataFrame(cursor, columns=['scheme', f"{prefix}s"])
    logging.debug(df_year)
    df_year.plot(kind='bar', x='scheme', y='jars',
                 title=f'Versionsschemata gesamt, {prefix}s', ax=ax1)
    df_2020 = pd.DataFrame(cursor_2020, columns=['scheme', f"{prefix}s"])
    logging.debug(df_2020)
    df_2020.plot(kind='bar', x='scheme', y=f"{prefix}s",
                 title=f"Versionsschemata in 2020, {prefix}s", ax=ax2)
    plt.show()


def version_schemes_raemakers(con: connection, from_date: str, to_date: str, title: str):
    labels = ['M.M', 'M.M.P', '3', 'M.M-p', 'M.M.P-p', 'other']
    # absolut pro Jahr
    logging.info(f"Evaluating {prefix}s by version scheme and year")
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
    plt.title(f'Anzahl {prefix}s nach Versionsschema pro Jahr ({title})')
    plt.show()

    # Anteile pro Jahr als Areaplot
    df_cross_n = pd.crosstab(index=df['year'], columns=df['scheme'], values=df['count'],
                             aggfunc=np.sum, dropna=False, normalize='index')
    df_cross_n.plot.area(stacked=True)
    logging.debug(df_cross_n)
    plt.legend(loc='center left', bbox_to_anchor=(1.0, 0.5), labels=labels)
    plt.title(f'Anteile der Versionsschemata in {prefix}s nach Jahr ({title})')
    plt.show()

    # Anteile pro Jahr als Lineplot
    df_cross_n.plot.line()
    plt.title(f'Anteile der Versionsschemata in {prefix}s nach Jahr ({title})')
    plt.legend(loc='center left', bbox_to_anchor=(1.0, 0.5))
    plt.show()


def analyze_version_schemes_per_artifact(con: connection):
    """ 1. Welche Versionsschema-Kombinationen kommen in einer Library/GA vor?
        2. Was passiert mit den M.M-Libraries?
        3. Wie sieht die Verteilung der Versionsschemata pro Library aus?"""

    # schemes per library
    logging.info(f"Evaluating {prefix}s by type")
    cursor = con.cursor()
    cursor.execute('''SELECT (groupid || artifactname) AS ga, versionscheme, COUNT(*) AS c FROM data 
                        GROUP BY ga, versionscheme
                        ORDER BY c DESC''')
    df = pd.DataFrame(cursor, columns=['type', f"{prefix}s"])
    pass


def version_scheme_changes(con: connection):
    cursor = con.cursor()
    # count the number of libraries that use combinations of version schemes
    cursor.execute('''SELECT COUNT(*) AS c, agg_vs FROM aggregated_ga
                      GROUP BY agg_vs
                      ORDER BY c DESC''')

    # have a look at those using all schemes
    cursor.execute('''SELECT ga FROM aggregated_ga WHERE agg_vs = ARRAY[1,2,3,4,5,6]''')
    df = pd.DataFrame(cursor, columns=['ga'])
    # logging.info(df)

    # have a look at those using just other
    cursor.execute('''SELECT ga FROM aggregated_ga WHERE agg_vs = ARRAY[6]''')
    df = pd.DataFrame(cursor, columns=['ga'])
    logging.info(df)
    pass


def analyze_version_schemes_strict_semver():
    pass


def find_packages_with_most_versions(con: connection, n: int):
    logging.info(f"Looking at the package with the most versions, primary artifact only")
    cursor = con.cursor()
    cursor.execute(
        '''
        SELECT groupid, artifactname, COUNT(*) FROM data 
        WHERE classifier IS NULL
        GROUP BY groupid, artifactname
        ORDER BY COUNT(*)
        DESC
        LIMIT %s
        ''', str(n))
    for row in cursor:
        logging.info(row)
        g = row[0]
        a = row[1]
        logging.info(f"{prefix}, GA with most versions: {g}:{a}")
        sql = '''SELECT version
              FROM data
              WHERE groupid=%s AND artifactname=%s
              GROUP BY version'''
        fetch_cursor = con.cursor()
        fetch_cursor.execute(sql, (g, a))
        for result in fetch_cursor:
            # logging.debug(result)
            pass


def analyze_types(con: connection):
    """Wie viele Artefakte welchen Typs sind in der Datenbank?"""
    logging.info(f"Evaluating {prefix}s by type")
    cursor = con.cursor()
    cursor.execute('''SELECT classifier, COUNT(*) FROM data
                      GROUP BY classifier 
                      ORDER BY COUNT(*) DESC''')
    df_type = pd.DataFrame(cursor, columns=['type', f"{prefix}s"])
    logging.debug(df_type)
    df_type.plot(kind='bar', x='type', y=f"{prefix}s", title="Artefakt-Typen")
    plt.show()


def analyze_types_by_year(con: connection):
    """Wie verteilen sich die Artefakt-Typen auf die Jahre?"""
    # get type counts
    logging.info(f"Evaluating {prefix}s by year and type")
    cursor = con.cursor()
    cursor.execute('''SELECT EXTRACT(YEAR FROM timestamp) AS year,classifier,COUNT(*) FROM data 
                             GROUP BY classifier,year
                             ORDER BY COUNT(*) DESC 
                             LIMIT 5''')
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
