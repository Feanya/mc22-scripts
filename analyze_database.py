import logging
import sqlite3

import numpy as np
import pandas as pd
from matplotlib import pyplot as plt


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
