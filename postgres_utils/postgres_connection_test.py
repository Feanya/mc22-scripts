"""Try to connect to postgres database"""
import logging
from configparser import ConfigParser

import psycopg2 as pg

logging.basicConfig(format='%(asctime)s %(levelname)s: (%(funcName)s) %(message)s', level=logging.DEBUG)


# logging.debug(f"Arguments: {sys.argv}")

def read_config(filename: str) -> dict[str, str]:
    section = 'postgresql'
    config_file_path = 'config/' + filename
    if len(config_file_path) > 0 and len(section) > 0:
        config_parser = ConfigParser()
        config_parser.read(config_file_path)
        if config_parser.has_section(section):
            # Read options and put them in dict
            config_params = config_parser.items(section)
            db_conn_dict = {}
            for param in config_params:
                db_conn_dict[param[0]] = param[1]
            return db_conn_dict


# connect
params = read_config("postgres.ini")
conn = pg.connect(**params)
logging.debug(conn.info)

# Open a cursor to perform database operations
cur = conn.cursor()

# Execute a command: create a new table and write an entry
cur.execute("CREATE TABLE IF NOT EXISTS test (id serial PRIMARY KEY, num integer, data varchar);")
cur.execute("INSERT INTO test (num, data) VALUES (%s, %s)", (100, "abc'def"))
cur.execute("SELECT * FROM test;")
logging.debug(cur.fetchone())

# Commit
conn.commit()

# Close database connection
cur.close()
conn.close()
