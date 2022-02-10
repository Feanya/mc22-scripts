"""Try to connect to postgres database"""
import logging

from postgres_utils import connect

logging.basicConfig(format='%(asctime)s %(levelname)s: (%(funcName)s) %(message)s', level=logging.DEBUG)

# connect
conn = connect.get_connection()
logging.debug(conn.info)

# Open a cursor to perform database operations
cur = conn.cursor()

# Execute a command: create a new table and write an entry
cur.execute('''CREATE TABLE IF NOT EXISTS test 
               (id serial PRIMARY KEY, 
                num integer, 
                data varchar);''')
cur.execute("INSERT INTO test (num, data) VALUES (%s, %s)", (100, "abc'def"))
cur.execute("SELECT * FROM test;")
logging.debug(cur.fetchall())

# Commit
conn.commit()

# Close database connection
cur.close()
conn.close()
