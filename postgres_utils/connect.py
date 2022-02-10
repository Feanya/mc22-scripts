import logging
from configparser import ConfigParser

import psycopg2 as pg


def read_config(config_file: str) -> dict[str, str]:
    """Read a config file and return config parameters as dict"""
    section = 'postgresql'
    config_file_path = 'postgres_utils/config/' + config_file
    if len(config_file_path) > 0 and len(section) > 0:
        config_parser = ConfigParser()
        config_parser.read(config_file_path)
        if config_parser.has_section(section):
            # Read options and put them in dict
            config_params = config_parser.items(section)
            db_conn_dict = {}
            for param in config_params:
                db_conn_dict[param[0]] = param[1]
            logging.debug(f"Read database config from: {config_file_path}")
            return db_conn_dict


def get_connection(config_file: str):
    """Get a database connection from specified config file"""
    params = read_config(config_file)
    con = pg.connect(**params)
    logging.debug(f"Connected to database: \"{con.info.dbname}\"")
    return con
