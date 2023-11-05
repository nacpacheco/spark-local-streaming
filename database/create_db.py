import sqlite3
from sqlite3 import Error
from ddls import TABLES_DDLS


def create_connection(db_file):
    """ create a database connection to a SQLite database """
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        print(sqlite3.version)
    except Error as e:
        print(e)
    if conn:
        return conn


def create_table(conn, create_table_sql):
    """ create a table from the create_table_sql statement
    :param conn: Connection object
    :param create_table_sql: a CREATE TABLE statement
    :return:
    """
    try:
        c = conn.cursor()
        c.execute(create_table_sql)
    except Error as e:
        print(e)


def setup_database(location: str) -> None:
    conn = create_connection(location)
    for table in TABLES_DDLS:
        create_table(conn=conn, create_table_sql=table)


if __name__ == '__main__':
    setup_database(r"./orders_database.db")
