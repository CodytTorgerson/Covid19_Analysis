import sqlite3
from sqlite3 import Error
import os
import db_connector
 
def create_connection(db_file):
    """ create a database connection to a SQLite database """
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        print(sqlite3.version)
    except Error as e:
        print(e)
    finally:
        if conn:
            conn.close()
 



if __name__ == '__main__':
    current_dir = os.getcwd()
    database_dir = os.path.join(current_dir,"db","covid_19.db")
    create_connection(database_dir)