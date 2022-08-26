import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    
    """
        - Dropping all tables if exists

        - Parameters:
            cur : cursor of database connection
            conn : connection of database
    """
    
    print("Dropping all tables if exists ...")
    
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    
    """
        - Creating staging and final tables

        - Parameters:
            cur : cursor of database connection
            conn : connection of database
    """
    
    print("Creating tables all tables IF NOT EXIST ...")
    
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    
    """
        - Read config file
        - Dropping all tables if exists
        - Creating staging and final tables
    """
    
    
    print("reading config ... \n")
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    
    print("connecting to Redshift host ... \n")
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    
    print("................ DROPPING TABLES ................ \n")
    drop_tables(cur, conn)
    print("................ DONE DROPPING TABLES ................ \n")
    
    
    print("................ CREATING TABLES ................ \n")
    create_tables(cur, conn)
    print("................ DONE CREATING TABLES ................ \n")

    conn.close()


if __name__ == "__main__":
    print("START create_tables.py ...")
    main()