import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    
    """
        - Load the staging tables from S3 files
        
        - Parameters:
            cur : cursor of database connection
            conn : connection of database
    """
    
    for query in copy_table_queries:
        print("Running the Query : \n" , query , "\n")
        cur.execute(query)
        conn.commit()
        


def insert_tables(cur, conn):
    
    """
        - populate tables from staging tables

        - Parameters
            cur : cursor of database connection
            conn : connection of database
    """
    
    for query in insert_table_queries:
        print("Running the Query : \n" , query , "\n")
        cur.execute(query)
        conn.commit()


def main():
    
    """
        - Read config file
        - Load the staging tables from S3 files
        - Load the final tables from staging tables
    """
    
    print("reading config ... \n")
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    
    print("connecting to Redshift host ... \n")
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    print("................ LOADING Staging TABLES ................ \n")
    load_staging_tables(cur, conn)
    print("................ DONE LOADING Staging TABLES ................ \n")
    
    print("................ INSERTING DATA INTO TABLES ................ \n")
    insert_tables(cur, conn)
    print("................ DONE INSERTING DATA INTO TABLESS ................ \n")

    conn.close()


if __name__ == "__main__":
    print("START etl.py ...")
    main()