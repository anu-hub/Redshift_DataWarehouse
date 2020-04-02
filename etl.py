import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Function to load staging tables from S3 storage with error handling
    :param cur: connection cursor
    :param conn: database connection handle
    :return: none
    """

    for query in copy_table_queries:
        try:
            cur.execute(query)
            conn.commit()
        except psycopg2.Error as e:
            cur.execute("rollback")
            print("Error: Issue copying data from S3")


def insert_tables(cur, conn):
    """
    Function to load fact & Dim tables from staging tables with error handling
    :param cur: connection cursor
    :param conn: database connection handle
    :return: none
    """
    for query in insert_table_queries:
        try:
            cur.execute(query)
            conn.commit()
        except psycopg2.Error as e:
            cur.execute("rollback")
            print("Error: Issue loading data from staging table")


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    # Setting schema sparkify
    query_setschema = "SET search_path TO sparkify;"
    cur.execute(query_setschema)

    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()