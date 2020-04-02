import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    Drop tables if exists
    :param cur: connection cursor
    :param conn: database connection handle
    :return:    none
    """
    for query in drop_table_queries:
        try:
            cur.execute(query)
            conn.commit()
        except psycopg2.Error as e:
            cur.execute("rollback")
            print("Error: Issue dropping table")


def create_tables(cur, conn):
    """
    create table if not exists
    :param cur: connection cursor
    :param conn: database connection handle
    :return: none
    """
    for query in create_table_queries:
        try:
            cur.execute(query)
            conn.commit()
        except psycopg2.Error as e:
            cur.execute("rollback")
            print("Error: Issue creating table")


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    print(conn)

    #Creating and setting schema sparkify

    query_schema = "CREATE SCHEMA IF NOT EXISTS sparkify;"
    cur.execute(query_schema)
    conn.commit()
    query_setschema = "SET search_path TO sparkify;"
    cur.execute(query_setschema)

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()