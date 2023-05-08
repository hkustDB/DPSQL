import os
import string

import psycopg2 as psql
from configparser import ConfigParser


# test the connection to pgsql
def pg_test(dbsetting):
    """ Connect to the PostgreSQL database server """
    try:
        # read connection parameters
        params = dbsetting

        print("testing database connection ")

        # connect to the PostgreSQL server
        conn = psql.connect(**params)

        # create a cursor
        cur = conn.cursor()

        # execute a statement
        cur.execute("select 1")
        # get the result
        res = cur.fetchall()


        # close the communication with the PostgreSQL
        cur.close()

        print("connection ok ")
    except (Exception, psql.DatabaseError) as error:
        print(error)

        return False
    finally:
        if conn is not None:
            conn.close()
            return True


# read the configuration file
def config(filename, section='postgresql'):
    # create a parser
    parser = ConfigParser()
    # read config file
    parser.read(filename)

    # get section, default to postgresql
    db = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            db[param[0]] = param[1]
    else:
        raise Exception('Section {0} not found in the {1} file'.format(section, filename))

    return db


def pg_single(dbsetting, file_path):
    try:
        # read connection parameters
        query_file = open(file_path, 'r')
        q = query_file.read()

        params = dbsetting
        con = psql.connect(**params)

        cur = con.cursor()

        # Run the query and get the results
        cur.execute(q)
        res = cur.fetchall()

        con.close()
        return res

    except (Exception, psql.DatabaseError) as error:
        print(error)
    # finally:
    #     if con is not None:


def pg_exec(dbsetting, query):
    try:
        # read connection parameters
        params = dbsetting
        con = psql.connect(**params)

        cur = con.cursor()

        # Run the query and get the results
        cur.execute(query)
        res = cur.fetchall()

        con.close()
        return res

    except (Exception, psql.DatabaseError) as error:
        print(error)


def get_table_attribute(dbsetting, table_name):
    template = "SELECT column_name FROM information_schema.columns WHERE table_name = '" + table_name + "';"

    res = pg_exec(dbsetting, template)
    return [item[0] for item in res]


def get_schema(dbsetting, query):
    schema = {}
    tables = pg_exec(dbsetting, query)
    for t in tables:
        t_attris = get_table_attribute(dbsetting, t[0])
        schema[t[0]] = t_attris
    return schema



if __name__ == '__main__':
    # this test is ok
    dbsetting = config("../config/database.ini")
    query_file = open("../config/table.txt", 'r')
    q = query_file.read()
    print(get_schema(dbsetting, q))