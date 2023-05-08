import argparse
import psycopg2
import os

def Preprocessing():
    global data_path
    global relations
    global primary_ids

    i = 0
    primary_ids = []

    for element in relations:
        input_file_path = data_path + element.lower() + ".tbl"
        output_file_path = data_path + "Temp/" + element.lower() + ".tbl"

        input_file = open(input_file_path, 'r')
        output_file = open(output_file_path, 'w')

        for line in input_file.readlines():
            output_file.write(str(i) + "|" + line[:-2] + "\n")

            primary_ids.append(i)

            i += 1

    output_file_path = data_path + "Temp/ids" + ".tbl"

    output_file = open(output_file_path, 'w')

    for i in primary_ids:
        output_file.write(str(i) + "\n")


def CreateTables():
    global database_name

    con = psycopg2.connect(database=database_name)
    cur = con.cursor()

    code = "CREATE TABLE REGION (R_ID INTEGER NOT NULL, R_REGIONKEY INTEGER NOT NULL, R_NAME CHAR(25) NOT NULL, R_COMMENT VARCHAR(152));"
    cur.execute(code)
    code = "CREATE TABLE NATION (N_ID INTEGER NOT NULL, N_NATIONKEY INTEGER NOT NULL, N_NAME CHAR(25) NOT NULL, N_REGIONKEY INTEGER NOT NULL, N_COMMENT VARCHAR(152));"
    cur.execute(code)
    code = "CREATE TABLE SUPPLIER (S_ID INTEGER NOT NULL, S_SUPPKEY INTEGER NOT NULL, S_NAME CHAR(25) NOT NULL, S_ADDRESS VARCHAR(40) NOT NULL, S_NATIONKEY INTEGER NOT NULL, S_PHONE CHAR(15) NOT NULL, S_ACCTBAL DECIMAL(15,2) NOT NULL, S_COMMENT VARCHAR(101) NOT NULL);"
    cur.execute(code)
    code = "CREATE TABLE CUSTOMER (C_ID INTEGER NOT NULL, C_CUSTKEY INTEGER NOT NULL, C_NAME VARCHAR(25) NOT NULL, C_ADDRESS VARCHAR(40) NOT NULL, C_NATIONKEY INTEGER NOT NULL, C_PHONE CHAR(15) NOT NULL, C_ACCTBAL DECIMAL(15,2) NOT NULL, C_MKTSEGMENT CHAR(10) NOT NULL, C_COMMENT VARCHAR(117) NOT NULL);"
    cur.execute(code)
    code = "CREATE TABLE PART (P_ID INTEGER NOT NULL, P_PARTKEY INTEGER NOT NULL, P_NAME VARCHAR(55) NOT NULL, P_MFGR CHAR(25) NOT NULL, P_BRAND CHAR(10) NOT NULL, P_TYPE VARCHAR(25) NOT NULL, P_SIZE INTEGER NOT NULL, P_CONTAINER CHAR(10) NOT NULL, P_RETAILPRICE DECIMAL(15,2) NOT NULL, P_COMMENT VARCHAR(23) NOT NULL);"
    cur.execute(code)
    code = "CREATE TABLE PARTSUPP (PS_ID INTEGER NOT NULL, PS_PARTKEY INTEGER NOT NULL, PS_SUPPKEY INTEGER NOT NULL, PS_AVAILQTY INTEGER NOT NULL, PS_SUPPLYCOST DECIMAL(15,2) NOT NULL, PS_COMMENT VARCHAR(199) NOT NULL);"
    cur.execute(code)
    code = "CREATE TABLE ORDERS (O_ID INTEGER NOT NULL, O_ORDERKEY INTEGER NOT NULL, O_CUSTKEY INTEGER NOT NULL, O_ORDERSTATUS CHAR(1) NOT NULL, O_TOTALPRICE DECIMAL(15,2) NOT NULL, O_ORDERDATE DATE NOT NULL, O_ORDERPRIORITY CHAR(15) NOT NULL, O_CLERK CHAR(15) NOT NULL, O_SHIPPRIORITY INTEGER NOT NULL, O_COMMENT VARCHAR(79) NOT NULL);"
    cur.execute(code)
    code = "CREATE TABLE LINEITEM (L_ID INTEGER NOT NULL, L_ORDERKEY INTEGER NOT NULL, L_PARTKEY INTEGER NOT NULL, L_SUPPKEY INTEGER NOT NULL, L_LINENUMBER INTEGER NOT NULL, L_QUANTITY DECIMAL(15,2) NOT NULL, L_EXTENDEDPRICE DECIMAL(15,2) NOT NULL, L_DISCOUNT DECIMAL(15,2) NOT NULL, L_TAX DECIMAL(15,2) NOT NULL, L_RETURNFLAG CHAR(1) NOT NULL, L_LINESTATUS CHAR(1) NOT NULL, L_SHIPDATE DATE NOT NULL, L_COMMITDATE DATE NOT NULL, L_RECEIPTDATE DATE NOT NULL, L_SHIPINSTRUCT CHAR(25) NOT NULL, L_SHIPMODE CHAR(10) NOT NULL, L_COMMENT VARCHAR(44) NOT NULL);"
    cur.execute(code)
    code = "CREATE TABLE IDS (I_ID INTEGER NOT NULL);"
    cur.execute(code)

    con.commit()
    con.close()


def CopyTables():
    global database_name
    global dataset
    global data_path
    global relations
    global primary_ids

    con = psycopg2.connect(database=database_name)
    cur = con.cursor()

    for element in relations:
        element_file_path = data_path + "Temp/" + element.lower() + ".tbl"

        element_file = open(element_file_path, 'r')
        cur.copy_from(element_file, element, sep='|')

        os.remove(element_file_path)

    ids_file_path = data_path + "Temp/ids" + ".tbl"

    ids_file = open(ids_file_path, 'r')
    cur.copy_from(ids_file, "IDS", sep='|')

    os.remove(ids_file_path)

    con.commit()
    con.close()


def IndexTables():
    global database_name

    con = psycopg2.connect(database=database_name)
    cur = con.cursor()

    code = "CREATE INDEX R_I on REGION(R_REGIONKEY);"
    cur.execute(code)
    code = "CLUSTER REGION USING R_I;"
    cur.execute(code)
    code = "CREATE INDEX N_I on NATION(N_NATIONKEY);"
    cur.execute(code)
    code = "CLUSTER NATION USING N_I;"
    cur.execute(code)
    code = "CREATE INDEX S_I on SUPPLIER(S_SUPPKEY);"
    cur.execute(code)
    code = "CLUSTER SUPPLIER USING S_I;"
    cur.execute(code)
    code = "CREATE INDEX C_I on CUSTOMER(C_CUSTKEY);"
    cur.execute(code)
    code = "CLUSTER CUSTOMER USING C_I;"
    cur.execute(code)
    code = "CREATE INDEX P_I on PART(P_PARTKEY);"
    cur.execute(code)
    code = "CLUSTER PART USING P_I;"
    cur.execute(code)
    code = "CREATE INDEX PS_I on PARTSUPP(PS_PARTKEY,PS_SUPPKEY);"
    cur.execute(code)
    code = "CLUSTER PARTSUPP USING PS_I;"
    cur.execute(code)
    code = "CREATE INDEX O_I on ORDERS(O_ORDERKEY);"
    cur.execute(code)
    code = "CLUSTER ORDERS USING O_I;"
    cur.execute(code)
    code = "CREATE INDEX L_I on LINEITEM(L_PARTKEY,L_SUPPKEY);"
    cur.execute(code)
    code = "CLUSTER LINEITEM USING L_I;"
    cur.execute(code)

    con.commit()
    con.close()


def AddKeys():
    global database_name

    con = psycopg2.connect(database=database_name)
    cur = con.cursor()

    code = "ALTER TABLE REGION ADD PRIMARY KEY (R_REGIONKEY);"
    cur.execute(code)
    code = "ALTER TABLE NATION ADD PRIMARY KEY (N_NATIONKEY);"
    cur.execute(code)
    code = "ALTER TABLE SUPPLIER ADD PRIMARY KEY (S_SUPPKEY);"
    cur.execute(code)
    code = "ALTER TABLE CUSTOMER ADD PRIMARY KEY (C_CUSTKEY);"
    cur.execute(code)
    code = "ALTER TABLE PART ADD PRIMARY KEY (P_PARTKEY);"
    cur.execute(code)
    code = "ALTER TABLE PARTSUPP ADD PRIMARY KEY (PS_PARTKEY,PS_SUPPKEY);"
    cur.execute(code)
    code = "ALTER TABLE ORDERS ADD PRIMARY KEY (O_ORDERKEY);"
    cur.execute(code)
    code = "ALTER TABLE LINEITEM ADD PRIMARY KEY (L_ORDERKEY,L_LINENUMBER);"
    cur.execute(code)
    code = "COMMIT WORK;"
    cur.execute(code)

    code = "ALTER TABLE NATION ADD FOREIGN KEY (N_REGIONKEY) references REGION;"
    cur.execute(code)
    code = "ALTER TABLE SUPPLIER ADD FOREIGN KEY (S_NATIONKEY) references NATION;"
    cur.execute(code)
    code = "ALTER TABLE CUSTOMER ADD FOREIGN KEY (C_NATIONKEY) references NATION;"
    cur.execute(code)
    code = "ALTER TABLE PARTSUPP ADD FOREIGN KEY (PS_SUPPKEY) references SUPPLIER;"
    cur.execute(code)
    code = "ALTER TABLE PARTSUPP ADD FOREIGN KEY (PS_PARTKEY) references PART;"
    cur.execute(code)
    code = "ALTER TABLE ORDERS ADD FOREIGN KEY (O_CUSTKEY) references CUSTOMER;"
    cur.execute(code)
    code = "ALTER TABLE LINEITEM ADD FOREIGN KEY (L_ORDERKEY) references ORDERS;"
    cur.execute(code)
    code = "ALTER TABLE LINEITEM ADD FOREIGN KEY (L_PARTKEY,L_SUPPKEY) references PARTSUPP;"
    cur.execute(code)
    code = "COMMIT WORK;"
    cur.execute(code)

    con.commit()
    con.close()


def DropTables():
    global database_name
    global relations

    con = psycopg2.connect(database=database_name)
    cur = con.cursor()

    for element in reversed(relations):
        code = "DROP TABLE " + element + ";"
        cur.execute(code)

    code = "DROP TABLE IDS;"
    cur.execute(code)

    con.commit()
    con.close()


# TODO: revise
def main():
    global database_name
    global data_path
    global relations

    argparser = argparse.ArgumentParser(description='DB set up')
    argparser.add_argument('--db', '--d', type=str, help='database name')
    argparser.add_argument('--model', '--m', type=int, choices=[0, 1], default=0, help='model 0 for import, 1 for clean')

    opt = argparser.parse_args()
    database_name = opt.db
    model = opt.model

    if model != 0:
        relations = ["REGION", "NATION", "SUPPLIER", "CUSTOMER", "PART", "PARTSUPP", "ORDERS", "LINEITEM"]
        DropTables()
    else:

        cur_path = os.getcwd()
        data_path = cur_path + "/Data/TPCH/"

        relations = ["REGION", "NATION", "SUPPLIER", "CUSTOMER", "PART", "PARTSUPP", "ORDERS", "LINEITEM"]

        Preprocessing()
        CreateTables()
        CopyTables()
        IndexTables()
        AddKeys()


if __name__ == "__main__":
    main()