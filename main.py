import argparse
import multiprocessing
import time
from pathlib import Path

from src.util import config, pg_single, pg_test, get_schema
from src.parser import check_type
from pglast import parser, prettify
from pglast import ast
import src.process


def get_project_root() -> Path:
    return Path(__file__).parent

def main():
    argparser = argparse.ArgumentParser(description='sql over DP')
    argparser.add_argument('--db', '--d', type=str, default='./config/database.ini',
                           help='path to database initialization file')
    argparser.add_argument('--query', '--q', type=str, default='./test.txt', help='path to query file')
    argparser.add_argument('--relation', '--r', type=str, help='path to private relation file', default="./test_relation.txt")
    argparser.add_argument('--config', '--c', type=str, help='path to the configuration file', default="./config/parameter.config")
    argparser.add_argument('--output', '--o', type=str, help='path to output file',
                           default="./out.txt")
    argparser.add_argument('--debug', action='store_true', help='debug mode, print more information')
    argparser.add_argument('--optimal', action='store_true', help='optimal mode for SJA')



    opt = argparser.parse_args()
    # load the config file
    dbsetting = config(opt.db)
    global_para = config(opt.config, 'global')
    fast_para = config(opt.config, 'FastSJA')
    multi_para = config(opt.config, 'MultiQ')
    max_para = config(opt.config, 'MaxSJA')

    # load the input query
    query = ""
    query_file = open(opt.query, 'r')
    for line in query_file.readlines():
        query = query + line
        if ";" in query:
            break

    # load the private relation
    relation_file = open(opt.relation, 'r')
    private_relations = ""
    for line in relation_file.readlines():
        private_relations = private_relations + line + ","

    # first parsing for type check
    root = parser.parse_sql(query)
    selectstmt = root[0].stmt
    if not isinstance(selectstmt, ast.SelectStmt):
        raise Exception
    check = check_type(private_relations)
    check(selectstmt)

    filepath = get_project_root()
    output_file = open(opt.output, 'w')
    if opt.debug:
        pg_test(dbsetting)
    # set up misc
    multiprocessing.set_start_method("fork")
    para = dict(global_para)
    # load misc
    pks = pg_single(dbsetting, str(filepath) + "/config/primary_keys.txt")
    fks = pg_single(dbsetting,  str(filepath) + "/config/foreign_keys.txt")
    table_file = open(str(filepath) + "/config/table.txt", 'r')
    q = table_file.read()
    schema = get_schema(dbsetting, q)

    if check.max is not None and check.groupby:
        para.update(multi_para)
        para.update(max_para)
        output_file.write('Query type: MultiMax' + "\n")
        process = src.process.MultiMax(check.l, pks, fks, schema, para, dbsetting)
    elif check.max is not None:
        para.update(max_para)
        # shiftedinverse1
        if check.l == 1:
            output_file.write('Query type: MaxSJA1' +  "\n")
            process = src.process.MaxSJA1(pks, fks, schema, para, dbsetting)
        # shiftedinverse2
        if check.l > 1:
            output_file.write('Query type: MaxSJA2' +  "\n")
            process = src.process.MaxSJA2(pks, fks, schema, para, dbsetting)
    elif check.groupby:
            para.update(multi_para)
            if check.selfjoin:
                # multiSJA
                output_file.write('Query type: multiSJA' + "\n")
                process = src.process.MultiSJA(pks, fks, schema, para, dbsetting)
            else:
                # multiSJF
                output_file.write('Query type: multiSJF' + "\n")
                process = src.process.MultiSJF(pks, fks, schema, para, dbsetting)
    else:
            # R2T

            para.update(fast_para)
            if opt.optimal:
                output_file.write('Query type: OptSJA' + "\n")
                process = src.process.OptSJA(pks, fks, schema, para, dbsetting)
            else:
                output_file.write('Query type: FastSJA' + "\n")
                process = src.process.FastSJA(pks, fks, schema, para, dbsetting)

    start = time.time()

    process.rewrite(query, private_relations)
    process.get_input_result()

    end1 = time.time()

    process.process()

    end2 = time.time()
    if opt.debug:
        output_file.write("original Query:" + "\n")
        output_file.write(prettify(query))
        output_file.write("\n" + "rewritten Query:" + "\n")
        output_file.write(prettify(process.rewrite_query))
        output_file.write("\n" + "true result:")
        output_file.write(str(process.true_result))
        if process.error is not None:
            output_file.write("\n" + "error:")
            output_file.write(str(process.error))

    output_file.write("\n" + "noise result:")
    output_file.write(str(process.noise_result))
    output_file.write("\n" + "rewrite time:")
    output_file.write(str(end1 - start))
    output_file.write("\n" + "process time:")
    output_file.write(str(end2 - end1))


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    main()
