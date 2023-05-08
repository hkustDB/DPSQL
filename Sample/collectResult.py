
import os
from pathlib import Path
import subprocess
from tqdm import tqdm


def get_project_root() -> Path:
    return Path(__file__).parent.parent

def main():
    TPCH_list = ["Q1", "Q2", "Q3", "Q4", "Q5", "Q6", "Q7", "Q8_100", "Q11", "Q12", "Q13", "Q17", "Q18", "Q20", "Q21", \
                 "Q31", "Q32", "newQ7", "newQ9", "newQ18"]
    test = ["Q1"]

    root = get_project_root()
    config_file = str(root) + "/config/parameter.config"
    db_file = str(root) + "/config/database.ini"
    for query in TPCH_list:
        program_file = str(root) + "/main.py"
        query_file = str(root) + "/Test/TPCH/" + query + ".txt"
        relation_file = str(root) + "/Test/TPCH/" + query + "_private_relation.txt"
        output_file = str(root) + "/Sample/result/TPCH/" + query + "_out.txt"
        # --d --q --r --c --o --debug
        cmd = "python " + program_file + " --d " + db_file + " --q " + query_file + " --r " + relation_file \
                + " --c " + config_file + " --o " + output_file + " --debug"
        subprocess.run(cmd, shell=True)

        print(query)


if __name__ == "__main__":
    main()