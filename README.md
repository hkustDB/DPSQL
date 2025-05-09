# Differential Privacy over SQL

## Table of Contents
* [About the Project](#about-the-project)  
* [Prerequisites](#prerequisites)
    * [Tools](#tools)
    * [Python Dependency](#python-dependency)
    * [Database Permission](#database-permission)
* [system structure](#system-structure)
* [Demo System](#demo-system)
* [Instruction for Collecting Result](#collect-result)
* [Future Plan](#future-plan)

## About The Project
Differential Privacy over SQL (DPSQL) is a system for answering queries over differential privacy.

The file structure is as below
```
project
│   
└───config
└───docs
└───Profile
└───src
│   └───algorithm
└───Test
│   └───TPCH
│   └───Graph
└───Sample
```
`./config` stores the configuration files users need for the system.

`./docs` stores the reference information users need to work with DPSQL:

`./Profile` stores the Profile information for using `mosek` in the system.

`./src` stores main source files.
* `./src/algorithm` stores 3 algorithm we integrated into this system.

`./Test` stores the queries used in the experiments of the system.

`./Sample` stores the script for setting up database and collecting experiment results.


## Prerequisites
### Tools
Before running this project, please install below tools
* [PostgreSQL](https://www.postgresql.org/)
* [Python3](https://www.python.org/download/releases/3.0/)
* [Cplex](https://www.ibm.com/analytics/cplex-optimizer)
* [Mosek](https://www.mosek.com/downloads/) and the licence is under `./Profile`.

Please do not install `Cplex` dependency, which can only handle a small dataset, but download the `Cplex API` and import that to python with this [instruction](https://www.ibm.com/docs/zh/icos/12.9.0?topic=cplex-setting-up-python-api).
(We are aware that this link is expired and are working on a substitute solution.)

### Python Dependency
Here are dependencies used in python programs:
* `matplotlib`
* `numpy`
* `sys`
* `os`
* `collections`
* `configparser`
* `math`
* `psycopg2`
* `pglast`v4.4
* `argparser`

### Database permission
The user should have the permission to read the schema of the database to use this system.

## System structure
TODO

## Demo System

To run the system,  run `main.py`. There are seven parameters
 - `--d`: path to database initialization file;
 - `--q`: path to query file;
 - `--r`: path to private relation file;
 - `--c`: path to the configuration file; 
 - `--o`: path to the output file;
 - `--debug`: debug mode for more information;
 - `--optimal`: choose to use optimal algorithm for SJA queries;

One can use `--h` to get help for parameter instruction.

For more information about input file, users can consult [here](./docs/system-input.md)

For the SQL syntax used in this system, users can consult [here](./docs/query-syntax.md)

Example:
```
python main.py --d ./config/database.ini --q ./test.txt --r ./test_relation.txt --c ./config/parameter.config --o out.txt
```

## collect result

1. install the dependency

2. create an empty database in `PosgreSQL`
3. generate `tbl` data files by using dbgen from [TPCH website](https://www.tpc.org/tpc_documents_current_versions/current_specifications5.asp)
and store them in `/Sample/data/TPCH`
4. run script we provide in `/Sample/setupDBTPCH.py`
``` 
python setupDBTPCH.py --db databasename
```
5. run script we provide in `/Sample/collectResult.py`
```commandline
python collectResult.py
```
6. find the result in `/Sample/result/TPCH`

## Future Plan

- Distinct count queries type (projection);
- User Interface
- Better user experience;
- Optimization;
