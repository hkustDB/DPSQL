# System Input

This document introduce the required inputs you need for using DPSQL:
1. [Database configuration.](#database-configuration)
2. [Parameters for differential privacy mechanism.](#mechanism-parameters)
3. [Input query.](#input-query)
4. [Private relations.](#private-relations)

<a name="database"></a>

## Database Configuration

This is a configuration file where each `[]` stand for one section, and in each section
 there should be 4 paramters: host, database, user, password. Different database engines should 
have differenct sections, and in this version we only support `PosgreSQL`.

The default file location is in `/config/database.ini`. A sample file is provided in [here](../config/database.ini).

<a name="parameters"></a>

## Mechanism Parameters

User should input the following parameter to fully utilized DPSQL:

### Global Section:

1. `epsilon` is a `Float` type for privacy budget. Default will be 1 if omitted.
2. `beta` is a `Float` type for failure probability. Default will be 0.1 if omitted.
3. `processor_num` is an `Int` type for number of process used in DPSQL for parallelism. Default will be 1 if omitted.

### FastSJA section:

1. `global_sensitivity` is an `Int` type for the pre-defined global sensitivity in the schema.
2. `approximate_factor`.

### MultiQ section:

1. `delta` is a `Float` type for differential privacy relaxation.

### MaxSJA Section:

1. `upper_bound` is an `int` type for domain size.
2. `error_level`

The default file location is in `/config/paramrter.config`. A sample file is provided in [here](../config/parameter.config).


<a name="query"></a>

## Input Query

Input query should be a text file with only one query in each file.
Detailed query syntax can be found in [here](query-syntax.md)

Default input location is `test.txt`. A sample input query can be found [here](../test.txt)

<a name="relation"></a>

## Private Relations

Private relation is a text file where there is only one relation name in each line.

Default input location is `test_relation.txt`. A sample input query can be found [here](../test_relation.txt)
