# Query Syntax

Query statements scan the database with related tables, and 
return the private computed result. This document describes the 
syntax for SQL queries accepted in DPSQL.

## SQL syntax

<pre class="lang-sql prettyprint">
<span class="var">query_statement</span>:

<span class="var">select</span>:
    SELECT
        < <a href="#aggegation_function"><span class="var">aggregation function</span></a> >
    < <a href="#from_clause">FROM</a> {tables, ...} >
    { <a href="#join_clause">JOIN</a> < tables > on < bool_expression >}
    [ <a href="#where_clause">WHERE</a> <span class="var">{ bool_expression, ...}</span> ]
    [ <a href="#group_by_clause">GROUP BY</a> { table.field / expression, ...  } ]
</pre>

**Notation rules**
+ Square brackets `[ ]` indicate optional expressions with 0 or 1 appearance.
+ round brackets `( )` indicate optional expressions with 0 or multiple appearances.
+ curly brackets `{ }` indicate required expressions with 1 or multiple appearances.
+ angle brackets `< >` indicate required expression with exact one appearance.


### Aggregation Function
<a id="aggegation_function"></a>

We have three supported aggregation functions in SQL syntax

+ count(*)

  Count aggregation function typically can be used for 
  get the number of record in a table.

  Note that distinct count is not supported in this version.

  Example:
    ```
    Select count(*) from lineitem;
    ```
+ sum(expression)

  Sum aggregation function typically can be used for 
  get the sum of an expression involved some numerical 
  columns in the schema.
  Example:
    ```
    Select sum(l_quantity) from lineitem;
    ```

+ max(expression, index)

  Max aggregation function typically can be used for 
  get the k-th largest value of an expression involved 
  some numerical columns in the schema.

  Value of the index should be an integer, and greater or
  equal to 0. While 0 index is used for minimum value, 
  natural number is used for the k-largest value.
  Example:
    ```
    Select max(l_quantity, 10) from lineitem; \\ get 10-th largest value
    ```
  
    ```
    Select max(l_quantity, 0) from lineitem; \\ get the minimum value
    ```

    ```
    Select max(l_quantity, -10) from lineitem; \\ INVALID
    ```

### From Clause
<a id="from_clause"></a>

After the From keyword, there should be a list of table names separated by comma ```,```.

Example:
```commandline
from lineitem, orders
```

### Join Clause
<a id="join_clause"></a>

Users can choose either explicit join keyword, or
implicit join in the SQL syntax.

We only consider inner equal-join in this version for now.

Example:
```commandline
join orders on o_orderkey = l_orderkey
```

### Where Clause
<a id="where_clause"></a>

Following the Where keyword, there should be a list of 
boolean expressions concatenated by logical operation ```and``` or ```or```.

Boolean expression can be used for two intentions: 
1. Join condition for implicit join .
2. Filter for record.

Example:
```commandline
WHERE lineitem.l_orderkey = orders.o_orderkey
  AND l_shipdate > CAST('1994-01-01' AS date)
```

### Group By Clause
<a id="group_by_clause"></a>

Following the Group By keyword, there should ba a list of table
columns or expressions that can be a grouping attribute seperated by comma ```,```.

Example:
```commandline
group by n_name, extract(year FROM l_shipdate)
```


### Caution

+ All the table column in the input query should not be ambiguous.
+ All the grouping attribute in the group by clause will be considered as public.










