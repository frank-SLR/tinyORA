# TinyORA

TinyORA is a small database engine. It allows you to manage data using SQL.
> Queries have to use the syntax decribe below.

---
# Table of content

[TOC]

---

> **[]** indicate an optional object
> **...** indicate object can be repeated
> **{}** indicate multiple objects delimited by |. Only one object is used for query
> **[[]]** indicate optional parentheses at the begin and the end of object

# SELECT
SELECT statement is formatted as :

```
select <SEL_COL> [ , <SEL_COL> ...]
from <FROM_OBJ> [ , <FROM_OBJ> ...]
inner join <FROM_OBJ> on <INNER_CLAUSE> [ { AND | OR } INNER_CLAUSE ...]
left outer join <FROM_OBJ> on <INNER_CLAUSE> [ { AND | OR} INNER_CLAUSE ...]
right outer join <FROM_OBJ> on <INNER_CLAUSE> [ { AND | OR} INNER_CLAUSE ...]
where <WHERE_CLAUSE>
group by <GROUP_COL> [ , <GROUP_COL> ...]
order by <SEL_COL> [ , <ORDER_COL> ...]
;
```

Example:

```sql
select e.age, d.dep_name, s.site_name count(*)
from hr.emp e
inner join hr.dep d on e.dep_id=d.dep_id
inner join hr.site s on d.site_id=s.site_id
where s.site_contry='USA'
group by e.age, d.dep_name;
```

See: [SEL_COL](#sel_col), [GROUP_COL](#group_col), [ORDER_COL](#order_col)

## Special SELECT
Generate range of integer with following syntax:

```sql
select level
from dual
connect by level < <INTEGER>;
```
or 
```sql
select level
from dual
connect by level <= <INTEGER>;
```

See: [SEL_COL](#sel_col), [FROM_OBJ](#from_obj), [INNER_CLAUSE](#inner_clause), [WHERE_CLAUSE](#where_clause)

# DESC | DESCRIBE:
Supply table definition.

`desc [ <SCHEMA>. ] <TABLE_NAME>`

# WITH
The WITH clause associates one or more subqueries with the query.

```
with <CURSOR_NAME> as ( <SELECT> ) [, <CURSOR_NAME> as ( <SELECT> ) ]
<SELECT>
```

Example:

```sql
with ds as (
    select d.dep_name, d.dep_id
    from hr.dep d on e.dep_id=d.dep_id
    inner join hr.site s on d.site_id=s.site_id
    where s.site_name='San Francisco'
)
select e.id, e.age, e.first_name, e.last_name, d.dep_name
from hr.emp e
inner join ds on e.dep_id=ds.dep_id
;
```

See: [SELECT](#select)

# GRANT

```
grant { select | insert | update | delete } on <SCHEMA> [ . <TABLE_NAME> ] to <USERNAME> [ with admin option ]
grant { create | drop } user to <USERNAME> [ with admin option ]
grant { create | drop } { table | index } on <SCHEMA> to <USERNAME> [ with admin option ]
```

Example:

```sql
grant select on hr.emp to john_doo with admin option;
```

# REVOKE

```
revoke { select | insert | update | delete } on <SCHEMA> [ . <TABLE_NAME> ] from <USERNAME>
revoke { create | drop } user from <USERNAME>
revoke { create | drop } { table | index } on <SCHEMA> from <USERNAME>
```

Example:

```sql
revoke select on hr.emp from john_doo;
```

# CREATE

```
create table [ <SCHEMA>. ] <TABLE_NAME> ( <COLUMN_NAME> <FORMAT> [ , <COLUMN_NAME> <FORMAT> ...])
create table [ <SCHEMA>. ] <TABLE_NAME> as { <SELECT> | <WITH> }
create user <USERNAME> identified by <PASSWORD>
```

Examples:

```sql
create table emp_with_dep (
    emp_id int,
    emp_age int,
    emp_f_name str,
    emp_l_name str,
    dep_name
)
as
select e.id, e.age, e.first_name, e.last_name, d.dep_name
from hr.emp e
inner join hr.dep d on e.dep_id=d.dep_id
inner join hr.site s on d.site_id=s.site_id
;
```

```sql
create user max_planck identified by mp%1234;
```

# DROP

```
drop table [ <SCHEMA>. ] <TABLE_NAME>
drop user <USERNAME>
```

Example:

```sql
drop table emp_with_dep;
```

# INSERT

```
insert into [ <SCHEMA>. ] <TABLE_NAME> [ ( <COLUMN_NAME> [ , <COLUMN_NAME> ...] ) ] { <WITH> | <SELECT> }
insert into [ <SCHEMA>. ] <TABLE_NAME> [ ( <COLUMN_NAME> [ , <COLUMN_NAME> ...] ) ]values (  <CONSTANT> [ ,  <CONSTANT> ...] )
```

Example:

```sql
insert into emp_with_dep values(12, 55, 'Max', 'Planck', 'Research');
```

# UPDATE

```
update [ <SCHEMA>. ] <TABLE_NAME> set <COLUMN_NAME>=<CONSTANT> [ , <COLUMN_NAME>=<CONSTANT> ...] where <WHERE_CLAUSE>
```

See: [WHERE_CLAUSE](#where_clause)

Example:

```sql
update emp_with_dep set dep_name='Research/Quantum' where emp_id=12;
```

# DELETE

```
delete from [ <SCHEMA>. ] <TABLE_NAME> where <WHERE_CLAUSE>
```

See: [WHERE_CLAUSE](#where_clause)

Example:

```sql
delete from emp_with_dep where emp_id=12;
```

# COMMIT
Validates all previous queries.

# ROLLBACK
Invalidates all previous queries.

# Objects of queries

## SEL_COL
In SELECT statement, the column is identified with:

### Single column with optional alias
```
[[<SCHEMA>.]<TABLE_NAME>.]<COLUMN_NAME> [<COL_ALIAS>]
[<TABLE_ALIAS>.]<COLUMN_NAME> [<COL_ALIAS>]
```
Example:
Select name and age columns from table using his alias:
```sql
select t1.name, t1.age from emp_with_dep t1 where emp_id=12;
```

### All columns from table or cursor
```
[[<SCHEMA>.]<TABLE_NAME>.]*
[<TABLE_ALIAS>.]*
```
Example:
Select all columns from table using his alias:
```sql
select t1.* from emp_with_dep t1 where emp_id=12;
```

Data, can have various formats (string, number,...):
```
<CONSTANT>
```
Example:
Select constant plus name column from table using his alias:
```sql
select 'The name :', t1.name from emp_with_dep t1 where emp_id=12;
```

### Bind variable
Bind variable is replaced in query with his value while parsing.
Bind varaible mus been supplied with the query and has format :
```
:NAME_Of_BIND
```
Example:
Concatenate bind variable VAR1 with column NAME:
```sql
select :VAR1 || t1.name from emp_with_dep t1 where emp_id=12;
```

## GROUP_COL
In GROUP BY statement, the column is identified with:

### Single column or an alias
```
[[<SCHEMA>.]<TABLE_NAME>.]<COLUMN_NAME>
[<TABLE_ALIAS>.]<COLUMN_NAME>
[<COL_ALIAS>]
```
Example:
Select age columns and count lines from table using his alias:
```sql
select t1.age age_grp, count(*) from emp_with_dep t1 group by age_grp;
```


## ORDER_COL
In ORDER BY statement, the column is identified with:

### Single column or an alias
```
[[<SCHEMA>.]<TABLE_NAME>.]<COLUMN_NAME>
[<TABLE_ALIAS>.]<COLUMN_NAME>
[<COL_ALIAS>]
```
Example:
Select age columns and count lines from table using his alias:
```sql
select t1.age age_grp, t1.dep_name from emp_with_dep t1 order by age_grp;
```


## Function to convert one or more columns or function or constant to a specific format
There is a lot of functions for convertion, see [FUNCTION](#function).

For example, convert string in uppercase or lowercase:

```
{ UPPER|LOWER } ( <SEL_COL> )
```
UPPER convert to uppercase the data between parentheses
LOWER convert to lowercase the data between parentheses

Example:
Select name column in uppercase from table using his alias:
```sql
select UPPER(t1.name) from emp_with_dep t1 where emp_id=12;
```

See: [FUNCTION](#function)

## FROM_OBJ
In FROM clause, objects can be tables or sub-queries

```
{ <FROM_TAB> | ( <SELECT> ) <CUR_ALIAS> }
```

If it is a subquery, it must be enclosed by parentheses.

See: [FROM_TAB](#from_tab), [CUR_ALIAS](#cur_alias), [SELECT](#select)

## INNER_CLAUSE
Comparison between columns of different tables used by INNER JOIN.

```
<GENERIC_COL> <WHERE_COMPARE> <GENERIC_COL>
```

See: [GENERIC_COL](#generic_col) [WHERE_COMPARE](#where_compare)

## WHERE_CLAUSE
List of tests between columns, constants and results of functions used to define eligible data for query result.

```
[[ ( ]] { <WHERE_CMP> | <WHERE_BETWEEN> | <WHERE_IN> } [ { and | or } <WHERE_CLAUSE> ] [[ ) ]]
```

See: [WHERE_CMP](#where_cmp), [WHERE_BETWEEN](#where_between), [WHERE_IN](#where_in)

## FROM_TAB
Table used in query.

```
{ [ <SCHEMA>. ] <TABLE_NAME> [ <TAB_ALIAS> ] | <CURSOR_NAME> [ <CUR_ALIAS> ] }
```

## CUR_ALIAS
Defines an alias for a cursor.

## GENERIC_COL
Table's column used by query.

```
[ { [ <SCHEMA>. ] <TABLE_NAME> . | <TAB_ALIAS> . } ] <COLUMN_NAME> [COL_ALIAS]
```

## WHERE_COMPARE
Operators available for the comparison.

```
{ = | > | < | >= | <= | <> | != }
```

## WHERE_CMP
Comparison between two objects.

```
[[ ( ]] <SEL_COL> <WHERE_COMPARE> <SEL_COL> [[ ) ]]
```

See: [SEL_COL](#sel_col), [WHERE_COMPARE](#where_compare)

## WHERE_BETWEEN
Comparison of one object between two other objects.

```
<SEL_COL> BETWEEN <SEL_COL> AND <SEL_COL>
```

See: [SEL_COL](#sel_col)

## WHERE_IN
Comparison of one object between content of list or of sub query result.

```
<SEL_COL> IN ( <CONSTANT>, [ <CONSTANT>, ...] )

or

<SEL_COL> IN ( <SELECT> )
```

See: [SEL_COL](#sel_col)


# FUNCTION
Functions convert supplied data to a specific format

## ABS
Retruns absolute value of parameter.

```sql
ABS ( <NUMERIC_VALUE> )
```

Example:
Obtain absolute value of -12.2:
```sql
ABS(-12.2)
```

## ACOS
return arc cosine of parameter.

```sql
ACOS ( <NUMERIC_VALUE> )
```

Example:
Obtain arc cosine for value 0.3:
```sql
ACOS(0.3)
```

## ASIN
return arcsine of parameter.

```sql
ASIN ( <NUMERIC_VALUE> )
```

Example:
Obtain arcsine for value 0.3:
```sql
ASIN(0.3)
```

## ATAN
return arctangent of parameter.

```sql
ATAN ( <NUMERIC_VALUE> )
```

Example:
Obtain arctangent for value 0.3:
```sql
ATAN(0.3)
```

## ATAN2
return arctangent of parameters.

```sql
ATAN2 ( <NUMERIC_VALUE>, <NUMERIC_VALUE> )
```

Example:
Obtain arctangent for values 0.3 and 0.7:
```sql
ATAN2(0.3, 0.7)
```

## AVG
Return average value for COL.

```sql
AVG ( <COL> )
```

Example:
Obtain average for SALARY:
```sql
AVG(SALARY)
```

## CHR
Converts an ASCII code, which is a number from 0 to 255, to a character.

```sql
CHR ( <ASCII_CODE> )
```

Example:
Obtain character "A":
```sql
CHR(65)
```

## CEIL
Returns the smallest integer that is greater than or equal to parameter.

```sql
CEIL ( <NUMERIC_VALUE> )
```

Example:
Function returns 4:
```sql
CEIL(3.141592)
```

## COS
return cosine of parameter.

```sql
COS ( <NUMERIC_VALUE> )
```

Example:
Obtain cosine for value 0.3:
```sql
COS(0.3)
```

## COSH
return hyperbolic cosine of parameter.

```sql
COSH ( <NUMERIC_VALUE> )
```

Example:
Obtain hyperbolic cosine for value 0.3:
```sql
COSH(0.3)
```

## COUNT
Count rows.

```sql
COUNT ( <COL> )
```

Example:
Count all rows:
```sql
COUNT(*)
```

## DECODE
DECODE compares FIRST_EXPR to each SEARCH value one by one. If FIRST_EXPR is equal to SEARCH, then DECODE returns the corresponding RESULT. If no match is found, then DECODE returns DEFAULT.

```sql
DECODE ( <FIST_EXPR>, <SEARCH>, <RESULT>, [ <SEARCH>, <RESULT>, ... ] <DEFAULT> )
```

Example:
If value is 1 then return 'FIRST', if value is 2 then return 'SECOND', for other values return 'LATE...':
```sql
DECODE(value, 1, 'FIRST', 2, 'SECOND', 'LATE...')
```

## EXP
return e raised to the parameter value. Value of e is  2.718281...

```sql
EXP ( <NUMERIC_VALUE> )
```

Example:
Obtain e^1.253:
```sql
EXP(1.253)
```

## FLOOR
Returns the largest integer equal to or less than parameter.

```sql
FLOOR ( <NUMERIC_VALUE> )
```

Example:
Function returns 9:
```sql
FLOOR(9.815)
```

## INSTR
Return position of SUBSTRING in STRING. Search start at POSITION and stops at OCCURENCE.
POSITION and OCCURENCE parameters are optionnal.

```sql
INSTR ( <SEL_COL>, <SEL_COL> [, <POSITION>, [ <OCCURENCE> ] ] )
```

*SEL_COL*: Input string from witch substring has to been extracted.
*POSITON*: Index of character where search starts. First character has index '1'.
*OCCURENCE*: Occurence of SUBSTRING searched in STRING.

If no data matches, function return 0.

Example:
Result of function is 15:
```sql
INSTR('My tailor is rich', 'i', 7, 2)
```

See: [SEL_COL](#sel_col)


## LN
LN returns the natural logarithm of parameter.

```sql
LN ( <SEL_COL> )
```

Example:
Result of function is 3.1354942159291496908067528318102:
```sql
LN(23)
```

See: [SEL_COL](#sel_col)

## LOG
LN returns the logarithm of parameter 2 in base of paameter 1.

```sql
LOG ( <SEL_COL>, <SEL_COL> )
```

Example:
Result of function is 6:
```sql
LN(2, 64)
```

See: [SEL_COL](#sel_col)

## LOWER
Convert data to lowercase.
Output format is 'str'.

```sql
LOWER ( <SEL_COL> )
```
See: [SEL_COL](#sel_col)

## LPAD
Return first parameter left padded to second parameter lenght. if third parameter is provide, padded part is filled with it.

```sql
LPAD ( <SEL_COL>, <SEL_COL> [, <SEL_COL>] )
```

Example:
Result of function is '---John':
```sql
LPAD('John', 7, '-')
```

See: [SEL_COL](#sel_col)

## LTRIM
LTRIM removes from the left end of first parameter all of the characters contained in second parameter. If second parameter is not provided, it defaults to a single blank.

```sql
LTRIM ( <SEL_COL> [, <SEL_COL>] )
```

Example:
Result of function is 'John':
```sql
LPAD('-------John', '-')
```

See: [SEL_COL](#sel_col)

## MAX
Return maximum value for COL.

```sql
MAX ( <COL> )
```

Example:
Obtain maximum for SALARY:
```sql
MAX(SALARY)
```

## MIN
Return minimum value for COL.

```sql
MIN ( <COL> )
```

Example:
Obtain minimum for BILL:
```sql
MIN(BILL)
```

## MOD
Returns the remainder of parameter 2 divided by parameter 1. Returns parameter 2 if parameter 1 is 0.

```sql
MOD ( <COL>, <COL> )
```

Example:
Obtain the remainder of 15 divided by 6. Result is 3:
```sql
MOD(15, 6)
```

## NVL
NVL returns first parameter if it is not null, else NVL returns second parameter

```sql
NVL ( <SEL_COL>, <SEL_COL> )
```

See: [SEL_COL](#sel_col)

## NVL2
NVL2 returns third parameter if first parameter is not null, else NVL2 returns second parameter

```sql
NVL ( <SEL_COL>, <SEL_COL>, <SEL_COL> )
```

See: [SEL_COL](#sel_col)

## PI
Return the value of pi. Function does not tale parameter.

```sql
PI ()
```

Example:
Result of function is 3,1415926535897932384626433832795:
```sql
PI()
```

## POWER
returns parameter 1 raised to the parameter 2 power.

```sql
POWER ( <NUMERIC_VALUE>, <NUMERIC_VALUE> )
```

Example:
Result of function is 125:
```sql
POWER(5, 3)
```

## RPAD
Return first parameter right padded to second parameter lenght. if third parameter is provide, padded part is filled with it.

```sql
RPAD ( <SEL_COL>, <SEL_COL> [, <SEL_COL>] )
```

Example:
Result of function is 'John---':
```sql
RPAD('John', 7, '-')
```

See: [SEL_COL](#sel_col)

## SIN
return sine of parameter.

```sql
SIN ( <NUMERIC_VALUE> )
```

Example:
Obtain sine for value 0.3:
```sql
SIN(0.3)
```

## SINH
return hyperbolic sine of parameter.

```sql
SINH ( <NUMERIC_VALUE> )
```

Example:
Obtain hyperbolic sine for value 0.3:
```sql
SINH(0.3)
```

## SQRT
Returns the square root of parameter.

```sql
SQRT ( <NUMERIC_VALUE> )
```

Example:
Function returns 7:
```sql
SQRT(49)
```

## SUBSTR
Extract substring of supplied data.
Output format is 'str'.

```sql
SUBSTR ( <SEL_COL>, <FIRST_CHAR_INDEX>, <LENGTH> )
```

*SEL_COL*: Input string from witch substring has to been extracted.
*FIRST_CHAR_INDEX*: Index of first character of the substring. Fist character has index '1'.
*LENGTH*: Length of the substring.

Example:
Extract substring 'CDEF' from string 'ABCDEFGH':
```sql
SUBSTR('ABCDEFGH', 3, 4)
```

See: [SEL_COL](#sel_col)

## SUM
Return sum of values for COL.

```sql
SUM ( <COL> )
```

Example:
Obtain sum for SALARY:
```sql
SUM(SALARY)
```

## TAN
return tangent of parameter.

```sql
TAN ( <NUMERIC_VALUE> )
```

Example:
Obtain tangent for value 0.3:
```sql
TAN(0.3)
```

## TANH
return hyperbolic tangent of parameter.

```sql
TANH ( <NUMERIC_VALUE> )
```

Example:
Obtain hyperbolic tangent for value 0.3:
```sql
TANH(0.3)
```

## TO_CHAR
Convert datetime to string with fixed format.
Output format is 'str'.

```sql
TO_CHAR ( <DATE>, <OUTPUT_FORMAT> )
```
*DATE*: Input date to convert.
*OUTPUT_FORMAT*: Length of the substring.

Format codes are:
|Code|Description|
| --- | :--- |
|YYYY|Year with 4 digits|
|YY|Year with 2 digits|
|MONTH|Full month name|
|MON|Abbreviated name of month|
|MM|Month (01-12; January = 01)|
|DDD|Day of year (1-366)|
|DAY|Name of Day|
|DY|Abbreviated name of day|
|HH24|Hour of day (0-23)|
|HH|Hour of day (1-12)|
|MI|Minute|
|ss|Second|

Example:
convert date to YYYY/MM/DD format:
```sql
TO_CHAR(date, 'YYYY/MM/DD')
```

See: [SEL_COL](#sel_col)

## TRUNC
Returns a value limited to a given number of decimal places

The first parameter is the value to process.
The second parameter is optional and defines the number of decimal places to return. The default value is 0.

```sql
TRUNC ( <NUMERIC_VALUE> )
```
or
```sql
TRUNC ( <NUMERIC_VALUE>, <NUMERIC_VALUE> )
```

Example:
Keep two decimal places for pi
```sql
TRUNC ( 3.141592653589793, 2 )
```

## UPPER
Convert data to uppercase.
Output format is 'str'.

```sql
UPPER ( <SEL_COL> )
```
See: [SEL_COL](#sel_col)
