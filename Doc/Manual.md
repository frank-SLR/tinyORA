# TinyORA

TinyORA is a small database engine. It allows you to manage data using SQL.
> Queries have to use the syntax decribe below.

---
# Table of content

[SELECT](#select)

[WITH](#with)

[DESC](#desc--describe)

[GRANT](#grant)

[REVOKE](#revoke)

[CREATE](#create)

[DROP](#drop)

[INSERT](#insert)

[UPDATE](#update)

[DELETE](#delete)

[COMMIT](#commit)

[ROLLBACK](#rollback)

[FUNCTION](#function)

---

> **[]** indicate an optional object
> **...** indicate object can be repeated
> **{}** indicate multiple objects delimited by |. Only one object is used for query
> **[[]]** indicate optional parentheses at the begin and the end of object

## SELECT
SELECT statement is formatted as :

```
select <SEL_COL> [ , <SEL_COL> ...]
from <FROM_OBJ> [ , <FROM_OBJ> ...]
inner join <FROM_OBJ> on <INNER_CLAUSE> [ { AND | OR } INNER_CLAUSE ...]
left outer join <FROM_OBJ> on <INNER_CLAUSE> [ { AND | OR} INNER_CLAUSE ...]
right outer join <FROM_OBJ> on <INNER_CLAUSE> [ { AND | OR} INNER_CLAUSE ...]
where <WHERE_CLAUSE>
--group by <SEL_COL> [ , <SEL_COL> ...]
--order by <SEL_COL> [ , <SEL_COL> ...]
;
```

Example:

```sql
select e.id, e.age, e.first_name, e.last_name, d.dep_name
from hr.emp e
inner join hr.dep d on e.dep_id=d.dep_id
inner join hr.site s on d.site_id=s.site_id
where s.site_name='San Francisco';
```

See: [SEL_COL](#sel_col), [FROM_OBJ](#from_obj), [INNER_CLAUSE](#inner_clause), [WHERE_CLAUSE](#where_clause)

## DESC | DESCRIBE:
Supply table definition.

`desc [ <SCHEMA>. ] <TABLE_NAME>`

## WITH
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

## GRANT

```
grant { select | insert | update | delete } on <SCHEMA> [ . <TABLE_NAME> ] to <USERNAME> [ with admin option ]
grant { create | drop } user to <USERNAME> [ with admin option ]
grant { create | drop } { table | index } on <SCHEMA> to <USERNAME> [ with admin option ]
```

Example:

```sql
grant select on hr.emp to john_doo with admin option;
```

## REVOKE

```
revoke { select | insert | update | delete } on <SCHEMA> [ . <TABLE_NAME> ] from <USERNAME>
revoke { create | drop } user from <USERNAME>
revoke { create | drop } { table | index } on <SCHEMA> from <USERNAME>
```

Example:

```sql
revoke select on hr.emp from john_doo;
```

## CREATE

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

## DROP

```
drop table [ <SCHEMA>. ] <TABLE_NAME>
drop user <USERNAME>
```

Example:

```sql
drop table emp_with_dep;
```

## INSERT

```
insert into [ <SCHEMA>. ] <TABLE_NAME> [ ( <COLUMN_NAME> [ , <COLUMN_NAME> ...] ) ] { <WITH> | <SELECT> }
insert into [ <SCHEMA>. ] <TABLE_NAME> [ ( <COLUMN_NAME> [ , <COLUMN_NAME> ...] ) ]values (  <CONSTANT> [ ,  <CONSTANT> ...] )
```

Example:

```sql
insert into emp_with_dep values(12, 55, 'Max', 'Planck', 'Research');
```

## UPDATE

```
update [ <SCHEMA>. ] <TABLE_NAME> set <COLUMN_NAME>=<CONSTANT> [ , <COLUMN_NAME>=<CONSTANT> ...] where <WHERE_CLAUSE>
```

See: [WHERE_CLAUSE](#where_clause)

Example:

```sql
update emp_with_dep set dep_name='Research/Quantum' where emp_id=12;
```

## DELETE

```
delete from [ <SCHEMA>. ] <TABLE_NAME> where <WHERE_CLAUSE>
```

See: [WHERE_CLAUSE](#where_clause)

Example:

```sql
delete from emp_with_dep where emp_id=12;
```

## COMMIT
Validates all previous queries.

## ROLLBACK
Invalidates all previous queries.

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

### Function to convert one or more columns or function or constant to a specific format
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

```
<GENERIC_COL> <WHERE_COMPARE> <GENERIC_COL>
```

See: [GENERIC_COL](#generic_col) [WHERE_COMPARE](#where_compare)

## WHERE_CLAUSE

```
[[ ( ]] { <WHERE_CMP> | <WHERE_BETWEEN> | <WHERE_IN> } [ { and | or } <WHERE_CLAUSE> ] [[ ) ]]
```

See: [WHERE_CMP](#where_cmp), [WHERE_BETWEEN](#where_between), [WHERE_IN](#where_in)

## FROM_TAB

```
{ [ <SCHEMA>. ] <TABLE_NAME> [ <TAB_ALIAS> ] | <CURSOR_NAME> [ <CUR_ALIAS> ] }
```

## CUR_ALIAS
Defines an alias for a cursor.

## GENERIC_COL
Defines a column.

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

Not yet implemented.

## FUNCTION
Functions convert supplied data to a specific format

### UPPER
Convert data to uppercase.
Output format is 'str'.

```sql
UPPER ( <SEL_COL> )
```
See: [SEL_COL](#sel_col)

### LOWER
Convert data to lowercase.
Output format is 'str'.

```sql
LOWER ( <SEL_COL> )
```
See: [SEL_COL](#sel_col)

### SUBSTR
Extract substring of supplied data.
Output format is 'str'.

```sql
SUBSTR ( <SEL_COL>, <FIRST_CHAR_INDEX>, <LENGTH> )
```
<SEL_COL>: Input string from witch substring has to been extracted.
<FIRST_CHAR_INDEX>: Index of first character of the substring. Fist character has index '0'.
<LENGTH>: Length of the substring.

Example:
Extract substring 'DEFG' from string 'ABCDEFGH':
```sql
SUBSTR('ABCDEFGH', 3, 4)
```

### TO_CHAR
Convert datetime to string with fixed format.
Output format is 'str'.

```sql
TO_CHAR ( <DATE>, <OUTPUT_FORMAT> )
```
<DATE>: Input date to convert.
<OUTPUT_FORMAT>: Length of the substring.

Formatcoes are:
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
TO_CHAR(date, 'YYY/MM/DD')
```

See: [SEL_COL](#sel_col)
