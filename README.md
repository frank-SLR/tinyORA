## tinyDB

TinyDB is a small database engine. It allows you to manage data using SQL.
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
[COMMIT](#commit)
[ROLLBACK](#rollback)

---

> [] indicate an optional object
> ... indicate object can be repeated
> {} indicate multiple objects delimited by |. Only one object is used for query
> [[]] indicate optional parentheses at the begin and the end of object

## SELECT
SELECT statement is formatted as :

`select <SEL_COL> [ , <SEL_COL> ...]`
`from <FROM_OBJ> [ , <FROM_OBJ> ...]`
`inner join <FROM_OBJ> on <INNER_CLAUSE> [ { AND | OR } INNER_CLAUSE ...]`
`where <WHERE_CLAUSE>;`

See: [SEL_COL](#sel_col), [FROM_OBJ](#from_obj), [INNER_CLAUSE](#inner_clause), [WHERE_CLAUSE](#where_clause)

## DESC | DESCRIBE:
Supply table definition.

`desc [ <SCHEMA>. ] <TABLE_NAME>`

## WITH
The WITH clause associates one or more subqueries with the query.

`with <CURSOR_NAME> as ( <SELECT> ) [, <CURSOR_NAME> as ( <SELECT> ) ]`
`<SELECT>`

See: [SELECT](#select)

## GRANT

`grant { select | insert | update | delete } on <SCHEMA> [ . <TABLE_NAME> ] to <USERNAME> [ with admin option ]`
`grant { create | drop } user to <USERNAME> [ with admin option ]`
`grant { create | drop } { table | index } on <SCHEMA> to <USERNAME> [ with admin option ]`

## REVOKE

`revoke { select | insert | update | delete } on <SCHEMA> [ . <TABLE_NAME> ] from <USERNAME>`
`revoke { create | drop } user from <USERNAME>`
`revoke { create | drop } { table | index } on <SCHEMA> from <USERNAME>`

## CREATE

`create table [ <SCHEMA>. ] <TABLE_NAME> ( <COLUMN_NAME> <FORMAT> [ , <COLUMN_NAME> <FORMAT> ...])`
`create table [ <SCHEMA>. ] <TABLE_NAME> as { <SELECT> | <WITH> }`
`create user <USERNAME> identified by <PASSWORD>`

## DROP

`drop table [ <SCHEMA>. ] <TABLE_NAME>`
`drop user <USERNAME>`

## INSERT

`insert into [ <SCHEMA>. ] <TABLE_NAME> [ ( <COLUMN_NAME> [ , <COLUMN_NAME> ...] ) ] { <WITH> | <SELECT> }`
`insert into [ <SCHEMA>. ] <TABLE_NAME> [ ( <COLUMN_NAME> [ , <COLUMN_NAME> ...] ) ]values (  <CONSTANT> [ ,  <CONSTANT> ...] )`

## UPDATE

`update [ <SCHEMA>. ] <TABLE_NAME> set <COLUMN_NAME>=<CONSTANT> [ , <COLUMN_NAME>=<CONSTANT> ...] where <WHERE_CLAUSE>`

See: [WHERE_CLAUSE](#where_clause)

## COMMIT
Validates all previous queries.

## ROLLBACK
Invalidates all previous queries.

## SEL_COL
In SELECT statement, the column is identified with:

Single column with optional alias:
`[[<SCHEMA>.]<TABLE_NAME>.]<COLUMN_NAME> [<COL_ALIAS>]`
`[<TABLE_ALIAS>.]<COLUMN_NAME> [<COL_ALIAS>]`

All columns:
`[[<SCHEMA>.]<TABLE_NAME>.]*`
`[<TABLE_ALIAS>.]*`

Data, can have various formats (string, number,...):
`<CONSTANT>`

## FROM_OBJ
In FROM clause, objects can be tables or sub-queries

`{ <FROM_TAB> | ( <SELECT> ) <CUR_ALIAS> }`

If it is a subquery, it must be enclosed by parentheses.

See: [FROM_TAB](#from_tab), [CUR_ALIAS](#cur_alias), [SELECT](#select)

## INNER_CLAUSE

`<GENERIC_COL> <WHERE_COMPARE> <GENERIC_COL>`

See: [GENERIC_COL](#generic_col) [WHERE_COMPARE](#where_compare)

## WHERE_CLAUSE

`[[ ( ]] { <WHERE_CMP> | <WHERE_BETWEEN> | <WHERE_IN> } [ { and | or } <WHERE_CLAUSE> ] [[ ) ]]`

See: [WHERE_CMP](#where_cmp), [WHERE_BETWEEN](#where_between), [WHERE_IN](#where_in)

## FROM_TAB

`{ [ <SCHEMA>. ] <TABLE_NAME> [ <TAB_ALIAS> ] | <CURSOR_NAME> [ <CUR_ALIAS> ] }`

## CUR_ALIAS
Defines an alias for a cursor.

## GENERIC_COL
Defines a column.

`[ { [ <SCHEMA>. ] <TABLE_NAME> . | <TAB_ALIAS> . } ] <COLUMN_NAME> [COL_ALIAS]`

## WHERE_COMPARE
Operators available for the comparison.

`{ = | > | < | >= | <= | <> | != }`

## WHERE_CMP
Comparison between two objects.

`[[ ( ]] <SEL_COL> <WHERE_COMPARE> <SEL_COL> [[ ) ]]`

See: [SEL_COL](#sel_col), [WHERE_COMPARE](#where_compare)

## WHERE_BETWEEN

Not yet implemented.

## WHERE_IN

Not yet implemented.

