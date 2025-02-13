# tinyORA

TinyORA is a small database engine. It allows you to manage data using SQL.
> Once server is started, all commands are submited with API.

---
# Table of content

[Welcome page](#welcome-page)

[Create database](#create-database)

[Connect to TinyORA](#connect-to-tinyora)

[Disconnect from TinyORA](#disconnect-from-tinyORA)

[Submit query](#submit-query)

[Fetch query result](#fetch-query-result)

[List tables owned by current session account](#list-tables-owned-by-current-session-account)

[List open sessions](#list-open-sessions)

[list databases](#list-databases)

---

> Some APIs need to connect to TinyORA as manager. The password for this account is defined in the parameters file.

> Online APIs documentation is available on <basic_TinyORA_URL>/docs
> For example : https://tinyora.mydomain.com:1521/tinyORA/docs

> For all following examples, <basic_TinyORA_URL> will be :
> https://tinyora.mydomain.com:1521/tinyORA

## Welcome page

Method: GET
URL: <basic_TinyORA_URL>
Parameters: None

This API returns a simple message with your IP address

The return codes:
|Code|Description|
| --- | :--- |
|200|Successful|

## Create database

Method: POST
URL: <basic_TinyORA_URL>/create
Parameters: 
|Parameter name|type|Description|
| --- | --- | :--- |
|mgrpassword|str|Manager password is stored in parameter file|
|database|str|The name of the database to be created|
|adminpwd|str|Password of database ADMIN account|

The return codes:
|Code|Description|
| --- | :--- |
|200|Successful|
|400|Error|

## Connect to TinyORA

Method: GET
URL: <basic_TinyORA_URL>/connect/db
Parameters: 
|Parameter name|type|Description|
| --- | --- | :--- |
|database|str|The name of database to connect to|
|username|str|Account used to open connection|
|password|str|Password of the account|

The return codes:
|Code|Description|
| --- | :--- |
|200|Successful|
|503|Error|

## Disconnect from TinyORA

Method: POST
URL: <basic_TinyORA_URL>/disconnect/db
Parameters: 
|Parameter name|type|Description|
| --- | --- | :--- |
|session_id|str|the session identifier|

The return codes:
|Code|Description|
| --- | :--- |
|200|Successful|
|401|Error|

## Submit query

Method: POST
URL: <basic_TinyORA_URL>/query
Parameters: 
|Parameter name|type|Description|
| --- | --- | :--- |
|query|str|Query to submit|
|session_id|str|the session identifier|

The return codes:
|Code|Description|
| --- | :--- |
|200|Successful|
|400|Error|

## Fetch query result

Method: GET
URL: <basic_TinyORA_URL>/query
Parameters: 
|Parameter name|type|Description|
| --- | --- | :--- |
|session_id|str|the session identifier|
|table|str|Format query result. OPTIONAL. Default value:JSON|

Authorized values for "table" parameters are :
|Value|Description|
| --- | --- |
|TEXT|structured as table with column names as header|
|JSON|should i really explain ?|
|HTML|an HTML table object|

The return codes:
|Code|Description|
| --- | :--- |
|200|Successful|
|204|Query process not yet finished|
|400|Error|

## List tables owned by current session account

Method: GET
URL: <basic_TinyORA_URL>/account/list_tables
Parameters: 
|Parameter name|type|Description|
| --- | --- | :--- |
|session_id|str|the session identifier|

The return codes:
|Code|Description|
| --- | :--- |
|200|Successful|
|400|Error|

## List open sessions

Method: GET
URL: <basic_TinyORA_URL>/admin/list_sessions
Parameters: 
|Parameter name|type|Description|
| --- | --- | :--- |
|mgrpassword|str|Manager password is stored in parameter file|

The return codes:
|Code|Description|
| --- | :--- |
|200|Successful|
|400|Error|

## list databases

Method: GET
URL: <basic_TinyORA_URL>/admin/list_databases
Parameters: 
|Parameter name|type|Description|
| --- | --- | :--- |
|mgrpassword|str|Manager password is stored in parameter file|

The return codes:
|Code|Description|
| --- | :--- |
|200|Successful|
|400|Error|
