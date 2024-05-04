# tinyDB

TinyDB is a small database engine. It allows you to manage data using SQL.
> Once server is started, all commands are submited with API.

---
# Table of content

[Welcome page](#welcome-page)

[Create database](#create-database)

---

> Some APIs need to connect to TinyDB as manager. The password for this account is defined in the parameters file.

> Online APIs documentation is available on <basic_TinyDB_URL>/docs
> For example : https://tinydb.mydomain.com:1521/tinyDB/docs

## Welcome page

Method: GET
URL: <basic_TinyDB_URL>
Parameters: None

This API returns a simple message with your IP address

## Create database

Method: POST
URL: <basic_TinyDB_URL>/create
Parameters: 
|Parameter name|type|Description|
| --- | --- | :--- |
|mgrpassword|str|Manager password is stored in parameter file|
|database|str|The name of the database to be created|
|adminpwd|str|Password of database ADMIN account|

## Connect to TinyDB

## Disconnect from TinyDB

## Submit query

## List tables owned by current session account

## List open sessions

## list databases
