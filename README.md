# TinyORA

TinyORA is a small database engine.
This doc explains the installation procedure.

---
# Table of content

[Needed modules](#needed-modules)

[SSL](#ssl)

[Parameters file](#parameters-file)

[Start server](#start-server)

---

## Needed modules

Following modules are mandatory:

- argparse
- uvicorn
- fastapi.FastAPI
- fastapi.Response
- fastapi.status
- fastapi.Request
- fastapi.responses.JSONResponse
- sys
- os
- ssl
- json
- re
- copy
- random
- time
- shutil
- tabulate
- concurrent

You can install them with:

```bash
pip install argparse
pip install fastapi[all]
pip install uvicorn[standard]
pip install cryptography
pip install requests
pip install tabulate
pip install concurrent
```

## SSL

To enable HTTPs you need certificats.
For example, you can create your own certificats with:

```bash
openssl req -x509 -newkey rsa:4096 -nodes -out cert.pem -keyout key.pem -days 365
```

For profesional purpose, you rather could contact your network security team.

## Parameters file
TinyORA needs a parameters file for his initialisation.
This file is ==parameters.json== and his format is JSON.

You should define the file with inital value :

```json
{
    "manager_password": "mgrpwd%1234",
    "root_dir": "/opt/tinyORA",
    "global_parameters":{
        "background_process":30,
        "root_path": "/tinyORA"
    },
    "db_list": [
    ]
}
```
- **manager_password** is the password you will use to manage tinyORA.
- **root_dir** is the path where database files will be created.
- **global_parameters** list of parameters for internal management

|Parameter|Type|Description|
| :--- | :--- | :--- |
|background_process|int|Number of background processes used to process queries|
|root_path|str|URL root|


## Start server

To start tinyORA server, submit following command:
```bash
python server.py -a <domain> -s <SSL_files_path> -p <port> -l <message_level> -d <parameters_file>
python server.py --address <domain> --ssl-path <SSL_files_path> --port <port> --log-level <message_level> --db-parameters-file <parameters_file>
```

All parameters except <parameters_file> are optional.

- **domain**: this is the address used for listening. Default value is ==127.0.0.1==.
- **SSL_files_path**: This is the path where certificat files are stored.
- **port**: the port used for listening. Default value is ==1521==.
- **message_level**: This is the minimal message level to display. Default value is ==warning==.
    Following values are defined:
    |Message level|
    | ---: |
    |critical|
    |error|
    |warning|
    |info|
    |debug|
    |trace|
- **parameters_file**: JSON file containing parameters for TinyORA. See: [Parameters file](#parameters-file)


### Example:
```bash
python server.py -a tinyora.mydomain.com -s /opt/tinyORA/OpenSSL -p 1521 -l warning -d /opt/tinyORA/pfile.json
```
