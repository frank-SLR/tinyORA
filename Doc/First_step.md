# tinyDB

This page will help ypou for the first queries you want to submit.

---
# Table of content

[Connect to DB](#Connect-to-DB)
[Submit query and fetch return values](#submit-query-and-fetch-return-values)

---

> For all following examples, <basic_TinyDB_URL> will be :
https://tinydb.mydomain.com:1521/tinyDB

> For all examples, parameters will be set with following values:
The database name : sampleDB
Username / password : stat / statpwd



## Connect to DB
Method : GET
URL : ```https://tinydb.mydomain.com:1521/tinyDB/connect/db?database=sampleDB&username=stat&password=statpwd```

Return values
```json
{
    "session_id": "0x6f00278cbc14e4b",
    "err_message": {}
}
```
Return code: 200

## Submit query and fetch return values
Query:
```sql
select t1.f1, t1.f2, t1.f3, t1.f4, t1.f5, t1.f6, t2.f1, t2.f2, t2.f3, t2.f4 
from stat.stat01 t1 
inner join stat.stat02 t2 on t2.f1=t1.f1  
where t1.f1 >1000 
  and t1.f1 < 10000 
  and t1.f3=1025 
  and t1.f2 > t2.f2 
  and t2.f1 < 2000
```

### First step, submit query:
Method : POST
URL : ```https://tinydb.mydomain.com:1521/tinyDB/query/?session_id=0x6f00278cbc14e4b&query=select t1.f1, t1.f2, t1.f3, t1.f4, t1.f5, t1.f6, t2.f1, t2.f2, t2.f3, t2.f4 from stat.stat01 t1 inner join stat.stat02 t2 on t2.f1=t1.f1  where t1.f1 >1000 and t1.f1 < 10000 and t1.f3=1025 and t1.f2 > t2.f2 and t2.f1 < 2000```

Return values
```json
{
    "result": "Query submitted",
    "err_message": {}
}
```
Return code: 200

### Second step, fetch data
Method : GET
URL : ```https://tinydb.mydomain.com:1521/tinyDB/query/?session_id=0x6f00278cbc14e4b&query=select t1.f1, t1.f2, t1.f3, t1.f4, t1.f5, t1.f6, t2.f1, t2.f2, t2.f3, t2.f4 from stat.stat01 t1 inner join stat.stat02 t2 on t2.f1=t1.f1  where t1.f1 >1000 and t1.f1 < 10000 and t1.f3=1025 and t1.f2 > t2.f2 and t2.f1 < 2000```

> Note : URL must be same as first step. 

#### If tinyDB has completed work for submited query, it returns:

Return values
```json
{
    "result": {
        "columns": [
            ["F1", "int"], ["F2", "int"], ["F3", "int"], ["F4", "int"],
            ["F5", "int"], ["F6", "int"], ["F1_1", "int"], ["F2_1", "int"],
            ["F3_1", "int"], ["F4_1", "int"]
        ],
        "rows": [
            [1344, 56, 1025, 3063, 3084, 3049, 1344, 1, 1071, 3039 ],
            [1675, 99, 1025, 3016, 3022, 3078, 1675, 15, 1081, 3034],
            [1716, 92, 1025, 3009, 3097, 3087, 1716, 14, 1074, 3078],
            [1763, 19, 1025, 3092, 3047, 3042, 1763, 3, 1010, 3092]
        ]
    },
    "err_message": {}
}
```
Return code: 200

#### If tinyDB has not yet completed work for submited query, it returns:

Return values
```json
{
}
```
Return code: 204
