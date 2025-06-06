import sys

# setting path
sys.path.append('D:/Python/tinyORA')

from vExceptLib import vExcept
from vSessionLib import vSession
from vtinyDBLib import vDB
import os, json
from shutil import copyfile
from tabulate import tabulate
import tabulate

try:
    db_parameters_file = 'D:/Python/data/pfile.json'
    parameters_file: str = "./parameters.json"
    if os.path.isfile(db_parameters_file):
        copyfile(db_parameters_file, parameters_file)

    # load parameters file
    try:
        __id_db = open(parameters_file)
        __meta_cfg = json.load(__id_db)
    finally:
        __id_db.close()

    username = 'test'
    password = 'testpwd'
    database = 'DBTEST'
    # query = ("with t as (select level v from dual connect by level <= 20) "
    #     + "select t.v, 'line: '||t.v, 'coef 2.9 : ' || t.v*2.9, t.v*2.9 "
    #     + "from t"
    #     )
    query = ("insert into rnd1 "
        + "select level, 'line: '||level, 'coef 2.9 : ' || level*2.9 "
        + "from dual connect by level <= :MAXVALUE"
        )
    bind = {'MAXVALUE': 20}

    db_found = False
    for dbidx, dbblk in enumerate(__meta_cfg["db_list"]):
        if dbblk["name"] == database:
            db_found = True
            break
    if not db_found:
        raise vExcept(600)

    # parameters
    root_dir: str = __meta_cfg["root_dir"]
    db_base_dir: str = f"{root_dir}/{database}"
 
    # open DB
    db = vDB(_db_base_dir=db_base_dir, g_params=__meta_cfg["global_parameters"])
    
    # open session with user1 on TestDB
    session: vSession = db.connect(user=username, password=password)
    print(f'session={session.session_id}')
    
    # Obtain a cursor
    cursor = session.cursor()

    # query table
    cursor.execute(query, bind)
 
    # print result
    print(cursor.message)
    
    # commit
    session.commit()
 
    # print commit result
    print("Commit")
    
except vExcept as e:
    print("error code: {}".format(e.errcode))
    for s in e.message.split("\n"):
        print("  {}".format(s))
finally:
    if os.path.isfile("./parameters.json"):
        # copyfile("./parameters.json", db_parameters_file)
        os.remove("./parameters.json")

