import sys

# setting path
sys.path.append('D:/python/parser')

from vExceptLib import vExcept
from vSessionLib import vSession
from vtinyDBLib import vDB
from tabulate import tabulate
import os, json
import tabulate
from shutil import copyfile

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

    username = 'resto'
    password = 'restopwd'
    database = 'db'
    query = ("create table resto.test01 as "
        + "select rownum num_row, id, name, 'je ''dirais: '||decode(id, :VAR1, :VAR3, :VAR2, 'quatre', 'autre') blablabla, "
        + "abs(-12.3) r, instr(name, 'o', 0, 2) positO, substr(name, rownum + 1, 5) souschaine "
        + "from resto.legumes "
        + "where lower(substr(name, 2, 1)) in ('o', 'h') "
        + "and id between 2 and 5 "
        + "and id > (rownum * 3)"
        )
    bind = {"VAR1": 2, "VAR2": 4, "VAR3": "deux"}

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
    session: vSession = db.create_session(username=username, password=password)
    print(f'session={session.session_id}')
    
    # query table
    result = session.submit_query(query, bind)
 
    # print result
    print(result)
    
except vExcept as e:
    print("error code: {}".format(e.errcode))
    for s in e.message.split("\n"):
        print("  {}".format(s))
finally:
    if os.path.isfile("./parameters.json"):
        copyfile("./parameters.json", db_parameters_file)
        os.remove("./parameters.json")
