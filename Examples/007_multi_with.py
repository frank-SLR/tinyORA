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
    query = ("with "
        + "l as (select * from resto.legumes), "
        + "p as (select * from resto.plats) "
        + "select l.id lid, l.name lname, p.id pid, p.name pname "
        + "from l, p "
        + "where l.id < p.id "
        + "order by lid asc, pid desc"
        )
    bind = {}

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
    entete = [x[0] for x in result["columns"]]
    print(tabulate.tabulate(result["rows"], headers=entete, tablefmt="grid"))
    
except vExcept as e:
    print("error code: {}".format(e.errcode))
    for s in e.message.split("\n"):
        print("  {}".format(s))
finally:
    if os.path.isfile("./parameters.json"):
        # copyfile("./parameters.json", db_parameters_file)
        os.remove("./parameters.json")
