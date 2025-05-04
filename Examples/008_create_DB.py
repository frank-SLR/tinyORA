import sys

# setting path
sys.path.append('D:/Python/tinyORA')

from vExceptLib import vExcept
from vtinyDBLib import vDB
from jtinyDBLib import JSONtinyDB
import os, json
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

    dbn = 'DBTEST'
    adminpwd= 'adminpwd'
    mgrpassword = 'managerpwd'

    # create JSON DB on disk
    if mgrpassword != __meta_cfg["manager_password"]:
        raise vExcept(660)
    
    db_base_dir = '{}/{}'.format(__meta_cfg["root_dir"], dbn)
    db = JSONtinyDB(None)
    db.create_db(_db_base_dir=db_base_dir, admin_password=adminpwd)
    del db
    # open DB
    db = vDB(db_base_dir, g_params=__meta_cfg["global_parameters"])
    try:
        # append DB in catalog
        __id_db = open(parameters_file, mode='w')
        __meta_cfg["db_list"].append({"name": dbn, "base_dir": db_base_dir})
        json.dump(__meta_cfg, indent=4, fp=__id_db)
    finally:
        __id_db.close()
    result = 'Database {} successfuly created'.format(dbn)

except vExcept as e:
    print("error code: {}".format(e.errcode))
    for s in e.message.split("\n"):
        print("  {}".format(s))
finally:
    if os.path.isfile("./parameters.json"):
        copyfile(parameters_file, db_parameters_file)
        os.remove("./parameters.json")

