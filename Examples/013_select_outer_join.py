import sys

# setting path
sys.path.append('D:/Python/tinyORA')

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
        __id_db = open(file=parameters_file, mode='tr')
        __meta_cfg = json.load(__id_db)
    finally:
        __id_db.close()

    username = 'resto'
    password = 'restopwd'
    database = 'db'
    query = ("select l.id, l.name, p.id, p.name, trunc(3.14156, 1) PI_TRUNC, acos(0.5) AC , asin(0.5) AS , atan(0.5) AT , atan2(1, 2) AT2, cos(pi()/3) cos_pi_3, "
             + "  sin(pi()/3) sin_pi_3, tan(pi()/3) tan_pi_3, cosh(pi()/3) cosh_pi_3, sinh(pi()/3) sinh_pi_3, tanh(pi()/3) tanh_pi_3, exp(1) exp_1, ln(1) ln_1, "
             + "  log(10, 100) log_100, floor(sin(p.id)) floor_id, ceil(sin(p.id)) floor_id, mod(3*p.id, 5) mod_3_5, sqrt(p.id-5) sqrt_id, power(p.id, 2) power_id "
             + "from resto.plats p "
             + "left outer join resto.legumes l on l.id=p.id"
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
    session: vSession = db.connect(user=username, password=password)
    print(f'session={session.session_id}')
    
    # Obtain a cursor
    cursor = session.cursor()
    
    # query table
    cursor.execute(query, bind)
 
    # print result
    entete = [x[0] for x in cursor.description]
    print(tabulate.tabulate(cursor.fetchall(), headers=entete, tablefmt="grid"))
    
except vExcept as e:
    print("error code: {}".format(e.errcode))
    for s in e.message.split("\n"):
        print("  {}".format(s))
finally:
    if os.path.isfile("./parameters.json"):
        # copyfile("./parameters.json", db_parameters_file)
        os.remove("./parameters.json")

