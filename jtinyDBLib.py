import json
import copy
import time
import os, copy
from vExceptLib import vExcept


# JSON DB
class JSONtinyDB():
    def __init__(self, _g_params, _db_base_dir=None):
        self.__g_params = _g_params
        self.__latchcnt = 0
        self.__RAZ()
        if _db_base_dir is None:
            self.__DB_enable = False
        else:
            self.__DB_enable = True
            self.__db_base_dir = _db_base_dir
            if os.path.isdir(self.__db_base_dir):
                self.__file_db = '{}/db.json'.format(self.__db_base_dir)
                self.__loadDB()
            else:
                raise vExcept(12, str(self.__db_base_dir))

    def create_db(self, _db_base_dir, admin_password):
        try:
            self.__RAZ()
            if not os.path.exists(_db_base_dir):
                os.makedirs(_db_base_dir)
            if os.path.isdir(_db_base_dir):
                if not os.path.exists('{}/ADMIN'.format(_db_base_dir)):
                    os.makedirs('{}/ADMIN'.format(_db_base_dir))
                if os.path.isdir('{}/ADMIN'.format(_db_base_dir)):
                    self.__db_base_dir = _db_base_dir
                    self.__meta_db = {
                        "Accounts": [
                                {
                                    "username": "admin",
                                    "password": admin_password,
                                    "grants": {
                                        "SELECT": [],
                                        "INSERT": [],
                                        "UPDATE": [],
                                        "DELETE": [],
                                        "CREATE": [],
                                        "DROP": []
                                    }
                                }
                            ],
                        "Tables": {
                            "ADMIN": []
                            }
                        }
                    self.__file_db = '{}/db.json'.format(self.__db_base_dir)
                    self.__DB_enable = True
                    self.saveDB()
                    self.__loadDB()
            else:
                raise vExcept(16, _db_base_dir)
        except:
            pass

    def __RAZ(self):
        self.__meta_db = None
        self.__file_db = None
        self.db = {"Accounts": [], "Tables": [], "Locks": []}
        self.__db_base_dir = None
        self.__locks = []
        # [ session_id, owner, name, lock_type]
        # lock_type: 0 exclusive, 1 update, 10 read only, 99 None

    def __loadDB(self):
        if os.path.isfile(self.__file_db):
            __id_db = open(self.__file_db)
            self.__meta_db = json.load(__id_db)
            for account in self.__meta_db["Accounts"]:
                __USR = str(account["username"]).upper()
                if os.path.isdir('{}/{}'.format(self.__db_base_dir, __USR)):
                    self.db["Accounts"].append(account)
                    for __TBL in self.__meta_db["Tables"][__USR]:
                        if os.path.isfile('{}/{}/{}.json'.format(self.__db_base_dir, __USR, __TBL)):
                            tbl_file_id = open('{}/{}/{}.json'.format(self.__db_base_dir, __USR, __TBL))
                            self.db["Tables"].append(json.load(tbl_file_id))
                            tbl_file_id.close()
                        else:
                            __id_db.close()
                            raise vExcept(24, str('{}.{}'.format(__USR, __TBL)))
                else:
                    __id_db.close()
                    raise vExcept(13, str(__USR))
            __id_db.close()
        else:
            raise vExcept(23, str(self.__file_db))

    def save(self, obj_to_commit):
        if self.__DB_enable:
            for __TAB in obj_to_commit["Tables"]:
                found = False
                for n in range(len(self.db["Tables"])):
                    if self.db["Tables"][n]["schema"] == __TAB[0] and self.db["Tables"][n]["table_name"] == __TAB[1]:
                        found = True
                        break
                if found:
                    dir_path = '{}/{}'.format(self.__db_base_dir, __TAB[0])
                    tbl_file_id = open('{}/{}.json'.format(dir_path, __TAB[1]), mode='w')
                    json.dump(self.db["Tables"][n], indent=4, fp=tbl_file_id)
                    tbl_file_id.close()
        else:
            raise vExcept(15)

    def add_lock(self, session_id, owner, name, lock_type):
        # return 0 => lock acquired
        # return 1 => lock not acquired because of previous lock
        if self.__DB_enable:
            self.__latchcnt += 1
            while self.__latchcnt > 1:
                self.__latchcnt -= 1
                time.sleep(self.__latchcnt / 1000) # wait some ms
                self.__latchcnt += 1
            for n in range(len(self.__locks)):
                if self.__locks[n][1:-1] == [owner, name]:
                    if self.__locks[n][0] == session_id:
                        if self.__locks[n][3] == lock_type:
                            self.__latchcnt -= 1
                            return 0
                    else:
                        match lock_type:
                            case 0:
                                self.__latchcnt -= 1
                                return 1
                            case 1:
                                if self.__locks[n][3] in [0, 1]:
                                    self.__latchcnt -= 1
                                    return 1
                            case 10:
                                if self.__locks[n][3] == 0:
                                    self.__latchcnt -= 1
                                    return 1
            self.__locks.append([session_id, owner, name, lock_type])
            self.__latchcnt -= 1
            # print(self.__locks)
            return 0
        else:
            raise vExcept(15)

    def del_locks(self, session_id, lock_type, owner=None, name=None):
        if self.__DB_enable:
            self.__latchcnt += 1
            while self.__latchcnt > 1:
                self.__latchcnt -= 1
                time.sleep(self.__latchcnt / 1000) # wait some ms
                self.__latchcnt += 1
            for n in range(len(self.__locks)-1, -1, -1):
                if (self.__locks[n][0] == session_id) and ((owner is None) or (owner == self.__locks[n][1])) and ((name is None) or (name == self.__locks[n][2])):
                    if lock_type == 99:
                        del self.__locks[n]
                    elif self.__locks[n][3] == lock_type:
                        del self.__locks[n]
            self.__latchcnt -= 1
        else:
            raise vExcept(15)

    def AddTableFile(self, owner, table_name, table_data):
        if self.__DB_enable:
            file_path_name = '{}/{}/{}.json'.format(self.__db_base_dir, owner.upper(), table_name.upper())
            if os.path.isfile(file_path_name):
                os.remove(file_path_name)
            tbl_file_id = open(file_path_name, mode='w')
            json.dump(table_data, indent=4, fp=tbl_file_id)
            tbl_file_id.close()
        else:
            raise vExcept(15)

    def AddTableToMeta(self, owner, table_name):
        if self.__DB_enable:
            self.__meta_db["Tables"][str(owner).upper()].append(table_name.upper())
        else:
            raise vExcept(15)

    def AddTableToDB(self, table_data):
        if self.__DB_enable:
            self.db["Tables"].append(table_data)
        else:
            raise vExcept(15)

    def UpdTableToDB(self, table_data):
        if self.__DB_enable:
            for n in range(len(self.db["Tables"])):
                if (self.db["Tables"][n]["table_name"] == table_data["table_name"]) and (self.db["Tables"][n]["schema"] == table_data["schema"]):
                    self.db["Tables"][n] = copy.deepcopy(table_data)
                    break
        else:
            raise vExcept(15)

    def DelTableFromMeta(self, owner, table_name):
        if self.__DB_enable:
            for n in range(len(self.__meta_db["Tables"])):
                if self.__meta_db["Tables"][owner.upper()][n] == table_name.upper():
                    del self.__meta_db["Tables"][owner.upper()][n]
                    break
            for aID in range(len(self.__meta_db["Accounts"])):
                for sID in range(len(self.__meta_db["Accounts"][aID]["grants"]["SELECT"])-1, -1, -1):
                    if (self.__meta_db["Accounts"][aID]["grants"]["SELECT"][sID][0] == 'TABLE') and \
                       (self.__meta_db["Accounts"][aID]["grants"]["SELECT"][sID][1] == '{}.{}'.format(owner, table_name)):
                        del self.__meta_db["Accounts"][aID]["grants"]["SELECT"][sID]
                for sID in range(len(self.__meta_db["Accounts"][aID]["grants"]["INSERT"])-1, -1, -1):
                    if (self.__meta_db["Accounts"][aID]["grants"]["INSERT"][sID][0] == 'TABLE') and \
                       (self.__meta_db["Accounts"][aID]["grants"]["INSERT"][sID][1] == '{}.{}'.format(owner, table_name)):
                        del self.__meta_db["Accounts"][aID]["grants"]["INSERT"][sID]
                for sID in range(len(self.__meta_db["Accounts"][aID]["grants"]["UPDATE"])-1, -1, -1):
                    if (self.__meta_db["Accounts"][aID]["grants"]["UPDATE"][sID][0] == 'TABLE') and \
                       (self.__meta_db["Accounts"][aID]["grants"]["UPDATE"][sID][1] == '{}.{}'.format(owner, table_name)):
                        del self.__meta_db["Accounts"][aID]["grants"]["UPDATE"][sID]
                for sID in range(len(self.__meta_db["Accounts"][aID]["grants"]["DELETE"])-1, -1, -1):
                    if (self.__meta_db["Accounts"][aID]["grants"]["DELETE"][sID][0] == 'TABLE') and \
                       (self.__meta_db["Accounts"][aID]["grants"]["DELETE"][sID][1] == '{}.{}'.format(owner, table_name)):
                        del self.__meta_db["Accounts"][aID]["grants"]["DELETE"][sID]
        else:
            raise vExcept(15)

    def DelTableFromDB(self, owner, table_name):
        if self.__DB_enable:
            for n in range(len(self.db["Tables"])):
                if (self.db["Tables"][n]["table_name"] == table_name.upper()) and (self.db["Tables"][n]["schema"] == owner.upper()):
                    del self.db["Tables"][n]
                    break
            for aID in range(len(self.db["Accounts"])):
                for sID in range(len(self.db["Accounts"][aID]["grants"]["SELECT"])-1, -1, -1):
                    if (self.db["Accounts"][aID]["grants"]["SELECT"][sID][0] == 'TABLE') and \
                       (self.db["Accounts"][aID]["grants"]["SELECT"][sID][1] == '{}.{}'.format(owner, table_name)):
                        del self.db["Accounts"][aID]["grants"]["SELECT"][sID]
                for sID in range(len(self.db["Accounts"][aID]["grants"]["INSERT"])-1, -1, -1):
                    if (self.db["Accounts"][aID]["grants"]["INSERT"][sID][0] == 'TABLE') and \
                       (self.db["Accounts"][aID]["grants"]["INSERT"][sID][1] == '{}.{}'.format(owner, table_name)):
                        del self.db["Accounts"][aID]["grants"]["INSERT"][sID]
                for sID in range(len(self.db["Accounts"][aID]["grants"]["UPDATE"])-1, -1, -1):
                    if (self.db["Accounts"][aID]["grants"]["UPDATE"][sID][0] == 'TABLE') and \
                       (self.db["Accounts"][aID]["grants"]["UPDATE"][sID][1] == '{}.{}'.format(owner, table_name)):
                        del self.db["Accounts"][aID]["grants"]["UPDATE"][sID]
                for sID in range(len(self.db["Accounts"][aID]["grants"]["DELETE"])-1, -1, -1):
                    if (self.db["Accounts"][aID]["grants"]["DELETE"][sID][0] == 'TABLE') and \
                       (self.db["Accounts"][aID]["grants"]["DELETE"][sID][1] == '{}.{}'.format(owner, table_name)):
                        del self.db["Accounts"][aID]["grants"]["DELETE"][sID]
        else:
            raise vExcept(15)

    def DelTableFile(self, owner, table_name):
        if self.__DB_enable:
            file_path_name = '{}/{}/{}.json'.format(self.__db_base_dir, owner.upper(), table_name.upper())
            if os.path.isfile(file_path_name):
                os.remove(file_path_name)
            else:
                raise vExcept(14, '{}.{}'.format(owner, table_name))
        else:
            raise vExcept(15)

    def AddGrantToMeta(self, grant, granted, grant_bloc):
        if self.__DB_enable:
            notfound = True
            updated = False
            accountID = self.getAccountID(username=granted)
            for n in range(len(self.__meta_db["Accounts"][accountID]["grants"][grant])):
                eg = self.__meta_db["Accounts"][accountID]["grants"][grant][n]
                if len(eg) == 2:
                    if eg[0] == grant_bloc[0]:
                        notfound = False
                        if (eg[1] != grant_bloc[1]) and (grant_bloc[1] == 'YES'):
                            self.__meta_db["Accounts"][accountID]["grants"][grant][n][1] = grant_bloc[1]
                            updated = True
                        break
                else:
                    if (eg[0] == grant_bloc[0]) and (eg[1] == grant_bloc[1]):
                        notfound = False
                        if (eg[2] != grant_bloc[2]) and (grant_bloc[2] == 'YES'):
                            self.__meta_db["Accounts"][accountID]["grants"][grant][n][2] = grant_bloc[2]
                            updated = True
                        break
            if notfound:
                self.__meta_db["Accounts"][accountID]["grants"][grant].append(grant_bloc)
                # self.__meta_db[grant_bloc["username"]] = []
                # print(self.__meta_db)
        else:
            raise vExcept(15)
        return notfound or updated

    def DelGrantFromMeta(self, grant, granted, grant_bloc):
        if self.__DB_enable:
            deleted = False
            accountID = self.getAccountID(username=granted)
            for n in range(len(self.__meta_db["Accounts"][accountID]["grants"][grant])):
                eg = self.__meta_db["Accounts"][accountID]["grants"][grant][n]
                if len(eg) == 2:
                    if eg[0] == grant_bloc[0]:
                        del self.__meta_db["Accounts"][accountID]["grants"][grant][n]
                        deleted = True
                        break
                else:
                    if (eg[0] == grant_bloc[0]) and (eg[1] == grant_bloc[1]):
                        del self.__meta_db["Accounts"][accountID]["grants"][grant][n]
                        deleted = True
                        break
        else:
            raise vExcept(15)
        return deleted

    def AddAccountToMeta(self, account_bloc):
        if self.__DB_enable:
            notfound = True
            for eg in self.__meta_db["Accounts"]:
                if eg["username"] == account_bloc["username"]:
                    notfound = False
                    break
            if notfound:
                self.__meta_db["Accounts"].append(account_bloc)
                self.__meta_db["Tables"][account_bloc["username"].upper()] = []
        else:
            raise vExcept(15)
        return notfound

    def AddAccountToDB(self, account_bloc):
        if self.__DB_enable:
            self.db["Accounts"].append(account_bloc)
        else:
            raise vExcept(15)

    def DelAccountFromMeta(self, username):
        if self.__DB_enable:
            # remove account
            for n in range(len(self.__meta_db["Accounts"])):
                if self.__meta_db["Accounts"][n]["username"] == username.lower():
                    del self.__meta_db["Accounts"][n]
                    break
            # remove grant on removed account objects
            for n in range(len(self.__meta_db["Accounts"])):
                for g_type in ['SELECT', 'INSERT', 'UPDATE', 'DELETE']:
                    for g in range(len(self.__meta_db["Accounts"][n]["grants"][g_type])-1, -1, -1):
                        if (self.__meta_db["Accounts"][n]["grants"][g_type][g][0] == "SCHEMA") and (self.__meta_db["Accounts"][n]["grants"][g_type][g][1] == username.upper()):
                            del self.__meta_db["Accounts"][n]["grants"][g_type][g]
                        elif (self.__meta_db["Accounts"][n]["grants"][g_type][g][0] == "TABLE") and (self.__meta_db["Accounts"][n]["grants"][g_type][g][1].split('.')[0] == username.upper()):
                            del self.__meta_db["Accounts"][n]["grants"][g_type][g]
                for g_type in ['CREATE', 'DROP']:
                    for g in range(len(self.__meta_db["Accounts"][n]["grants"][g_type])-1, -1, -1):
                        if (self.__meta_db["Accounts"][n]["grants"][g_type][g][0] == "TABLE") and (self.__meta_db["Accounts"][n]["grants"][g_type][g][1] == username.upper()):
                            del self.__meta_db["Accounts"][n]["grants"][g_type][g]
            # remove tables
            for tbl in self.__meta_db["Tables"][username.upper()]:
                self.DelTableFile(owner=username.upper(), table_name=tbl)
            del self.__meta_db["Tables"][username.upper()]
            dir_path = '{}/{}'.format(self.__db_base_dir, username.upper())
            if os.path.exists(dir_path):
                os.rmdir(dir_path)
        else:
            raise vExcept(15)

    def DelAccountFromDB(self, username):
        if self.__DB_enable:
            # remove account
            for n in range(len(self.db["Accounts"])):
                if self.db["Accounts"][n]["username"] == username.lower():
                    del self.db["Accounts"][n]
                    break
            # remove grant on removed account objects
            for n in range(len(self.db["Accounts"])):
                for g_type in ['SELECT', 'INSERT', 'UPDATE', 'DELETE']:
                    for g in range(len(self.db["Accounts"][n]["grants"][g_type])-1, -1, -1):
                        if (self.db["Accounts"][n]["grants"][g_type][g][0] == "SCHEMA") and (self.db["Accounts"][n]["grants"][g_type][g][1] == username.upper()):
                            del self.db["Accounts"][n]["grants"][g_type][g]
                        elif (self.db["Accounts"][n]["grants"][g_type][g][0] == "TABLE") and (self.db["Accounts"][n]["grants"][g_type][g][1].split('.')[0] == username.upper()):
                            del self.db["Accounts"][n]["grants"][g_type][g]
                for g_type in ['CREATE', 'DROP']:
                    for g in range(len(self.db["Accounts"][n]["grants"][g_type])-1, -1, -1):
                        if (self.db["Accounts"][n]["grants"][g_type][g][0] == "TABLE") and (self.db["Accounts"][n]["grants"][g_type][g][1] == username.upper()):
                            del self.db["Accounts"][n]["grants"][g_type][g]
            # remove tables
            for n in range(len(self.db["Tables"])-1, -1, -1):
                if self.db["Tables"][n]["schema"] == username.upper():
                    del self.db["Tables"][n]
        else:
            raise vExcept(15)

    def getAccountID(self, username):
        if self.__DB_enable:
            for n in range(len(self.__meta_db["Accounts"])):
                if self.__meta_db["Accounts"][n]["username"] == username:
                    return n
            raise vExcept(1800, username)
        else:
            raise vExcept(15)

    def saveDB(self):
        if self.__DB_enable:
            __id_db = open(self.__file_db, mode='w')
            json.dump(self.__meta_db, indent=4, fp=__id_db)
            __id_db.close()
            for account in self.__meta_db["Accounts"]:
                dir_path = '{}/{}'.format(self.__db_base_dir, account['username'].upper())
                if not os.path.exists(dir_path):
                    os.makedirs(dir_path)
        else:
            raise vExcept(15)

    def reload(self):
        if self.__DB_enable:
            self.__RAZ()
            self.__loadDB()
        else:
            raise vExcept(15)

    def getTable(self, owner, table_name):
        count = 0
        result = None
        if table_name == 'DUAL':
            result = {
                "table_name": "DUAL",
                "schema": owner,
                "columns": [
                    ["LEVEL", "int"],
                    ["X", "str"]
                ],
                "rows": []
                }
        else:
            for t in self.db["Tables"]:
                if (t["table_name"] == table_name) and (((owner is not None) and (t["schema"] == owner)) or (owner is None)):
                    count += 1
                    result = copy.deepcopy(t)
            if count == 0:
                if owner is None:
                    raise vExcept(210, table_name)
                else:
                    raise vExcept(210, '{}.{}'.format(owner, table_name))
            elif count > 1:
                if owner is None:
                    raise vExcept(211, table_name)
                else:
                    raise vExcept(211, '{}.{}'.format(owner, table_name))
        return result

    def checkTableExists(self, owner, table_name):
        result = False
        for t in self.db["Tables"]:
            if (t["table_name"] == table_name) and (t["schema"] == owner):
                result = True
                break
        return result

    def checkUserExists(self, username):
        result = False
        for t in self.db["Accounts"]:
            if t["username"] == username.lower():
                result = True
                break
        return result
