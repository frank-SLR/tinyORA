import re
import copy
import random
from vExceptLib import vExept
from jtinyDBLib import JSONtinyDB
from parserLib import vParser

class vSession(object):
    def __init__(self, db:JSONtinyDB, username, password):
        self.db = db
        self.current_schema = None
        for account in self.db.db["Accounts"]:
            if (username == account["username"]) and (password == account["password"]):
                self.current_schema = username
                break
        if self.current_schema is None:
            raise vExept(109)
        self.session_id = hex(random.randint(1, 9999999999999999999))
        self.__password = password
        self.__session_username = username
        self.__parsed_query = None
        # self.__parsed_query_flg = False
        self.__updated_tables = []
        super().__init__()

    def submit_query(self, _query:str):
        self.__parsed_query = vParser().parse_query(query=_query)
        # print(self.__parsed_query)

        if self.__parsed_query["querytype"] in ['SELECT']:
            result = {"columns": [], "rows": []}
        elif self.__parsed_query["querytype"] in ['DESCRIBE']:
            result = {"columns": [], "schema": [], "table_name": []}
        elif self.__parsed_query["querytype"] in ['GRANT', 'CREATE', 'DROP', 'INSERT', 'COMMIT', 'ROLLBACK', 'UPDATE', 'DELETE']:
            result = {"message": None}

        # put locks
        if self.__parsed_query["querytype"] in ['SELECT', 'DESCRIBE']:
            for n in range(len(self.__parsed_query["from"])):
                tbl = self.__parsed_query["from"][n]
                if tbl[3] == 'TABLE':
                    lock_flg = True
                    while lock_flg:
                        lock_val = self.db.add_lock(session_id=self.session_id, owner=tbl[1], name=tbl[2], lock_type=10)
                        match lock_val:
                            case 0:
                                lock_flg = False
                            case 1:
                                raise vExept(1900, '{}.{}'.format(tbl[1], tbl[2]))
        elif self.__parsed_query["querytype"] in ['INSERT']:
            lock_flg = True
            while lock_flg:
                lock_val = self.db.add_lock(session_id=self.session_id, owner=self.__parsed_query["insert"][0], name=self.__parsed_query["insert"][1], lock_type=1)
                match lock_val:
                    case 0:
                        lock_flg = False
                    case 1:
                        raise vExept(1900, '{}.{}'.format(self.__parsed_query["insert"][0], self.__parsed_query["insert"][1]))
        if self.__parsed_query["querytype"] in ['UPDATE', 'DELETE']:
            tbl = self.__parsed_query["from"][0]
            lock_flg = True
            while lock_flg:
                lock_val = self.db.add_lock(session_id=self.session_id, owner=tbl[1], name=tbl[2], lock_type=1)
                match lock_val:
                    case 0:
                        lock_flg = False
                    case 1:
                        raise vExept(1900, '{}.{}'.format(tbl[1], tbl[2]))
        elif self.__parsed_query["querytype"] in ['DROP']:
            if self.__parsed_query["drop"][0][0] == 'TABLE':
                lock_flg = True
                while lock_flg:
                    lock_val = self.db.add_lock(session_id=self.session_id, owner=self.__parsed_query["drop"][0][1], name=self.__parsed_query["drop"][0][2], lock_type=0)
                    match lock_val:
                        case 0:
                            lock_flg = False
                        case 1:
                            raise vExept(1900, '{}.{}'.format(self.__parsed_query["drop"][0][1], self.__parsed_query["drop"][0][2]))
            elif self.__parsed_query["drop"][0][0] == 'USER':
                for TAB in self.db.db["Tables"]:
                    if TAB["schema"] == self.__parsed_query["drop"][0][1]:
                        lock_flg = True
                        while lock_flg:
                            lock_val = self.db.add_lock(session_id=self.session_id, owner=TAB["schema"], name=TAB["table_name"], lock_type=0)
                            match lock_val:
                                case 0:
                                    lock_flg = False
                                case 1:
                                    raise vExept(1900, '{}.{}'.format(TAB["schema"], TAB["table_name"]))

        # load tables
        if self.__parsed_query["querytype"] in ['SELECT', 'DESCRIBE', 'UPDATE', 'DELETE']:
            self.__validate_tables()

        # check GRANT
        if self.__parsed_query["querytype"] in ['SELECT', 'DESCRIBE']:
            for n in range(len(self.__parsed_query["from"])):
                tbl = self.__parsed_query["from"][n]
                if tbl[3] == 'TABLE':
                    if not self.__get_grant_for_object(owner=tbl[1], obj_name=tbl[2], grant_needed='SELECT'):
                        raise vExept(210, '{}.{}'.format(tbl[1], tbl[2]))
        elif self.__parsed_query["querytype"] in ['GRANT']:
            grt = self.__parsed_query["grant"][0]
            match grt[1]:
                case 'SELECT' | 'INSERT' | 'UPDATE' | 'DELETE':
                    o_t = grt[3].split('.')
                    if len(o_t) == 1:
                        if not self.__get_grant_for_object(owner=grt[3], obj_name=None, grant_needed=grt[1], admin='YES'):
                            raise vExept(900)
                    else:
                        if not self.__get_grant_for_object(owner=o_t[0], obj_name=o_t[1], grant_needed=grt[1], admin='YES'):
                            raise vExept(900)
                    self.db.AddGrantToMeta(grant=grt[1], granted=str(grt[0]).lower(), grant_bloc=[grt[2], grt[3], grt[4]])
                case 'CREATE' | 'DROP':
                    match grt[2]:
                        case 'TABLE' | 'INDEX':
                            if not self.__get_grant_for_object(owner=grt[3], obj_name=grt[2], grant_needed=grt[1], admin='YES'):
                                    raise vExept(900)
                            self.db.AddGrantToMeta(grant=grt[1], granted=grt[0], grant_bloc=[grt[2], grt[3], grt[4]])
                        case 'USER':
                            if not self.__get_grant_for_object(owner=None, obj_name=grt[3], grant_needed=grt[1], admin='YES'):
                                    raise vExept(900)
                            self.db.AddGrantToMeta(grant=grt[1], granted=grt[0], grant_bloc=[grt[2], grt[4]])
            self.db.saveDB()
            result = {"message": "Grant processed"}
        elif self.__parsed_query["querytype"] in ['REVOKE']:
            grt = self.__parsed_query["revoke"][0]
            match grt[1]:
                case 'SELECT' | 'INSERT' | 'UPDATE' | 'DELETE':
                    o_t = grt[3].split('.')
                    if len(o_t) == 1:
                        if not self.__get_grant_for_object(owner=grt[3], obj_name=None, grant_needed=grt[1], admin='YES'):
                            raise vExept(900)
                    else:
                        if not self.__get_grant_for_object(owner=o_t[0], obj_name=o_t[1], grant_needed=grt[1], admin='YES'):
                            raise vExept(900)
                    self.db.DelGrantFromMeta(grant=grt[1], granted=str(grt[0]).lower(), grant_bloc=[grt[2], grt[3]])
                case 'CREATE' | 'DROP':
                    match grt[2]:
                        case 'TABLE' | 'INDEX':
                            if not self.__get_grant_for_object(owner=grt[3], obj_name=grt[2], grant_needed=grt[1], admin='YES'):
                                    raise vExept(900)
                            self.db.DelGrantFromMeta(grant=grt[1], granted=grt[0], grant_bloc=[grt[2], grt[3]])
                        case 'USER':
                            if not self.__get_grant_for_object(owner=None, obj_name=grt[3], grant_needed=grt[1], admin='YES'):
                                    raise vExept(900)
                            self.db.DelGrantFromMeta(grant=grt[1], granted=grt[0], grant_bloc=[grt[2]])
            self.db.saveDB()
            result = {"message": "Revoke processed"}
        elif self.__parsed_query["querytype"] in ['CREATE']:
            if self.__parsed_query["create"][0][0] == 'TABLE':
                if self.__parsed_query["create"][0][1] is None:
                    self.__parsed_query["create"][0][1] = self.current_schema
                if self.__parsed_query["create"][0][1] != self.current_schema:
                    if not self.__get_grant_for_object(owner=self.__parsed_query["create"][0][1], obj_name='TABLE', grant_needed='CREATE'):
                        raise vExept(901)
            elif self.__parsed_query["create"][0][0] == 'USER':
                if not self.__get_grant_for_object(owner=None, obj_name='USER', grant_needed='CREATE'):
                    raise vExept(901)
        elif self.__parsed_query["querytype"] in ['DROP']:
            if self.__parsed_query["drop"][0][0] == 'TABLE':
                if self.__parsed_query["drop"][0][1] is None:
                    self.__parsed_query["drop"][0][1] = self.current_schema
                if self.__parsed_query["drop"][0][1] != self.current_schema:
                    if not self.__get_grant_for_object(owner=self.__parsed_query["drop"][0][1], obj_name='TABLE', grant_needed='DROP'):
                        raise vExept(902)
            elif self.__parsed_query["drop"][0][0] == 'USER':
                if not self.__get_grant_for_object(owner=None, obj_name='USER', grant_needed='DROP'):
                    raise vExept(902)
        elif self.__parsed_query["querytype"] in ['INSERT']:
            if self.__parsed_query["insert"][0] is None:
                self.__parsed_query["insert"][0] = self.current_schema
            u_name=self.__parsed_query["insert"][0]
            t_name=self.__parsed_query["insert"][1]
            if not self.__get_grant_for_object(owner=u_name, obj_name=t_name, grant_needed='INSERT'):
                raise vExept(210, '{}.{}'.format(u_name, t_name))
        elif self.__parsed_query["querytype"] in ['UPDATE']:
            if self.__parsed_query["from"][0][1] is None:
                self.__parsed_query["from"][0][1] = self.current_schema
            u_name=self.__parsed_query["from"][0][1]
            t_name=self.__parsed_query["from"][0][2]
            if not self.__get_grant_for_object(owner=u_name, obj_name=t_name, grant_needed='UPDATE'):
                raise vExept(210, '{}.{}'.format(u_name, t_name))
        elif self.__parsed_query["querytype"] in ['DELETE']:
            if self.__parsed_query["from"][0][1] is None:
                self.__parsed_query["from"][0][1] = self.current_schema
            u_name=self.__parsed_query["from"][0][1]
            t_name=self.__parsed_query["from"][0][2]
            if not self.__get_grant_for_object(owner=u_name, obj_name=t_name, grant_needed='DELETE'):
                raise vExept(210, '{}.{}'.format(u_name, t_name))

        # process query
        if self.__parsed_query["querytype"] in ['CREATE']:
            if self.__parsed_query["create"][0][0] == 'TABLE':
                self.__process_create_table()
                result = {"message": "Table created"}
            elif self.__parsed_query["create"][0][0] == 'USER':
                self.__process_create_user()
                result = {"message": "User created"}
        elif self.__parsed_query["querytype"] in ['DROP']:
            if self.__parsed_query["drop"][0][0] == 'TABLE':
                self.__process_drop_table()
                result = {"message": "Table dropped"}
            elif self.__parsed_query["drop"][0][0] == 'USER':
                self.__process_drop_user()
                result = {"message": "User dropped"}
        elif self.__parsed_query["querytype"] in ['INSERT']:
            cnt = self.__process_insert()
            result = {"message": "{} line(s) inserted".format(cnt)}
        elif self.__parsed_query["querytype"] in ['SELECT']:
            result = self.__process_select(result)
        elif self.__parsed_query["querytype"] in ['UPDATE']:
            cnt = self.__process_update()
            result = {"message": "{} line(s) updated".format(cnt)}
        elif self.__parsed_query["querytype"] in ['DELETE']:
            cnt = self.__process_delete()
            result = {"message": "{} line(s) deleted".format(cnt)}
        elif self.__parsed_query["querytype"] in ['DESCRIBE']:
            result["schema"] = self.__parsed_query["from"][0][1]
            result["table_name"] = self.__parsed_query["from"][0][2]
            result["columns"] = self.__parsed_query["from"][0][4][0]["columns"]
        elif self.__parsed_query["querytype"] in ['COMMIT']:
            self.__commit()
            result["message"] = 'Commited'
        elif self.__parsed_query["querytype"] in ['ROLLBACK']:
            self.__rollback()
            result["message"] = 'Rollbacked'

        # remove locks
        if self.__parsed_query["querytype"] in ['SELECT', 'DESCRIBE']:
            self.db.del_locks(session_id=self.session_id, lock_type=10)
        elif self.__parsed_query["querytype"] in ['INSERT']:
            self.db.del_locks(session_id=self.session_id, lock_type=10)
        elif self.__parsed_query["querytype"] in ['DROP']:
            if self.__parsed_query["drop"][0][0] == 'TABLE':
                self.db.del_locks(session_id=self.session_id, owner=self.__parsed_query["drop"][0][1], obj_name=self.__parsed_query["drop"][0][2], lock_type=0)
            elif self.__parsed_query["drop"][0][0] == 'USER':
                self.db.del_locks(session_id=self.session_id, owner=self.__parsed_query["drop"][0][1], lock_type=0)
        elif self.__parsed_query["querytype"] in ['COMMIT', 'ROLLBACK']:
            self.db.del_locks(session_id=self.session_id, lock_type=99)
        return result

    def get_tables(self):
        result = []
        for tbl in self.db.db["Tables"]:
            if tbl["schema"] == self.current_schema.upper():
                result.append([tbl["schema"], tbl["table_name"]])
        return result

    def __commit(self):
        __obj_to_commit = {"Tables": []}
        for TAB in self.__updated_tables:
            self.db.UpdTableToDB(table_data=TAB)
            __obj_to_commit["Tables"].append([TAB["schema"], TAB["table_name"]])
        self.db.save(obj_to_commit=__obj_to_commit)
        self.__updated_tables = []

    def __rollback(self):
        self.__updated_tables = []

    def __searchColInFromTables(self, colin, aliasin, table_namein, schemain):
        count = 0
        for tf in range(len(self.__parsed_query["from"])):
            if (aliasin is not None) and (aliasin == self.__parsed_query["from"][tf][0]) or \
               ((aliasin is None) and ( (table_namein is None) or (\
               (table_namein is not None) and (table_namein == self.__parsed_query["from"][tf][2]) \
               and ((schemain is None) or (schemain == self.__parsed_query["from"][tf][1]))))):
                for ctf in range(len(self.__parsed_query["from"][tf][4][0]["columns"])):
                    if colin == self.__parsed_query["from"][tf][4][0]["columns"][ctf][0]:
                        count += 1
                        if aliasin is None:
                            aliasin = self.__parsed_query["from"][tf][0]
                        if schemain is None:
                            schemain = self.__parsed_query["from"][tf][1]
                        if table_namein is None:
                            table_namein = self.__parsed_query["from"][tf][2]
                        memtf = tf
                        memctf = ctf
                        ctype = self.__parsed_query["from"][tf][4][0]["columns"][ctf][1]
                        break
        if count == 0:
            raise vExept(311, colin)
        elif count == 1:
            return colin, aliasin, table_namein, schemain, memtf, memctf, ctype
        else:
            raise vExept(313, colin)

    def __searchColsInFromTables(self, colin, aliasin, table_namein, schemain):
        count = 0
        result = []
        for tf in range(len(self.__parsed_query["from"])):
            if (aliasin is not None) and (aliasin == self.__parsed_query["from"][tf][0]) or \
               ((aliasin is None) and ( (table_namein is None) or (\
               (table_namein is not None) and (table_namein == self.__parsed_query["from"][tf][2]) \
               and ((schemain is None) or (schemain == self.__parsed_query["from"][tf][1]))))):
                count += 1
                for ctf in range(len(self.__parsed_query["from"][tf][4][0]["columns"])):
                    if aliasin is None:
                        aliasin = self.__parsed_query["from"][tf][0]
                    if schemain is None:
                        schemain = self.__parsed_query["from"][tf][1]
                    if table_namein is None:
                        table_namein = self.__parsed_query["from"][tf][2]
                    memtf = tf
                    memctf = ctf
                    ctype = self.__parsed_query["from"][tf][4][0]["columns"][ctf][1]
                    colin = self.__parsed_query["from"][tf][4][0]["columns"][ctf][0]
                    tab_cur = self.__parsed_query["from"][tf][3]
                    result.append([colin, aliasin, table_namein, schemain, memtf, memctf, ctype, tab_cur])
        if count == 0:
            raise vExept(311, colin)
        elif count == 1:
            return result
        else:
            raise vExept(313, colin)

    def __get_rows(self, cur_idx):
        for n in range(len(self.__parsed_query["from"][cur_idx][4][0]["rows"])):
            self.__RowsPosInTables[cur_idx] = n
            if cur_idx < len(self.__parsed_query["from"])-1:
                self.__get_rows(cur_idx=cur_idx+1)
            else:
                if self.__process_tests():
                    rrow = []
                    for s in self.__parsed_query['select']:
                        if s[5] == 'COLUMN':
                            rrow.append(self.__parsed_query["from"][s[6]][4][0]["rows"][self.__RowsPosInTables[s[6]]][s[7]])
                        elif s[5] == 'INT':
                            rrow.append(int(s[3]))
                        elif s[5] == 'FLOAT':
                            rrow.append(float(s[3]))
                        elif s[5] == 'HEX':
                            rrow.append(s[3])
                        elif s[5] == 'STR':
                            rrow.append(s[3][1:-1])
                        else:
                            raise vExept(801)
                    self.__result.append(rrow)

    def __prefetch_get_rows(self):
        for cur_idx in range(len(self.__parsed_query["from"])):
            if self.__parsed_query["from"][cur_idx][3] == 'TABLE':
                Validate_prefetch_process = False
                for tst in self.__parsed_query['parsed_where']:
                    if tst[1][0] == "TST":
                        if (tst[1][5] == "COLUMN") and (tst[1][1] == cur_idx) and (tst[3][5] != "COLUMN") or \
                           (tst[3][5] == "COLUMN") and (tst[3][1] == cur_idx) and (tst[1][5] != "COLUMN") or \
                           (tst[1][5] == "COLUMN") and (tst[1][1] == cur_idx) and (tst[3][5] == "COLUMN") and (tst[3][1] == cur_idx):
                            Validate_prefetch_process = True
                            break
                if Validate_prefetch_process:
                    tmp_rows = []
                    for n in range(len(self.__parsed_query["from"][cur_idx][4][0]["rows"])):
                        self.__RowsPosInTables[cur_idx] = n
                        if self.__prefetch_process_tests(cur_idx):
                            tmp_rows.append(self.__parsed_query["from"][cur_idx][4][0]["rows"][n])
                    self.__parsed_query["from"][cur_idx][4][0]["rows"] = tmp_rows

    def __process_tests(self):
        # parsed_where: item_id, field1, oper, field2
        #            or item_id, ['META', item_id], oper, ['META', item_id]
        #            or item_id, ['TST', num_table, num_col, alias1, field1, type, schema, table_name, table or cursor], oper, ['TST', num_table, num_col, alias2, field2, type, schema, table_name, table or cursor]
        if len(self.__parsed_query["parsed_where"]) > 0:
            temp_res = []
            for tst in self.__parsed_query['parsed_where']:
                if tst[1][0] != tst[3][0]:
                    raise vExept(899, tst)
                if tst[1][0] == "TST":
                    if tst[1][5] == "COLUMN":
                        c1 = self.__parsed_query["from"][tst[1][1]][4][0]["rows"][self.__RowsPosInTables[tst[1][1]]][tst[1][2]]
                    else:
                        c1 = tst[1][4]
                    if tst[3][5] == "COLUMN":
                        c2 = self.__parsed_query["from"][tst[3][1]][4][0]["rows"][self.__RowsPosInTables[tst[3][1]]][tst[3][2]]
                    else:
                        c2 = tst[3][4]
                    tstoper = tst[2]
                    result = self.__compare_cols(str(c1), str(c2), tstoper)
                else:
                    result = self.__compare_cols(temp_res[tst[1][1]], temp_res[tst[3][1]], tst[2])
                temp_res.append(result)
            where_tests = temp_res[-1]
        else:
            where_tests = True
        inner_tests = True
        if len(self.__parsed_query["parsed_inner_where"]) > 0:
            for blk in range(len(self.__parsed_query["parsed_inner_where"])):
                temp_res = []
                for tst in self.__parsed_query['parsed_inner_where'][blk]:
                    if tst[1][0] != tst[3][0]:
                        raise vExept(899, tst)
                    if tst[1][0] == "TST":
                        if tst[1][5] == "COLUMN":
                            c1 = self.__parsed_query["from"][tst[1][1]][4][0]["rows"][self.__RowsPosInTables[tst[1][1]]][tst[1][2]]
                        else:
                            c1 = tst[1][4]
                        if tst[3][5] == "COLUMN":
                            c2 = self.__parsed_query["from"][tst[3][1]][4][0]["rows"][self.__RowsPosInTables[tst[3][1]]][tst[3][2]]
                        else:
                            c2 = tst[3][4]
                        tstoper = tst[2]
                        result = self.__compare_cols(str(c1), str(c2), tstoper)
                    else:
                        result = self.__compare_cols(temp_res[tst[1][1]], temp_res[tst[3][1]], tst[2])
                    temp_res.append(result)
                if len(temp_res) > 0:
                    inner_tests = inner_tests and temp_res[-1]
        return where_tests and inner_tests

    def __prefetch_process_tests(self, tab_num):
        # parsed_where: item_id, field1, oper, field2
        #            or item_id, ['META', item_id], oper, ['META', item_id]
        #            or item_id, ['TST', num_table, num_col, alias1, field1, type, schema, table_name, table or cursor], oper, ['TST', num_table, num_col, alias2, field2, type, schema, table_name, table or cursor]
        if len(self.__parsed_query["parsed_where"]) > 0:
            temp_res = []
            for tst in self.__parsed_query['parsed_where']:
                if tst[1][0] != tst[3][0]:
                    raise vExept(899, tst)
                if tst[1][0] == "TST":
                    if tst[1][5] == "COLUMN":
                        if tst[1][1] == tab_num:
                            c1 = self.__parsed_query["from"][tst[1][1]][4][0]["rows"][self.__RowsPosInTables[tst[1][1]]][tst[1][2]]
                    else:
                        c1 = tst[1][4]
                    if tst[3][5] == "COLUMN":
                        if tst[3][1] == tab_num:
                            c2 = self.__parsed_query["from"][tst[3][1]][4][0]["rows"][self.__RowsPosInTables[tst[3][1]]][tst[3][2]]
                    else:
                        c2 = tst[3][4]
                    tstoper = tst[2]
                    if (tst[1][5] == "COLUMN") and (tst[1][1] == tab_num) and (tst[3][5] != "COLUMN") or \
                       (tst[3][5] == "COLUMN") and (tst[3][1] == tab_num) and (tst[1][5] != "COLUMN") or \
                       (tst[1][5] == "COLUMN") and (tst[1][1] == tab_num) and (tst[3][5] == "COLUMN") and (tst[3][1] == tab_num):
                        result = self.__compare_cols(str(c1), str(c2), tstoper)
                    else:
                        result = None
                else:
                    if temp_res[tst[1][1]] is None:
                        if temp_res[tst[3][1]] is None:
                            result = True
                        else:
                            result = temp_res[tst[3][1]]
                    else:
                        if temp_res[tst[3][1]] is None:
                            result = temp_res[tst[1][1]]
                        else:
                            result = self.__compare_cols(temp_res[tst[1][1]], temp_res[tst[3][1]], tst[2])
                temp_res.append(result)
            where_tests = temp_res[-1]
            if where_tests is None:
                where_tests = True
        else:
            where_tests = True
        return where_tests

    def __process_select(self, result):
        # generate select columns for result
        result = self.__validate_select(result)
        # generate data for where clause
        self.__validate_where()
        # generate data for where clause of inner join
        self.__validate_inner_where()
        # init gloal variable for rows fetch
        self.__result = []
        self.__RowsPosInTables = []
        for n in range(len(self.__parsed_query["from"])):
            self.__RowsPosInTables.append(None)
        # prefetch rows in source tables
        self.__prefetch_get_rows()
        # fetch rows for query
        self.__get_rows(cur_idx=0)
        result["rows"] = self.__result
        del self.__result
        del self.__RowsPosInTables
        return result

    def __process_create_table(self):
        if len(self.__parsed_query["create"][0][3]) == 1: # create table with cursor
            vsess = vSession(self.db, self.__session_username, self.__password)
            cur = vsess.submit_query(_query=self.__getCursorQuery(self.__parsed_query["create"][0][3][0]))
            del vsess
            blck = {
                "table_name": self.__parsed_query["create"][0][2],
                "schema": self.__parsed_query["create"][0][1],
                "columns": cur["columns"],
                "rows": cur["rows"]
                }
        else: # create table with columns
            blck = {
                "table_name": self.__parsed_query["create"][0][2],
                "schema": self.__parsed_query["create"][0][1],
                "columns": self.__parsed_query["create"][0][3],
                "rows": []
                }
        owner = self.__parsed_query["create"][0][1]
        table_name = self.__parsed_query["create"][0][2]
        if self.db.checkTableExists(owner=owner, table_name=table_name):
            raise vExept(212)
        else:
            self.db.AddTableFile(owner=owner, table_name=table_name, table_data=blck)
            self.db.AddTableToMeta(owner=owner, table_name=table_name)
            self.db.AddTableToDB(table_data=blck)
            self.db.saveDB()

    def __process_create_user(self):
        blk = {
            "username": self.__parsed_query["create"][0][1],
            "password": self.__parsed_query["create"][0][2],
            "grants": {
                "SELECT": [],
                "INSERT": [],
                "UPDATE": [],
                "DELETE": [],
                "CREATE": [],
                "DROP": []
                }
            }
        if self.db.checkUserExists(username=self.__parsed_query["create"][0][1]):
            raise vExept(1801, self.__parsed_query["create"][0][1])
        self.db.AddAccountToMeta(account_bloc=blk)
        self.db.AddAccountToDB(account_bloc=blk)
        self.db.saveDB()

    def __process_drop_table(self):
        owner = self.__parsed_query["drop"][0][1]
        table_name = self.__parsed_query["drop"][0][2]
        if self.db.checkTableExists(owner=owner, table_name=table_name):
            self.db.DelTableFromDB(owner=owner, table_name=table_name)
            self.db.DelTableFromMeta(owner=owner, table_name=table_name)
            self.db.DelTableFile(owner=owner, table_name=table_name)
            self.db.saveDB()
        else:
            raise vExept(210, '{].{}'.format(owner, table_name))
        
    def __process_drop_user(self):
        usr = self.__parsed_query["drop"][0][1]
        if self.db.checkUserExists(username=usr):
            self.db.DelAccountFromMeta(username=usr)
            self.db.DelAccountFromDB(username=usr)
            self.db.saveDB()
        else:
            raise vExept(1800, usr)

    def __process_insert(self):
        tbl =  self.__get_table(owner=self.__parsed_query["insert"][0], table_name=self.__parsed_query["insert"][1])
        if len(self.__parsed_query["insert"][2]) > len(tbl["columns"]):
            raise vExept(310)
        col_mat = []
        if len(self.__parsed_query["insert"][2]) > 0:
            for i in range(len(self.__parsed_query["insert"][2])):
                for n in range(len(tbl["columns"])):
                    found = False
                    if self.__parsed_query["insert"][2][i].upper() == tbl["columns"][n][0].upper():
                        found = True
                        break
                if not found:
                    raise vExept(311, self.__parsed_query["insert"][2][i])
                col_mat.append([n, i])
        else:
            for i in range(len(self.__parsed_query["insert"][2])):
                col_mat.append([i, i])
        if self.__parsed_query["insert"][4] is None:
            row = []
            for n in range(len(tbl["columns"])):
                row.append(None)
            for cm in col_mat:
                row[cm[0]] = self.__format_value(value=self.__parsed_query["insert"][3][cm[1]], type_value=tbl["columns"][cm[0]][1])
            tbl["rows"].append(row)
            self.__add_updated_table(tbl)
            return 1
        else:
            vsess = vSession(self.db, self.__session_username, self.__password)
            cur = vsess.submit_query(_query=self.__getCursorQuery(self.__parsed_query['insert'][4]))
            del vsess
            if len(cur["columns"]) != len(self.__parsed_query["insert"][2]):
                raise vExept(312)
            for lgn in cur["rows"]:
                row = []
                for n in range(len(tbl["columns"])):
                    row.append(None)
                for cm in col_mat:
                    row[cm[0]] = self.__format_value(value=lgn[cm[1]], type_value=tbl["columns"][cm[0]][1])
                tbl["rows"].append(row)
            self.__add_updated_table(tbl)
            return len(cur["rows"])

    def __process_update(self):
        # generate data for where clause
        self.__validate_where()
        # redefine columns to update
        for n in range(len(self.__parsed_query["update"])):
            colin, aliasin, table_namein, schemain, memtf, memctf, ctype = self.__searchColInFromTables(colin=self.__parsed_query["update"][n][0].upper(),
                                                                                                        aliasin=self.__parsed_query["from"][0][0],
                                                                                                        table_namein=None,
                                                                                                        schemain=None)
            self.__parsed_query["update"][n][0] = [colin, aliasin, table_namein, schemain, memtf, memctf, ctype]
        col_mat = []
        for i in range(len(self.__parsed_query["update"])):
            for n in range(len(self.__parsed_query["from"][0][4][0]["columns"])):
                found = False
                if self.__parsed_query["update"][i][0][0].upper() == self.__parsed_query["from"][0][4][0]["columns"][n][0].upper():
                    found = True
                    break
            if not found:
                raise vExept(311, self.__parsed_query["update"][i][0])
            col_mat.append([n, i])
        # search rows to update
        updt_rows_cnt = 0
        self.__RowsPosInTables = [None]
        for n in range(len(self.__parsed_query["from"][0][4][0]["rows"])):
            self.__RowsPosInTables[0] = n
            if self.__process_tests():
                updt_rows_cnt += 1
                for updt in self.__parsed_query["update"]:
                    match updt[0][6]:
                        case 'int':
                            self.__parsed_query["from"][0][4][0]["rows"][n][updt[0][5]] = int(updt[2])
                        case 'float':
                            self.__parsed_query["from"][0][4][0]["rows"][n][updt[0][5]] = float(updt[2])
                        case 'str':
                            if (updt[2][0] == "'") and (updt[2][-1] == "'") or (updt[2][0] == '"') and (updt[2][-1] == '"'):
                                updt[2] = updt[2][1:-1]
                            self.__parsed_query["from"][0][4][0]["rows"][n][updt[0][5]] = str(updt[2])
                        case 'hex':
                            self.__parsed_query["from"][0][4][0]["rows"][n][updt[0][5]] = hex(updt[2])
        del self.__RowsPosInTables
        self.__add_updated_table(self.__parsed_query["from"][0][4][0])
        return updt_rows_cnt

    def __process_delete(self):
        # generate data for where clause
        self.__validate_where()
        # search rows to update
        del_rows_cnt = 0
        RowsToDel = []
        self.__RowsPosInTables = [None]
        for n in range(len(self.__parsed_query["from"][0][4][0]["rows"])):
            self.__RowsPosInTables[0] = n
            if self.__process_tests():
                del_rows_cnt += 1
                RowsToDel.append(n)
        del self.__RowsPosInTables
        while len(RowsToDel) > 0:
            del self.__parsed_query["from"][0][4][0]["rows"][RowsToDel[0]]
            del RowsToDel[0]
        self.__add_updated_table(self.__parsed_query["from"][0][4][0])
        return del_rows_cnt

    def __format_value(self, value, type_value):
        try:
            match type_value:
                case "int":
                    return int(value)
                case "hex":
                    return int(value)
                case "float":
                    return float(value)
                case "str":
                    value = str(value)
                    if ((value[0] == '"') and (value[-1] == '"')) or ((value[0] == "'") and (value[-1] == "'")):
                        return value[1:-1]
                    else:
                        return value
                case _:
                    raise vExept(2200, '"{}" as {}'.format(value, type_value))
        except vExept as e:
            raise vExept(2200, '"{}" as {}'.format(value, type_value))

    def __compare_cols(self, c1, c2, oper):
        match oper:
            case '=':
                result = bool(c1 == c2)
            case '>':
                if self.__check_FLOAT(c1) and self.__check_FLOAT(c2):
                    result = bool(float(c1) > float(c2))
                elif self.__check_INT(c1) and self.__check_INT(c2):
                    result = bool(int(c1) > int(c2))
                elif self.__check_HEX(c1) and self.__check_HEX(c2):
                    result = bool(int(c1) > int(c2))
                else:
                    result = bool(c1 > c2)
            case '>=':
                if self.__check_FLOAT(c1) and self.__check_FLOAT(c2):
                    result = bool(float(c1) >= float(c2))
                elif self.__check_INT(c1) and self.__check_INT(c2):
                    result = bool(int(c1) >= int(c2))
                elif self.__check_HEX(c1) and self.__check_HEX(c2):
                    result = bool(int(c1) >= int(c2))
                else:
                    result = bool(c1 >= c2)
            case '<>':
                result = bool(c1 != c2)
            case '!=':
                result = bool(c1 != c2)
            case '<':
                if self.__check_FLOAT(c1) and self.__check_FLOAT(c2):
                    result = bool(float(c1) < float(c2))
                elif self.__check_INT(c1) and self.__check_INT(c2):
                    result = bool(int(c1) < int(c2))
                elif self.__check_HEX(c1) and self.__check_HEX(c2):
                    result = bool(int(c1) < int(c2))
                else:
                    result = bool(c1 < c2)
            case '<=':
                if self.__check_FLOAT(c1) and self.__check_FLOAT(c2):
                    result = bool(float(c1) <= float(c2))
                elif self.__check_INT(c1) and self.__check_INT(c2):
                    result = bool(int(c1) <= int(c2))
                elif self.__check_HEX(c1) and self.__check_HEX(c2):
                    result = bool(int(c1) <= int(c2))
                else:
                    result = bool(c1 <= c2)
            case 'AND':
                result = bool(c1 and c2)
            case 'OR':
                result = bool(c1 or c2)
            case other:
                raise vExept(510, oper)
        return result

    def __check_INT(self, varin):
        if varin[0] == "'":
            varin = varin[1:len(varin)]
        if varin[-1] == "'":
            varin = varin[0:-1]
        try:
            reg = re.search('^[+-]?[0-9]+$', varin)
            return bool(reg is not None)
        except Exception as e:
            return False

    def __check_STR(self, varin):
        if (varin[0] == '"') and (varin[-1] == '"'):
            return True
        else:
            return False

    def __check_FLOAT(self, varin):
        if varin[0] == "'":
            varin = varin[1:len(varin)]
        if varin[-1] == "'":
            varin = varin[0:-1]
        try:
            reg = re.search(r"^[-+]?(\d+([.,]\d*)?|[.,]\d+)([eE][-+]?\d+)?$", varin)
            return bool(reg is not None)
        except Exception as e:
            return False

    def __check_HEX(self, varin):
        if varin[0] == "'":
            varin = varin[1:len(varin)]
        if varin[-1] == "'":
            varin = varin[0:-1]
        try:
            reg = re.search(r"^[-+]?(0[xX][\dA-Fa-f]+|0[0-7]*|\d+)$", varin)
            return bool(reg is not None)
        except Exception as e:
            return False

    def __validate_tables(self):
        # from: table_alias, schema, table_name, TABLE or CURSOR, {table}
        for n in range(len(self.__parsed_query["from"])):
            tbl = self.__parsed_query["from"][n]
            if tbl[3] == 'TABLE':
                self.__parsed_query["from"][n].append([self.__get_table(owner=tbl[1], table_name=tbl[2])])
            elif tbl[3] == 'CURSOR':
                vsess = vSession(self.db, self.__session_username, self.__password)
                result = vsess.submit_query(_query=self.__getCursorQuery(tbl[2]))
                del vsess
                self.__parsed_query["from"][n].append([result])

    def __get_table(self, owner, table_name):
        for ut in self.__updated_tables:
            if (ut['schema'] == owner) and (ut['table_name'] == table_name):
                # print('table locale: ', table_name)
                return copy.deepcopy(ut)
        # print('table de la base: ', table_name)
        return copy.deepcopy(self.db.getTable(owner=owner, table_name=table_name))

    def __add_updated_table(self, tbl):
        found = False
        for n in range(len(self.__updated_tables)):
            if (self.__updated_tables[n]['schema'] == tbl['schema']) and (self.__updated_tables[n]['table_name'] == tbl['table_name']):
                self.__updated_tables[n] = copy.deepcopy(tbl)
                found = True
                break
        if not found:
            self.__updated_tables.append(copy.deepcopy(tbl))

    def __validate_select(self, result):
        # select: table_alias, schema, table_name, col_name/value, alias, type(COL, INT, FLOAT, STR, HEX, FUNCTION), table position, position in table, table or cursor, [parameters of function]
        chk = True
        cs = 0
        while chk:
            if self.__parsed_query["select"][cs][3] == '*':
                col_mat = self.__searchColsInFromTables(colin=self.__parsed_query["select"][cs][3],
                                                        aliasin=self.__parsed_query["select"][cs][0],
                                                        table_namein=self.__parsed_query["select"][cs][2],
                                                        schemain=self.__parsed_query["select"][cs][1])
                # [colin, aliasin, table_namein, schemain, memtf, memctf, ctype, tab_cur]
                del self.__parsed_query["select"][cs]
                for cm in range(len(col_mat)):
                    self.__parsed_query["select"].insert(cs+cm, [col_mat[cm][1], col_mat[cm][3], col_mat[cm][2], col_mat[cm][0], None, 'COLUMN', col_mat[cm][4], col_mat[cm][5], col_mat[cm][7], []])
                    result["columns"].append([col_mat[cm][0], col_mat[cm][6]])
                cs += len(col_mat)
            else:
                if self.__parsed_query["select"][cs][5] == 'COLUMN':
                    colin, aliasin, table_namein, schemain, tf, ctf, ctype = self.__searchColInFromTables(colin=self.__parsed_query["select"][cs][3],
                                                                                                          aliasin=self.__parsed_query["select"][cs][0],
                                                                                                          table_namein=self.__parsed_query["select"][cs][2],
                                                                                                          schemain=self.__parsed_query["select"][cs][1])
                    self.__parsed_query["select"][cs][6] = tf
                    self.__parsed_query["select"][cs][7] = ctf
                    self.__parsed_query["select"][cs][0] = aliasin
                    self.__parsed_query["select"][cs][1] = schemain
                    self.__parsed_query["select"][cs][2] = table_namein
                    if self.__parsed_query["select"][cs][4] is None:
                        result["columns"].append([self.__parsed_query["select"][cs][3], ctype])
                    else:
                        result["columns"].append([self.__parsed_query["select"][cs][4], ctype])
                else:
                    if self.__parsed_query["select"][cs][4] is None:
                        result["columns"].append([self.__parsed_query["select"][cs][3], self.__parsed_query["select"][cs][5]])
                    else:
                        result["columns"].append([self.__parsed_query["select"][cs][4], self.__parsed_query["select"][cs][5]])
                cs += 1
            if cs >= len(self.__parsed_query["select"]):
                chk = False
        return result

    def __validate_where(self):
        for n in range (len(self.__parsed_query["parsed_where"])):
            if (self.__parsed_query["parsed_where"][n][1][0] == "TST") and (self.__parsed_query["parsed_where"][n][1][5] == 'COLUMN'):
                _, aliasin, table_namein, schemain, tf, ctf, ctype = self.__searchColInFromTables(colin=self.__parsed_query["parsed_where"][n][1][4],
                                                                                                  aliasin=self.__parsed_query["parsed_where"][n][1][3],
                                                                                                  table_namein=self.__parsed_query["parsed_where"][n][1][7],
                                                                                                  schemain=self.__parsed_query["parsed_where"][n][1][6])
                self.__parsed_query["parsed_where"][n][1][1] = tf
                self.__parsed_query["parsed_where"][n][1][2] = ctf
                self.__parsed_query["parsed_where"][n][1][7] = table_namein
                self.__parsed_query["parsed_where"][n][1][6] = schemain
                self.__parsed_query["parsed_where"][n][1][3] = aliasin
            if (self.__parsed_query["parsed_where"][n][3][0] == "TST") and (self.__parsed_query["parsed_where"][n][3][5] == 'COLUMN'):
                _, aliasin, table_namein, schemain, tf, ctf, _ = self.__searchColInFromTables(colin=self.__parsed_query["parsed_where"][n][3][4],
                                                                                              aliasin=self.__parsed_query["parsed_where"][n][3][3],
                                                                                              table_namein=self.__parsed_query["parsed_where"][n][3][7],
                                                                                              schemain=self.__parsed_query["parsed_where"][n][3][6])
                self.__parsed_query["parsed_where"][n][3][1] = tf
                self.__parsed_query["parsed_where"][n][3][2] = ctf
                self.__parsed_query["parsed_where"][n][3][7] = table_namein
                self.__parsed_query["parsed_where"][n][3][6] = schemain
                self.__parsed_query["parsed_where"][n][3][3] = aliasin

    def __validate_inner_where(self):
        for b in range (len(self.__parsed_query["parsed_inner_where"])):
            for n in range (len(self.__parsed_query["parsed_inner_where"][b])):
                if (self.__parsed_query["parsed_inner_where"][b][n][1][0] == "TST") and (self.__parsed_query["parsed_inner_where"][b][n][1][5] == 'COLUMN'):
                    _, aliasin, _, _, tf, ctf, _ = self.__searchColInFromTables(colin=self.__parsed_query["parsed_inner_where"][b][n][1][4],
                                                                                aliasin=self.__parsed_query["parsed_inner_where"][b][n][1][3],
                                                                                table_namein=self.__parsed_query["parsed_inner_where"][b][n][1][7],
                                                                                schemain=self.__parsed_query["parsed_inner_where"][b][n][1][6])
                    self.__parsed_query["parsed_inner_where"][b][n][1][1] = tf
                    self.__parsed_query["parsed_inner_where"][b][n][1][2] = ctf
                    self.__parsed_query["parsed_inner_where"][b][n][1][3] = aliasin
                if (self.__parsed_query["parsed_inner_where"][b][n][3][0] == "TST") and (self.__parsed_query["parsed_inner_where"][b][n][3][5] == 'COLUMN'):
                    _, aliasin, _, _, tf, ctf, _ = self.__searchColInFromTables(colin=self.__parsed_query["parsed_inner_where"][b][n][3][4],
                                                                                aliasin=self.__parsed_query["parsed_inner_where"][b][n][3][3],
                                                                                table_namein=self.__parsed_query["parsed_inner_where"][b][n][3][7],
                                                                                schemain=self.__parsed_query["parsed_inner_where"][b][n][3][6])
                    self.__parsed_query["parsed_inner_where"][b][n][3][1] = tf
                    self.__parsed_query["parsed_inner_where"][b][n][3][2] = ctf
                    self.__parsed_query["parsed_inner_where"][b][n][3][3] = aliasin

    def __getCursorQuery(self, cur_name):
        # cursors: cursor_alias, query
        count = 0
        for n in range(len(self.__parsed_query['cursors'])):
            c = self.__parsed_query['cursors'][n]
            if c[0] == cur_name:
                count += 1
                result = c[1]
        if count == 0:
            raise vExept(2100, cur_name)
        elif count == 1:
            return result
        else:
            raise vExept(2101, cur_name)

    def __get_grant_for_object(self, owner, obj_name, grant_needed, admin='NO'):
        # print(owner, obj_name, grant_needed, admin)
        if ((owner is not None) and (owner.upper() == str(self.current_schema).upper())) or (str(self.current_schema).upper() == 'ADMIN'):
            result = True
        else:
            result = False
            for n in range(len(self.db.db["Accounts"])):
                account = self.db.db["Accounts"][n]
                if str(account["username"]).upper() == str(self.current_schema).upper():
                    grants = account["grants"]
                    break
            match str(grant_needed).upper():
                case 'SELECT' | 'INSERT' | 'UPDATE' | 'DELETE':
                    for grant in grants[str(grant_needed).upper()]:
                        if (((grant[1] == str(owner).upper()) and (grant[0] == 'SCHEMA')) or \
                           ((grant[1] == '{}.{}'.format(owner, obj_name).upper()) and (grant[0] == 'TABLE'))) and \
                           (admin == 'NO' or admin == grant[2]):
                            result = True
                            break
                case 'CREATE' | 'DROP' :
                    for grant in grants[str(grant_needed).upper()]:
                        match grant[0]:
                            case 'TABLE' | 'INDEX':
                                if (obj_name == grant[0]) and \
                                   ((owner is not None) and (grant[1] == owner.upper())) and \
                                   (admin == 'NO' or admin == grant[2]):
                                    result = True
                                    break
                            case 'USER':
                                if (obj_name == grant[0]) and \
                                   (owner is None) and \
                                   (admin == 'NO' or admin == grant[1]):
                                    result = True
                                    break
        return result
