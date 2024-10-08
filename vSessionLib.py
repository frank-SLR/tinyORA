import re
import copy
import random
from datetime import datetime
from vExceptLib import vExcept
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
            raise vExcept(109)
        self.session_id = hex(random.randint(1, 9999999999999999999))
        self.__password = password
        self.__session_username = username
        self.__parsed_query = None
        # self.__parsed_query_flg = False
        self.__updated_tables = []
        super().__init__()

    def submit_query(self, _query:str, bind:dict = []):
        self.__parsed_query = vParser().parse_query(query=_query, bind=bind)
        # print(self.__parsed_query['select'])
        # print(self.__parsed_query['from'])
        # print(self.__parsed_query['where'])
        # print(self.__parsed_query['parsed_where'])
        # print(self.__parsed_query['in'])
        # print(self.__parsed_query['functions'])
        # print(self.__parsed_query['connect'])
        # print(self.__parsed_query['maths'])
        # print(self.__parsed_query['pipe'])
        # print(self.__parsed_query['bind'])

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
                                raise vExcept(1900, '{}.{}'.format(tbl[1], tbl[2]))
        elif self.__parsed_query["querytype"] in ['INSERT']:
            lock_flg = True
            while lock_flg:
                lock_val = self.db.add_lock(session_id=self.session_id, owner=self.__parsed_query["insert"][0], name=self.__parsed_query["insert"][1], lock_type=1)
                match lock_val:
                    case 0:
                        lock_flg = False
                    case 1:
                        raise vExcept(1900, '{}.{}'.format(self.__parsed_query["insert"][0], self.__parsed_query["insert"][1]))
        if self.__parsed_query["querytype"] in ['UPDATE', 'DELETE']:
            tbl = self.__parsed_query["from"][0]
            lock_flg = True
            while lock_flg:
                lock_val = self.db.add_lock(session_id=self.session_id, owner=tbl[1], name=tbl[2], lock_type=1)
                match lock_val:
                    case 0:
                        lock_flg = False
                    case 1:
                        raise vExcept(1900, '{}.{}'.format(tbl[1], tbl[2]))
        elif self.__parsed_query["querytype"] in ['DROP']:
            if self.__parsed_query["drop"][0][0] == 'TABLE':
                lock_flg = True
                while lock_flg:
                    lock_val = self.db.add_lock(session_id=self.session_id, owner=self.__parsed_query["drop"][0][1], name=self.__parsed_query["drop"][0][2], lock_type=0)
                    match lock_val:
                        case 0:
                            lock_flg = False
                        case 1:
                            raise vExcept(1900, '{}.{}'.format(self.__parsed_query["drop"][0][1], self.__parsed_query["drop"][0][2]))
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
                                    raise vExcept(1900, '{}.{}'.format(TAB["schema"], TAB["table_name"]))

        # load tables
        if self.__parsed_query["querytype"] in ['SELECT', 'DESCRIBE', 'UPDATE', 'DELETE']:
            self.__validate_tables()

        # check GRANT
        if self.__parsed_query["querytype"] in ['SELECT', 'DESCRIBE']:
            for n in range(len(self.__parsed_query["from"])):
                tbl = self.__parsed_query["from"][n]
                if tbl[3] == 'TABLE':
                    if not self.__get_grant_for_object(owner=tbl[1], obj_name=tbl[2], grant_needed='SELECT'):
                        raise vExcept(210, '{}.{}'.format(tbl[1], tbl[2]))
        elif self.__parsed_query["querytype"] in ['GRANT']:
            grt = self.__parsed_query["grant"][0]
            match grt[1]:
                case 'SELECT' | 'INSERT' | 'UPDATE' | 'DELETE':
                    o_t = grt[3].split('.')
                    if len(o_t) == 1:
                        if not self.__get_grant_for_object(owner=grt[3], obj_name=None, grant_needed=grt[1], admin='YES'):
                            raise vExcept(900)
                    else:
                        if not self.__get_grant_for_object(owner=o_t[0], obj_name=o_t[1], grant_needed=grt[1], admin='YES'):
                            raise vExcept(900)
                    self.db.AddGrantToMeta(grant=grt[1], granted=str(grt[0]).lower(), grant_bloc=[grt[2], grt[3], grt[4]])
                case 'CREATE' | 'DROP':
                    match grt[2]:
                        case 'TABLE' | 'INDEX':
                            if not self.__get_grant_for_object(owner=grt[3], obj_name=grt[2], grant_needed=grt[1], admin='YES'):
                                    raise vExcept(900)
                            self.db.AddGrantToMeta(grant=grt[1], granted=grt[0], grant_bloc=[grt[2], grt[3], grt[4]])
                        case 'USER':
                            if not self.__get_grant_for_object(owner=None, obj_name=grt[3], grant_needed=grt[1], admin='YES'):
                                    raise vExcept(900)
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
                            raise vExcept(900)
                    else:
                        if not self.__get_grant_for_object(owner=o_t[0], obj_name=o_t[1], grant_needed=grt[1], admin='YES'):
                            raise vExcept(900)
                    self.db.DelGrantFromMeta(grant=grt[1], granted=str(grt[0]).lower(), grant_bloc=[grt[2], grt[3]])
                case 'CREATE' | 'DROP':
                    match grt[2]:
                        case 'TABLE' | 'INDEX':
                            if not self.__get_grant_for_object(owner=grt[3], obj_name=grt[2], grant_needed=grt[1], admin='YES'):
                                    raise vExcept(900)
                            self.db.DelGrantFromMeta(grant=grt[1], granted=grt[0], grant_bloc=[grt[2], grt[3]])
                        case 'USER':
                            if not self.__get_grant_for_object(owner=None, obj_name=grt[3], grant_needed=grt[1], admin='YES'):
                                    raise vExcept(900)
                            self.db.DelGrantFromMeta(grant=grt[1], granted=grt[0], grant_bloc=[grt[2]])
            self.db.saveDB()
            result = {"message": "Revoke processed"}
        elif self.__parsed_query["querytype"] in ['CREATE']:
            if self.__parsed_query["create"][0][0] == 'TABLE':
                if self.__parsed_query["create"][0][1] is None:
                    self.__parsed_query["create"][0][1] = self.current_schema
                if self.__parsed_query["create"][0][1] != self.current_schema:
                    if not self.__get_grant_for_object(owner=self.__parsed_query["create"][0][1], obj_name='TABLE', grant_needed='CREATE'):
                        raise vExcept(901)
            elif self.__parsed_query["create"][0][0] == 'USER':
                if not self.__get_grant_for_object(owner=None, obj_name='USER', grant_needed='CREATE'):
                    raise vExcept(901)
        elif self.__parsed_query["querytype"] in ['DROP']:
            if self.__parsed_query["drop"][0][0] == 'TABLE':
                if self.__parsed_query["drop"][0][1] is None:
                    self.__parsed_query["drop"][0][1] = self.current_schema
                if self.__parsed_query["drop"][0][1] != self.current_schema:
                    if not self.__get_grant_for_object(owner=self.__parsed_query["drop"][0][1], obj_name='TABLE', grant_needed='DROP'):
                        raise vExcept(902)
            elif self.__parsed_query["drop"][0][0] == 'USER':
                if not self.__get_grant_for_object(owner=None, obj_name='USER', grant_needed='DROP'):
                    raise vExcept(902)
        elif self.__parsed_query["querytype"] in ['INSERT']:
            if self.__parsed_query["insert"][0] is None:
                self.__parsed_query["insert"][0] = self.current_schema
            u_name=self.__parsed_query["insert"][0]
            t_name=self.__parsed_query["insert"][1]
            if not self.__get_grant_for_object(owner=u_name, obj_name=t_name, grant_needed='INSERT'):
                raise vExcept(210, '{}.{}'.format(u_name, t_name))
        elif self.__parsed_query["querytype"] in ['UPDATE']:
            if self.__parsed_query["from"][0][1] is None:
                self.__parsed_query["from"][0][1] = self.current_schema
            u_name=self.__parsed_query["from"][0][1]
            t_name=self.__parsed_query["from"][0][2]
            if not self.__get_grant_for_object(owner=u_name, obj_name=t_name, grant_needed='UPDATE'):
                raise vExcept(210, '{}.{}'.format(u_name, t_name))
        elif self.__parsed_query["querytype"] in ['DELETE']:
            if self.__parsed_query["from"][0][1] is None:
                self.__parsed_query["from"][0][1] = self.current_schema
            u_name=self.__parsed_query["from"][0][1]
            t_name=self.__parsed_query["from"][0][2]
            if not self.__get_grant_for_object(owner=u_name, obj_name=t_name, grant_needed='DELETE'):
                raise vExcept(210, '{}.{}'.format(u_name, t_name))

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
        # print('__searchColInFromTables in', colin, aliasin, table_namein, schemain)
        for tf in range(len(self.__parsed_query["from"])):
            # print(self.__parsed_query["from"][tf][0:3])
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
                        # print('__searchColInFromTables trouve:', colin, aliasin, table_namein, schemain, memtf, memctf, ctype)
                        break
        if count == 0:
            raise vExcept(311, colin)
        elif count == 1:
            return colin, aliasin, table_namein, schemain, memtf, memctf, ctype
        else:
            raise vExcept(313, colin)

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
            raise vExcept(311, colin)
        elif count == 1:
            return result
        else:
            raise vExcept(313, colin)

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
                        elif s[5] == 'DATETIME':
                            rrow.append(s[3])
                        elif s[5] == 'STR':
                            rrow.append(self.__remove_quote(s[3]).replace("''", "'"))
                        elif s[5] == 'FUNCTION':
                            rrow.append(self.__compute_function(s[3]))
                        elif s[5] == 'MATHS':
                            rrow.append(self.__compute_maths(s[3]))
                        elif s[5] == 'PIPE':
                            rrow.append(self.__remove_quote(self.__compute_pipe(s[3])).replace("''", "'"))
                        else:
                            raise vExcept(801)
                    self.__result.append(rrow)

    def __get_function(self, fct_id):
        for n in range(len(self.__parsed_query["functions"])):
            if self.__parsed_query["functions"][n][0] == fct_id:
                return n
        raise vExcept(802, fct_id)

    def __get_maths(self, maths_id):
        for n in range(len(self.__parsed_query["maths"])):
            if self.__parsed_query["maths"][n][0] == maths_id:
                return n
        raise vExcept(804, maths_id)

    def __get_pipe(self, pipe_id):
        for n in range(len(self.__parsed_query["pipe"])):
            if self.__parsed_query["pipe"][n][0] == pipe_id:
                return n
        raise vExcept(805, pipe_id)

    def __get_in(self, in_id):
        for n in range(len(self.__parsed_query["in"])):
            if self.__parsed_query["in"][n][0] == in_id:
                return n
        raise vExcept(803, in_id)

    def __remove_quote(self, strin):
        if self.__check_STR(strin) and (len(strin) >= 2):
            if (strin[0] == '"' and strin[-1] == '"') or (strin[0] == "'" and strin[-1] == "'"):
                strin = strin[1:-1]
        return strin

    def __convert_value(self, varin, fmtin:str):
        try:
            match fmtin.upper():
                case 'INT'|'FLOAT'|'HEX'|'DATETIME':
                    if self.__check_STR(varin):
                        if varin[0] in ['"', "'"]:
                            varin = varin[1:]
                        if varin[-1] in ['"', "'"]:
                            varin = varin[:-1]
            match fmtin.upper():
                case 'INT':
                    return int(varin)
                case 'FLOAT':
                    return float(varin)
                case 'STR':
                    return str(varin)
                case 'HEX':
                    return hex(varin)
                case 'DATETIME':
                    return datetime(varin)
        except vExcept as e:
            raise vExcept(2200, "convert {} into '{}'".format(fmtin, fmtin))
        raise vExcept(2201, fmtin)

    def __check_cols_name(self, result):
        """If multiple columns have the same name, tey are renamed

        Args:
            result (_type_): _description_

        Returns:
            _type_: _description_
        """        
        for x, name1 in enumerate(result["columns"]):
            cpt = 1
            for y, name2 in enumerate(result["columns"]):
                if y > x:
                    if name1[0].upper() == name2[0].upper():
                        result["columns"][y][0] = f'{result["columns"][y][0]}_{cpt}'
                        cpt += 1
        return result

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
                # print ('__prefetch_get_rows', self.__parsed_query["from"][cur_idx][2], Validate_prefetch_process)
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
                    raise vExcept(899, tst)
                if len(tst) == 5:
                    if tst[3][0] != tst[4][0]:
                        raise vExcept(899, tst)
                if tst[1][0] == "TST":
                    if tst[1][5] == "COLUMN":
                        c1 = self.__parsed_query["from"][tst[1][1]][4][0]["rows"][self.__RowsPosInTables[tst[1][1]]][tst[1][2]]
                    elif tst[1][5] == "FUNCTION":
                        c1 = self.__compute_function(tst[1][4])
                    elif tst[1][5] == 'MATHS':
                        c1 = self.__compute_maths(tst[1][4])
                    elif tst[1][5] == 'PIPE':
                        c1 = self.__compute_pipe(tst[1][4])
                    else:
                        c1 = tst[1][4]
                    if tst[3][5] == "COLUMN":
                        c2 = self.__parsed_query["from"][tst[3][1]][4][0]["rows"][self.__RowsPosInTables[tst[3][1]]][tst[3][2]]
                    elif tst[3][5] == "FUNCTION":
                        c2 = self.__compute_function(tst[3][4])
                    elif tst[3][5] == 'MATHS':
                        c2 = self.__compute_maths(tst[3][4])
                    elif tst[3][5] == 'PIPE':
                        c2 = self.__compute_pipe(tst[3][4])
                    else:
                        c2 = tst[3][4]
                    tstoper = tst[2]
                    if len(tst) == 5:
                        if tst[4][5] == "COLUMN":
                            c3 = self.__parsed_query["from"][tst[4][1]][4][0]["rows"][self.__RowsPosInTables[tst[4][1]]][tst[4][2]]
                        elif tst[4][5] == "FUNCTION":
                            c3 = self.__compute_function(tst[4][4])
                        elif tst[4][5] == 'MATHS':
                            c3 = self.__compute_maths(tst[4][4])
                        elif tst[4][5] == 'PIPE':
                            c3 = self.__compute_pipe(tst[4][4])
                        else:
                            c3 = tst[4][4]
                        result = self.__compare_cols(str(c1), str(c2), '>=') and self.__compare_cols(str(c1), str(c3), '<=')
                    elif tstoper == 'IN':
                        in_id = self.__get_in(c2)
                        result = False
                        for mbr in self.__parsed_query["in"][in_id][2]:
                            if c1 == mbr[3]:
                                result = True
                                break
                    else:
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
                        raise vExcept(899, tst)
                    if tst[1][0] == "TST":
                        if tst[1][5] == "COLUMN":
                            c1 = self.__parsed_query["from"][tst[1][1]][4][0]["rows"][self.__RowsPosInTables[tst[1][1]]][tst[1][2]]
                        elif tst[1][5] == "FUNCTION":
                            c1 = self.__compute_function(tst[1][4])
                        elif tst[1][5] == "MATHS":
                            c1 = self.__compute_maths(tst[1][4])
                        elif tst[1][5] == "PIPE":
                            c1 = self.__compute_pipe(tst[1][4])
                        else:
                            c1 = tst[1][4]
                        if tst[3][5] == "COLUMN":
                            c2 = self.__parsed_query["from"][tst[3][1]][4][0]["rows"][self.__RowsPosInTables[tst[3][1]]][tst[3][2]]
                        elif tst[3][5] == "FUNCTION":
                            c2 = self.__compute_function(tst[3][4])
                        elif tst[3][5] == "MATHS":
                            c2 = self.__compute_maths(tst[3][4])
                        elif tst[3][5] == "PIPE":
                            c2 = self.__compute_pipe(tst[3][4])
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
                    raise vExcept(899, tst)
                if tst[1][0] == "TST":
                    if tst[1][5] == "COLUMN":
                        if tst[1][1] == tab_num:
                            c1 = self.__parsed_query["from"][tst[1][1]][4][0]["rows"][self.__RowsPosInTables[tst[1][1]]][tst[1][2]]
                    elif tst[1][5] == "FUNCTION":
                        c1 = self.__compute_function(tst[1][4])
                    else:
                        c1 = tst[1][4]
                    if tst[3][5] == "COLUMN":
                        if tst[3][1] == tab_num:
                            c2 = self.__parsed_query["from"][tst[3][1]][4][0]["rows"][self.__RowsPosInTables[tst[3][1]]][tst[3][2]]
                    elif tst[3][5] == "FUNCTION":
                        c2 = self.__compute_function(tst[3][4])
                    else:
                        c2 = tst[3][4]
                    tstoper = tst[2]
                    if len(tst) == 5:
                        if tst[4][5] == "COLUMN":
                            if tst[4][1] == tab_num:
                                c3 = self.__parsed_query["from"][tst[4][1]][4][0]["rows"][self.__RowsPosInTables[tst[4][1]]][tst[4][2]]
                        elif tst[4][5] == "FUNCTION":
                            if tst[4][1] == tab_num:
                                c3 = self.__compute_function(tst[4][4])
                        else:
                            c3 = tst[4][4]
                        if (tst[1][5] == "COLUMN") and (tst[1][1] == tab_num) and (tst[3][5] != "COLUMN") or \
                        (tst[3][5] == "COLUMN") and (tst[3][1] == tab_num) and (tst[1][5] != "COLUMN") or \
                        (tst[1][5] == "COLUMN") and (tst[1][1] == tab_num) and (tst[3][5] == "COLUMN") and (tst[3][1] == tab_num):
                            result = self.__compare_cols(str(c1), str(c2), '>=')
                            if (tst[1][5] == "COLUMN") and (tst[1][1] == tab_num) and (tst[4][5] != "COLUMN") or \
                            (tst[4][5] == "COLUMN") and (tst[4][1] == tab_num) and (tst[1][5] != "COLUMN") or \
                            (tst[1][5] == "COLUMN") and (tst[1][1] == tab_num) and (tst[4][5] == "COLUMN") and (tst[4][1] == tab_num):
                                result = result and self.__compare_cols(str(c1), str(c3), '<=')
                            else:
                                result = None
                        else:
                            result = None
                    elif tstoper == 'IN':
                        in_id = self.__get_in(c2)
                        result = False
                        for mbr in self.__parsed_query["in"][in_id][2]:
                            if c1 == mbr[3]:
                                result = True
                                break
                    else:
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
        # generate columns for functions
        self.__validate_function()
        # generate columns for maths
        self.__validate_maths()
        # generate columns for pipe
        self.__validate_pipe()
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
        self.__check_cols_name(result=result)
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
                "table_name": self.__parsed_query["create"][0][2].upper(),
                "schema": self.__parsed_query["create"][0][1].upper(),
                "columns": cur["columns"],
                "rows": cur["rows"]
                }
        else: # create table with columns
            blck = {
                "table_name": self.__parsed_query["create"][0][2].upper(),
                "schema": self.__parsed_query["create"][0][1].upper(),
                "columns": self.__parsed_query["create"][0][3],
                "rows": []
                }
        # upcase for columns name
        for n in range(len(blck["columns"])):
            blck["columns"][n][0] = str(blck["columns"][n][0]).upper()
        owner = self.__parsed_query["create"][0][1]
        table_name = self.__parsed_query["create"][0][2]
        if self.db.checkTableExists(owner=owner, table_name=table_name):
            raise vExcept(212)
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
            raise vExcept(1801, self.__parsed_query["create"][0][1])
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
            raise vExcept(210, '{].{}'.format(owner, table_name))

    def __process_drop_user(self):
        usr = self.__parsed_query["drop"][0][1]
        if self.db.checkUserExists(username=usr):
            self.db.DelAccountFromMeta(username=usr)
            self.db.DelAccountFromDB(username=usr)
            self.db.saveDB()
        else:
            raise vExcept(1800, usr)

    def __process_insert(self):
        tbl =  self.__get_table(owner=self.__parsed_query["insert"][0], table_name=self.__parsed_query["insert"][1])
        if len(self.__parsed_query["insert"][2]) > len(tbl["columns"]):
            raise vExcept(310)
        col_mat = []
        if len(self.__parsed_query["insert"][2]) > 0:
            for i in range(len(self.__parsed_query["insert"][2])):
                for n in range(len(tbl["columns"])):
                    found = False
                    if self.__parsed_query["insert"][2][i].upper() == tbl["columns"][n][0].upper():
                        found = True
                        break
                if not found:
                    raise vExcept(311, self.__parsed_query["insert"][2][i])
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
                raise vExcept(312)
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
                raise vExcept(311, self.__parsed_query["update"][i][0])
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
                        case 'datetime':
                            self.__parsed_query["from"][0][4][0]["rows"][n][updt[0][5]] = updt[2]
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
                case "datetime":
                    return value
                case "str":
                    value = str(value)
                    if ((value[0] == '"') and (value[-1] == '"')) or ((value[0] == "'") and (value[-1] == "'")):
                        return value[1:-1]
                    else:
                        return value
                case _:
                    raise vExcept(2200, '"{}" as {}'.format(value, type_value))
        except vExcept as e:
            raise vExcept(2200, '"{}" as {}'.format(value, type_value))

    def __compare_cols(self, c1, c2, oper):
        if self.__check_STR(c1):
            if ((c1[0] == '"') and (c1[-1] == '"')) or ((c1[0] == "'") and (c1[-1] == "'")):
                c1 = c1[1:-1]
        if self.__check_STR(c2):
            if ((c2[0] == '"') and (c2[-1] == '"')) or ((c2[0] == "'") and (c2[-1] == "'")):
                c2 = c2[1:-1]
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
                elif self.__check_DATETIME(c1) and self.__check_DATETIME(c2):
                    result = bool(c1 > c2)
                else:
                    result = bool(c1 > c2)
            case '>=':
                if self.__check_FLOAT(c1) and self.__check_FLOAT(c2):
                    result = bool(float(c1) >= float(c2))
                elif self.__check_INT(c1) and self.__check_INT(c2):
                    result = bool(int(c1) >= int(c2))
                elif self.__check_HEX(c1) and self.__check_HEX(c2):
                    result = bool(int(c1) >= int(c2))
                elif self.__check_DATETIME(c1) and self.__check_DATETIME(c2):
                    result = bool(c1 >= c2)
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
                elif self.__check_DATETIME(c1) and self.__check_DATETIME(c2):
                    result = bool(c1 < c2)
                else:
                    result = bool(c1 < c2)
            case '<=':
                if self.__check_FLOAT(c1) and self.__check_FLOAT(c2):
                    result = bool(float(c1) <= float(c2))
                elif self.__check_INT(c1) and self.__check_INT(c2):
                    result = bool(int(c1) <= int(c2))
                elif self.__check_HEX(c1) and self.__check_HEX(c2):
                    result = bool(int(c1) <= int(c2))
                elif self.__check_DATETIME(c1) and self.__check_DATETIME(c2):
                    result = bool(c1 <= c2)
                else:
                    result = bool(c1 <= c2)
            case 'AND':
                result = bool(c1 and c2)
            case 'OR':
                result = bool(c1 or c2)
            case other:
                raise vExcept(510, oper)
        return result

    def __check_INT(self, varin):
        try:
            return isinstance(int(varin), int)
        except Exception as e:
            return False

    def __check_STR(self, varin):
        return isinstance(varin, str)

    def __check_FLOAT(self, varin):
        try:
            return isinstance(float(varin), float)
        #     reg = re.search(r"^[-+]?(\d+([.,]\d*)?|[.,]\d+)([eE][-+]?\d+)?$", varin)
        #     return bool(reg is not None)
        except Exception as e:
            return False

    def __check_HEX(self, varin):
        try:
            reg = re.search(r"^[-+]?(0[xX][\dA-Fa-f]+|0[0-7]*|\d+)$", varin)
            return bool(reg is not None)
        except Exception as e:
            return False

    def __check_DATETIME(self, varin):
        try:
            reg = datetime.datetime.fromtimestamp(varin, tz=None)
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
        tbl = copy.deepcopy(self.db.getTable(owner=owner, table_name=table_name))
        if len(self.__parsed_query['connect']) == 2:
            if self.__parsed_query['connect'][0] == '<':
                for n in range(self.__parsed_query['connect'][1]-1):
                    tbl["rows"].append([n+1, None])
            else:
                for n in range(self.__parsed_query['connect'][1]):
                    tbl["rows"].append([n+1, None])
        return tbl

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
        # select format: 
        #   0: table_alias
        #   1: schema
        #   2: table_name
        #   3: col_name/value
        #   4: alias
        #   5: type(COL, INT, FLOAT, STR, HEX, FUNCTION, MATHS, PIPE)
        #   6: table position
        #   7: position in table
        #   8: table or cursor
        #   9: [-]
        chk = True
        cs = 0
        while chk:
            # print('ckh', cs, self.__parsed_query["select"][cs])
            if self.__parsed_query["select"][cs][3] == '*':
                col_mat = self.__searchColsInFromTables(colin=self.__parsed_query["select"][cs][3],
                                                        aliasin=self.__parsed_query["select"][cs][0],
                                                        table_namein=self.__parsed_query["select"][cs][2],
                                                        schemain=self.__parsed_query["select"][cs][1])
                # print(f'__validate_select self.__parsed_query["select"][cs]={self.__parsed_query["select"][cs]}')
                # print(f'__validate_select col_mat={col_mat}')
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
                    # print('ckh0', cs, self.__parsed_query["select"][cs])
                    self.__parsed_query["select"][cs][6] = tf
                    self.__parsed_query["select"][cs][7] = ctf
                    self.__parsed_query["select"][cs][0] = aliasin
                    self.__parsed_query["select"][cs][1] = schemain
                    self.__parsed_query["select"][cs][2] = table_namein
                    if self.__parsed_query["select"][cs][4] is None:
                        result["columns"].append([self.__parsed_query["select"][cs][3], ctype])
                    else:
                        result["columns"].append([self.__parsed_query["select"][cs][4], ctype])
                elif self.__parsed_query["select"][cs][5] == 'FUNCTION':
                    fct_id = self.__get_function(self.__parsed_query["select"][cs][3])
                    fct_type = self.__get_function_type(self.__parsed_query['functions'][fct_id][1], self.__parsed_query['functions'][fct_id][2][0][4])
                    if self.__parsed_query["select"][cs][4] is None:
                        result["columns"].append([self.__parsed_query["select"][cs][3], fct_type])
                    else:
                        result["columns"].append([self.__parsed_query["select"][cs][4], fct_type])
                elif self.__parsed_query["select"][cs][5] == 'PIPE':
                    if self.__parsed_query["select"][cs][4] is None:
                        result["columns"].append([self.__parsed_query["select"][cs][3], 'str'])
                    else:
                        result["columns"].append([self.__parsed_query["select"][cs][4], 'str'])
                elif self.__parsed_query["select"][cs][5] == 'MATHS':
                    if self.__parsed_query["select"][cs][4] is None:
                        result["columns"].append([self.__parsed_query["select"][cs][3], 'float'])
                    else:
                        result["columns"].append([self.__parsed_query["select"][cs][4], 'float'])
                else:
                    if self.__parsed_query["select"][cs][4] is None:
                        result["columns"].append([self.__parsed_query["select"][cs][3], self.__parsed_query["select"][cs][5]])
                    else:
                        result["columns"].append([self.__parsed_query["select"][cs][4], self.__parsed_query["select"][cs][5]])
                cs += 1
            if cs >= len(self.__parsed_query["select"]):
                chk = False
        return result

    def __validate_maths(self):
        for n in range(len(self.__parsed_query["maths"])):
            for m in range(len(self.__parsed_query["maths"][n][2])):
                cblk = self.__parsed_query["maths"][n][2][m]
                if (len(cblk) == 2) and (cblk[1][5] == 'COLUMN'):
                    colin, aliasin, table_namein, schemain, tf, ctf, ctype = self.__searchColInFromTables(colin=cblk[1][4],
                                                                                                          aliasin=cblk[1][1],
                                                                                                          table_namein=cblk[1][3],
                                                                                                          schemain=cblk[1][2])
                    self.__parsed_query["maths"][n][2][m][1][1] = aliasin
                    self.__parsed_query["maths"][n][2][m][1][2] = schemain
                    self.__parsed_query["maths"][n][2][m][1][3] = table_namein
                    self.__parsed_query["maths"][n][2][m][1][6] = tf
                    self.__parsed_query["maths"][n][2][m][1][7] = ctf

    def __validate_pipe(self):
        # pipe : pipe_id, [[
            # 0: table_alias
            # 1: schema
            # 2: table_name
            # 3: col_name/value
            # 4: alias
            # 5: type(COL, INT, FLOAT, STR, HEX, DATETIME, FUNCTION, MATHS, PIPE)
            # 6: table position
            # 7: position in table
            # 8: table or cursor]]
        for n in range(len(self.__parsed_query["pipe"])):
            for m in range(len(self.__parsed_query["pipe"][n][1])):
                cblk = self.__parsed_query["pipe"][n][1][m]
                if (len(cblk) == 9) and (cblk[5] == 'COLUMN'):
                    colin, aliasin, table_namein, schemain, tf, ctf, ctype = self.__searchColInFromTables(colin=cblk[3],
                                                                                                          aliasin=cblk[0],
                                                                                                          table_namein=cblk[2],
                                                                                                          schemain=cblk[1])
                    # print(f'__validate_pipe colin={colin}, aliasin={aliasin}, table_namein={table_namein}, schemain={schemain}, tf={tf}, ctf={ctf}, ctype={ctype}')
                    self.__parsed_query["pipe"][n][1][m][0] = aliasin
                    self.__parsed_query["pipe"][n][1][m][1] = schemain
                    self.__parsed_query["pipe"][n][1][m][2] = table_namein
                    self.__parsed_query["pipe"][n][1][m][6] = tf
                    self.__parsed_query["pipe"][n][1][m][7] = ctf

    def __validate_function(self):
        # # functions format:
        #   0: fct_id
        #   1: fct_name
        #   2: [
        #     0: table_alias
        #     1: schema
        #     2: table_name
        #     3: col_name/value
        #     4: type(COLUMN, INT, FLOAT, STR, HEX, DATETIME, FUNCTION, MATHS, PIPE)
        #     5: table position
        #     6: position in table
        #     7: table or cursor
        for n in range(len(self.__parsed_query["functions"])):
            for m in range(len(self.__parsed_query["functions"][n][2])):
                cblk = self.__parsed_query["functions"][n][2][m]
                if cblk[4] == 'COLUMN':
                    colin, aliasin, table_namein, schemain, tf, ctf, ctype = self.__searchColInFromTables(colin=cblk[3],
                                                                                                          aliasin=cblk[0],
                                                                                                          table_namein=cblk[2],
                                                                                                          schemain=cblk[1])
                    self.__parsed_query["functions"][n][2][m][0] = aliasin
                    self.__parsed_query["functions"][n][2][m][1] = schemain
                    self.__parsed_query["functions"][n][2][m][2] = table_namein
                    self.__parsed_query["functions"][n][2][m][5] = tf
                    self.__parsed_query["functions"][n][2][m][6] = ctf

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
            raise vExcept(2100, cur_name)
        elif count == 1:
            return result
        else:
            raise vExcept(2101, cur_name)

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
                    if str(grant_needed).upper() == 'SELECT' and  obj_name == 'DUAL':
                        result = True
                    else:
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

    def __compute_maths(self, maths_id):
        # maths
        #   0: (TST, META)
        #   1: table_alias
        #   2: schema
        #   3: table_name
        #   4: col_name/value
        #   5: type(COL, INT, FLOAT, STR, HEX, DATETIME, COLUMN, FUNCTION, MATHS, PIPE)
        #   6: table position
        #   7: position in table
        #   8: table or cursor
        maths_num = self.__get_maths(maths_id)
        tmp_res = []
        for blk in self.__parsed_query["maths"][maths_num][2]:
            if blk[1][0] == 'TST':
                match blk[1][5]:
                    case 'INT':
                        tmp_res.append(int(blk[1][4]))
                    case 'FLOAT':
                        tmp_res.append(float(blk[1][4]))
                    case 'FUNCTION':
                        tmp_res.append(self.__compute_function(blk[1][4]))
                    case 'PIPE':
                        tmp_res.append(self.__compute_pipe(blk[1][4]))
                    case 'COLUMN':
                        tmp_res.append(self.__parsed_query["from"][blk[1][6]][4][0]["rows"][self.__RowsPosInTables[blk[1][6]]][blk[1][7]])
            else:
                match blk[2]:
                    case '+':
                        tmp_res.append(tmp_res[blk[1][1]] + tmp_res[blk[3][1]])
                    case '-':
                        tmp_res.append(tmp_res[blk[1][1]] - tmp_res[blk[3][1]])
                    case '*':
                        tmp_res.append(tmp_res[blk[1][1]] * tmp_res[blk[3][1]])
                    case '/':
                        if tmp_res[blk[3][1]] != 0:
                            tmp_res.append(tmp_res[blk[1][1]] / tmp_res[blk[3][1]])
                        else:
                            raise vExcept(2500)
        return tmp_res[-1]

    def __compute_pipe(self, pipe_id):
        # pipe : pipe_id, [[
        # 0: table_alias
        # 1: schema
        # 2: table_name
        # 3: col_name/value
        # 4: alias
        # 5: type(COL, INT, FLOAT, STR, HEX, DATETIME, FUNCTION, MATHS, PIPE)
        # 6: table position
        # 7: position in table
        # 8: table or cursor]]
        pipe_num = self.__get_pipe(pipe_id)
        tmp_res : str = ''
        for blk in self.__parsed_query["pipe"][pipe_num][1]:
            match blk[5]:
                case 'STR':
                    tmp_res = tmp_res + self.__remove_quote(blk[3])
                case 'FUNCTION':
                    tmp_res = tmp_res + self.__remove_quote(self.__compute_function(blk[3]))
                case 'MATHS':
                    tmp_res = tmp_res + self.__compute_maths(blk[3])
                case 'COLUMN':
                    tmp_res = tmp_res + self.__remove_quote(self.__parsed_query["from"][blk[6]][4][0]["rows"][self.__RowsPosInTables[blk[6]]][blk[7]])
        return tmp_res

    def __get_function_col(self, colblk):
        if colblk[4] == 'COLUMN':
            return self.__parsed_query["from"][colblk[5]][4][0]["rows"][self.__RowsPosInTables[colblk[5]]][colblk[6]]
        elif colblk[4] == 'FUNCTION':
            return self.__compute_function(colblk[3])
        elif colblk[4] == 'MATHS':
            return self.__compute_maths(colblk[3])
        elif colblk[4] == 'PIPE':
            return self.__compute_pipe(colblk[3])
        else:
            if colblk[4] is None:
                return colblk[3]
            else:
                return self.__convert_value(colblk[3], colblk[4])

    def __compute_function(self, fct_id):
        # functions : [fct_id, fct_name, [[table_alias, schema, table_name, col_name/value, type(COL, INT, FLOAT, STR, HEX, DATETIME, FUNCTION, MATHS, PIPE), table position, position in table, table or cursor]]]
        fct_num = self.__get_function(fct_id)
        match self.__parsed_query["functions"][fct_num][1]:
            case 'UPPER':
                value = self.__get_function_col(self.__parsed_query["functions"][fct_num][2][0])
                return str(value).upper()
            case 'LOWER':
                value = self.__get_function_col(self.__parsed_query["functions"][fct_num][2][0])
                return str(value).lower()
            case 'SUBSTR':
                if len(self.__parsed_query["functions"][fct_num][2]) != 3:
                    raise vExcept(2300, len(self.__parsed_query["functions"][fct_num][2]))
                strin = self.__remove_quote(self.__get_function_col(self.__parsed_query["functions"][fct_num][2][0]))
                sttin = int(self.__get_function_col(self.__parsed_query["functions"][fct_num][2][1])) - 1
                if sttin < 0:
                    sttin = 0
                lenin = int(self.__get_function_col(self.__parsed_query["functions"][fct_num][2][2]))
                print(f'__compute_function SUBSTR strin={strin} sttin={sttin} lenin={lenin}')
                if not self.__check_STR(strin):
                    raise vExcept(2301, strin)
                if not self.__check_INT(sttin):
                    raise vExcept(2302, sttin)
                if not self.__check_INT(lenin):
                    raise vExcept(2303, lenin)
                return strin[sttin:sttin+lenin]
            case 'TO_CHAR':
                if len(self.__parsed_query["functions"][fct_num][2]) != 2:
                    raise vExcept(2305, len(self.__parsed_query["functions"][fct_num][2]))
                dtein = self.__get_function_col(self.__parsed_query["functions"][fct_num][2][0])
                fmtin = self.__get_function_col(self.__parsed_query["functions"][fct_num][2][1])
                if (not self.__check_DATETIME(dtein)) and (not self.__check_FLOAT(dtein)):
                    raise vExcept(2306, dtein)
                if not self.__check_STR(fmtin):
                    raise vExcept(2307, fmtin)
                fmtin = fmtin.replace('YYYY', '%Y').replace('YY', '%y')
                fmtin = fmtin.replace('MM', '%m').replace('MONTH', '%B').replace('MON', '%'+'b')
                fmtin = fmtin.replace('DDD', '%j').replace('DD', '%'+'d').replace('DAY', '%A').replace('DY', '%'+'a')
                fmtin = fmtin.replace('HH24', '%H').replace('HH', '%I')
                fmtin = fmtin.replace('MI', '%M')
                fmtin = fmtin.replace('SS', '%S')
                return datetime.fromtimestamp(dtein).strftime(fmtin)[1:-1]
            case 'DECODE':
                if len(self.__parsed_query["functions"][fct_num][2]) % 2 != 0:
                    raise vExcept(2308, len(self.__parsed_query["functions"][fct_num][2]))
                mainval = self.__get_function_col(self.__parsed_query["functions"][fct_num][2][0])
                n = 1
                dont_stop_flg = True
                while (n+1 < len(self.__parsed_query["functions"][fct_num][2])) and dont_stop_flg:
                    if self.__get_function_col(self.__parsed_query["functions"][fct_num][2][n]) == mainval:
                        res = self.__get_function_col(self.__parsed_query["functions"][fct_num][2][n+1])
                        dont_stop_flg = False
                    else:
                        n += 2
                if dont_stop_flg:
                    res = self.__get_function_col(self.__parsed_query["functions"][fct_num][2][-1])
                return res
            case 'ABS':
                if len(self.__parsed_query["functions"][fct_num][2]) != 1:
                    raise vExcept(2311, len(self.__parsed_query["functions"][fct_num][2]))
                val_int = self.__get_function_col(self.__parsed_query["functions"][fct_num][2][0])
                if self.__parsed_query['functions'][fct_num][2][0][4].upper() == 'INT':
                    return abs(int(val_int))
                elif self.__parsed_query['functions'][fct_num][2][0][4].upper() == 'FLOAT':
                    return abs(float(val_int))
                elif self.__parsed_query['functions'][fct_num][2][0][4].upper() == 'MATHS':
                    maths_num = self.__get_maths(self.__parsed_query['functions'][fct_num][2][0][3])
                    if self.__parsed_query["maths"][maths_num][3] == 'INT':
                        return abs(int(val_int))
                    else:
                        return abs(float(val_int))
                else:
                    raise vExcept(2312, f'{val_int} [{self.__parsed_query['functions'][fct_num][2][0][4].upper()}]')
            case 'CHR':
                if len(self.__parsed_query["functions"][fct_num][2]) != 1:
                    raise vExcept(2309, len(self.__parsed_query["functions"][fct_num][2]))
                val_int = self.__get_function_col(self.__parsed_query["functions"][fct_num][2][0])
                if self.__check_INT(val_int):
                    return str(chr(int(val_int)))
                else:
                    raise vExcept(2310, val_int)
            case 'INSTR':
                if len(self.__parsed_query["functions"][fct_num][2]) not in [2, 3, 4]:
                    raise vExcept(2313, len(self.__parsed_query["functions"][fct_num][2]))
                instr = self.__remove_quote(self.__get_function_col(self.__parsed_query["functions"][fct_num][2][0]))
                insubstr = self.__remove_quote(self.__get_function_col(self.__parsed_query["functions"][fct_num][2][1]))
                match len(self.__parsed_query["functions"][fct_num][2]):
                    case 2:
                        inposition = 0
                        inoccurence = 1
                    case 3:
                        inposition = int(self.__get_function_col(self.__parsed_query["functions"][fct_num][2][2]))
                        inoccurence = 1
                    case 4:
                        inposition = int(self.__get_function_col(self.__parsed_query["functions"][fct_num][2][2]))
                        inoccurence = int(self.__get_function_col(self.__parsed_query["functions"][fct_num][2][3]))
                if (inposition >= len(instr)) or (inoccurence < 1):
                    return 0
                while inposition < len(instr):
                    print(f'__compute_function instr={instr[inposition:]} insubstr={insubstr} inposition={inposition} inoccurence={inoccurence}')
                    try:
                        foundin = instr[inposition:].index(insubstr)
                        if foundin >= 0:
                            print(f'__compute_function found')
                            if inoccurence == 1:
                                print(f'__compute_function trouve')
                                return foundin+inposition+1
                            else:
                                print(f'__compute_function continu')
                                inoccurence -= 1
                                inposition = inposition+foundin+1
                        else:
                            return 0
                    except ValueError:
                        return 0

    def __get_function_type(self, fct_name: str, ref_col_typ: str):
        match fct_name:
            case 'UPPER'|'LOWER'|'SUBSTR'|'TO_CHAR'|'CHR':
                return 'str'
            case 'INSTR':
                return 'int'
            case 'ABS':
                if ref_col_typ.upper() in ['INT', 'FLOAT']:
                    return ref_col_typ.lower()
                else:
                    return 'float'
            case 'DECODE':
                if ref_col_typ.upper() in ['INT', 'FLOAT', 'STR', 'HEX', 'DATETIME']:
                    return ref_col_typ.lower()
                else:
                    return 'str'
            case _:
                raise vExcept(2304, fct_name)
