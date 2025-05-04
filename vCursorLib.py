import re
import copy
import random
import math
from datetime import datetime
from vExceptLib import vExcept
from jtinyDBLib import JSONtinyDB
from parserLib import vParser

class vCursor(object):
    def __init__(self, db:JSONtinyDB, username, password, updated_tables, session):
        self.db = db
        self.__session = session
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

        self.__RAZ()
        self.__updated_tables = updated_tables
        self.__group_functions = ['AVG', 'COUNT', 'MAX', 'MIN', 'SUM']
        super().__init__()
        
    def __RAZ(self):
        self.__parsed_query = None
        self.__group_post_data = {}
        self.__bind = {}
        self.__query_result = {}
        self.__query_executed = False

    def submit_query_put_locks(self):
        """Put the locks on all objetcs used by query

        Raises:
            vExcept: _description_
            vExcept: _description_
            vExcept: _description_
            vExcept: _description_
            vExcept: _description_
        """
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

    def submit_query_check_GRANT(self, result):
        """Check the grants for all objects

        Args:
            result (dict): preformated result set of the query

        Raises:
            vExcept: _description_

        Returns:
            dict: the result set of the query
        """
        match self.__parsed_query["querytype"]:
            case 'SELECT'|'DESCRIBE':
                for n in range(len(self.__parsed_query["from"])):
                    tbl = self.__parsed_query["from"][n]
                    if tbl[3] == 'TABLE':
                        if not self.__get_grant_for_object(owner=tbl[1], obj_name=tbl[2], grant_needed='SELECT'):
                            raise vExcept(210, '{}.{}'.format(tbl[1], tbl[2]))
            case 'GRANT':
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
            case 'REVOKE':
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
            case 'CREATE':
                if self.__parsed_query["create"][0][0] == 'TABLE':
                    if self.__parsed_query["create"][0][1] is None:
                        self.__parsed_query["create"][0][1] = self.current_schema
                    if self.__parsed_query["create"][0][1] != self.current_schema:
                        if not self.__get_grant_for_object(owner=self.__parsed_query["create"][0][1], obj_name='TABLE', grant_needed='CREATE'):
                            raise vExcept(901)
                elif self.__parsed_query["create"][0][0] == 'USER':
                    if not self.__get_grant_for_object(owner=None, obj_name='USER', grant_needed='CREATE'):
                        raise vExcept(901)
            case 'DROP':
                if self.__parsed_query["drop"][0][0] == 'TABLE':
                    if self.__parsed_query["drop"][0][1] is None:
                        self.__parsed_query["drop"][0][1] = self.current_schema
                    if self.__parsed_query["drop"][0][1] != self.current_schema:
                        if not self.__get_grant_for_object(owner=self.__parsed_query["drop"][0][1], obj_name='TABLE', grant_needed='DROP'):
                            raise vExcept(902)
                elif self.__parsed_query["drop"][0][0] == 'USER':
                    if not self.__get_grant_for_object(owner=None, obj_name='USER', grant_needed='DROP'):
                        raise vExcept(902)
            case 'INSERT':
                if self.__parsed_query["insert"][0] is None:
                    self.__parsed_query["insert"][0] = self.current_schema
                u_name=self.__parsed_query["insert"][0]
                t_name=self.__parsed_query["insert"][1]
                if not self.__get_grant_for_object(owner=u_name, obj_name=t_name, grant_needed='INSERT'):
                    raise vExcept(210, '{}.{}'.format(u_name, t_name))
            case 'UPDATE':
                if self.__parsed_query["from"][0][1] is None:
                    self.__parsed_query["from"][0][1] = self.current_schema
                u_name=self.__parsed_query["from"][0][1]
                t_name=self.__parsed_query["from"][0][2]
                if not self.__get_grant_for_object(owner=u_name, obj_name=t_name, grant_needed='UPDATE'):
                    raise vExcept(210, '{}.{}'.format(u_name, t_name))
            case 'DELETE':
                if self.__parsed_query["from"][0][1] is None:
                    self.__parsed_query["from"][0][1] = self.current_schema
                u_name=self.__parsed_query["from"][0][1]
                t_name=self.__parsed_query["from"][0][2]
                if not self.__get_grant_for_object(owner=u_name, obj_name=t_name, grant_needed='DELETE'):
                    raise vExcept(210, '{}.{}'.format(u_name, t_name))
        return result

    def submit_query_process_query(self, result):
        """Process the query

        Args:
            result (dict): preformated result set of the query

        Returns:
            dict: the result set of the query
        """
        match self.__parsed_query["querytype"]:
            case 'CREATE':
                if self.__parsed_query["create"][0][0] == 'TABLE':
                    self.__process_create_table()
                    result = {"message": "Table created"}
                elif self.__parsed_query["create"][0][0] == 'USER':
                    self.__process_create_user()
                    result = {"message": "User created"}
            case 'DROP':
                if self.__parsed_query["drop"][0][0] == 'TABLE':
                    self.__process_drop_table()
                    result = {"message": "Table dropped"}
                elif self.__parsed_query["drop"][0][0] == 'USER':
                    self.__process_drop_user()
                    result = {"message": "User dropped"}
            case 'INSERT':
                cnt = self.__process_insert()
                result = {"message": "{} line(s) inserted".format(cnt)}
            case 'SELECT':
                result = self.__process_select(result)
            case 'UPDATE':
                cnt = self.__process_update()
                result = {"message": "{} line(s) updated".format(cnt)}
            case 'DELETE':
                cnt = self.__process_delete()
                result = {"message": "{} line(s) deleted".format(cnt)}
            case 'DESCRIBE':
                result["schema"] = self.__parsed_query["from"][0][1]
                result["table_name"] = self.__parsed_query["from"][0][2]
                result["columns"] = self.__parsed_query["from"][0][4][0]["columns"]
            case 'COMMIT':
                self.__commit()
                result["message"] = 'Commited'
            case 'ROLLBACK':
                self.__rollback()
                result["message"] = 'Rollbacked'
        return result

    def submit_query_remove_locks(self):
        if self.__parsed_query["querytype"] in ['SELECT', 'DESCRIBE']:
            self.db.del_locks(session_id=self.session_id, lock_type=10)
        elif self.__parsed_query["querytype"] in ['INSERT']:
            self.db.del_locks(session_id=self.session_id, lock_type=10)
        elif self.__parsed_query["querytype"] in ['DROP']:
            if self.__parsed_query["drop"][0][0] == 'TABLE':
                self.db.del_locks(session_id=self.session_id, owner=self.__parsed_query["drop"][0][1], name=self.__parsed_query["drop"][0][2], lock_type=0)
            elif self.__parsed_query["drop"][0][0] == 'USER':
                self.db.del_locks(session_id=self.session_id, owner=self.__parsed_query["drop"][0][1], lock_type=0)
        elif self.__parsed_query["querytype"] in ['COMMIT', 'ROLLBACK']:
            self.db.del_locks(session_id=self.session_id, lock_type=99)

    def submit_query_prepare_post_tasks(self):
    # post_data_model : col_id= {
    #   obj_name= {
    #   [
    #     table_alias, 
    #     schema,
    #     table_name,
    #     col_name/value,
    #     alias,
    #     type(COL, INT, FLOAT, STR, HEX, DATETIME, FUNCTION, MATHS, PIPE),
    #     table position,
    #     position in table,
    #     table or cursor]
    #     }
    #   }
        NColGrp = 0
        for n, col in enumerate(self.__parsed_query["select"]):
            res = {}
            flg = False
            match col[5]:
                case 'FUNCTION':
                    flg, res = self.submit_query_prepare_post_tasks_function(col[3], res, False)
                case 'MATHS':
                    flg, res = self.submit_query_prepare_post_tasks_maths(col[3], res, False)
                case 'PIPE':
                    flg, res = self.submit_query_prepare_post_tasks_pipe(col[3], res, False)
            if flg:
                NColGrp += 1
                self.__parsed_query["post_data_model"][n] = res
        if NColGrp+len(self.__parsed_query["group_by"]) < len(self.__parsed_query["select"]):
            raise vExcept(752)
        elif NColGrp+len(self.__parsed_query["group_by"]) > len(self.__parsed_query["select"]):
            raise vExcept(754)
        # print(f'submit_query_prepare_post_tasks  post_data_model={self.__parsed_query["post_data_model"]}')

    def submit_query_prepare_post_tasks_function(self, fct_name, res, dependant):
        flg, tmpflg = False, False
        fct_id = self.__get_function(fct_name)
        res[self.__parsed_query["functions"][fct_id][0]] = {
            "columns": self.__parsed_query["functions"][fct_id][2],
            "colvalmodel": [[None, False] for x in range(len(self.__parsed_query["functions"][fct_id][2]))],
            "colval": [],
            "result": [],
            "dependant": dependant,
            "completed": [],
            "rowscompleted": [],
            "done": False,
            "function": self.__parsed_query["functions"][fct_id][1]}
        colcount = len(res[self.__parsed_query["functions"][fct_id][0]]["colvalmodel"])
        match res[self.__parsed_query["functions"][fct_id][0]]["function"]:
            case 'ABS'|'ACOS'|'ASIN'|'ATAN'|'AVG'|'CHR'|'COS'|'COUNT'|'EXP'|'LENGTH'|'LN'|'LOG'|'LOWER'|'MAX'|'MIN'|'SIN'|'SUM'|'TAN'|'UPPER':
                if colcount != 1:
                    raise vExcept(2323, res[self.__parsed_query["functions"][fct_id][0]]["function"])
            case 'DECODE':
                if colcount % 2 != 0:
                    raise vExcept(2308, colcount)
            case 'INSTR':
                if colcount not in [2, 3, 4]:
                    raise vExcept(2313, colcount)
            case 'LPAD':
                if colcount not in [2, 3]:
                    raise vExcept(2316, colcount)
            case 'LTRIM':
                if colcount not in [1, 2]:
                    raise vExcept(2320, colcount)
            case 'NVL':
                if colcount != 2:
                    raise vExcept(2314, colcount)
            case 'ATAN2':
                if colcount != 2:
                    raise vExcept(2332, colcount)
            case 'NVL2':
                if colcount != 3:
                    raise vExcept(2315, colcount)
            case 'RPAD':
                if colcount not in [2, 3]:
                    raise vExcept(2318, colcount)
            case 'RTRIM'|'TRUNC':
                if colcount not in [1, 2]:
                    raise vExcept(2321, colcount)
            case 'SUBSTR':
                if colcount != 3:
                    raise vExcept(2300, colcount)
            case 'TO_CHAR':
                if colcount != 2:
                    raise vExcept(2305, colcount)
        if self.__parsed_query["functions"][fct_id][1] in self.__group_functions:
            flg = True
        for n, col in enumerate(self.__parsed_query["functions"][fct_id][2]):
            match col[4]:
                case 'FUNCTION':
                    tmpflg, res = self.submit_query_prepare_post_tasks_function(col[3], res, True)
                case 'MATHS':
                    tmpflg, res = self.submit_query_prepare_post_tasks_maths(col[3], res, True)
                case 'PIPE':
                    tmpflg, res = self.submit_query_prepare_post_tasks_pipe(col[3], res, True)
            if tmpflg:
                flg = True
        return flg, res

    def submit_query_prepare_post_tasks_maths(self, maths_name, res, dependant):
        flg, tmpflg = False, False
        maths_id = self.__get_maths(maths_name)
        res[self.__parsed_query["maths"][maths_id][0]] = {
            "columns": [x for x in self.__parsed_query["maths"][maths_id][2]],
            "colvalmodel": [[None, False] for x in self.__parsed_query["maths"][maths_id][2]],
            "colval": [],
            "result": [],
            "dependant": dependant,
            "completed": [],
            "rowscompleted": [],
            "done": False}
        for n, col in enumerate(self.__parsed_query["maths"][maths_id][2]):
            if col[1][0] == 'META':
                continue
            match col[1][5]:
                case 'FUNCTION':
                    tmpflg, res = self.submit_query_prepare_post_tasks_function(col[1][4], res, True)
                case 'MATHS':
                    tmpflg, res = self.submit_query_prepare_post_tasks_maths(col[1][4], res, True)
                case 'PIPE':
                    tmpflg, res = self.submit_query_prepare_post_tasks_pipe(col[1][4], res, True)
            if tmpflg:
                flg = True
        return flg, res

    def submit_query_prepare_post_tasks_pipe(self, pipe_name, res, dependant):
        flg, tmpflg = False, False
        pipe_id = self.__get_pipe(pipe_name)
        res[self.__parsed_query["pipe"][pipe_id][0]] = {
            "columns": self.__parsed_query["pipe"][pipe_id][1],
            "colvalmodel": [[None, False] for x in self.__parsed_query["pipe"][pipe_id][1]],
            "colval": [],
            "result": [],
            "dependant": dependant,
            "completed": [],
            "rowscompleted": [],
            "done": False}
        for n, col in enumerate(self.__parsed_query["pipe"][pipe_id][1]):
            match col[5]:
                case 'FUNCTION':
                    tmpflg, res = self.submit_query_prepare_post_tasks_function(col[3], res, True)
                case 'MATHS':
                    tmpflg, res = self.submit_query_prepare_post_tasks_maths(col[3], res, True)
                case 'PIPE':
                    tmpflg, res = self.submit_query_prepare_post_tasks_pipe(col[3], res, True)
            if tmpflg:
                flg = True
        return flg, res

    def execute(self, _query:str, bind:dict = []):
        self.__RAZ()
        result = {}
        self.__bind = bind
        self.__parsed_query = vParser().parse_query(query=_query, bind=bind)
        # print(f'execute select={self.__parsed_query["select"]}')
        # print(f'execute from={self.__parsed_query["from"]}')
        # print(f'execute where={self.__parsed_query["where"]}')
        # print(f'execute parsed_where={self.__parsed_query["parsed_where"]}')
        # print(f'execute parsed_inner_where={self.__parsed_query["parsed_inner_where"]}')
        # print(f'execute left_outer_where={self.__parsed_query["left_outer_where"]}')
        # print(f'execute parsed_left_outer_where={self.__parsed_query["parsed_left_outer_where"]}')
        # print(f'execute in={self.__parsed_query["in"]}')
        # print(f'execute functions={self.__parsed_query["functions"]}')
        # print(f'execute connect={self.__parsed_query["connect"]}')
        # print(f'execute maths={self.__parsed_query["maths"]}')
        # print(f'execute pipe={self.__parsed_query["pipe"]}')
        # print(f'execute bind={self.__parsed_query["bind"]}')
        # print(f'execute group_by={self.__parsed_query["group_by"]}')
        # print(f'execute order_by={self.__parsed_query["order_by"]}')
        # print(f'execute cursors={self.__parsed_query["cursors"]}')
        if self.__parsed_query["querytype"] in ['SELECT']:
            result = {"columns": [], "rows": []}
        elif self.__parsed_query["querytype"] in ['DESCRIBE']:
            result = {"columns": [], "schema": [], "table_name": []}
        elif self.__parsed_query["querytype"] in ['GRANT', 'CREATE', 'DROP', 'INSERT', 'COMMIT', 'ROLLBACK', 'UPDATE', 'DELETE']:
            result = {"message": None}

        # put locks
        self.submit_query_put_locks()

        # load tables
        if self.__parsed_query["querytype"] in ['SELECT', 'DESCRIBE', 'UPDATE', 'DELETE']:
            self.__validate_tables()

        # check GRANT
        result = self.submit_query_check_GRANT(result)

        # prepare post tasks
        if self.__parsed_query["post_tasks"]:
            self.submit_query_prepare_post_tasks()

        # print(f'execute post_tasks={self.__parsed_query["post_tasks"]}')
        # print(f'execute post_data_model={self.__parsed_query["post_data_model"]}')

        # process query
        result = self.submit_query_process_query(result)

        # remove locks
        self.submit_query_remove_locks()
        self.__query_result = result
        self.__query_executed = True

    def fetchall(self):
        if self.__query_executed:
            if "rows" in self.__query_result.keys():
                return self.__query_result["rows"]

    @property
    def description(self):
        if self.__query_executed and "columns" in self.__query_result.keys():
            return self.__query_result["columns"]
        else:
            return []

    @property
    def message(self):
        if self.__query_executed and "message" in self.__query_result.keys():
            return self.__query_result["message"]

    def get_tables(self):
        result = []
        for tbl in self.db.db["Tables"]:
            if tbl["schema"] == self.current_schema.upper():
                result.append([tbl["schema"], tbl["table_name"]])
        return result

    def __searchColInFromTables(self, colin, aliasin, table_namein, schemain):
        if colin == 'ROWNUM':
            count = 1
            memtf, memctf, ctype = None, None, 'INT'
        else:
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
        result = []
        if colin == 'ROWNUM':
            count = 1
            result.append([colin, aliasin, None, None, None, None, 'INT', None])
        else:
            count = 0
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
        # print(f'__get_rows post_data_model={self.__parsed_query["post_data_model"]}')
        # print(f'__get_rows cur_idx={cur_idx} nblignes={len(self.__parsed_query["from"][cur_idx][4][0]["rows"])}')
        for n in range(len(self.__parsed_query["from"][cur_idx][4][0]["rows"])):
            self.__RowsPosInTables[cur_idx] = n
            # self.empty_table : [table is in outer join, current row, all rows for current loop in table]
            if cur_idx == 0:
                self.empty_table = [[False, False, False] for x in range(len(self.__parsed_query["from"]))]
            if n == 0:
                self.empty_table[cur_idx] = [False, False, False]
            if cur_idx < len(self.__parsed_query["from"])-1:
                self.__get_rows(cur_idx=cur_idx+1)
            else:
                if self.__process_tests():
                    rrow = []
                    for n, s in enumerate(self.__parsed_query['select']):
                        if self.__parsed_query['post_tasks'] and (n in self.__parsed_query['post_data_model']):
                            rrow.append(None)
                            for colkey in self.__parsed_query['post_data_model'][n].keys():
                                match colkey[0:3]:
                                    case 'MAT':
                                        for ncol, col_parse in enumerate(self.__parsed_query['post_data_model'][n][colkey]["columns"]):
                                            if len(self.__parsed_query['post_data_model'][n][colkey]["colval"]) < len(self.__result) + 1:
                                                self.__parsed_query['post_data_model'][n][colkey]["colval"].append(copy.deepcopy(self.__parsed_query['post_data_model'][n][colkey]["colvalmodel"]))
                                                self.__parsed_query['post_data_model'][n][colkey]["result"].append(None)
                                                self.__parsed_query['post_data_model'][n][colkey]["completed"].append(False)
                                                self.__parsed_query['post_data_model'][n][colkey]["rowscompleted"].append(False)
                                            if len(col_parse) == 2:
                                                col = col_parse[1][1:]
                                                match col[4]:
                                                    case 'COLUMN':
                                                        if col[3] == 'ROWNUM':
                                                            self.__parsed_query['post_data_model'][n][colkey]["colval"][len(self.__result)][ncol] = [len(self.__result), True]
                                                        elif col[3] == '*':
                                                            self.__parsed_query['post_data_model'][n][colkey]["colval"][len(self.__result)][ncol] = ['||ALL_ROWS||', True]
                                                        else:
                                                            self.__parsed_query['post_data_model'][n][colkey]["colval"][len(self.__result)][ncol] = [self.__parsed_query["from"][col[5]][4][0]["rows"][self.__RowsPosInTables[col[5]]][col[6]], True]
                                                    case 'INT':
                                                        self.__parsed_query['post_data_model'][n][colkey]["colval"][len(self.__result)][ncol] = [int(col[3]), True]
                                                    case 'FLOAT':
                                                        self.__parsed_query['post_data_model'][n][colkey]["colval"][len(self.__result)][ncol] = [float(col[3]), True]
                                                    case 'HEX':
                                                        self.__parsed_query['post_data_model'][n][colkey]["colval"][len(self.__result)][ncol] = [col[3], True]
                                                    case 'DATETIME':
                                                        self.__parsed_query['post_data_model'][n][colkey]["colval"][len(self.__result)][ncol] = [col[3], True]
                                                    case 'STR':
                                                        self.__parsed_query['post_data_model'][n][colkey]["colval"][len(self.__result)][ncol] = [self.__remove_quote(col[3]).replace("''", "'"), True]
                                                    case 'FUNCTION':
                                                        self.__parsed_query['post_data_model'][n][colkey]["colval"][len(self.__result)][ncol] = [None, False]
                                                    case 'MATHS':
                                                        self.__parsed_query['post_data_model'][n][colkey]["colval"][len(self.__result)][ncol] = [None, False]
                                                    case 'PIPE':
                                                        self.__parsed_query['post_data_model'][n][colkey]["colval"][len(self.__result)][ncol] = [None, False]
                                                    case _:
                                                        raise vExcept(801)
                                            else:
                                                self.__parsed_query['post_data_model'][n][colkey]["colval"][len(self.__result)][ncol] = [None, False]
                                    case 'FCT':
                                        for ncol, col in enumerate(self.__parsed_query['post_data_model'][n][colkey]["columns"]):
                                            if len(self.__parsed_query['post_data_model'][n][colkey]["colval"]) < len(self.__result) + 1:
                                                self.__parsed_query['post_data_model'][n][colkey]["colval"].append(copy.deepcopy(self.__parsed_query['post_data_model'][n][colkey]["colvalmodel"]))
                                                self.__parsed_query['post_data_model'][n][colkey]["result"].append(None)
                                                self.__parsed_query['post_data_model'][n][colkey]["completed"].append(False)
                                                self.__parsed_query['post_data_model'][n][colkey]["rowscompleted"].append(False)
                                            match col[4]:
                                                case 'COLUMN':
                                                    if col[3] == 'ROWNUM':
                                                        self.__parsed_query['post_data_model'][n][colkey]["colval"][len(self.__result)][ncol] = [len(self.__result), True]
                                                    elif col[3] == '*':
                                                        self.__parsed_query['post_data_model'][n][colkey]["colval"][len(self.__result)][ncol] = ['||ALL_ROWS||', True]
                                                    else:
                                                        self.__parsed_query['post_data_model'][n][colkey]["colval"][len(self.__result)][ncol] = [self.__parsed_query["from"][col[5]][4][0]["rows"][self.__RowsPosInTables[col[5]]][col[6]], True]
                                                case 'INT':
                                                    self.__parsed_query['post_data_model'][n][colkey]["colval"][len(self.__result)][ncol] = [int(col[3]), True]
                                                case 'FLOAT':
                                                    self.__parsed_query['post_data_model'][n][colkey]["colval"][len(self.__result)][ncol] = [float(col[3]), True]
                                                case 'HEX':
                                                    self.__parsed_query['post_data_model'][n][colkey]["colval"][len(self.__result)][ncol] = [col[3], True]
                                                case 'DATETIME':
                                                    self.__parsed_query['post_data_model'][n][colkey]["colval"][len(self.__result)][ncol] = [col[3], True]
                                                case 'STR':
                                                    self.__parsed_query['post_data_model'][n][colkey]["colval"][len(self.__result)][ncol] = [self.__remove_quote(col[3]).replace("''", "'"), True]
                                                case 'FUNCTION':
                                                    self.__parsed_query['post_data_model'][n][colkey]["colval"][len(self.__result)][ncol] = [None, False]
                                                case 'MATHS':
                                                    self.__parsed_query['post_data_model'][n][colkey]["colval"][len(self.__result)][ncol] = [None, False]
                                                case 'PIPE':
                                                    self.__parsed_query['post_data_model'][n][colkey]["colval"][len(self.__result)][ncol] = [None, False]
                                                case _:
                                                    raise vExcept(801)
                            # self.__parsed_query['post_data_model'][n][colkey]["columns"]
                        else:
                            match s[5]:
                                case 'COLUMN':
                                    if s[3] == 'ROWNUM':
                                        rrow.append(len(self.__result))
                                    else:
                                        if self.empty_table[s[6]][0] and self.empty_table[s[6]][1]:
                                            rrow.append(None)
                                        else:
                                            rrow.append(self.__parsed_query["from"][s[6]][4][0]["rows"][self.__RowsPosInTables[s[6]]][s[7]])
                                case 'INT':
                                    rrow.append(int(s[3]))
                                case 'FLOAT':
                                    rrow.append(float(s[3]))
                                case 'HEX':
                                    rrow.append(s[3])
                                case 'DATETIME':
                                    rrow.append(s[3])
                                case 'STR':
                                    rrow.append(self.__remove_quote(s[3]).replace("''", "'"))
                                case 'FUNCTION':
                                    rrow.append(self.__compute_function(s[3]))
                                case 'MATHS':
                                    rrow.append(self.__compute_maths(s[3]))
                                case 'PIPE':
                                    rrow.append(self.__remove_quote(self.__compute_pipe(s[3])).replace("''", "'"))
                                case _:
                                    raise vExcept(801)
                    self.__result.append(rrow)
        # print(f'__get_rows post_data_model={self.__parsed_query["post_data_model"]}')

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
                        if (tst[1][5] == "COLUMN") and (tst[1][1] == cur_idx) and (tst[3][5] in ["INT", "FLOAT", "STR", "HEX", "DATETIME"]) or \
                           (tst[3][5] == "COLUMN") and (tst[3][1] == cur_idx) and (tst[1][5] in ["INT", "FLOAT", "STR", "HEX", "DATETIME"]) or \
                           (tst[1][5] == "COLUMN") and (tst[1][1] == cur_idx) and (tst[3][5] == "COLUMN") and (tst[3][1] == cur_idx):
                            Validate_prefetch_process = True
                            break
                # print (f'__prefetch_get_rows {self.__parsed_query["from"][cur_idx][2]}  {Validate_prefetch_process}')
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
                        if tst[1][4] == "ROWNUM":
                            c1 = len(self.__result)
                        else:
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
                        if tst[3][4] == "ROWNUM":
                            c2 = len(self.__result)
                        else:
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
                            if tst[4][4] == "ROWNUM":
                                c3 = len(self.__result)
                            else:
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
                        # print(f'__process_tests c1={c1} BETWEEN c2={c2} AND c3={c3}  result={result}')
                    elif tstoper == 'IN':
                        in_id = self.__get_in(c2)
                        result = False
                        for mbr in self.__parsed_query["in"][in_id][2]:
                            if c1 == mbr[3]:
                                result = True
                                break
                    elif tstoper == 'IN_SELECT':
                        cid = self.__getCursorID(c2)
                        if len(self.__parsed_query['cursors'][cid]) == 2:
                            vsess = vCursor(self.db, self.__session_username, self.__password, self.__updated_tables, self.__session)
                            vsess.execute(_query=self.__getCursorQuery(c2), bind=self.__bind)
                            sel_cur = {"rows":vsess.fetchall(), "columns":vsess.description}
                            if len(sel_cur["columns"]) > 1:
                                raise vExcept(500)
                            del vsess
                            self.__parsed_query['cursors'][cid].append(sel_cur)
                        else:
                            sel_cur = self.__parsed_query['cursors'][cid][2]
                        result = False
                        for mbr in sel_cur["rows"]:
                            if c1 == mbr[0]:
                                result = True
                                break
                    else:
                        result = self.__compare_cols(str(c1), str(c2), tstoper)
                        # print(f'__process_tests c1={c1} {tstoper} c2={c2}   result={result}')
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
                            if tst[1][4] == "ROWNUM":
                                c1 = len(self.__result)
                            else:
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
                            if tst[3][4] == "ROWNUM":
                                c2 = len(self.__result)
                            else:
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
        left_outer_tests = True
        if len(self.__parsed_query["parsed_left_outer_where"]) > 0:
            for row in range(len(self.__parsed_query["parsed_left_outer_where"])):
                temp_res = []
                for tst in self.__parsed_query["parsed_left_outer_where"][row][1]:
                    if tst[1][0] != tst[3][0]:
                        raise vExcept(899, tst)
                    if tst[1][0] == "TST":
                        if tst[1][5] == "COLUMN":
                            if tst[1][4] == "ROWNUM":
                                c1 = len(self.__result)
                            else:
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
                            if tst[3][4] == "ROWNUM":
                                c2 = len(self.__result)
                            else:
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
                    left_outer_tests = left_outer_tests and temp_res[-1]
                table_idx = self.__parsed_query["parsed_left_outer_where"][row][0]
                self.empty_table[table_idx][0] = True
                self.empty_table[table_idx][1] = not left_outer_tests
                self.empty_table[table_idx][2] = self.empty_table[table_idx][2] or left_outer_tests
                if self.__RowsPosInTables[table_idx]+1 == len(self.__parsed_query["from"][table_idx][4][0]["rows"]) and not self.empty_table[table_idx][2]:
                    self.empty_table[table_idx][1] = True
                    left_outer_tests = True
        # print(f'__process_tests result={where_tests and inner_tests}')
        return where_tests and inner_tests and left_outer_tests

    def __prefetch_process_tests(self, tab_num):
        # parsed_where: item_id, field1, oper, field2
        #            or item_id, ['META', item_id], oper, ['META', item_id]
        #            or item_id, ['TST', num_table, num_col, alias1, field1, type, schema, table_name, table or cursor], oper, ['TST', num_table, num_col, alias2, field2, type, schema, table_name, table or cursor]
        if len(self.__parsed_query["parsed_where"]) > 0:
            temp_res = []
            for tst in self.__parsed_query['parsed_where']:
                no_test = False
                if tst[1][0] != tst[3][0]:
                    raise vExcept(899, tst)
                if tst[1][0] == "TST":
                    if (tst[1][5] == "COLUMN") and (tst[1][1] == tab_num):
                        c1 = self.__parsed_query["from"][tst[1][1]][4][0]["rows"][self.__RowsPosInTables[tst[1][1]]][tst[1][2]]
                    else:
                        no_test = True
                    if (tst[3][5] == "COLUMN") and (tst[3][1] == tab_num):
                        c2 = self.__parsed_query["from"][tst[3][1]][4][0]["rows"][self.__RowsPosInTables[tst[3][1]]][tst[3][2]]
                    else:
                        no_test = True
                    tstoper = tst[2]
                    if no_test:
                        result = None
                    elif len(tst) == 5:
                        if (tst[4][5] == "COLUMN") and (tst[4][1] == tab_num):
                            c3 = self.__parsed_query["from"][tst[4][1]][4][0]["rows"][self.__RowsPosInTables[tst[4][1]]][tst[4][2]]
                            if (tst[1][5] == "COLUMN") and (tst[1][1] == tab_num) and (tst[3][5] in ["INT", "FLOAT", "STR", "HEX", "DATETIME"]) or \
                            (tst[3][5] == "COLUMN") and (tst[3][1] == tab_num) and (tst[1][5] in ["INT", "FLOAT", "STR", "HEX", "DATETIME"]) or \
                            (tst[1][5] == "COLUMN") and (tst[1][1] == tab_num) and (tst[3][5] == "COLUMN") and (tst[3][1] == tab_num):
                                result = self.__compare_cols(str(c1), str(c2), '>=')
                                if (tst[1][5] == "COLUMN") and (tst[1][1] == tab_num) and (tst[4][5] in ["INT", "FLOAT", "STR", "HEX", "DATETIME"]) or \
                                (tst[4][5] == "COLUMN") and (tst[4][1] == tab_num) and (tst[1][5] in ["INT", "FLOAT", "STR", "HEX", "DATETIME"]) or \
                                (tst[1][5] == "COLUMN") and (tst[1][1] == tab_num) and (tst[4][5] == "COLUMN") and (tst[4][1] == tab_num):
                                    result = result and self.__compare_cols(str(c1), str(c3), '<=')
                                else:
                                    result = None
                            else:
                                result = None
                        else:
                            result = None
                    elif (tstoper == 'IN'):
                        in_id = self.__get_in(c2)
                        result = False
                        if self.__parsed_query["in"][in_id][1] == 'LIST':
                            for mbr in self.__parsed_query["in"][in_id][2]:
                                if mbr[5] in ["INT", "FLOAT", "STR", "HEX", "DATETIME"]:
                                    if c1 == mbr[3]:
                                        result = True
                                        break
                                else:
                                    result = None
                        else:
                            result = None
                    elif tstoper == 'IN_SELECT':
                        cid = self.__getCursorID(c2)
                        if len(self.__parsed_query['cursors'][cid]) == 2:
                            vsess = vCursor(self.db, self.__session_username, self.__password, self.__updated_tables, self.__session)
                            vsess.execute(_query=self.__getCursorQuery(c2), bind=self.__bind)
                            sel_cur = {"rows":vsess.fetchall(), "columns":vsess.description}
                            if len(sel_cur["columns"]) > 1:
                                raise vExcept(500)
                            del vsess
                            self.__parsed_query['cursors'][cid].append(sel_cur)
                        else:
                            sel_cur = self.__parsed_query['cursors'][cid][2]
                        result = False
                        for mbr in sel_cur["rows"]:
                            if c1 == mbr[0]:
                                result = True
                                break
                    else:
                        if (tst[1][5] == "COLUMN") and (tst[1][1] == tab_num) and (tst[3][5] in ["INT", "FLOAT", "STR", "HEX", "DATETIME"]) or \
                        (tst[3][5] == "COLUMN") and (tst[3][1] == tab_num) and (tst[1][5] in ["INT", "FLOAT", "STR", "HEX", "DATETIME"]) or \
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
        # print(f'__prefetch_process_tests  where_tests={where_tests} temp_res={temp_res}')
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
        # generate data for where clause of left_ outer join
        self.__validate_left_outer_where()
        # init gloal variable for rows fetch
        self.__result = []
        self.__RowsPosInTables = []
        for n in range(len(self.__parsed_query["from"])):
            self.__RowsPosInTables.append(None)
        # prefetch rows in source tables
        self.__prefetch_get_rows()
        # fetch rows for query
        self.__get_rows(cur_idx=0)
        # post tasks for GROUP BY
        if self.__parsed_query["post_tasks"] and (len(self.__result) > 0):
            self.__process_groupby()
        # parse ORDER BY
        if (len(self.__parsed_query["order_by"]) > 0) and (len(self.__result) > 0):
            self.__process_orderby(result["columns"])
        # rename duplicated loumns
        self.__check_cols_name(result=result)
        result["rows"] = self.__result
        del self.__result
        del self.__RowsPosInTables
        return result

    def __process_create_table(self):
        if len(self.__parsed_query["create"][0][3]) == 1: # create table with cursor
            vsess = vCursor(self.db, self.__session_username, self.__password, self.__updated_tables, self.__session)
            vsess.execute(_query=self.__getCursorQuery(self.__parsed_query["create"][0][3][0]), bind=self.__bind)
            # print(f'__process_create_table, cur={vsess.message}')
            blck = {
                "table_name": self.__parsed_query["create"][0][2].upper(),
                "schema": self.__parsed_query["create"][0][1].upper(),
                "columns": vsess.description,
                "rows": vsess.fetchall()
                }
        else: # create table with columns
            blck = {
                "table_name": self.__parsed_query["create"][0][2].upper(),
                "schema": self.__parsed_query["create"][0][1].upper(),
                "columns": self.__parsed_query["create"][0][3],
                "rows": []
                }
        # define upper/lower for columns
        for n in range(len(blck["columns"])):
            blck["columns"][n][0] = blck["columns"][n][0].upper()
            blck["columns"][n][1] = blck["columns"][n][1].lower()
        # # upcase for columns name
        # for n in range(len(blck["columns"])):
        #     blck["columns"][n][0] = str(blck["columns"][n][0]).upper()
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
            raise vExcept(210, '{}.{}'.format(owner, table_name))

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
            for i in range(len(tbl["columns"])):
                self.__parsed_query["insert"][2].append(tbl["columns"][i][0].upper())
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
            vsess = vCursor(self.db, self.__session_username, self.__password, self.__updated_tables, self.__session)
            vsess.execute(_query=self.__getCursorQuery(self.__parsed_query['insert'][4]), bind=self.__bind)
            if len(vsess.description) != len(self.__parsed_query["insert"][2]):
                raise vExcept(312)
            for lgn in vsess.fetchall():
                row = []
                for n in range(len(tbl["columns"])):
                    row.append(None)
                for cm in col_mat:
                    row[cm[0]] = self.__format_value(value=lgn[cm[1]], type_value=tbl["columns"][cm[0]][1])
                tbl["rows"].append(row)
            self.__add_updated_table(tbl)
            return len(vsess.fetchall())

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

    def __process_groupby(self):
        matriceCOL = []
        matriceROW = []
        parsedROW = [False for x in range(len(self.__result))]
        for n in range(len(self.__parsed_query["select"])):
            matriceCOL.append(bool(n in self.__parsed_query["post_data_model"].keys()))
        StillWork = True
        rowidx = 0
        while StillWork and rowidx < len(self.__result):
            # init selected rows matrice
            matriceROW = [False for x in range(len(self.__result))]
            # search first available row
            StillWork = False
            for n, value in enumerate(parsedROW):
                if not value:
                    rowidx = n
                    matriceROW[rowidx] = True
                    parsedROW[rowidx] = True
                    StillWork = True
                    break
            if StillWork:
                # serach all rows to be grouped => all rows are flaged TRUE in matriceROW
                numrow = rowidx + 1
                while numrow < len(self.__result):
                    if (self.__result[numrow] == self.__result[rowidx]) and (not parsedROW[numrow]):
                        matriceROW[numrow] = True
                        parsedROW[numrow] = True
                    numrow += 1
            for WorkOnRowIdx, WorkOnRow in enumerate(matriceROW):
                # if not WorkOnRow:
                #     continue
                for WorkOnCol in self.__parsed_query["post_data_model"].keys():
                    for obj in self.__parsed_query["post_data_model"][WorkOnCol].keys():
                        # test if "obj" is fully computed
                        if not (self.__parsed_query["post_data_model"][WorkOnCol][obj]["done"] or self.__parsed_query["post_data_model"][WorkOnCol][obj]["completed"][WorkOnRowIdx]):
                            match obj[0:3]:
                                case 'FCT':
                                    # all columns data are available, function can be parsed
                                    match self.__parsed_query["post_data_model"][WorkOnCol][obj]["function"]:
                                        case 'ABS'|'AVG'|'LOWER'|'MAX'|'MIN'|'SUM':
                                            match self.__parsed_query["post_data_model"][WorkOnCol][obj]["columns"][0][4]:
                                                case 'FUNCTION'|'MATHS'|'PIPE':
                                                    fct = self.__parsed_query["post_data_model"][WorkOnCol][obj]["columns"][0][3]
                                                    if self.__parsed_query["post_data_model"][WorkOnCol][fct]["done"]:
                                                        colval = self.__parsed_query["post_data_model"][WorkOnCol][fct]["result"][WorkOnRowIdx]
                                                        self.__parsed_query["post_data_model"][WorkOnCol][obj]["colval"][WorkOnRowIdx][0] = [colval, True]
                                                        self.__parsed_query["post_data_model"][WorkOnCol][obj]["result"][WorkOnRowIdx] = colval
                                                case _:
                                                    colval = self.__parsed_query["post_data_model"][WorkOnCol][obj]["colval"][WorkOnRowIdx][0][0]
                                                    self.__parsed_query["post_data_model"][WorkOnCol][obj]["result"][WorkOnRowIdx] = colval
                                        case 'COUNT':
                                            if self.__parsed_query["post_data_model"][WorkOnCol][obj]["colval"][WorkOnRowIdx][0][0] is None:
                                                self.__parsed_query["post_data_model"][WorkOnCol][obj]["result"][WorkOnRowIdx] = 0
                                            else:
                                                self.__parsed_query["post_data_model"][WorkOnCol][obj]["result"][WorkOnRowIdx] = 1
                                        case _:
                                            for ncol in range(len(self.__parsed_query["post_data_model"][WorkOnCol][obj]["columns"])):
                                                match self.__parsed_query["post_data_model"][WorkOnCol][obj]["columns"][ncol][4]:
                                                    case 'FUNCTION'|'MATHS'|'PIPE':
                                                        fct = self.__parsed_query["post_data_model"][WorkOnCol][obj]["columns"][ncol][3]
                                                        if self.__parsed_query["post_data_model"][WorkOnCol][fct]["done"]:
                                                            colval = self.__parsed_query["post_data_model"][WorkOnCol][fct]["result"][WorkOnRowIdx]
                                                            self.__parsed_query["post_data_model"][WorkOnCol][obj]["colval"][WorkOnRowIdx][ncol] = [colval, True]
                                                    case _:
                                                        colval = self.__parsed_query["post_data_model"][WorkOnCol][obj]["colval"][WorkOnRowIdx][ncol][0]
                                                        self.__parsed_query["post_data_model"][WorkOnCol][obj]["result"][WorkOnRowIdx] = colval
                                    # set "completed" if all rows are parsed for each "obj"
                                    AllRowsParsed = True
                                    for chkcol in self.__parsed_query["post_data_model"][WorkOnCol][obj]["colval"][WorkOnRowIdx]:
                                        if not chkcol[1]:
                                            AllRowsParsed = False
                                    if AllRowsParsed:
                                        self.__parsed_query["post_data_model"][WorkOnCol][obj]["completed"][WorkOnRowIdx] = True
                                case 'MAT':
                                    # all columns data are available, function can be parsed
                                    for n, column in enumerate(self.__parsed_query["post_data_model"][WorkOnCol][obj]["columns"]):
                                        if not self.__parsed_query["post_data_model"][WorkOnCol][obj]["colval"][WorkOnRowIdx][n][1]:
                                            match column[1][0]:
                                                case 'META':
                                                    if self.__parsed_query["post_data_model"][WorkOnCol][obj]["colval"][WorkOnRowIdx][column[1][1]][1] and self.__parsed_query["post_data_model"][WorkOnCol][obj]["colval"][WorkOnRowIdx][column[3][1]][1]:
                                                        v1 = self.__parsed_query["post_data_model"][WorkOnCol][obj]["colval"][WorkOnRowIdx][column[1][1]][0]
                                                        v2 = self.__parsed_query["post_data_model"][WorkOnCol][obj]["colval"][WorkOnRowIdx][column[3][1]][0]
                                                        match column[2]:
                                                            case '+':
                                                                self.__parsed_query["post_data_model"][WorkOnCol][obj]["colval"][WorkOnRowIdx][n] = [v1 + v2, True]
                                                            case '-':
                                                                self.__parsed_query["post_data_model"][WorkOnCol][obj]["colval"][WorkOnRowIdx][n] = [v1 - v2, True]
                                                            case '*':
                                                                self.__parsed_query["post_data_model"][WorkOnCol][obj]["colval"][WorkOnRowIdx][n] = [v1 * v2, True]
                                                            case '/':
                                                                if v2 != 0:
                                                                    self.__parsed_query["post_data_model"][WorkOnCol][obj]["colval"][WorkOnRowIdx][n] = [v1 / v2, True]
                                                                else:
                                                                    raise vExcept(2500)
                                                case 'TST':
                                                    match column[1][4][0:3]:
                                                        case 'FCT':
                                                            if self.__parsed_query["post_data_model"][WorkOnCol][column[1][4]]["done"]:
                                                                self.__parsed_query["post_data_model"][WorkOnCol][obj]["colval"][WorkOnRowIdx][n] = [self.__parsed_query["post_data_model"][WorkOnCol][column[1][4]]["result"][WorkOnRowIdx], True]
                                                        case 'MAT':
                                                            pass
                                                        case 'PIP':
                                                            pass
                                    # set "completed" if all rows are parsed for each "obj"
                                    AllRowsParsed = True
                                    for chkcol in self.__parsed_query["post_data_model"][WorkOnCol][obj]["colval"][WorkOnRowIdx]:
                                        if not chkcol[1]:
                                            AllRowsParsed = False
                                    if AllRowsParsed:
                                        self.__parsed_query["post_data_model"][WorkOnCol][obj]["completed"][WorkOnRowIdx] = True
                                        self.__parsed_query["post_data_model"][WorkOnCol][obj]["result"][WorkOnRowIdx] = self.__parsed_query["post_data_model"][WorkOnCol][obj]["colval"][WorkOnRowIdx][-1][0]
            for WorkOnRowIdx, WorkOnRow in enumerate(matriceROW):
                for WorkOnCol in self.__parsed_query["post_data_model"].keys():
                    for obj in self.__parsed_query["post_data_model"][WorkOnCol].keys():
                        if not self.__parsed_query["post_data_model"][WorkOnCol][obj]["done"]:
                            NotAllCompleted = False
                            for n in self.__parsed_query["post_data_model"][WorkOnCol][obj]["completed"]:
                                if not n:
                                    NotAllCompleted = True
                            if self.__parsed_query["post_data_model"][WorkOnCol][obj]["rowscompleted"][WorkOnRowIdx] or NotAllCompleted:
                                continue
                            match obj[0:3]:
                                case 'MAT':
                                    for n in range(len(self.__result)):
                                        if not self.__parsed_query["post_data_model"][WorkOnCol][obj]["dependant"] and self.__parsed_query["post_data_model"][WorkOnCol][obj]["completed"][n]:
                                            self.__result[n][WorkOnCol] = self.__parsed_query["post_data_model"][WorkOnCol][obj]["result"][n]
                                            self.__parsed_query["post_data_model"][WorkOnCol][obj]["rowscompleted"][n] = True
                                    AllRowsParsed = 0
                                    for n in range(len(self.__result)):
                                        if self.__parsed_query["post_data_model"][WorkOnCol][obj]["rowscompleted"][n]:
                                            AllRowsParsed += 1
                                    if AllRowsParsed == len(self.__result):
                                        self.__parsed_query["post_data_model"][WorkOnCol][obj]["done"] = True
                                case 'FCT':
                                    match self.__parsed_query["post_data_model"][WorkOnCol][obj]["function"]:
                                        case 'ABS':
                                            for n in range(len(self.__result)):
                                                if matriceROW[n] and not self.__parsed_query["post_data_model"][WorkOnCol][obj]["rowscompleted"][n]:
                                                    self.__parsed_query["post_data_model"][WorkOnCol][obj]["rowscompleted"][n] = True
                                                    if self.__parsed_query["post_data_model"][WorkOnCol][obj]["dependant"]:
                                                        self.__parsed_query["post_data_model"][WorkOnCol][obj]["result"][n] = abs(self.__parsed_query["post_data_model"][WorkOnCol][obj]["result"][n])
                                                    else:
                                                        self.__result[n][WorkOnCol] = abs(self.__parsed_query["post_data_model"][WorkOnCol][obj]["result"][n])
                                        case 'AVG':
                                            total = 0
                                            nbrows = 0
                                            for n in range(len(self.__result)):
                                                if matriceROW[n]:
                                                    total += self.__parsed_query["post_data_model"][WorkOnCol][obj]["result"][n]
                                                    nbrows += 1
                                            total = total / nbrows
                                            for n in range(len(self.__result)):
                                                if matriceROW[n] and not self.__parsed_query["post_data_model"][WorkOnCol][obj]["rowscompleted"][n]:
                                                    self.__parsed_query["post_data_model"][WorkOnCol][obj]["rowscompleted"][n] = True
                                                    if self.__parsed_query["post_data_model"][WorkOnCol][obj]["dependant"]:
                                                        self.__parsed_query["post_data_model"][WorkOnCol][obj]["result"][n] = total
                                                    else:
                                                        self.__result[n][WorkOnCol] = total
                                        case 'CHR':
                                            for n in range(len(self.__result)):
                                                if matriceROW[n] and not self.__parsed_query["post_data_model"][WorkOnCol][obj]["rowscompleted"][n]:
                                                    val_int = self.__parsed_query["post_data_model"][WorkOnCol][obj]["result"][n]
                                                    if self.__check_INT(val_int):
                                                        self.__parsed_query["post_data_model"][WorkOnCol][obj]["rowscompleted"][n] = True
                                                        if self.__parsed_query["post_data_model"][WorkOnCol][obj]["dependant"]:
                                                            self.__parsed_query["post_data_model"][WorkOnCol][obj]["result"][n] = str(chr(int(val_int)))
                                                        else:
                                                            self.__result[n][WorkOnCol] = str(chr(int(val_int)))
                                                    else:
                                                        raise vExcept(2310, val_int)
                                        case 'COUNT':
                                            total = 0
                                            for n in range(len(self.__result)):
                                                if matriceROW[n]:
                                                    total += self.__parsed_query["post_data_model"][WorkOnCol][obj]["result"][n]
                                            for n in range(len(self.__result)):
                                                if matriceROW[n] and not self.__parsed_query["post_data_model"][WorkOnCol][obj]["rowscompleted"][n]:
                                                    self.__parsed_query["post_data_model"][WorkOnCol][obj]["rowscompleted"][n] = True
                                                    if self.__parsed_query["post_data_model"][WorkOnCol][obj]["dependant"]:
                                                        self.__parsed_query["post_data_model"][WorkOnCol][obj]["result"][n] = total
                                                    else:
                                                        self.__result[n][WorkOnCol] = total
                                        case 'DECODE':
                                            for n in range(len(self.__result)):
                                                if matriceROW[n] and not self.__parsed_query["post_data_model"][WorkOnCol][obj]["rowscompleted"][n]:
                                                    param = []
                                                    for m in self.__parsed_query["post_data_model"][WorkOnCol][obj]["colval"][n]:
                                                        param.append(m[0])
                                                    self.__parsed_query["post_data_model"][WorkOnCol][obj]["rowscompleted"][n] = True
                                                    if self.__parsed_query["post_data_model"][WorkOnCol][obj]["dependant"]:
                                                        self.__parsed_query["post_data_model"][WorkOnCol][obj]["result"][n] = self.__DECODE(param)
                                                    else:
                                                        self.__result[n][WorkOnCol] = self.__DECODE(param)
                                        case 'LENGTH':
                                            for n in range(len(self.__result)):
                                                # print(self.__parsed_query["post_data_model"][WorkOnCol])
                                                # print(f'WorkOnCol={WorkOnCol}   obj={obj}')
                                                if matriceROW[n] and not self.__parsed_query["post_data_model"][WorkOnCol][obj]["rowscompleted"][n]:
                                                    val_int = self.__parsed_query["post_data_model"][WorkOnCol][obj]["result"][n]
                                                    self.__parsed_query["post_data_model"][WorkOnCol][obj]["rowscompleted"][n] = True
                                                    if self.__parsed_query["post_data_model"][WorkOnCol][obj]["dependant"]:
                                                        self.__parsed_query["post_data_model"][WorkOnCol][obj]["result"][n] = len(val_int)
                                                    else:
                                                        self.__result[n][WorkOnCol] = len(val_int)
                                        case 'LOWER':
                                            for n in range(len(self.__result)):
                                                if matriceROW[n] and not self.__parsed_query["post_data_model"][WorkOnCol][obj]["rowscompleted"][n]:
                                                    self.__parsed_query["post_data_model"][WorkOnCol][obj]["rowscompleted"][n] = True
                                                    val_int = str(self.__parsed_query["post_data_model"][WorkOnCol][obj]["result"][n]).lower()
                                                    if self.__parsed_query["post_data_model"][WorkOnCol][obj]["dependant"]:
                                                        self.__parsed_query["post_data_model"][WorkOnCol][obj]["result"][n] = val_int
                                                    else:
                                                        self.__result[n][WorkOnCol] = val_int
                                        case 'LPAD':
                                            for n in range(len(self.__result)):
                                                if matriceROW[n] and not self.__parsed_query["post_data_model"][WorkOnCol][obj]["rowscompleted"][n]:
                                                    self.__parsed_query["post_data_model"][WorkOnCol][obj]["rowscompleted"][n] = True
                                                    v1 = self.__parsed_query["post_data_model"][WorkOnCol][obj]["colval"][n][0][0]
                                                    v2 = self.__parsed_query["post_data_model"][WorkOnCol][obj]["colval"][n][1][0]
                                                    if len (self.__parsed_query["post_data_model"][WorkOnCol][obj]["colval"][n]) == 3:
                                                        v3 = self.__parsed_query["post_data_model"][WorkOnCol][obj]["colval"][n][2][0]
                                                    else:
                                                        v3 = ' '
                                                    vbegin = str(v3 * v2)[0:v2-len(v1)]
                                                    r = f'{vbegin}{v1}'
                                                    if self.__parsed_query["post_data_model"][WorkOnCol][obj]["dependant"]:
                                                        self.__parsed_query["post_data_model"][WorkOnCol][obj]["result"][n] = r
                                                    else:
                                                        self.__result[n][WorkOnCol] = r
                                        case 'LTRIM':
                                            for n in range(len(self.__result)):
                                                if matriceROW[n] and not self.__parsed_query["post_data_model"][WorkOnCol][obj]["rowscompleted"][n]:
                                                    self.__parsed_query["post_data_model"][WorkOnCol][obj]["rowscompleted"][n] = True
                                                    v1 = self.__parsed_query["post_data_model"][WorkOnCol][obj]["colval"][n][0][0]
                                                    if len (self.__parsed_query["post_data_model"][WorkOnCol][obj]["colval"][n]) == 2:
                                                        v2 = self.__parsed_query["post_data_model"][WorkOnCol][obj]["colval"][n][1][0]
                                                    else:
                                                        v2 = ' '
                                                    while v1[0:len(v2)] == v2:
                                                        v1 = v1[len(v2):]
                                                    if self.__parsed_query["post_data_model"][WorkOnCol][obj]["dependant"]:
                                                        self.__parsed_query["post_data_model"][WorkOnCol][obj]["result"][n] = v1
                                                    else:
                                                        self.__result[n][WorkOnCol] = v1
                                        case 'NVL':
                                            for n in range(len(self.__result)):
                                                if matriceROW[n] and not self.__parsed_query["post_data_model"][WorkOnCol][obj]["rowscompleted"][n]:
                                                    self.__parsed_query["post_data_model"][WorkOnCol][obj]["rowscompleted"][n] = True
                                                    v1 = self.__parsed_query["post_data_model"][WorkOnCol][obj]["colval"][n][0][0]
                                                    v2 = self.__parsed_query["post_data_model"][WorkOnCol][obj]["colval"][n][1][0]
                                                    if (v1 is None) or (v1 == ''):
                                                        if self.__parsed_query["post_data_model"][WorkOnCol][obj]["dependant"]:
                                                            self.__parsed_query["post_data_model"][WorkOnCol][obj]["result"][n] = v2
                                                        else:
                                                            self.__result[n][WorkOnCol] = v2
                                                    else:
                                                        if self.__parsed_query["post_data_model"][WorkOnCol][obj]["dependant"]:
                                                            self.__parsed_query["post_data_model"][WorkOnCol][obj]["result"][n] = v1
                                                        else:
                                                            self.__result[n][WorkOnCol] = v1
                                        case 'NVL2':
                                            for n in range(len(self.__result)):
                                                if matriceROW[n] and not self.__parsed_query["post_data_model"][WorkOnCol][obj]["rowscompleted"][n]:
                                                    self.__parsed_query["post_data_model"][WorkOnCol][obj]["rowscompleted"][n] = True
                                                    v1 = self.__parsed_query["post_data_model"][WorkOnCol][obj]["colval"][n][0][0]
                                                    v2 = self.__parsed_query["post_data_model"][WorkOnCol][obj]["colval"][n][1][0]
                                                    v3 = self.__parsed_query["post_data_model"][WorkOnCol][obj]["colval"][n][2][0]
                                                    if (v1 is None) or (v1 == ''):
                                                        if self.__parsed_query["post_data_model"][WorkOnCol][obj]["dependant"]:
                                                            self.__parsed_query["post_data_model"][WorkOnCol][obj]["result"][n] = v3
                                                        else:
                                                            self.__result[n][WorkOnCol] = v3
                                                    else:
                                                        if self.__parsed_query["post_data_model"][WorkOnCol][obj]["dependant"]:
                                                            self.__parsed_query["post_data_model"][WorkOnCol][obj]["result"][n] = v2
                                                        else:
                                                            self.__result[n][WorkOnCol] = v2
                                        case 'MAX':
                                            total = None
                                            for n in range(len(self.__result)):
                                                if matriceROW[n]:
                                                    v = self.__parsed_query["post_data_model"][WorkOnCol][obj]["result"][n]
                                                    if v is not None:
                                                        if (total is None) or (total < v):
                                                            total = v
                                            for n in range(len(self.__result)):
                                                if matriceROW[n] and not self.__parsed_query["post_data_model"][WorkOnCol][obj]["rowscompleted"][n]:
                                                    self.__parsed_query["post_data_model"][WorkOnCol][obj]["rowscompleted"][n] = True
                                                    if self.__parsed_query["post_data_model"][WorkOnCol][obj]["dependant"]:
                                                        self.__parsed_query["post_data_model"][WorkOnCol][obj]["result"][n] = total
                                                    else:
                                                        self.__result[n][WorkOnCol] = total
                                        case 'MIN':
                                            total = None
                                            for n in range(len(self.__result)):
                                                if matriceROW[n]:
                                                    v = self.__parsed_query["post_data_model"][WorkOnCol][obj]["result"][n]
                                                    if v is not None:
                                                        if (total is None) or (total > v):
                                                            total = v
                                            for n in range(len(self.__result)):
                                                if matriceROW[n] and not self.__parsed_query["post_data_model"][WorkOnCol][obj]["rowscompleted"][n]:
                                                    self.__parsed_query["post_data_model"][WorkOnCol][obj]["rowscompleted"][n] = True
                                                    if self.__parsed_query["post_data_model"][WorkOnCol][obj]["dependant"]:
                                                        self.__parsed_query["post_data_model"][WorkOnCol][obj]["result"][n] = total
                                                    else:
                                                        self.__result[n][WorkOnCol] = total
                                        case 'RPAD':
                                            for n in range(len(self.__result)):
                                                if matriceROW[n] and not self.__parsed_query["post_data_model"][WorkOnCol][obj]["rowscompleted"][n]:
                                                    self.__parsed_query["post_data_model"][WorkOnCol][obj]["rowscompleted"][n] = True
                                                    v1 = self.__parsed_query["post_data_model"][WorkOnCol][obj]["colval"][n][0][0]
                                                    v2 = self.__parsed_query["post_data_model"][WorkOnCol][obj]["colval"][n][1][0]
                                                    if len (self.__parsed_query["post_data_model"][WorkOnCol][obj]["colval"][n]) == 3:
                                                        v3 = self.__parsed_query["post_data_model"][WorkOnCol][obj]["colval"][n][2][0]
                                                    else:
                                                        v3 = ' '
                                                    vbegin = str(v3 * v2)[0:v2-len(v1)]
                                                    r = str(v1 + vbegin)
                                                    if self.__parsed_query["post_data_model"][WorkOnCol][obj]["dependant"]:
                                                        self.__parsed_query["post_data_model"][WorkOnCol][obj]["result"][n] = r
                                                    else:
                                                        self.__result[n][WorkOnCol] = r
                                        case 'RTRIM':
                                            for n in range(len(self.__result)):
                                                if matriceROW[n] and not self.__parsed_query["post_data_model"][WorkOnCol][obj]["rowscompleted"][n]:
                                                    self.__parsed_query["post_data_model"][WorkOnCol][obj]["rowscompleted"][n] = True
                                                    v1 = self.__parsed_query["post_data_model"][WorkOnCol][obj]["colval"][n][0][0]
                                                    if len (self.__parsed_query["post_data_model"][WorkOnCol][obj]["colval"][n]) == 2:
                                                        v2 = self.__parsed_query["post_data_model"][WorkOnCol][obj]["colval"][n][1][0]
                                                    else:
                                                        v2 = ' '
                                                    while v1[-len(v2):] == v2:
                                                        v1 = v1[:-len(v2)]
                                                    if self.__parsed_query["post_data_model"][WorkOnCol][obj]["dependant"]:
                                                        self.__parsed_query["post_data_model"][WorkOnCol][obj]["result"][n] = v1
                                                    else:
                                                        self.__result[n][WorkOnCol] = v1
                                        case 'SUBSTR':
                                            for n in range(len(self.__result)):
                                                if matriceROW[n] and not self.__parsed_query["post_data_model"][WorkOnCol][obj]["rowscompleted"][n]:
                                                    self.__parsed_query["post_data_model"][WorkOnCol][obj]["rowscompleted"][n] = True
                                                    strin = self.__parsed_query["post_data_model"][WorkOnCol][obj]["colval"][n][0][0]
                                                    sttin = self.__parsed_query["post_data_model"][WorkOnCol][obj]["colval"][n][1][0]
                                                    lenin = self.__parsed_query["post_data_model"][WorkOnCol][obj]["colval"][n][2][0]
                                                    if self.__parsed_query["post_data_model"][WorkOnCol][obj]["dependant"]:
                                                        self.__parsed_query["post_data_model"][WorkOnCol][obj]["result"][n] = self.__SUBSTR(strin, sttin, lenin)
                                                    else:
                                                        self.__result[n][WorkOnCol] = self.__SUBSTR(strin, sttin, lenin)
                                        case 'SUM':
                                            total = 0
                                            for n in range(len(self.__result)):
                                                if matriceROW[n]:
                                                    total += self.__parsed_query["post_data_model"][WorkOnCol][obj]["result"][n]
                                            for n in range(len(self.__result)):
                                                if matriceROW[n] and not self.__parsed_query["post_data_model"][WorkOnCol][obj]["rowscompleted"][n]:
                                                    self.__parsed_query["post_data_model"][WorkOnCol][obj]["rowscompleted"][n] = True
                                                    if self.__parsed_query["post_data_model"][WorkOnCol][obj]["dependant"]:
                                                        self.__parsed_query["post_data_model"][WorkOnCol][obj]["result"][n] = total
                                                    else:
                                                        self.__result[n][WorkOnCol] = total
                                        case 'TO_CHAR':
                                            for n in range(len(self.__result)):
                                                if matriceROW[n] and not self.__parsed_query["post_data_model"][WorkOnCol][obj]["rowscompleted"][n]:
                                                    self.__parsed_query["post_data_model"][WorkOnCol][obj]["rowscompleted"][n] = True
                                                    dtein = self.__parsed_query["post_data_model"][WorkOnCol][obj]["colval"][n][0][0]
                                                    fmtin = self.__parsed_query["post_data_model"][WorkOnCol][obj]["colval"][n][1][0]
                                                    if self.__parsed_query["post_data_model"][WorkOnCol][obj]["dependant"]:
                                                        self.__parsed_query["post_data_model"][WorkOnCol][obj]["result"][n] = self.__TO_CHAR(dtein, fmtin)
                                                    else:
                                                        self.__result[n][WorkOnCol] = self.__TO_CHAR(dtein, fmtin)
                                        case 'INSTR':
                                            for n in range(len(self.__result)):
                                                if matriceROW[n] and not self.__parsed_query["post_data_model"][WorkOnCol][obj]["rowscompleted"][n]:
                                                    self.__parsed_query["post_data_model"][WorkOnCol][obj]["rowscompleted"][n] = True
                                                    instr = self.__parsed_query["post_data_model"][WorkOnCol][obj]["colval"][n][0][0]
                                                    insubstr = self.__parsed_query["post_data_model"][WorkOnCol][obj]["colval"][n][1][0]
                                                    match len(self.__parsed_query["post_data_model"][WorkOnCol][obj]["colval"][n]):
                                                        case 2:
                                                            inposition = 0
                                                            inoccurence = 1
                                                        case 3:
                                                            inposition = int(self.__parsed_query["post_data_model"][WorkOnCol][obj]["colval"][n][2][0])
                                                            inoccurence = 1
                                                        case 4:
                                                            inposition = int(self.__parsed_query["post_data_model"][WorkOnCol][obj]["colval"][n][2][0])
                                                            inoccurence = int(self.__parsed_query["post_data_model"][WorkOnCol][obj]["colval"][n][3][0])
                                                    if self.__parsed_query["post_data_model"][WorkOnCol][obj]["dependant"]:
                                                        self.__parsed_query["post_data_model"][WorkOnCol][obj]["result"][n] = self.__INSTR(instr, insubstr, inposition, inoccurence)
                                                    else:
                                                        self.__result[n][WorkOnCol] = self.__INSTR(instr, insubstr, inposition, inoccurence)
                                        case 'TRUNC':
                                            for n in range(len(self.__result)):
                                                if matriceROW[n] and not self.__parsed_query["post_data_model"][WorkOnCol][obj]["rowscompleted"][n]:
                                                    self.__parsed_query["post_data_model"][WorkOnCol][obj]["rowscompleted"][n] = True
                                                    match len(self.__parsed_query["post_data_model"][WorkOnCol][obj]["colval"][n]):
                                                        case 1:
                                                            instr = self.__parsed_query["post_data_model"][WorkOnCol][obj]["colval"][n][0][0]
                                                            precision = 0
                                                        case 2:
                                                            instr = self.__parsed_query["post_data_model"][WorkOnCol][obj]["colval"][n][0][0]
                                                            precision = int(self.__parsed_query["post_data_model"][WorkOnCol][obj]["colval"][n][1][0])
                                                    if self.__parsed_query["post_data_model"][WorkOnCol][obj]["dependant"]:
                                                        self.__parsed_query["post_data_model"][WorkOnCol][obj]["result"][n] = self.__TRUNC(instr, precision)
                                                    else:
                                                        self.__result[n][WorkOnCol] = self.__TRUNC(instr, precision)
                                        case 'EXP':
                                            for n in range(len(self.__result)):
                                                if matriceROW[n] and not self.__parsed_query["post_data_model"][WorkOnCol][obj]["rowscompleted"][n]:
                                                    self.__parsed_query["post_data_model"][WorkOnCol][obj]["rowscompleted"][n] = True
                                                    inval = self.__parsed_query["post_data_model"][WorkOnCol][obj]["colval"][n][0][0]
                                                    if self.__parsed_query["post_data_model"][WorkOnCol][obj]["dependant"]:
                                                        self.__parsed_query["post_data_model"][WorkOnCol][obj]["result"][n] = self.__EXP(inval)
                                                    else:
                                                        self.__result[n][WorkOnCol] = self.__EXP(inval)
                                        case 'LN':
                                            for n in range(len(self.__result)):
                                                if matriceROW[n] and not self.__parsed_query["post_data_model"][WorkOnCol][obj]["rowscompleted"][n]:
                                                    self.__parsed_query["post_data_model"][WorkOnCol][obj]["rowscompleted"][n] = True
                                                    inval = self.__parsed_query["post_data_model"][WorkOnCol][obj]["colval"][n][0][0]
                                                    if self.__parsed_query["post_data_model"][WorkOnCol][obj]["dependant"]:
                                                        self.__parsed_query["post_data_model"][WorkOnCol][obj]["result"][n] = self.__LN(inval)
                                                    else:
                                                        self.__result[n][WorkOnCol] = self.__LN(inval)
                                        case 'LOG':
                                            for n in range(len(self.__result)):
                                                if matriceROW[n] and not self.__parsed_query["post_data_model"][WorkOnCol][obj]["rowscompleted"][n]:
                                                    self.__parsed_query["post_data_model"][WorkOnCol][obj]["rowscompleted"][n] = True
                                                    inval = self.__parsed_query["post_data_model"][WorkOnCol][obj]["colval"][n][0][0]
                                                    if self.__parsed_query["post_data_model"][WorkOnCol][obj]["dependant"]:
                                                        self.__parsed_query["post_data_model"][WorkOnCol][obj]["result"][n] = self.__LOG(inval)
                                                    else:
                                                        self.__result[n][WorkOnCol] = self.__LOG(inval)
                                        case 'ACOS':
                                            for n in range(len(self.__result)):
                                                if matriceROW[n] and not self.__parsed_query["post_data_model"][WorkOnCol][obj]["rowscompleted"][n]:
                                                    self.__parsed_query["post_data_model"][WorkOnCol][obj]["rowscompleted"][n] = True
                                                    inval = self.__parsed_query["post_data_model"][WorkOnCol][obj]["colval"][n][0][0]
                                                    if self.__parsed_query["post_data_model"][WorkOnCol][obj]["dependant"]:
                                                        self.__parsed_query["post_data_model"][WorkOnCol][obj]["result"][n] = self.__ACOS(inval)
                                                    else:
                                                        self.__result[n][WorkOnCol] = self.__ACOS(inval)
                                        case 'ASIN':
                                            for n in range(len(self.__result)):
                                                if matriceROW[n] and not self.__parsed_query["post_data_model"][WorkOnCol][obj]["rowscompleted"][n]:
                                                    self.__parsed_query["post_data_model"][WorkOnCol][obj]["rowscompleted"][n] = True
                                                    inval = self.__parsed_query["post_data_model"][WorkOnCol][obj]["colval"][n][0][0]
                                                    if self.__parsed_query["post_data_model"][WorkOnCol][obj]["dependant"]:
                                                        self.__parsed_query["post_data_model"][WorkOnCol][obj]["result"][n] = self.__ASIN(inval)
                                                    else:
                                                        self.__result[n][WorkOnCol] = self.__ASIN(inval)
                                        case 'ATAN':
                                            for n in range(len(self.__result)):
                                                if matriceROW[n] and not self.__parsed_query["post_data_model"][WorkOnCol][obj]["rowscompleted"][n]:
                                                    self.__parsed_query["post_data_model"][WorkOnCol][obj]["rowscompleted"][n] = True
                                                    inval = self.__parsed_query["post_data_model"][WorkOnCol][obj]["colval"][n][0][0]
                                                    if self.__parsed_query["post_data_model"][WorkOnCol][obj]["dependant"]:
                                                        self.__parsed_query["post_data_model"][WorkOnCol][obj]["result"][n] = self.__ATAN(inval)
                                                    else:
                                                        self.__result[n][WorkOnCol] = self.__ATAN(inval)
                                        case 'ATAN2':
                                            for n in range(len(self.__result)):
                                                if matriceROW[n] and not self.__parsed_query["post_data_model"][WorkOnCol][obj]["rowscompleted"][n]:
                                                    self.__parsed_query["post_data_model"][WorkOnCol][obj]["rowscompleted"][n] = True
                                                    inval1 = self.__parsed_query["post_data_model"][WorkOnCol][obj]["colval"][n][0][0]
                                                    inval2 = self.__parsed_query["post_data_model"][WorkOnCol][obj]["colval"][n][1][0]
                                                    if self.__parsed_query["post_data_model"][WorkOnCol][obj]["dependant"]:
                                                        self.__parsed_query["post_data_model"][WorkOnCol][obj]["result"][n] = self.__ATAN2(inval1, inval2)
                                                    else:
                                                        self.__result[n][WorkOnCol] = self.__ATAN2(inval1, inval2)
                                        case 'COS':
                                            for n in range(len(self.__result)):
                                                if matriceROW[n] and not self.__parsed_query["post_data_model"][WorkOnCol][obj]["rowscompleted"][n]:
                                                    self.__parsed_query["post_data_model"][WorkOnCol][obj]["rowscompleted"][n] = True
                                                    inval = self.__parsed_query["post_data_model"][WorkOnCol][obj]["colval"][n][0][0]
                                                    if self.__parsed_query["post_data_model"][WorkOnCol][obj]["dependant"]:
                                                        self.__parsed_query["post_data_model"][WorkOnCol][obj]["result"][n] = self.__COS(inval)
                                                    else:
                                                        self.__result[n][WorkOnCol] = self.__COS(inval)
                                        case 'SIN':
                                            for n in range(len(self.__result)):
                                                if matriceROW[n] and not self.__parsed_query["post_data_model"][WorkOnCol][obj]["rowscompleted"][n]:
                                                    self.__parsed_query["post_data_model"][WorkOnCol][obj]["rowscompleted"][n] = True
                                                    inval = self.__parsed_query["post_data_model"][WorkOnCol][obj]["colval"][n][0][0]
                                                    if self.__parsed_query["post_data_model"][WorkOnCol][obj]["dependant"]:
                                                        self.__parsed_query["post_data_model"][WorkOnCol][obj]["result"][n] = self.__SIN(inval)
                                                    else:
                                                        self.__result[n][WorkOnCol] = self.__SIN(inval)
                                        case 'TAN':
                                            for n in range(len(self.__result)):
                                                if matriceROW[n] and not self.__parsed_query["post_data_model"][WorkOnCol][obj]["rowscompleted"][n]:
                                                    self.__parsed_query["post_data_model"][WorkOnCol][obj]["rowscompleted"][n] = True
                                                    inval = self.__parsed_query["post_data_model"][WorkOnCol][obj]["colval"][n][0][0]
                                                    if self.__parsed_query["post_data_model"][WorkOnCol][obj]["dependant"]:
                                                        self.__parsed_query["post_data_model"][WorkOnCol][obj]["result"][n] = self.__TAN(inval)
                                                    else:
                                                        self.__result[n][WorkOnCol] = self.__TAN(inval)
                                        case 'UPPER':
                                            for n in range(len(self.__result)):
                                                if matriceROW[n] and not self.__parsed_query["post_data_model"][WorkOnCol][obj]["rowscompleted"][n]:
                                                    self.__parsed_query["post_data_model"][WorkOnCol][obj]["rowscompleted"][n] = True
                                                    val_int = str(self.__parsed_query["post_data_model"][WorkOnCol][obj]["result"][n]).upper()
                                                    if self.__parsed_query["post_data_model"][WorkOnCol][obj]["dependant"]:
                                                        self.__parsed_query["post_data_model"][WorkOnCol][obj]["result"][n] = val_int
                                                    else:
                                                        self.__result[n][WorkOnCol] = val_int
                                    AllRowsParsed = 0
                                    for n in range(len(self.__result)):
                                        if self.__parsed_query["post_data_model"][WorkOnCol][obj]["rowscompleted"][n]:
                                            AllRowsParsed += 1
                                    if AllRowsParsed == len(self.__result):
                                        self.__parsed_query["post_data_model"][WorkOnCol][obj]["done"] = True
                # print(f'__process_groupby post_data_model={self.__parsed_query["post_data_model"]}')
            # check if all rows are parsed
            AllRowsParsed = True
            for value in parsedROW:
                if not value:
                    AllRowsParsed = False
            # check if all columns are parsed
            AllObjDone = True
            for WorkOnCol in self.__parsed_query["post_data_model"].keys():
                for obj in self.__parsed_query["post_data_model"][WorkOnCol].keys():
                    if not self.__parsed_query["post_data_model"][WorkOnCol][obj]["done"]:
                        AllObjDone = False
            if AllObjDone:
                StillWork = False
            else:
                StillWork = True
                rowidx = 0
                if AllRowsParsed:
                    parsedROW = [False for x in range(len(self.__result))]
        # remove duplicate rows
        tmpres = []
        for row in self.__result:
            if row not in tmpres:
                tmpres.append(row)
        self.__result = tmpres

    def __process_orderby(self, col_lst):
        self.__result = self.__sort(self.__result, col_lst, 0, bool(self.__parsed_query["order_by"][0][1] == 'ASC'))

    def __sort(self, src, col_lst, col, asc):
        a=0
        result = []
        colsort = self.__parsed_query["order_by"][col][0]
        while len(src) > 0:
            min_idx = 0
            srt_cols = []
            # print(f'__sort len={len(src)} col={colsort} asc={asc}')
            for n in range(len(src)):
                match col_lst[colsort][1].upper():
                    case 'INT':
                        if asc:
                            if int(src[n][colsort]) < int(src[min_idx][colsort]):
                                min_idx = n
                        else:
                            if int(src[n][colsort]) > int(src[min_idx][colsort]):
                                min_idx = n
                    case 'FLOAT':
                        if asc:
                            if float(src[n][colsort]) < float(src[min_idx][colsort]):
                                min_idx = n
                        else:
                            if float(src[n][colsort]) > float(src[min_idx][colsort]):
                                min_idx = n
                    case _:
                        if asc:
                            if str(src[n][colsort]) < str(src[min_idx][colsort]):
                                min_idx = n
                        else:
                            if str(src[n][colsort]) > str(src[min_idx][colsort]):
                                min_idx = n
            min_val = src[min_idx][colsort]
            for n in range(len(src)-1, -1, -1):
                # print(f'__sort src[n][colsort]={src[n][colsort]} src[min_idx][colsort]={min_val}')
                if src[n][colsort] == min_val:
                    srt_cols.append(src[n])
                    del src[n]
            if col < len(self.__parsed_query["order_by"])-1:
                srt_cols = self.__sort(srt_cols, col_lst, col+1, bool(self.__parsed_query["order_by"][col+1][1] == 'ASC'))
            result = result + copy.deepcopy(srt_cols)
            a += 1
            if a > 10:
                break
        return result

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
        # print(f'__compare_cols c1={c1} c2={c2} oper={oper}')
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
            case _:
                raise vExcept(510, oper)
        # print(f'__compare_cols result={result}')
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
            reg = datetime.fromtimestamp(varin, tz=None)
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
                vsess = vCursor(self.db, self.__session_username, self.__password, self.__updated_tables, self.__session)
                vsess.execute(_query=self.__getCursorQuery(tbl[2]), bind=self.__bind)
                result = {"rows":vsess.fetchall(), "columns":vsess.description}
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
                if (cblk[3] == '*') and (self.__parsed_query["functions"][n][1] == 'COUNT'):
                    pass
                elif cblk[4] == 'COLUMN':
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

    def __validate_left_outer_where(self):
        for b in range (len(self.__parsed_query["parsed_left_outer_where"])):
            for n in range (len(self.__parsed_query["parsed_left_outer_where"][b][1])):
                if (self.__parsed_query["parsed_left_outer_where"][b][1][n][1][0] == "TST") and (self.__parsed_query["parsed_left_outer_where"][b][1][n][1][5] == 'COLUMN'):
                    _, aliasin, _, _, tf, ctf, _ = self.__searchColInFromTables(colin=self.__parsed_query["parsed_left_outer_where"][b][1][n][1][4],
                                                                                aliasin=self.__parsed_query["parsed_left_outer_where"][b][1][n][1][3],
                                                                                table_namein=self.__parsed_query["parsed_left_outer_where"][b][1][n][1][7],
                                                                                schemain=self.__parsed_query["parsed_left_outer_where"][b][1][n][1][6])
                    self.__parsed_query["parsed_left_outer_where"][b][1][n][1][1] = tf
                    self.__parsed_query["parsed_left_outer_where"][b][1][n][1][2] = ctf
                    self.__parsed_query["parsed_left_outer_where"][b][1][n][1][3] = aliasin
                if (self.__parsed_query["parsed_left_outer_where"][b][1][n][3][0] == "TST") and (self.__parsed_query["parsed_left_outer_where"][b][1][n][3][5] == 'COLUMN'):
                    _, aliasin, _, _, tf, ctf, _ = self.__searchColInFromTables(colin=self.__parsed_query["parsed_left_outer_where"][b][1][n][3][4],
                                                                                aliasin=self.__parsed_query["parsed_left_outer_where"][b][1][n][3][3],
                                                                                table_namein=self.__parsed_query["parsed_left_outer_where"][b][1][n][3][7],
                                                                                schemain=self.__parsed_query["parsed_left_outer_where"][b][1][n][3][6])
                    self.__parsed_query["parsed_left_outer_where"][b][1][n][3][1] = tf
                    self.__parsed_query["parsed_left_outer_where"][b][1][n][3][2] = ctf
                    self.__parsed_query["parsed_left_outer_where"][b][1][n][3][3] = aliasin

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

    def __getCursorID(self, cur_name):
        # cursors: cursor_alias, query
        id = None
        for n in range(len(self.__parsed_query['cursors'])):
            if self.__parsed_query['cursors'][n][0] == cur_name:
                id = n
                break
        if id is None:
            raise vExcept(2100, cur_name)
        else:
            return id

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
                        if blk[1][4] == 'ROWNUM':
                            tmp_res.append(len(self.__result))
                        else:
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
                    tmp_res = tmp_res + str(self.__compute_maths(blk[3]))
                case 'COLUMN':
                    if blk[4] == 'ROWNUM':
                        tmp_res = tmp_res + f'{len(self.__result)}'
                    else:
                        val = self.__remove_quote(self.__parsed_query["from"][blk[6]][4][0]["rows"][self.__RowsPosInTables[blk[6]]][blk[7]])
                        if (val is not None) and (val != ''):
                            tmp_res = tmp_res + str(self.__remove_quote(val))
        return tmp_res

    def __get_function_col(self, colblk):
        if colblk[4] == 'COLUMN':
            if colblk[3] == 'ROWNUM':
                return len(self.__result)
            else:
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
            case 'DECODE':
                if len(self.__parsed_query["functions"][fct_num][2]) % 2 != 0:
                    raise vExcept(2308, len(self.__parsed_query["functions"][fct_num][2]))
                param = []
                for x in self.__parsed_query["functions"][fct_num][2]:
                    param.append(self.__get_function_col(x))
                return self.__DECODE(param)
                # mainval = self.__get_function_col(self.__parsed_query["functions"][fct_num][2][0])
                # n = 1
                # dont_stop_flg = True
                # while (n+1 < len(self.__parsed_query["functions"][fct_num][2])) and dont_stop_flg:
                #     if self.__get_function_col(self.__parsed_query["functions"][fct_num][2][n]) == mainval:
                #         res = self.__get_function_col(self.__parsed_query["functions"][fct_num][2][n+1])
                #         dont_stop_flg = False
                #     else:
                #         n += 2
                # if dont_stop_flg:
                #     res = self.__get_function_col(self.__parsed_query["functions"][fct_num][2][-1])
                # return res
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
                return self.__INSTR(instr, insubstr, inposition, inoccurence)
                # if (inposition >= len(instr)) or (inoccurence < 1):
                #     return 0
                # while inposition < len(instr):
                #     try:
                #         foundin = instr[inposition:].index(insubstr)
                #         if foundin >= 0:
                #             if inoccurence == 1:
                #                 return foundin+inposition+1
                #             else:
                #                 inoccurence -= 1
                #                 inposition = inposition+foundin+1
                #         else:
                #             return 0
                #     except ValueError:
                #         return 0
            case 'LENGTH':
                if len(self.__parsed_query["functions"][fct_num][2]) != 1:
                    raise vExcept(2322, len(self.__parsed_query["functions"][fct_num][2]))
                return len(self.__remove_quote(self.__get_function_col(self.__parsed_query["functions"][fct_num][2][0])))
            case 'LOWER':
                value = self.__get_function_col(self.__parsed_query["functions"][fct_num][2][0])
                return str(value).lower()
            case 'LPAD':
                if len(self.__parsed_query["functions"][fct_num][2]) not in [2, 3]:
                    raise vExcept(2316, len(self.__parsed_query["functions"][fct_num][2]))
                v1 = self.__remove_quote(self.__get_function_col(self.__parsed_query["functions"][fct_num][2][0]))
                v2 = self.__remove_quote(self.__get_function_col(self.__parsed_query["functions"][fct_num][2][1]))
                if not self.__check_INT(v2):
                    raise vExcept(2317, v2)
                if len(self.__parsed_query["functions"][fct_num][2]) == 3:
                    v3 = self.__remove_quote(self.__get_function_col(self.__parsed_query["functions"][fct_num][2][2]))
                else:
                    v3 = ' '
                vbegin = str(v3 * v2)[0:v2-len(v1)]
                return str(vbegin + v1)
            case 'LTRIM':
                if len(self.__parsed_query["functions"][fct_num][2]) not in [1, 2]:
                    raise vExcept(2320, len(self.__parsed_query["functions"][fct_num][2]))
                v1 = self.__remove_quote(self.__get_function_col(self.__parsed_query["functions"][fct_num][2][0]))
                if len(self.__parsed_query["functions"][fct_num][2]) == 2:
                    v2 = self.__remove_quote(self.__get_function_col(self.__parsed_query["functions"][fct_num][2][1]))
                else:
                    v2 = ' '
                while v1[0:len(v2)] == v2:
                    v1 = v1[len(v2):]
                return v1
            case 'NVL':
                if len(self.__parsed_query["functions"][fct_num][2]) != 2:
                    raise vExcept(2314, len(self.__parsed_query["functions"][fct_num][2]))
                v1 = self.__remove_quote(self.__get_function_col(self.__parsed_query["functions"][fct_num][2][0]))
                v2 = self.__remove_quote(self.__get_function_col(self.__parsed_query["functions"][fct_num][2][1]))
                if (v1 is None) or (v1 == ''):
                    return v2
                else:
                    return v1
            case 'NVL2':
                if len(self.__parsed_query["functions"][fct_num][2]) != 3:
                    raise vExcept(2315, len(self.__parsed_query["functions"][fct_num][2]))
                v1 = self.__remove_quote(self.__get_function_col(self.__parsed_query["functions"][fct_num][2][0]))
                v2 = self.__remove_quote(self.__get_function_col(self.__parsed_query["functions"][fct_num][2][1]))
                v3 = self.__remove_quote(self.__get_function_col(self.__parsed_query["functions"][fct_num][2][2]))
                if (v1 is None) or (v1 == ''):
                    return v3
                else:
                    return v2
            case 'RPAD':
                if len(self.__parsed_query["functions"][fct_num][2]) not in [2, 3]:
                    raise vExcept(2318, len(self.__parsed_query["functions"][fct_num][2]))
                v1 = self.__remove_quote(self.__get_function_col(self.__parsed_query["functions"][fct_num][2][0]))
                v2 = self.__remove_quote(self.__get_function_col(self.__parsed_query["functions"][fct_num][2][1]))
                if not self.__check_INT(v2):
                    raise vExcept(2319, v2)
                if len(self.__parsed_query["functions"][fct_num][2]) == 3:
                    v3 = self.__remove_quote(self.__get_function_col(self.__parsed_query["functions"][fct_num][2][2]))
                else:
                    v3 = ' '
                vbegin = str(v3 * v2)[0:v2-len(v1)]
                return str(v1 + vbegin)
            case 'RTRIM':
                if len(self.__parsed_query["functions"][fct_num][2]) not in [1, 2]:
                    raise vExcept(2321, len(self.__parsed_query["functions"][fct_num][2]))
                v1 = self.__remove_quote(self.__get_function_col(self.__parsed_query["functions"][fct_num][2][0]))
                if len(self.__parsed_query["functions"][fct_num][2]) == 2:
                    v2 = self.__remove_quote(self.__get_function_col(self.__parsed_query["functions"][fct_num][2][1]))
                else:
                    v2 = ' '
                while v1[-len(v2):] == v2:
                    v1 = v1[:-len(v2)]
                return v1
            case 'SUBSTR':
                if len(self.__parsed_query["functions"][fct_num][2]) != 3:
                    raise vExcept(2300, len(self.__parsed_query["functions"][fct_num][2]))
                strin = self.__remove_quote(self.__get_function_col(self.__parsed_query["functions"][fct_num][2][0]))
                sttin = int(self.__get_function_col(self.__parsed_query["functions"][fct_num][2][1])) - 1
                lenin = int(self.__get_function_col(self.__parsed_query["functions"][fct_num][2][2]))
                return self.__SUBSTR(strin, sttin, lenin)
            case 'TO_CHAR':
                if len(self.__parsed_query["functions"][fct_num][2]) != 2:
                    raise vExcept(2305, len(self.__parsed_query["functions"][fct_num][2]))
                dtein = self.__get_function_col(self.__parsed_query["functions"][fct_num][2][0])
                fmtin = self.__get_function_col(self.__parsed_query["functions"][fct_num][2][1])
                return self.__TO_CHAR(dtein, fmtin)
            case 'UPPER':
                value = self.__get_function_col(self.__parsed_query["functions"][fct_num][2][0])
                return str(value).upper()
            case 'SIN':
                value = self.__get_function_col(self.__parsed_query["functions"][fct_num][2][0])
                return self.__SIN(value)
            case 'COS':
                value = self.__get_function_col(self.__parsed_query["functions"][fct_num][2][0])
                return self.__COS(value)
            case 'TAN':
                value = self.__get_function_col(self.__parsed_query["functions"][fct_num][2][0])
                return self.__TAN(value)
            case 'EXP':
                value = self.__get_function_col(self.__parsed_query["functions"][fct_num][2][0])
                return self.__EXP(value)
            case 'LN':
                value = self.__get_function_col(self.__parsed_query["functions"][fct_num][2][0])
                return self.__LN(value)
            case 'LOG':
                value = self.__get_function_col(self.__parsed_query["functions"][fct_num][2][0])
                return self.__LOG(value)
            case 'ACOS':
                value = self.__get_function_col(self.__parsed_query["functions"][fct_num][2][0])
                return self.__ACOS(value)
            case 'ASIN':
                value = self.__get_function_col(self.__parsed_query["functions"][fct_num][2][0])
                return self.__ASIN(value)
            case 'ATAN':
                value = self.__get_function_col(self.__parsed_query["functions"][fct_num][2][0])
                return self.__ATAN(value)
            case 'ATAN2':
                val1 = self.__get_function_col(self.__parsed_query["functions"][fct_num][2][0])
                val2 = self.__get_function_col(self.__parsed_query["functions"][fct_num][2][1])
                return self.__ATAN2(val1, val2)
            case 'TRUNC':
                if len(self.__parsed_query["functions"][fct_num][2]) == 1:
                    valin = self.__get_function_col(self.__parsed_query["functions"][fct_num][2][0])
                    precision = 0
                elif len(self.__parsed_query["functions"][fct_num][2]) == 2:
                    valin = self.__get_function_col(self.__parsed_query["functions"][fct_num][2][0])
                    precision = self.__get_function_col(self.__parsed_query["functions"][fct_num][2][1])
                else:
                    raise vExcept(2327, len(self.__parsed_query["functions"][fct_num][2]))
                return self.__TRUNC(valin, precision)

    def __get_function_type(self, fct_name: str, ref_col_typ: str):
        match fct_name:
            case 'CHR'|'LENGTH'|'LOWER'|'LPAD'|'LTRIM'|'RPAD'|'RTRIM'|'SUBSTR'|'TO_CHAR'|'UPPER':
                return 'str'
            case 'COUNT'|'INSTR':
                return 'int'
            case 'AVG'|'ACOS'|'ASIN'|'ATAN'|'ATAN2'|'COS'|'EXP'|'LN'|'LOG'|'MAX'|'MIN'|'SIN'|'SUM'|'TAN':
                return 'float'
            case 'ABS':
                if ref_col_typ.upper() in ['INT', 'FLOAT']:
                    return ref_col_typ.lower()
                else:
                    return 'float'
            case 'DECODE'|'NVL'|'NVL2':
                if ref_col_typ.upper() in ['INT', 'FLOAT', 'STR', 'HEX', 'DATETIME']:
                    return ref_col_typ.lower()
                else:
                    return 'str'
            case 'TRUNC':
                if ref_col_typ.upper() == 'DATETIME':
                    return 'datetime'
                else:
                    return 'float'
            case _:
                raise vExcept(2304, fct_name)

    def __INSTR(self, instr, insubstr, inposition, inoccurence):
        if (inposition >= len(instr)) or (inoccurence < 1):
            return 0
        while inposition < len(instr):
            try:
                foundin = instr[inposition:].index(insubstr)
                if foundin >= 0:
                    if inoccurence == 1:
                        return foundin+inposition+1
                    else:
                        inoccurence -= 1
                        inposition = inposition+foundin+1
                else:
                    return 0
            except ValueError:
                return 0

    def __DECODE(self, param):
        mainval = param[0]
        n = 1
        dont_stop_flg = True
        while (n+1 < len(param)) and dont_stop_flg:
            if param[n] == mainval:
                res = param[n+1]
                dont_stop_flg = False
            else:
                n += 2
        if dont_stop_flg:
            res = param[-1]
        return res

    def __SUBSTR(self, strin, sttin, lenin):
        if sttin < 0:
            sttin = 0
        if not self.__check_STR(strin):
            raise vExcept(2301, strin)
        if not self.__check_INT(sttin):
            raise vExcept(2302, sttin)
        if not self.__check_INT(lenin):
            raise vExcept(2303, lenin)
        return strin[sttin:sttin+lenin]
        
    def __TO_CHAR(self, dtein, fmtin):
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

    def __TRUNC(self, inval, precision):
        if (not self.__check_DATETIME(inval)) and (not self.__check_FLOAT(inval)):
            raise vExcept(2324, inval)
        if not self.__check_INT(precision):
            raise vExcept(2325, precision)
        if precision > 18:
            raise vExcept(2326, precision)
        factor = 10 ** precision
        return int(inval * factor) / factor

    def __COS(self, inval):
        if (not self.__check_FLOAT(inval)):
            raise vExcept(2333, inval)
        return math.cos(inval)

    def __SIN(self, inval):
        if (not self.__check_FLOAT(inval)):
            raise vExcept(2334, inval)
        return math.sin(inval)

    def __TAN(self, inval):
        if (not self.__check_FLOAT(inval)):
            raise vExcept(2335, inval)
        return math.tan(inval)

    def __EXP(self, inval):
        if (not self.__check_FLOAT(inval)):
            raise vExcept(2336, inval)
        return math.exp(inval)

    def __LN(self, inval):
        if (not self.__check_FLOAT(inval)):
            raise vExcept(2337, inval)
        return math.log(inval)

    def __LOG(self, inval):
        if (not self.__check_FLOAT(inval)):
            raise vExcept(2338, inval)
        return math.log10(inval)

    def __ACOS(self, inval):
        if (not self.__check_FLOAT(inval)):
            raise vExcept(2328, inval)
        return math.acos(inval)

    def __ASIN(self, inval):
        if (not self.__check_FLOAT(inval)):
            raise vExcept(2329, inval)
        return math.asin(inval)

    def __ATAN(self, inval):
        if (not self.__check_FLOAT(inval)):
            raise vExcept(2330, inval)
        return math.atan(inval)

    def __ATAN2(self, inval1, inval2):
        if (not self.__check_FLOAT(inval1)):
            raise vExcept(2331, inval1)
        if (not self.__check_FLOAT(inval2)):
            raise vExcept(2331, inval2)
        return math.atan2(inval1, inval2)
