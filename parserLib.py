import re
import random
import datetime
from vExceptLib import vExcept

# querytype: (SELECT, INSERT, UPDATE, DELETE, GRANT, REVOKE, CREATE, DROP, DESCRIBE, COMMIT, ROLLBACK)
# select: table_alias, schema, table_name, col_name/value, alias, type(COL, INT, FLOAT, STR, HEX, DATETIME, FUNCTION, MATHS, PIPE), table position, position in table, table or cursor, [-]
# create: 'TABLE', t_owner, t_name, ([c_cols], [cursor_id]
#         'USER', username, password
# from: table_alias, schema, table_name, TABLE or CURSOR, {table}
# cursors: cursor_alias, query
# inner_where: list of items
# parsed_inner_where: item_id, field1, oper, field2
#                  or item_id, ['META', item_id], oper, ['META', item_id]
#                  or item_id, ['TST', num_table, num_col, alias1, field1, type, schema, table_name, table or cursor], oper, ['TST', num_table, num_col, alias2, field2, type, schema, table_name, table or cursor]
# parsed_where: item_id, field1, oper, field2
#            or item_id, ['META', item_id], oper, ['META', item_id]
#            or item_id, ['TST', num_table, num_col, alias1, field1, type, schema, table_name, table or cursor], oper, ['TST', num_table, num_col, alias2, field2, type, schema, table_name, table or cursor]
#            or item_id, ['TST', num_table, num_col, alias1, field1, type, schema, table_name, table or cursor], 'BETWEEN', ['TST', num_table, num_col, alias2, field2, type, schema, table_name, table or cursor], ['TST', num_table, num_col, alias2, field2, type, schema, table_name, table or cursor]
# functions : [fct_id, fct_name, [[table_alias, schema, table_name, col_name/value, type(COL, INT, FLOAT, STR, HEX, DATETIME, FUNCTION, MATHS, PIPE), table position, position in table, table or cursor]]]
# in : [in_id, (LIST, CURSOR), [[table_alias, schema, table_name, col_name/value, type(COL, INT, FLOAT, STR, HEX, DATETIME, FUNCTION, MATHS, PIPE), table position, position in table, table or cursor]]]
# connect : oper, value
# maths : maths_id, [[table_alias, schema, table_name, col_name/value, type(COL, INT, FLOAT, STR, HEX, DATETIME, FUNCTION, MATHS, PIPE), table position, position in table, table or cursor]], [
#                    item_id, ['META', item_id], oper, ['META', item_id]
#                 or item_id, ['TST', num_table, num_col, alias1, field1, type, schema, table_name, table or cursor], oper, ['TST', num_table, num_col, alias2, field2, type, schema, table_name, table or cursor],
#                   , type_maths(INT, FLOAT)
#                   ...]
# pipe : pipe_id, [[table_alias, schema, table_name, col_name/value, alias, type(COL, INT, FLOAT, STR, HEX, DATETIME, FUNCTION, MATHS, PIPE), table position, position in table, table or cursor]]
# bind : [[name, value] ...]
# group_by : table_alias, schema, table_name, col_name/value, alias, type(COL, INT, FLOAT, STR, HEX, DATETIME, FUNCTION, MATHS, PIPE), table position, position in table, table or cursor
# post_tasks : (TRUE, FALSE)
# post_data_model : col_id= {obj_name= {column:[table_alias, schema,table_name,col_name/value,alias,type(COL, INT, FLOAT, STR, HEX, DATETIME, FUNCTION, MATHS, PIPE),table position,position in table,table or cursor], 
#                            colvalmodel= [value, (TRUE, FALSE)],
#                            colval= [[value, (TRUE, FALSE)]],
#                            result= [val1, ...],
#                            parsed= (TRUE, FALSE),
#                            completed= (TRUE, FALSE),
#                            function= function_name}}

class vParser():
    def __init__(self) -> None:
        self.__intCurSeq = 0
        self.__intFctSeq = 0
        self.__intInSeq = 0
        self.__intMathsSeq = 0
        self.__intPipeSeq = 0
        self.__raz()
        self.__list_of_functions = ['ABS', 'AVG', 'CHR', 'COUNT', 'DECODE', 'INSTR', 'LENGTH', 'LOWER', 'LPAD', 'LTRIM', 'MAX', 'MIN',
                                    'NVL', 'NVL2', 'RPAD', 'RTRIM', 'SUBSTR', 'SUM', 'TO_CHAR', 'UPPER']

    def __raz(self) -> None:
        self.__parsed_query = {"querytype": None, "select": [], "from": [], "where": [], "orderby": [], "groupby": [], "cursors": [],
                               "inner_where": [], "parsed_where": [], "parsed_inner_where": [], "grant": [], "revoke": [], "create": [], "drop": [],
                               "insert": [], "update": [], "functions": [], "in": [], "connect": [], "maths": [], "pipe": [], "bind":{},
                               "group_by": [], "order_by": [],"post_tasks": False, "post_data_model": {}}
        self.__query = ''

    def parse_query(self, query:str, bind:dict = []) -> dict:
        # print(query)
        self.__raz()
        self.__parsed_query["bind"] = bind
        if len(query) > 0:
            self.__query = query.strip()
            if self.__query[-1] == ';':
                self.__query = self.__query[:-1].strip()
            
            pos = 0
            query_type, pos = self.__parse_word(pos)
            match query_type.upper():
                case 'SELECT':
                    self.__parsed_query["querytype"] = 'SELECT'
                    self.__parse_select(pos)
                case 'WITH':
                    self.__parsed_query["querytype"] = 'SELECT'
                    self.__parse_with(pos)
                case 'DECS' | 'DESCRIBE':
                    self.__parsed_query["querytype"] = 'DESCRIBE'
                    self.__parse_desc(pos)
                case 'GRANT':
                    self.__parsed_query["querytype"] = 'GRANT'
                    self.__parse_grant(pos)
                case 'REVOKE':
                    self.__parsed_query["querytype"] = 'REVOKE'
                    self.__parse_revoke(pos)
                case 'CREATE':
                    self.__parsed_query["querytype"] = 'CREATE'
                    self.__parse_create(pos)
                case 'DROP':
                    self.__parsed_query["querytype"] = 'DROP'
                    self.__parse_drop(pos)
                case 'INSERT':
                    self.__parsed_query["querytype"] = 'INSERT'
                    self.__parse_insert(pos)
                case 'UPDATE':
                    self.__parsed_query["querytype"] = 'UPDATE'
                    self.__parse_update(pos)
                case 'DELETE':
                    self.__parsed_query["querytype"] = 'DELETE'
                    self.__parse_delete(pos)
                case 'COMMIT':
                    if pos < len(self.__query):
                        raise vExcept(735)
                    self.__parsed_query["querytype"] = 'COMMIT'
                case 'ROLLBACK':
                    if pos < len(self.__query):
                        raise vExcept(736)
                    self.__parsed_query["querytype"] = 'ROLLBACK'
        return self.__parsed_query

    def __parse_word(self, pos):
        __continue = True
        result = ''
        while __continue:
            if pos < len(self.__query):
                # print(f'__parse_word char={self.__query[pos]}')
                match self.__query[pos]:
                    case ' ':
                        pos += 1
                        if len(result) > 0:
                            __continue = False
                    case '+' | '-' | '/':
                        if len(result) == 0:
                            result = self.__query[pos]
                            pos += 1
                        __continue = False
                    case '*':
                        if len(result) == 0:
                            result = self.__query[pos]
                            pos += 1
                            __continue = False
                        elif result[-1] == '.':
                            result = result + self.__query[pos]
                            pos += 1
                        else:
                            __continue = False
                    case '(' | ')' | ',' | '=':
                        if len(result) == 0:
                            result = self.__query[pos]
                            pos += 1
                        __continue = False
                    case '>' | '<':
                        if len(result) == 0:
                            result = self.__query[pos]
                            pos += 1
                            if pos < len(self.__query):
                                if self.__query[pos] == '=':
                                    result += self.__query[pos]
                                    pos += 1
                        __continue = False
                    case '|':
                        if len(result) == 0:
                            result = self.__query[pos]
                            pos += 1
                            if pos < len(self.__query):
                                if self.__query[pos] == '|':
                                    result += self.__query[pos]
                                    pos += 1
                                else:
                                    raise vExcept(746, self.__query[pos])
                            else:
                                raise vExcept(747)
                        __continue = False
                    case ';':
                        pos += 1
                        if pos < len(self.__query):
                            while pos < len(self.__query):
                                if self.__query[pos] != ' ':
                                    raise vExcept(707)
                                pos += 1
                        __continue = False
                    case "'":
                        result += self.__query[pos]
                        n = 1
                        pos += 1
                        while (n == 1) and (pos < len(self.__query)):
                            if self.__query[pos] == "'":
                                n = -n
                            result += self.__query[pos]
                            pos += 1
                            if (n == -1) and (pos < len(self.__query)) and (self.__query[pos] == "'"):
                                result += self.__query[pos]
                                pos += 1
                                n = -n
                        if n == 1:
                            raise vExcept(700)
                    case ':':
                        result += self.__query[pos]
                        # print(f'__parse_word result={result}')
                        pos += 1
                    case _:
                        result += self.__query[pos]
                        # print(f'__parse_word result={result}')
                        pos += 1
            else:
                __continue = False
        if (len(result) > 0) and (result[0] == ':'):
            # print(f'__parse_word result={result}')
            if result[1:] in self.__parsed_query["bind"]:
                tmpbind = self.__parsed_query["bind"][result[1:]]
                rtype = self.__get_format(tmpbind)
                # print(f'__parse_word tmpbind={tmpbind}  rtype={rtype}')
                match rtype:
                    case 'STR'|'HEX':
                        result = f"""'{self.__remove_quote(tmpbind)}'"""
                        # print(f'__parse_word result={result}')
                    case _:
                        result = f'{tmpbind}'
                        # print(f'__parse_word result={result}')
            else:
                raise vExcept(1100, result[1:])
            # print(f'__parse_word result={result}')
        # if pos < len(self.__query):
        #     print(f'__parse_word result={result} pos={pos} char={self.__query[pos]}')
        # else:
        #     print(f'__parse_word result={result} pos={pos} char=--fin_de_chaine--')
        return result, pos

    def __parse_parenthesis(self, pos):
        p = 1
        p_value = ''
        while (p >= 1) and (pos < len (self.__query)):
            if self.__query[pos] == '(':
                p += 1
            if self.__query[pos] == ')':
                p -= 1
            p_value += self.__query[pos]
            pos += 1
        if p != 0:
            raise vExcept(704)
        return p_value[0:-1], pos

    def __parse_select(self, pos):
        word = ''
        # SELECT
        word, pos = self.__parse_SEL_COL(pos)
        # FROM
        while (word.upper() not in self.__list_of_word(['FROM'])) and (pos < len(self.__query)):
            word, pos = self.__parse_FROM_OBJ(pos)
        while (pos < len(self.__query)) and (word.upper() not in self.__list_of_word(['INNER', 'LEFT', 'RIGHT'])):
            print(word)
            # INNER JOIN
            if (word.upper() == 'INNER') and (pos < len(self.__query)):
                word, pos = self.__parse_INNER_JOIN(pos)
            # LEFT OIUTER JOIN
            if (word.upper() == 'LEFT') and (pos < len(self.__query)):
                ### a traiter LEFT OIUTER JOIN
                pass
            # RIGHT OUTER JOIN
            if (word.upper() == 'RIGHT') and (pos < len(self.__query)):
                ### a traiter RIGHT OUTER JOIN
                pass
        if (word.upper() == 'WHERE') and (pos < len(self.__query)):
            word, pos = self.__parse_WHERE_CLAUSE(pos)
        if (word.upper() == 'CONNECT') and (pos < len(self.__query)):
            word, pos = self.__parse_CONNECT_BY_VALUE(pos)
        if (word.upper() == 'GROUP') and (pos < len(self.__query)):
            word, pos = self.__parse_GROUP_BY_CLAUSE(pos)
        if (word.upper() == 'ORDER') and (pos < len(self.__query)):
            word, pos = self.__parse_ORDER_BY_CLAUSE(pos)
        # parse selected columns
        # select: 
        #   0: table_alias
        #   1: schema
        #   2: table_name
        #   3: col_name/value
        #   4: alias
        #   5: type(COL, INT, FLOAT, STR, HEX, DATETIME, FUNCTION, MATHS, PIPE)
        #   6: table position
        #   7: position in table
        #   8: table or cursor
        #   9: [parameters of function]
        for n in range(len(self.__parsed_query["select"])):
            if (self.__parsed_query["select"][n][5] is None) or (self.__parsed_query["select"][n][5] == 'COLUMN'):
                col_alias = self.__parsed_query["select"][n][4]
                if (col_alias is not None) and self.__check_STR(col_alias):
                    col_alias = col_alias.upper()
                fmt, al, cn, sh, tn, tc, nt = self.__getColFromTable(self.__parsed_query["select"][n][3])
                self.__parsed_query["select"][n] = [al, sh, tn, cn, col_alias, fmt, nt, None, tc]
        # functions:
        #   0: table_alias
        #   1: schema
        #   2: table_name
        #   3: col_name/value
        #   4: type(COL, INT, FLOAT, STR, HEX, DATETIME, FUNCTION, MATHS, PIPE)
        #   5: table position
        #   6: position in table
        #   7: table or cursor
        for f in range(len(self.__parsed_query["functions"])):
            for n in range (len(self.__parsed_query["functions"][f][2])):
                if (self.__parsed_query["functions"][f][2][n][4] is None) or (self.__parsed_query["functions"][f][2][n][4] == 'COLUMN'):
                    fmt, al, cn, sh, tn, tc, nt = self.__getColFromTable(self.__parsed_query["functions"][f][2][n][3])
                    self.__parsed_query["functions"][f][2][n] = [al, sh, tn, cn, fmt, nt, None, tc]
        # in:
        #   0: table_alias
        #   1: schema
        #   2: table_name
        #   3: col_name/value
        #   4: type(COL, INT, FLOAT, STR, HEX, DATETIME, COLUMN, FUNCTION, MATHS, PIPE)
        #   5: table position
        #   6: position in table
        #   7: table or cursor
        for f in range(len(self.__parsed_query["in"])):
            for n in range (len(self.__parsed_query["in"][f][2])):
                if (self.__parsed_query["in"][f][2][n][4] is None) or (self.__parsed_query["in"][f][2][n][4] == 'COLUMN'):
                    fmt, al, cn, sh, tn, tc, nt = self.__getColFromTable(self.__parsed_query["in"][f][2][n][3])
                    self.__parsed_query["in"][f][2][n] = [al, sh, tn, cn, fmt, nt, None, tc]
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
        for f in range(len(self.__parsed_query["maths"])):
            for n in range (len(self.__parsed_query["maths"][f][1])):
                val = self.__parsed_query["maths"][f][1][n]
                if val in ['+', '-', '*', '/']:
                    self.__parsed_query["maths"][f][1][n] = [val]
                elif val in ['(', ')']:
                    self.__parsed_query["maths"][f][1][n] = [val]
                elif self.__check_INT(val):
                    self.__parsed_query["maths"][f][1][n] = [None, None, None, val, 'INT', None, None, None]
                elif self.__check_FLOAT(val):
                    self.__parsed_query["maths"][f][1][n] = [None, None, None, val, 'FLOAT', None, None, None]
                    self.__parsed_query["maths"][f][3] = 'FLOAT'
                else:
                    flg, _ = self.__get_function(val)
                    if flg:
                        self.__parsed_query["maths"][f][1][n] = [None, None, None, val, 'FUNCTION', None, None, None]
                    else:
                        fmt, al, cn, sh, tn, tc, nt = self.__getColFromTable(val)
                        self.__parsed_query["maths"][f][1][n] = [al, sh, tn, cn, fmt, nt, None, tc]
        # pipe
        # 0: table_alias
        # 1: schema
        # 2: table_name
        # 3: col_name/value
        # 4: alias
        # 5: type(COL, INT, FLOAT, STR, HEX, DATETIME, FUNCTION, MATHS, PIPE)
        # 6: table position
        # 7: position in table
        # 8: table or cursor
        for f in range(len(self.__parsed_query["pipe"])):
            for n in range(len(self.__parsed_query["pipe"][f][1])):
                if self.__parsed_query["pipe"][f][1][n][5] in ['FUNCTION', 'MATHS']:
                    pass
                elif self.__parsed_query["pipe"][f][1][n][5] == 'COLUMN':
                    fmt, al, cn, sh, tn, tc, nt = self.__getColFromTable(self.__parsed_query["pipe"][f][1][n][3])
                    self.__parsed_query["pipe"][f][1][n] = [al, sh, tn, cn, None, fmt, nt, None, tc]
                elif self.__check_DATETIME(self.__parsed_query["pipe"][f][1][n][3]):
                    raise vExcept(750)
                elif self.__check_INT(self.__parsed_query["pipe"][f][1][n][3]):
                    self.__parsed_query["pipe"][f][1][n][5] = 'STR'
                    self.__parsed_query["pipe"][f][1][n][3] = str(self.__parsed_query["pipe"][f][1][n][3])
                elif self.__check_FLOAT(self.__parsed_query["pipe"][f][1][n][3]):
                    self.__parsed_query["pipe"][f][1][n][5] = 'STR'
                    self.__parsed_query["pipe"][f][1][n][3] = str(self.__parsed_query["pipe"][f][1][n][3])
                elif self.__check_HEX(self.__parsed_query["pipe"][f][1][n][3]):
                    self.__parsed_query["pipe"][f][1][n][5] = 'STR'
                    self.__parsed_query["pipe"][f][1][n][3] = str(self.__parsed_query["pipe"][f][1][n][3])
                elif self.__check_STR(self.__parsed_query["pipe"][f][1][n][3]):
                    self.__parsed_query["pipe"][f][1][n][5] = 'STR'
        # group by
        col_found = False
        for gcol in range(len(self.__parsed_query["group_by"])):
            for scol in range(len(self.__parsed_query["select"])):
                gcolname = str(self.__parsed_query["group_by"][gcol][3]).upper().split('.')
                if len(gcolname) == 1:
                    if gcolname[0] == str(self.__parsed_query["select"][scol][3]).upper():
                        self.__parsed_query["group_by"][gcol] = self.__parsed_query["select"][scol][0:9]
                        col_found = True
                    elif gcolname[0] == str(self.__parsed_query["select"][scol][4]).upper():
                        self.__parsed_query["group_by"][gcol] = self.__parsed_query["select"][scol][0:9]
                        col_found = True
                elif len(gcolname) == 2:
                    if ((gcolname[0] == str(self.__parsed_query["select"][scol][2]).upper()) or (gcolname[0] == str(self.__parsed_query["select"][scol][0]).upper())) and (gcolname[1] == str(self.__parsed_query["select"][scol][3]).upper()):
                        self.__parsed_query["group_by"][gcol] = self.__parsed_query["select"][scol][0:9]
                        col_found = True
                elif len(gcolname) == 3:
                    if (gcolname[0] == str(self.__parsed_query["select"][scol][1]).upper()) and (gcolname[1] == str(self.__parsed_query["select"][scol][2]).upper()) and (gcolname[2] == str(self.__parsed_query["select"][scol][3].upper())):
                        self.__parsed_query["group_by"][gcol] = self.__parsed_query["select"][scol][0:9]
                        col_found = True
            if not col_found:
                raise vExcept(753, self.__parsed_query["group_by"][gcol][3])
        # generate maths
        for f in range(len(self.__parsed_query["maths"])):
            lst_maths = []
            m_idx = 0
            bracket = 0
            v_idx = 0
            for im in self.__parsed_query["maths"][f][1]:
                m_idx, lst_maths, v_idx, bracket = self.__compute_maths(self.__parsed_query["maths"][f][1], m_idx, lst_maths, v_idx, None, bracket)
            self.__parsed_query["maths"][f][2] = lst_maths
        # generate where test
        lst_where = []
        w_idx = 0
        bracket = 0
        v_idx = 0
        while w_idx < len(self.__parsed_query['where']):
            w_idx, lst_where, v_idx, bracket = self.__compute_where(self.__parsed_query['where'], w_idx, lst_where, v_idx, 'AND', bracket)
        self.__parsed_query['parsed_where'] = lst_where
        # generate inner join test
        for iw in self.__parsed_query['inner_where']:
            lst_where = []
            w_idx = 0
            bracket = 0
            v_idx = 0
            while w_idx < len(iw):
                w_idx, lst_where, v_idx, bracket = self.__compute_where(iw, w_idx, lst_where, v_idx, 'AND', bracket)
            self.__parsed_query['parsed_inner_where'].append(lst_where)
        # order_by
        # 0: table_alias
        # 1: schema
        # 2: table_name
        # 3: col_name/value
        # 4: alias
        # 5: type(COL, INT, FLOAT, STR, HEX, DATETIME, FUNCTION, MATHS, PIPE)
        # 6: table position
        # 7: position in table
        # 8: table or cursor
        for f in range(len(self.__parsed_query["order_by"])):
            ocol = self.__parsed_query["order_by"][f][0]
            if self.__check_INT(ocol):
                pass
            else:
                col = ocol.upper().split('.')
                found = 0
                match len(col):
                    case 1:
                        for n, scol in enumerate(self.__parsed_query["select"]):
                            if (col[0] == str(scol[3]).upper()) or (col[0] == str(scol[4]).upper()):
                                self.__parsed_query["order_by"][f][0] = n
                                found += 1
                    case 2:
                        for n, scol in enumerate(self.__parsed_query["select"]):
                            if ((col[0] == str(scol[0]).upper()) or (col[0] == str(scol[4])).upper()) and (col[1] == str(scol[3]).upper()):
                                self.__parsed_query["order_by"][f][0] = n
                                found += 1
                    case 3:
                        for n, scol in enumerate(self.__parsed_query["select"]):
                            if (col[0] == str(scol[1]).upper()) and (col[1] == str(scol[2]).upper()) and (col[2] == str(scol[3]).upper()):
                                self.__parsed_query["order_by"][f][0] = n
                                found += 1
                if found == 0:
                    raise vExcept(759, ocol)
                elif found > 1:
                    raise vExcept(760, ocol)

    def __get_function(self, fct_id):
        for n in range(len(self.__parsed_query["functions"])):
            if self.__parsed_query["functions"][n][0] == fct_id:
                return True, n
        return False, None

    def __parse_insert(self, pos):
        word, pos = self.__parse_word(pos)
        if word.upper() != 'INTO':
            raise vExcept(729, word)
        word, pos = self.__parse_word(pos)
        s_t = word.upper().split('.')
        if len(s_t) == 1:
            self.__parsed_query['insert'] = [None, s_t[0], [], None, None]
        elif len(s_t) == 2:
            self.__parsed_query['insert'] = [s_t[0], s_t[1], [], None, None]
        else:
            raise vExcept(730, word)
        word, pos = self.__parse_word(pos)
        if word == '(':
            not_ended = True
            memo = ''
            while not_ended:
                word, pos = self.__parse_word(pos)
                if word == ')':
                    if memo == '':
                        raise vExcept(731, pos)
                    else:
                        self.__parsed_query['insert'][2].append(memo)
                        not_ended = False
                elif word == ',':
                    if memo == '':
                        raise vExcept(731, pos)
                    else:
                        self.__parsed_query['insert'][2].append(memo)
                        memo = ''
                else:
                    if memo != '':
                        raise vExcept(725, word)
                    else:
                        memo = word.upper()
            word, pos = self.__parse_word(pos)
        if word.upper() == 'VALUES':
            self.__parsed_query['insert'][3] = []
            word, pos = self.__parse_word(pos)
            if word != '(':
                raise vExcept(732, word)
            not_ended = True
            memo = ''
            while not_ended:
                word, pos = self.__parse_word(pos)
                if word == ')':
                    if memo == '':
                        raise vExcept(733, pos)
                    else:
                        self.__parsed_query['insert'][3].append(memo)
                        not_ended = False
                elif word == ',':
                    if memo == '':
                        raise vExcept(733, pos)
                    else:
                        self.__parsed_query['insert'][3].append(memo)
                        memo = ''
                else:
                    if memo != '':
                        raise vExcept(725, word)
                    else:
                        memo = word.upper()
            word, pos = self.__parse_word(pos)
            if word != '':
                raise vExcept(719, word)
            if len(self.__parsed_query['insert'][3]) != len(self.__parsed_query['insert'][2]):
                raise vExcept(734)
        elif word.upper() in ['SELECT', 'WITH']:
            c_name = self.__get_cur_name().upper()
            c_query = word + ' ' + self.__query[pos:]
            self.__parsed_query["cursors"].append([c_name, c_query])
            self.__parsed_query['insert'][4] = c_name
            pos = len(self.__parsed_query)
        return pos

    def __parse_with(self, pos):
        c_name, pos = self.__parse_word(pos)
        while c_name.upper() != 'SELECT':
            word, pos = self.__parse_word(pos)
            if word.upper() != 'AS':
                raise vExcept(709, word)
            word, pos = self.__parse_word(pos)
            if word != '(':
                raise vExcept(710, c_name)
            c_query, pos = self.__parse_parenthesis(pos)
            # self.__parsed_query["from"].append([self.__get_cur_name(), None, c_name.upper(), 'CURSOR'])
            self.__parsed_query["cursors"].append([c_name.upper(), c_query])
            c_name, pos = self.__parse_word(pos)
            if c_name == ',':
                c_name, pos = self.__parse_word(pos)
        self.__parse_select(pos)

    def __parse_desc(self, pos):
        word, pos = self.__parse_word(pos)
        if pos < len( self.__query):
            raise vExcept(708)
        a_t = word.upper().split('.')
        if len(a_t) == 1:
            self.__parsed_query["from"].append([self.__get_cur_name(), None, a_t[0], 'TABLE'])
        elif len(a_t) == 2:
            self.__parsed_query["from"].append([self.__get_cur_name(), a_t[0], a_t[1], 'TABLE'])
        else:
            raise vExcept(209, word)
        word, pos = self.__parse_word(pos)
        if word != '':
            raise vExcept(719, word)

    def __parse_grant(self, pos):
        word, pos = self.__parse_word(pos)
        match word.upper():
            case 'SELECT' | 'INSERT' | 'UPDATE' | 'DELETE':
                g_name = word.upper()
                word, pos = self.__parse_word(pos)
                if word.upper() != 'ON':
                    raise vExcept(706, word)
                word, pos = self.__parse_word(pos)
                s_t = word.upper().split('.')
                if len(s_t) == 1:
                    g_type = 'SCHEMA'
                    g_table = s_t[0]
                elif len(s_t) == 2:
                    g_type = 'TABLE'
                    g_table = '{}.{}'.format(s_t[0], s_t[1])
                else:
                    raise vExcept(711, word)
                word, pos = self.__parse_word(pos)
                if word.upper() != 'TO':
                    raise vExcept(712, word)
                word, pos = self.__parse_word(pos)
                g_user = word.lower()
                g_admin, pos = self.__parse_GRANT_with_admin_option(pos)
                self.__parsed_query["grant"].append([g_user, g_name, g_type, g_table, g_admin])
            case 'CREATE' | 'DROP':
                g_name = word.upper()
                word, pos = self.__parse_word(pos)
                match word.upper():
                    case 'USER': # grant { create | drop } user to <USERNAME> [ with admin option ]
                        word, pos = self.__parse_word(pos)
                        if word.upper() != 'TO':
                            raise vExcept(712, word)
                        word, pos = self.__parse_word(pos)
                        g_user = word.lower()
                        g_admin, pos = self.__parse_GRANT_with_admin_option(pos)
                        self.__parsed_query["grant"].append([g_user, g_name, 'USER', None, g_admin])
                    case 'TABLE' | 'INDEX': # grant { create | drop } { table | index } on <SCHEMA> to <USERNAME> [ with admin option ]
                        g_type = word.upper()
                        word, pos = self.__parse_word(pos)
                        if word.upper() != 'ON':
                            raise vExcept(706, word)
                        word, pos = self.__parse_word(pos)
                        s_t = word.upper().split('.')
                        g_table = s_t[0]
                        if len(s_t) != 1:
                            raise vExcept(717, word)
                        if word.upper() != 'TO':
                            raise vExcept(712, word)
                        word, pos = self.__parse_word(pos)
                        g_user = word.lower()
                        g_admin, pos = self.__parse_GRANT_with_admin_option(pos)
                        self.__parsed_query["grant"].append([g_user, g_name, g_type, g_table, g_admin])
                    case _:
                        raise vExcept(716, word)
            case _:
                raise vExcept(720, word)
        word, pos = self.__parse_word(pos)
        if word != '':
            raise vExcept(719, word)

    def __parse_revoke(self, pos):
        word, pos = self.__parse_word(pos)
        match word.upper():
            case 'SELECT' | 'INSERT' | 'UPDATE' | 'DELETE':
                g_name = word.upper()
                word, pos = self.__parse_word(pos)
                if word.upper() != 'ON':
                    raise vExcept(706, word)
                word, pos = self.__parse_word(pos)
                s_t = word.upper().split('.')
                if len(s_t) == 1:
                    g_type = 'SCHEMA'
                    g_table = s_t[0]
                elif len(s_t) == 2:
                    g_type = 'TABLE'
                    g_table = '{}.{}'.format(s_t[0], s_t[1])
                else:
                    raise vExcept(711, word)
                word, pos = self.__parse_word(pos)
                if word.upper() != 'FROM':
                    raise vExcept(718, word)
                word, pos = self.__parse_word(pos)
                g_user = word.lower()
                self.__parsed_query["revoke"].append([g_user, g_name, g_type, g_table])
            case 'CREATE' | 'DROP':
                g_name = word.upper()
                word, pos = self.__parse_word(pos)
                match word.upper():
                    case 'USER': # grant { create | drop } user to <USERNAME> [ with admin option ]
                        g_type = word.upper()
                        word, pos = self.__parse_word(pos)
                        if word.upper() != 'FROM':
                            raise vExcept(718, word)
                        word, pos = self.__parse_word(pos)
                        g_user = word.lower()
                        self.__parsed_query["revoke"].append([g_user, g_name, g_type, None])
                    case 'TABLE' | 'INDEX': # grant { create | drop } { table | index } on <SCHEMA> to <USERNAME> [ with admin option ]
                        g_type = word.upper()
                        word, pos = self.__parse_word(pos)
                        if word.upper() != 'ON':
                            raise vExcept(706, word)
                        word, pos = self.__parse_word(pos)
                        s_t = word.upper().split('.')
                        g_table = s_t[0]
                        if len(s_t) != 1:
                            raise vExcept(717, word)
                        if word.upper() != 'FROM':
                            raise vExcept(718, word)
                        word, pos = self.__parse_word(pos)
                        g_user = word.lower()
                        self.__parsed_query["revoke"].append([g_user, g_name, g_type, g_table])
                    case _:
                        raise vExcept(716, word)
            case _:
                raise vExcept(720, word)
        word, pos = self.__parse_word(pos)
        if word != '':
            raise vExcept(719, word)

    def __parse_create(self, pos):
        word, pos = self.__parse_word(pos)
        match word.upper():
            case 'TABLE':
                word, pos = self.__parse_word(pos)
                o_t = word.upper().split('.')
                if len(o_t) == 1:
                    t_owner = None
                    t_name = word.upper()
                elif len(o_t) == 2:
                    t_owner = o_t[0]
                    t_name = o_t[1]
                else:
                    raise vExcept(722, word)
                if t_name in self.__list_of_word(except_this = []):
                    raise vExcept(722, t_name)
                word, pos = self.__parse_word(pos)
                match word.upper():
                    case 'AS':
                        c_name = self.__get_cur_name().upper()
                        c_query = self.__query[pos:]
                        self.__parsed_query["cursors"].append([c_name, c_query])
                        self.__parsed_query["create"].append(['TABLE', t_owner, t_name, [c_name]])
                        pos = len(self.__query)
                    case '(':
                        c_cols = []
                        while word not in (')', ''):
                            c_col, pos = self.__parse_word(pos)
                            c_type, pos = self.__parse_word(pos)
                            word, pos = self.__parse_word(pos)
                            if (len(c_col.split('.')) > 1) or (len(c_col.split(' ')) > 1) or (c_col in self.__list_of_word(except_this = [])):
                                raise vExcept(723, c_col)
                            if c_type.lower() not in ['str', 'int', 'float', 'hex', 'datetime']:
                                raise vExcept(724, c_type)
                            if word not in [',', ')']:
                                raise vExcept(725, word)
                            c_cols.append([c_col, c_type])
                            c_col, c_type = None, None
                        self.__parsed_query["create"].append(['TABLE', t_owner, t_name, c_cols])
                    case _:
                        raise vExcept(709, word)
            case 'USER':
                username, pos = self.__parse_word(pos)
                if (len(username.split('.')) > 1) or (len(username.split(' ')) > 1):
                    raise vExcept(726, username)
                word, pos = self.__parse_word(pos)
                if word.upper() != 'IDENTIFIED':
                    raise vExcept(727, word)
                word, pos = self.__parse_word(pos)
                if word.upper() != 'BY':
                    raise vExcept(728, word)
                password, pos = self.__parse_word(pos)
                self.__parsed_query["create"].append(['USER', username, password])
            case _:
                raise vExcept(721, word)
        word, pos = self.__parse_word(pos)
        if word != '':
            raise vExcept(719, word)

    def __parse_update(self, pos):
        word, pos = self.__parse_word(pos)
        a_t = word.upper().split('.')
        if len(a_t) == 1:
            self.__parsed_query["from"].append([self.__get_cur_name(), None, a_t[0], 'TABLE'])
        elif len(a_t) == 2:
            self.__parsed_query["from"].append([self.__get_cur_name(), a_t[0], a_t[1], 'TABLE'])
        else:
            raise vExcept(209, word)
        word, pos = self.__parse_word(pos)
        if word.upper() != 'SET':
            raise vExcept(737, word)
        word, pos = self.__parse_word(pos)
        mbr1, op = None, None
        while (pos < len(self.__query)) and (word.upper() != 'WHERE'):
            if mbr1 is None:
                mbr1 = word
            elif op is None:
                op = word
            else:
                self.__parsed_query["update"].append([mbr1, op, word])
                mbr1, op = None, None
            word, pos = self.__parse_word(pos)
            if word == ',':
                word, pos = self.__parse_word(pos)
        if pos < len(self.__query):
            if word.upper() == 'WHERE':
                word, pos = self.__parse_WHERE_CLAUSE(pos)
            else:
                raise vExcept(738, word)
        # generate where test
        lst_where = []
        w_idx = 0
        bracket = 0
        v_idx = 0
        while w_idx < len(self.__parsed_query['where']):
            w_idx, lst_where, v_idx, bracket = self.__compute_where(self.__parsed_query['where'], w_idx, lst_where, v_idx, 'AND', bracket)
        self.__parsed_query['parsed_where'] = lst_where

    def __parse_delete(self, pos):
        word, pos = self.__parse_word(pos)
        if word.upper() != 'FROM':
            raise vExcept(718, word)
        word, pos = self.__parse_word(pos)
        a_t = word.upper().split('.')
        if len(a_t) == 1:
            self.__parsed_query["from"].append([self.__get_cur_name(), None, a_t[0], 'TABLE'])
        elif len(a_t) == 2:
            self.__parsed_query["from"].append([self.__get_cur_name(), a_t[0], a_t[1], 'TABLE'])
        else:
            raise vExcept(209, word)
        word, pos = self.__parse_word(pos)
        if pos < len(self.__query):
            if word.upper() == 'WHERE':
                word, pos = self.__parse_WHERE_CLAUSE(pos)
            else:
                raise vExcept(738, word)
        # generate where test
        lst_where = []
        w_idx = 0
        bracket = 0
        v_idx = 0
        while w_idx < len(self.__parsed_query['where']):
            w_idx, lst_where, v_idx, bracket = self.__compute_where(self.__parsed_query['where'], w_idx, lst_where, v_idx, 'AND', bracket)
        self.__parsed_query['parsed_where'] = lst_where

    def __parse_drop(self, pos):
        word, pos = self.__parse_word(pos)
        match word.upper():
            case 'TABLE':
                word, pos = self.__parse_word(pos)
                s_t = word.upper().split('.')
                if len(s_t) == 1:
                    self.__parsed_query["drop"].append(['TABLE', None, s_t[0]])
                elif len(s_t) == 2:
                    self.__parsed_query["drop"].append(['TABLE', s_t[0], s_t[1]])
                else:
                    raise vExcept(722, word)
            case 'USER':
                word, pos = self.__parse_word(pos)
                usr = word.upper().split('.')
                if len(word.split('.')) == 1:
                    self.__parsed_query["drop"].append(['USER', word.upper()])
                else:
                    raise vExcept(726, word)
            case _:
                raise vExcept(721, word)
        word, pos = self.__parse_word(pos)
        if word != '':
            raise vExcept(719, word)

    def __parse_SEL_COL(self, pos):
        # 0: table_alias
        # 1: schema
        # 2: table_name
        # 3: col_name/value
        # 4: alias
        # 5: type(COL, INT, FLOAT, STR, HEX, DATETIME, FUNCTION, MATHS, PIPE)
        # 6: table position
        # 7: position in table
        # 8: table or cursor
        # 9: [-]
        word=''
        while (word.upper() != 'FROM') and (pos < len(self.__query)):
            col, pos = self.__parse_word(pos)
            # print(f'__parse_SEL_COL 1  col={col}')
            if col == ',':
                raise vExcept(701, pos)
            word, pos = self.__parse_word(pos)
            # print(f'__parse_SEL_COL 2  word={word}')
            if word.upper() in [',', 'FROM']:
                self.__parsed_query["select"].append([None, None, None, col, None, None, None, None, None, []])
            elif word in ['+', '-', '*', '/'] or col == '(':
                word, maths_id, pos = self.__parse_maths(col, word, pos)
                if word.upper() in [',', 'FROM']:
                    self.__parsed_query["select"].append([None, None, None, maths_id, None, 'MATHS', None, None, None, []])
                else:
                    self.__parsed_query["select"].append([None, None, None, maths_id, word, 'MATHS', None, None, None, []])
                    word, pos = self.__parse_word(pos)
            elif word == '||':
                word, pipe_id, pos = self.__parse_pipe(col, word, pos)
                # print(f'__parse_SEL_COL 3  word={word}')
                if word.upper() in [',', 'FROM']:
                    self.__parsed_query["select"].append([None, None, None, pipe_id, None, 'PIPE', None, None, None, []])
                else:
                    self.__parsed_query["select"].append([None, None, None, pipe_id, word, 'PIPE', None, None, None, []])
                    word, pos = self.__parse_word(pos)
            else:
                if self.__is_function(col.upper()):
                    if word != '(':
                        raise vExcept(739, word)
                    # print (f'__parse_SEL_COL avant __parse_COL_FCT  col={col} word={word}')
                    fct_col, pos = self.__parse_COL_FCT(col.upper(), pos)
                    word, pos = self.__parse_word(pos)
                    # print (f'__parse_SEL_COL apres __parse_COL_FCT  fct_col={fct_col} word={word}')
                    if word.upper() not in [',', 'FROM']:
                        self.__parsed_query["select"].append([None, None, None, fct_col, word, 'FUNCTION', None, None, None, []])
                        word, pos = self.__parse_word(pos)
                    else:
                        self.__parsed_query["select"].append([None, None, None, fct_col, None, 'FUNCTION', None, None, None, []])
                else:
                    if word.upper() not in [',', 'FROM']:
                        self.__parsed_query["select"].append([None, None, None, col, word, None, None, None, None, []])
                        # print(f'__parse_SEL_COL 3  col={col} word={word}')
                        word, pos = self.__parse_word(pos)
                    else:
                        # print(f'__parse_SEL_COL 4  col={col} word={word}')
                        self.__parsed_query["select"].append([None, None, None, col, None, None, None, None, None, []])
                if word.upper() not in [',', 'FROM']:
                    # print(f'__parse_SEL_COL word={word}')
                    raise vExcept(702, word)
        # print(f'__parse_SEL_COL self.__parsed_query["select"]={self.__parsed_query["select"]}')
        return word, pos

    def __parse_pipe(self, col, word, pos):
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
        pipe_id = self.__get_pipe_name()
        word = self.__remove_quote(col)
        if word == col:
            tmpP = [pipe_id, [[None, None, None, word, None, 'COLUMN', None, None, None]]]
        else:
            tmpP = [pipe_id, [[None, None, None, self.__remove_quote(word), None, 'STR', None, None, None]]]
        encore = True
        last_is_pipe = True
        while encore  and (pos < len(self.__query)):
            word, pos = self.__parse_word(pos)
            encore = bool((word.upper() not in ['FROM', ',', 'CONNECT', 'IN', 'BETWEEN', '>', '>=', '=', '<', '<=', '!=', '<>']))
            if last_is_pipe and not encore:
                raise vExcept(748, word)
            if self.__is_function(word.upper()):
                wordpar, pos = self.__parse_word(pos)
                if wordpar != '(':
                    raise vExcept(739, wordpar)
                fct_col, pos = self.__parse_COL_FCT(word.upper(), pos)
                tmpP[1].append([None, None, None, fct_col, None, 'FUNCTION', None, None, None])
                last_is_pipe = False
            elif word == '||':
                if last_is_pipe:
                    raise vExcept(749, word)
                else:
                    last_is_pipe = True
            elif word == '(':
                col, pos = self.__parse_word(pos)
                word, maths_id, pos = self.__parse_maths(word, col, pos)
                tmpP[1].append([None, None, None, maths_id, None, 'MATHS', None, None, None])
                last_is_pipe = False
            elif word in ['+', '-', '*', '/']:
                word, maths_id, pos = self.__parse_maths(tmpP[1][-1][3], word, pos)
                tmpP[1][-1] = [None, None, None, maths_id, None, 'MATHS', None, None, None]
                last_is_pipe = False
                encore = bool((word.upper() not in ['FROM', ',', 'CONNECT', 'IN', 'BETWEEN', '>', '>=', '=', '<', '<=', '!=', '<>']))
            else:
                if last_is_pipe:
                    col = self.__remove_quote(word)
                    if word == col:
                        tmpP[1].append([None, None, None, word, None, 'COLUMN', None, None, None])
                    else:
                        tmpP[1].append([None, None, None, self.__remove_quote(col), None, 'STR', None, None, None])
                    last_is_pipe = False
                else:
                    encore = False
        if last_is_pipe:
            raise vExcept(748, self.__remove_quote(word))
        self.__parsed_query["pipe"].append(tmpP)
        return word, pipe_id, pos

    def __remove_quote(self, strin):
        if self.__check_STR(strin) and (len(strin) >= 2):
            if (strin[0] == '"' and strin[-1] == '"') or (strin[0] == "'" and strin[-1] == "'"):
                strin = strin[1:-1]
        return strin

    def __parse_maths(self, col, word, pos):
        # maths : maths_id, [[element1, type(INT, FLOAT, MATHS), ...], [
        #                    item_id, ['META', item_id], oper, ['META', item_id]
        #                 or item_id, ['TST', num_table, num_col, alias1, field1, type, schema, table_name, table or cursor], oper, ['TST', num_table, num_col, alias2, field2, type, schema, table_name, table or cursor]
        #                   , type_maths(INT, FLOAT)
        #                   ...]
        lvl = 0
        maths_id = self.__get_maths_name()
        tmpM = [maths_id, [col, word], [], 'INT']
        if col == '(':
            lvl += 1
        encore = True
        wait_for_sign = False
        while encore  and (pos < len(self.__query)):
            word, pos = self.__parse_word(pos)
            encore = bool((word.upper() not in ['FROM', ',']))
            if word == '(':
                lvl += 1
            elif word == ')':
                if lvl > 0:
                    lvl -= 1
                else:
                    encore = False
            elif word.upper() in ['FROM', 'CONNECT', 'IN', 'BETWEEN', '>', '>=', '=', '<', '<=', '!=', '<>']:
                # print(f'__parse_maths word={word}')
                encore = False
            if encore:
                if self.__is_function(word.upper()):
                    col, pos = self.__parse_word(pos)
                    if col != '(':
                        raise vExcept(739, word)
                    fct_col, pos = self.__parse_COL_FCT(word.upper(), pos)
                    tmpM[1].append(fct_col)
                    wait_for_sign = not wait_for_sign
                else:
                    if wait_for_sign:
                        if word in ['+', '-', '*', '/']:
                            tmpM[1].append(word)
                            wait_for_sign = not wait_for_sign
                        else:
                            encore = False
                    else:
                        tmpM[1].append(word)
                        wait_for_sign = not wait_for_sign
        if lvl > 0:
            raise vExcept(740, word)
        self.__parsed_query["maths"].append(tmpM)
        return word, maths_id, pos

    def __is_function(self, fct_name):
        if fct_name.upper() in self.__list_of_functions:
            if fct_name in ['COUNT', 'MIN', 'MAX', 'AVG', 'SUM']:
                self.__parsed_query["post_tasks"] = True
            return True
        else:
            return False

    def __parse_COL_FCT(self, fct_name, pos):
        # functions : [fct_id, fct_name, [[
        #   1: table_alias
        #   2: schema
        #   3: table_name
        #   4: col_name/value
        #   5: type(COL, INT, FLOAT, STR, HEX, DATETIME, FUNCTION, MATHS, PIPE)
        #   6: table position
        #   7: position in table
        #   8: table or cursor]]]
        fct_idx = len(self.__parsed_query["functions"])
        fct_id = self.__get_fct_name()
        self.__parsed_query["functions"].append([fct_id, fct_name, []])
        word = ''
        while (word != ')') and (pos < len(self.__query)):
            word, pos = self.__parse_word(pos)
            # print(f'__parse_COL_FCT 0 word={word}')
            if self.__is_function(word.upper()):
                col, pos = self.__parse_word(pos)
                if col != '(':
                    raise vExcept(739, col)
                sub_fct, pos = self.__parse_COL_FCT(word.upper(), pos)
                self.__parsed_query["functions"][fct_idx][2].append([None, None, None, sub_fct, 'FUNCTION', None, None, None])
                word, pos = self.__parse_word(pos)
                if (word != ',') and (word != ')'):
                    # print(f'__parse_COL_FCT 1 word={word}')
                    raise vExcept(702, word)
            else:
                if word in ['-', '+']:
                    elem = '0'
                else:
                    elem = word
                    word, pos = self.__parse_word(pos)
                    # print(f'__parse_COL_FCT 2 elem={elem} word={word}')
                if word in ['+', '-', '*', '/']:
                    # print(f'__parse_COL_FCT 3 word={word}')
                    word, maths_id, pos = self.__parse_maths(elem, word, pos)
                    # print(f'__parse_COL_FCT word={word} maths_id={maths_id}')
                    self.__parsed_query["functions"][fct_idx][2].append([None, None, None, maths_id, 'MATHS', None, None, None])
                    if (word != ',') and (word != ')'):
                        # print(f'__parse_COL_FCT 4 word={word}')
                        raise vExcept(702, word)
                elif word == '||':
                    word, pipe_id, pos = self.__parse_pipe(elem, word, pos)
                    if (word != ',') and (word != ')'):
                        # print(f'__parse_COL_FCT 5 word={word}')
                        raise vExcept(702, word)
                    self.__parsed_query["functions"][fct_idx][2].append([None, None, None, pipe_id, 'PIPE', None, None, None])
                elif (word != ',') and (word != ')'):
                    # print(f'__parse_COL_FCT 6 word={word}')
                    raise vExcept(702, word)
                else:
                    # print(f'__parse_COL_FCT 3 elem={elem}')
                    self.__parsed_query["functions"][fct_idx][2].append([None, None, None, elem, None, None, None, None])
                    if (word != ',') and (word != ')'):
                        # print(f'__parse_COL_FCT 7 word={word}')
                        raise vExcept(702, word)
        if word != ')':
            raise vExcept(740, col)
        return fct_id, pos

    def __parse_COL_IN(self, pos):
        # functions : [fct_id, fct_name, [[
        #   1: table_alias
        #   2: schema
        #   3: table_name
        #   4: col_name/value
        #   5: type(COL, INT, FLOAT, STR, HEX, DATETIME, FUNCTION, MATHS, PIPE)
        #   6: table position
        #   7: position in table
        #   8: table or cursor]]]
        in_idx = len(self.__parsed_query["in"])
        in_id = self.__get_in_name()
        self.__parsed_query["in"].append([in_id, 'LIST', []]) #   /!\ /!\ other : CURSOR
        word = ''
        while word != ')':
            word, pos = self.__parse_word(pos)
            if self.__is_function(word.upper()):
                col, pos = self.__parse_word(pos)
                if col != '(':
                    raise vExcept(739, col)
                sub_fct, pos = self.__parse_COL_FCT(word.upper(), pos)
                self.__parsed_query["in"][in_idx][2].append([None, None, None, sub_fct, 'FUNCTION', None, None, None])
            else:
                word = self.__remove_quote(word)
                # if (word[0] == "'") and (word[-1] == "'") or (word[0] == '"') and (word[-1] == '"'):
                #     word = word[1:-1]
                self.__parsed_query["in"][in_idx][2].append([None, None, None, self.__remove_quote(str(word)), 'STR', None, None, None])
            word, pos = self.__parse_word(pos)
            if (word != ',') and (word != ')'):
                # print(f'__parse_COL_IN word={word}')
                raise vExcept(702, word)
        if word != ')':
            raise vExcept(740, col)
        return in_id, pos
        
    def __parse_FROM_OBJ(self, pos):
        col, pos = self.__parse_word(pos)
        if col == ',':
            raise vExcept(701, pos)
        if col == '(':
            cur_query, pos = self.__parse_parenthesis(pos)
            word, pos = self.__parse_word(pos)
            if (word.upper() == ',') or (word.upper() in self.__list_of_word(['NONE'])):
                raise vExcept(703, word)
            self.__parsed_query["from"].append([self.__get_cur_name(), None, word.upper(), 'CURSOR'])
            self.__parsed_query["cursors"].append([word.upper(), cur_query])
            word, pos = self.__parse_word(pos)
        else:
            word, pos = self.__parse_word(pos)
            if (word.upper() in [',', '']) or (word.upper() in self.__list_of_word(['NONE'])):
                a_t = col.upper().split('.')
                if len(a_t) == 1:
                    if self.__getCursor(a_t[0]):
                        self.__parsed_query["from"].append([self.__get_cur_name(), None, a_t[0], 'CURSOR'])
                    else:
                        self.__parsed_query["from"].append([self.__get_cur_name(), None, a_t[0], 'TABLE'])
                elif len(a_t) == 2:
                    self.__parsed_query["from"].append([self.__get_cur_name(), a_t[0], a_t[1], 'TABLE'])
                else:
                    raise vExcept(209, col)
            else:
                a_t = col.upper().split('.')
                if len(a_t) == 1:
                    if self.__getCursor(a_t[0]):
                        self.__parsed_query["from"].append([self.__get_cur_name(), None, a_t[0], 'CURSOR'])
                    else:
                        self.__parsed_query["from"].append([word.upper(), None, a_t[0], 'TABLE'])
                elif len(a_t) == 2:
                    self.__parsed_query["from"].append([word.upper(), a_t[0], a_t[1], 'TABLE'])
                else:
                    raise vExcept(209, col)
                word, pos = self.__parse_word(pos)
                if (word.upper() not in [',', '']) and (word.upper() not in self.__list_of_word(['NONE'])):
                    # print(f'__parse_FROM_OBJ word={word}')
                    raise vExcept(702, word)
        return word, pos

    def __parse_GRANT_with_admin_option(self, pos):
        if pos < len(self.__query):
            w, pos = self.__parse_word(pos)
            a, pos = self.__parse_word(pos)
            o, pos = self.__parse_word(pos)
            if w.upper() != 'WITH':
                raise vExcept(713, w)
            if a.upper() != 'ADMIN':
                raise vExcept(714, a)
            if o.upper() != 'OPTION':
                raise vExcept(715, o)
            result = 'YES'
        else:
            result = 'NO'
        return result, pos

    def __parse_INNER_JOIN(self, pos):
        col, pos = self.__parse_word(pos)
        if col.upper() != 'JOIN':
            raise vExcept(705, col)
        # par table/cursor
        col, pos = self.__parse_word(pos)
        if col == ',':
            raise vExcept(701, pos)
        if col == '(':
            cur_query, pos = self.__parse_parenthesis(pos)
            word, pos = self.__parse_word(pos)
            if (word.upper() == ',') or (word.upper() in self.__list_of_word(['NONE'])):
                raise vExcept(703, word)
            self.__parsed_query["from"].append([self.__get_cur_name(), None, word.upper(), 'CURSOR'])
            self.__parsed_query["cursors"].append([word.upper(), cur_query])
            word, pos = self.__parse_word(pos)
        else:
            word, pos = self.__parse_word(pos)
            if word.upper() == 'ON':
                a_t = col.upper().split('.')
                if len(a_t) == 1:
                    if self.__getCursor(a_t[0]):
                        self.__parsed_query["from"].append([self.__get_cur_name(), None, a_t[0], 'CURSOR'])
                    else:
                        self.__parsed_query["from"].append([self.__get_cur_name(), None, a_t[0], 'TABLE'])
                elif len(a_t) == 2:
                    self.__parsed_query["from"].append([self.__get_cur_name(), a_t[0], a_t[1], 'TABLE'])
                else:
                    raise vExcept(209, col)
            else:
                a_t = col.upper().split('.')
                if len(a_t) == 1:
                    if self.__getCursor(a_t[0]):
                        self.__parsed_query["from"].append([word.upper(), None, a_t[0], 'CURSOR'])
                    else:
                        self.__parsed_query["from"].append([word.upper(), None, a_t[0], 'TABLE'])
                elif len(a_t) == 2:
                    self.__parsed_query["from"].append([word.upper(), a_t[0], a_t[1], 'TABLE'])
                else:
                    raise vExcept(209, col)
                word, pos = self.__parse_word(pos)
                if word.upper() != 'ON':
                    raise vExcept(706, word)
        # parse clause
        parse_clause = []
        word, pos = self.__parse_word(pos)
        m1, op = None, None
        while (word.upper() not in self.__list_of_word(['NONE'])) and (pos < len(self.__query)):
            if word.upper() in ['(', ')', 'AND', 'OR']:
                parse_clause.append([word])
            elif m1 is None:
                m1 = word
            elif op is None:
                op = word
            else:
                parse_clause.append([m1, op, word])
                m1, op = None, None
            word, pos = self.__parse_word(pos)
        if pos >= len(self.__query):
            parse_clause.append([m1, op, word])
        self.__parsed_query["inner_where"].append(parse_clause)
        return word, pos

    def __parse_WHERE_CLAUSE(self, pos):
        word, pos = self.__parse_word(pos)
        m1, op = None, None
        while (word.upper() not in ['GROUP', 'ORDER', 'CONNECT']) and (pos < len(self.__query)):
            # print(f'__parse_WHERE_CLAUSE  1 word={word}')
            if self.__is_function(word.upper()):
                par, pos = self.__parse_word(pos)
                if par != '(':
                    raise vExcept(739, par)
                word, pos = self.__parse_COL_FCT(word.upper(), pos)
                # print(f'__parse_WHERE_CLAUSE  2 word={word}')
            if word.upper() in [')', 'AND', 'OR']:
                self.__parsed_query["where"].append([word])
                m1, op = None, None
                word, pos = self.__parse_word(pos)
                # print(f'__parse_WHERE_CLAUSE  3 word={word}')
            elif word == '(':
                if m1 is None:
                    self.__parsed_query["where"].append([word])
                else:
                    word, maths_id, pos = self.__parse_maths(m1, word, pos)
                    if word == ')':
                        if m1 is None:
                            m1 = maths_id
                        else:
                            self.__parsed_query["where"].append([m1, op, maths_id])
                            m1, op = None, None
                            word, pos = self.__parse_word(pos)
                    else:
                        raise vExcept(740, word)
            elif m1 is None:
                m1 = word
                word, pos = self.__parse_word(pos)
                # print(f'__parse_WHERE_CLAUSE  4 m1={m1}')
            elif op is None:
                op = word
                # print(f'__parse_WHERE_CLAUSE  5 op={op}')
                if op.upper() == 'BETWEEN':
                    v1, pos = self.__parse_word(pos)
                    if self.__is_function(v1.upper()):
                        par, pos = self.__parse_word(pos)
                        if par != '(':
                            raise vExcept(739, par)
                        v1, pos = self.__parse_COL_FCT(v1.upper(), pos)
                    btwand, pos = self.__parse_word(pos)
                    if btwand.upper() != 'AND':
                        raise vExcept(741, btwand)
                    v2, pos = self.__parse_word(pos)
                    if self.__is_function(v2.upper()):
                        par, pos = self.__parse_word(pos)
                        if par != '(':
                            raise vExcept(739, par)
                        v2, pos = self.__parse_COL_FCT(v2.upper(), pos)
                    self.__parsed_query["where"].append([m1, 'BETWEEN', v1, 'AND', v2])
                    m1, op = None, None
                    word, pos = self.__parse_word(pos)
                elif op.upper() == 'IN':
                    par, pos = self.__parse_word(pos)
                    if par != '(':
                        raise vExcept(739, par)
                    col_lst_name, pos = self.__parse_COL_IN(pos)
                    self.__parsed_query["where"].append([m1, 'IN', col_lst_name])
                    m1, op = None, None
                    word, pos = self.__parse_word(pos)
                    # print(f'__parse_WHERE_CLAUSE  6 word={word}')
                elif op in ['+', '-', '*', '/']:
                    word, m1, pos = self.__parse_maths(m1, op, pos)
                    op = None
                elif op == '||':
                    word, m1, pos = self.__parse_pipe(m1, word, pos)
                    op = None
                else:
                    word, pos = self.__parse_word(pos)
            else:
                wp1, _ = self.__parse_word(pos)
                if wp1 == '||':
                    wp1, pos = self.__parse_word(pos)
                    word, wp1, pos = self.__parse_pipe(word, op, pos)
                    self.__parsed_query["where"].append([m1, op, wp1])
                else:
                    self.__parsed_query["where"].append([m1, op, word])
                    word, pos = self.__parse_word(pos)
                m1, op = None, None
        if m1 is not None:
            self.__parsed_query["where"].append([m1, op, word])

        self.__remove_parenthesis(_where = self.__parsed_query["where"])
        return word, pos

    def __parse_CONNECT_BY_VALUE(self, pos):
        word, pos = self.__parse_word(pos)
        if word.upper() != 'BY':
            raise vExcept(742, word)
        word, pos = self.__parse_word(pos)
        if word.upper() != 'LEVEL':
            raise vExcept(743, word)
        oper, pos = self.__parse_word(pos)
        if oper not in ['<', '<=']:
            raise vExcept(744, oper)
        value, pos = self.__parse_word(pos)
        if not self.__check_INT(value):
            raise vExcept(745, value)
        self.__parsed_query["connect"] = [oper, int(value)]
        return word, pos

    def __parse_GROUP_BY_CLAUSE(self, pos):
        # 0: table_alias
        # 1: schema
        # 2: table_name
        # 3: col_name/value
        # 4: alias
        # 5: type(COL, INT, FLOAT, STR, HEX, DATETIME, FUNCTION, MATHS, PIPE)
        # 6: table position
        # 7: position in table
        # 8: table or cursor
        self.__parsed_query["post_tasks"] = True
        word, pos = self.__parse_word(pos)
        if word.upper() != 'BY':
            raise vExcept(751, word)
        while pos < len(self.__query):
            col, pos = self.__parse_word(pos)
            # print(f'__parse_GROUP_BY_CLAUSE 1  col={col}')
            if col == ',':
                raise vExcept(701, pos)
            word, pos = self.__parse_word(pos)
            # print(f'__parse_GROUP_BY_CLAUSE 2  word={word}')
            if word == ',':
                self.__parsed_query["group_by"].append([None, None, None, col, None, None, None, None, None])
            elif word in ['+', '-', '*', '/'] or col == '(':
                word, maths_id, pos = self.__parse_maths(col, word, pos)
                if word == ',':
                    self.__parsed_query["group_by"].append([None, None, None, maths_id, None, 'MATHS', None, None, None])
                else:
                    self.__parsed_query["group_by"].append([None, None, None, maths_id, word, 'MATHS', None, None, None])
                    word, pos = self.__parse_word(pos)
            elif word == '||':
                word, pipe_id, pos = self.__parse_pipe(col, word, pos)
                if word == ',':
                    self.__parsed_query["group_by"].append([None, None, None, pipe_id, None, 'PIPE', None, None, None])
                else:
                    self.__parsed_query["group_by"].append([None, None, None, pipe_id, word, 'PIPE', None, None, None])
                    word, pos = self.__parse_word(pos)
            else:
                if self.__is_function(col.upper()):
                    if word != '(':
                        raise vExcept(739, word)
                    # print (f'__parse_GROUP_BY_CLAUSE avant __parse_COL_FCT  col={col} word={word}')
                    fct_col, pos = self.__parse_COL_FCT(col.upper(), pos)
                    word, pos = self.__parse_word(pos)
                    # print (f'__parse_GROUP_BY_CLAUSE apres __parse_COL_FCT  fct_col={fct_col} word={word}')
                    if word != ',':
                        self.__parsed_query["group_by"].append([None, None, None, fct_col, word, 'FUNCTION', None, None, None])
                        word, pos = self.__parse_word(pos)
                    else:
                        self.__parsed_query["group_by"].append([None, None, None, fct_col, None, 'FUNCTION', None, None, None])
                else:
                    if word != ',':
                        self.__parsed_query["group_by"].append([None, None, None, col, word, None, None, None, None])
                        # print(f'__parse_GROUP_BY_CLAUSE 3  col={col} word={word}')
                        word, pos = self.__parse_word(pos)
                    else:
                        # print(f'__parse_GROUP_BY_CLAUSE 4  col={col} word={word}')
                        self.__parsed_query["group_by"].append([None, None, None, col, None, None, None, None, None])
                if (word != ',') and (pos < len(self.__query)):
                    # print(f'__parse_GROUP_BY_CLAUSE word={word}')
                    raise vExcept(702, word)
        # print(f'__parse_GROUP_BY_CLAUSE self.__parsed_query["group_by"]={self.__parsed_query["group_by"]}')
        return word, pos

    def __parse_ORDER_BY_CLAUSE(self, pos):
        # 0: col_name/value/alias
        # 1: ASC ou DESC
        word, pos = self.__parse_word(pos)
        if word.upper() != 'BY':
            raise vExcept(755, word)
        while pos < len(self.__query):
            col, pos = self.__parse_word(pos)
            # print(f'__parse_ORDER_BY_CLAUSE 1  col={col}')
            if col == ',':
                raise vExcept(701, pos)
            word, pos = self.__parse_word(pos)
            # print(f'__parse_ORDER_BY_CLAUSE 2  word={word}')
            if word == ',':
                self.__parsed_query["order_by"].append([col, 'ASC'])
            elif word in ['+', '-', '*', '/'] or col == '(':
                raise vExcept(756)
            elif word == '||':
                raise vExcept(757)
            else:
                if self.__is_function(col.upper()):
                    raise vExcept(758, col)
                else:
                    if word != ',':
                        if word.upper() in ['ASC', 'DESC']:
                            self.__parsed_query["order_by"].append([col, word.upper()])
                            # print(f'__parse_ORDER_BY_CLAUSE 3  col={col} word={word}')
                            word, pos = self.__parse_word(pos)
                        else:
                            self.__parsed_query["order_by"].append([col, 'ASC'])
                    else:
                        # print(f'__parse_ORDER_BY_CLAUSE 4  col={col} word={word}')
                        self.__parsed_query["order_by"].append([col, 'ASC'])
                if (word != ',') and (pos < len(self.__query)):
                    # print(f'__parse_ORDER_BY_CLAUSE word={word}')
                    raise vExcept(702, word)
        # print(f'__parse_ORDER_BY_CLAUSE self.__parsed_query["order_by"]={self.__parsed_query["order_by"]}')
        return word, pos

    def __getCursor(self, curin):
        found = False
        if len(self.__parsed_query["cursors"]) > 0:
            for c in self.__parsed_query["cursors"]:
                if c[0] == curin:
                    found = True
                    break
        return found

    def __getColFromTable(self, colin:str):
        fmt1 = self.__get_item_format(colin)
        if colin.upper() == 'ROWNUM':
            alias1 = None
            schema1 = None
            table_name1 = None
            col1 = 'ROWNUM'
            tab_cur1 = None
            num_tab = None
        elif fmt1 == 'COLUMN':
            tmp1=colin.upper().split('.')
            if len(tmp1) == 1:
                alias1 = None
                schema1 = None
                table_name1 = None
                col1 = tmp1[0]
                tab_cur1 = None
                num_tab = None
            elif len(tmp1) == 2:
                alias1 = tmp1[0]
                col1 = tmp1[1]
                found = False
                for n in range(len(self.__parsed_query["from"])):
                    if alias1 == self.__parsed_query["from"][n][0]:
                        found = True
                        schema1 = self.__parsed_query["from"][n][1]
                        table_name1 = self.__parsed_query["from"][n][2]
                        tab_cur1 = self.__parsed_query["from"][n][3]
                        num_tab = n
                        break
                    elif alias1 == self.__parsed_query["from"][n][2]:
                        found = True
                        alias1 = self.__parsed_query["from"][n][0]
                        schema1 = self.__parsed_query["from"][n][1]
                        table_name1 = self.__parsed_query["from"][n][2]
                        tab_cur1 = self.__parsed_query["from"][n][3]
                        num_tab = n
                        break
                if not found:
                    raise vExcept(1701, colin)
            elif len(tmp1) == 3:
                # from: table_alias, schema, table_name, TABLE or CURSOR
                schema1 = tmp1[0]
                table_name1 = tmp1[1]
                col1 = tmp1[2]
                found = False
                for n in range(len(self.__parsed_query["from"])):
                    if (schema1 == self.__parsed_query["from"][n][1]) and (table_name1 == self.__parsed_query["from"][n][2]):
                        found = True
                        alias1 = self.__parsed_query["from"][n][0]
                        tab_cur1 = self.__parsed_query["from"][n][3]
                        num_tab = n
                        break
                if not found:
                    for n in range(len(self.__parsed_query["from"])):
                        if (self.__parsed_query["from"][n][1] is None) and (table_name1 == self.__parsed_query["from"][n][2]):
                            found = True
                            alias1 = self.__parsed_query["from"][n][0]
                            tab_cur1 = self.__parsed_query["from"][n][3]
                            num_tab = None
                            break
                if not found:
                    raise vExcept(1701, colin)
            else:
                raise vExcept(1700, colin)
        else:
            alias1, col1, schema1, table_name1, tab_cur1, num_tab = None, colin, None, None, None, None
        return fmt1, alias1, col1, schema1, table_name1, tab_cur1, num_tab
        
    def __compute_where(self, parsed_where, w_idx, lst_where:list, v_idx, oper, bracket):
        if w_idx < len(parsed_where):
            x : list = parsed_where[w_idx]
            if len(x) > 1:
                # 1: 'TST'
                # 2: num_table
                # 3: num_col
                # 4: alias1
                # 5: field1
                # 6: type
                # 7: schema
                # 8: table_name
                # 9: table or cursor
                if self.__is_parsed_function(x[0]):
                    fmt1, alias1, col1, schema1, table_name1, tab_cur1, num_tab1 = 'FUNCTION', None, x[0], None, None, None, None
                elif self.__is_parsed_maths(x[0]):
                    fmt1, alias1, col1, schema1, table_name1, tab_cur1, num_tab1 = 'MATHS', None, x[0], None, None, None, None
                elif self.__is_parsed_pipe(x[0]):
                    fmt1, alias1, col1, schema1, table_name1, tab_cur1, num_tab1 = 'PIPE', None, x[0], None, None, None, None
                else:
                    fmt1, alias1, col1, schema1, table_name1, tab_cur1, num_tab1 = self.__getColFromTable(x[0])
                if self.__is_parsed_function(x[2]):
                    fmt2, alias2, col2, schema2, table_name2, tab_cur2, num_tab2 = 'FUNCTION', None, x[2], None, None, None, None
                elif self.__is_parsed_maths(x[2]):
                    fmt2, alias2, col2, schema2, table_name2, tab_cur2, num_tab2 = 'MATHS', None, x[2], None, None, None, None
                elif self.__is_parsed_pipe(x[2]):
                    fmt2, alias2, col2, schema2, table_name2, tab_cur2, num_tab2 = 'PIPE', None, x[2], None, None, None, None
                elif x[1].upper() == 'IN':
                    list2 = x[2]
                else:
                    fmt2, alias2, col2, schema2, table_name2, tab_cur2, num_tab2 = self.__getColFromTable(x[2])
                if (len(x) == 5) and (x[1].upper() == 'BETWEEN') and (x[3].upper() == 'AND'):
                    if self.__is_parsed_function(x[4]):
                        fmt3, alias3, col3, schema3, table_name3, tab_cur3, num_tab3 = 'FUNCTION', None, x[4], None, None, None, None
                    elif self.__is_parsed_maths(x[4]):
                        fmt3, alias3, col3, schema3, table_name3, tab_cur3, num_tab3 = 'MATHS', None, x[4], None, None, None, None
                    elif self.__is_parsed_pipe(x[4]):
                        fmt3, alias3, col3, schema3, table_name3, tab_cur3, num_tab3 = 'PIPE', None, x[4], None, None, None, None
                    else:
                        fmt3, alias3, col3, schema3, table_name3, tab_cur3, num_tab3 = self.__getColFromTable(x[4])
                    lst_where.append([v_idx, ['TST', num_tab1, None, alias1, col1, fmt1, schema1, table_name1, tab_cur1], 'BETWEEN', ['TST', num_tab2, None, alias2, col2, fmt2, schema2, table_name2, tab_cur2], ['TST', num_tab3, None, alias3, col3, fmt3, schema3, table_name3, tab_cur3]])
                elif (x[1].upper() == 'IN'):
                    lst_where.append([v_idx, ['TST', num_tab1, None, alias1, col1, fmt1, schema1, table_name1, tab_cur1], 'IN', ['TST', None, None, None, list2, None, None, None, None]])
                else:
                    lst_where.append([v_idx, ['TST', num_tab1, None, alias1, col1, fmt1, schema1, table_name1, tab_cur1], x[1], ['TST', num_tab2, None, alias2, col2, fmt2, schema2, table_name2, tab_cur2]])
                # x.insert(0, v_idx)
                v_idx += 1
                w_idx += 1
        if w_idx < len(parsed_where):
            current_v_idx = v_idx-1
            x = parsed_where[w_idx]
            w_idx += 1
            if (str(x[0]).upper() == 'AND') and (oper == 'AND'):
                w_idx, lst_where, v_idx, bracket = self.__compute_where(parsed_where, w_idx, lst_where, v_idx, oper, bracket)
                lst_where.append([v_idx, ['META', current_v_idx], 'AND', ['META', v_idx-1]])
                v_idx += 1
            elif (str(x[0]).upper() == 'OR') and (oper == 'OR'):
                w_idx, lst_where, v_idx, bracket = self.__compute_where(parsed_where, w_idx, lst_where, v_idx, oper, bracket)
                lst_where.append([v_idx, ['META', current_v_idx], 'OR', ['META', v_idx-1]])
                v_idx += 1
            elif x[0] == '(':
                bracket += 1
                current_v_idx = v_idx-1
                w_idx, lst_where, v_idx, bracket = self.__compute_where(parsed_where, w_idx, lst_where, v_idx, oper, bracket)
            elif x[0] == ')':
                bracket -= 1
            else:
                w_idx, lst_where, v_idx, bracket = self.__compute_where(parsed_where, w_idx, lst_where, v_idx, str(x[0]).upper(), bracket)
                lst_where.append([v_idx, ['META', current_v_idx], str(x[0]).upper(), ['META', v_idx-1]])
                v_idx += 1
        return w_idx, lst_where, v_idx, bracket

    def __compute_maths(self, parsed_where, w_idx, lst_where:list, v_idx, oper, bracket):
        while w_idx < len(parsed_where):
            x : list = parsed_where[w_idx]
            w_idx += 1
            if len(x) > 1:
                # 1: 'TST'
                # 2: num_table
                # 3: num_col
                # 4: alias1
                # 5: field1
                # 6: type
                # 7: schema
                # 8: table_name
                # 9: table or cursor
                lst_where.append([v_idx, ['TST', x[0], x[1], x[2], x[3], x[4], x[5], x[6], x[7]]])
                current_v_idx = v_idx
                v_idx += 1
            elif w_idx < len(parsed_where):    
                if (x[0] in ['*', '/']) and ((oper == '*/') or (oper is None)):
                    w_idx, lst_where, v_idx, bracket = self.__compute_maths(parsed_where, w_idx, lst_where, v_idx, '*/', bracket)
                    lst_where.append([v_idx, ['META', current_v_idx], x[0], ['META', v_idx-1]])
                    current_v_idx = v_idx
                    v_idx += 1
                elif (x[0] in ['+', '-']) and ((oper == '+-') or (oper is None)):
                    w_idx, lst_where, v_idx, bracket = self.__compute_maths(parsed_where, w_idx, lst_where, v_idx, '+-', bracket)
                    lst_where.append([v_idx, ['META', current_v_idx], x[0], ['META', v_idx-1]])
                    current_v_idx = v_idx
                    v_idx += 1
                elif x[0] == '(':
                    bracket += 1
                    current_v_idx = v_idx-1
                    w_idx, lst_where, v_idx, bracket = self.__compute_maths(parsed_where, w_idx, lst_where, v_idx, None, bracket)
                elif x[0] == ')':
                    bracket -= 1
                    return w_idx, lst_where, v_idx, bracket
                else:
                    w_idx -= 1
                    return w_idx, lst_where, v_idx, bracket
        return w_idx, lst_where, v_idx, bracket

    def __is_parsed_function(self, fct_id):
        for n in range(len(self.__parsed_query["functions"])):
            if self.__parsed_query["functions"][n][0] == fct_id:
                return True
        return False

    def __is_parsed_maths(self, maths_id):
        for n in range(len(self.__parsed_query["maths"])):
            if self.__parsed_query["maths"][n][0] == maths_id:
                return True
        return False

    def __is_parsed_pipe(self, pipe_id):
        for n in range(len(self.__parsed_query["pipe"])):
            if self.__parsed_query["pipe"][n][0] == pipe_id:
                return True
        return False

    def __get_item_format(self, varin):
        if self.__check_INT(varin):
            return 'INT'
        elif self.__check_FLOAT(varin):
            return 'FLOAT'
        elif self.__check_HEX(varin):
            return 'HEXA'
        elif self.__check_STR(varin):
            if ((varin[0] == '"') and (varin[-1] == '"') or (varin[0] == "'") and (varin[-1] == "'")):
                return 'STR'
            else:
                return 'COLUMN'
        elif self.__check_DATETIME(varin):
            return 'DATETIME'
        elif self.__is_function(varin.upper()):
            return 'FUNCTION'

    def __remove_parenthesis(self, _where:list):
        for n in range(len(_where)-1, -1, -1):
            if (len(_where[n]) == 1) and (_where[n][0] == '(') and (n+2 < len(_where)) and (_where[n+2][0] == ')'):
                del _where[n+2]
                del _where[n]

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

    def __get_format(self, varin):
        if self.__check_INT(varin):
            return 'INT'
        elif self.__check_FLOAT(varin):
            return 'FLOAT'
        elif self.__check_DATETIME(varin):
            return 'DATETIME'
        elif self.__check_HEX(varin):
            return 'HEX'
        elif self.__check_STR(varin):
            return 'STR'
        else:
            raise vExcept(2202, varin)

    def __list_of_word(self, except_this:list):
        lst = ['SELECT', 'INSERT', 'UPDATE', 'DELETE', 'FROM', 'INNER', 'LEFT', 'RIGHT', 'WHERE', 'GROUP', 'ORDER', 'CREATE', 'DROP', 'TABLE', 'INDEX', 'CONNECT']
        for et in except_this:
            if et in lst:
                del lst[lst.index(et)]
        return lst

    def __get_cur_name(self):
        result = 'CUR_{}{}'.format(self.__intCurSeq, random.randint(1, 99999999))
        self.__intCurSeq += 1
        return result

    def __get_fct_name(self):
        result = 'FCT_{}{}'.format(self.__intFctSeq, random.randint(1, 99999999))
        self.__intFctSeq += 1
        return result

    def __get_in_name(self):
        result = 'IN_{}{}'.format(self.__intInSeq, random.randint(1, 99999999))
        self.__intInSeq += 1
        return result

    def __get_maths_name(self):
        result = 'MATHS_{}{}'.format(self.__intMathsSeq, random.randint(1, 99999999))
        self.__intMathsSeq += 1
        return result

    def __get_pipe_name(self):
        result = 'PIPE_{}{}'.format(self.__intPipeSeq, random.randint(1, 99999999))
        self.__intPipeSeq += 1
        return result

# a = vParser()
# b= a.parse_query("select a.t, 'c''est l''exception', 23.23, bbb.g from toto a, (select hh from titi where gg>10) tit inner join truc.blurp bbb on bbb.g=tit.hh and bbb.y=a.t where (b=12) and ((c=23) or   (d>=5)  );    ")
# b = a.parse_query("select a.type, a.name plat, c.name legume, 23.89 from resto.plats a, resto.plat_legume b, resto.legumes c where a.id=b.id_plat and b.id_legume=c.id;   ")

# print(b)
# print('SELECT: ', b["select"])
# print('FROM: ', b["from"])
# print('CURSORS: ', b["cursors"])
# print('INNER_WHERE: ', b["inner_where"])
# print('WHERE: ', b["where"])
# print('GROUPBY: ', b["groupby"])
# print('ORDERBY: ', b["orderby"])
# print('PARSED_WHERE: ', b['parsed_where'])
# print('PARSED_INNER_WHERE: ', b['parsed_inner_where'])
