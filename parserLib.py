import re
import random
from vExceptLib import vExept

class vParser():
    def __init__(self) -> None:
        self.__intCurSeq = 0
        self.__raz()

    def __raz(self) -> None:
        # querytype: (SELECT, INSERT, UPDATE, DELETE, GRANT, REVOKE, CREATE, DROP, DESCRIBE, COMMIT, ROLLBACK)
        # select: table_alias, schema, table_name, col_name/value, alias, type(COL, INT, FLOAT, STR, HEX, FUNCTION), table position, position in table, table or cursor, [parameters of function]
        # from: table_alias, schema, table_name, TABLE or CURSOR, {table}
        # cursors: cursor_alias, query
        # inner_where: list of items
        # parsed_inner_where: item_id, field1, oper, field2
        #                  or item_id, ['META', item_id], oper, ['META', item_id]
        #                  or item_id, ['TST', num_table, num_col, alias1, field1, type, schema, table_name, table or cursor], oper, ['TST', num_table, num_col, alias2, field2, type, schema, table_name, table or cursor]
        # parsed_where: item_id, field1, oper, field2
        #            or item_id, ['META', item_id], oper, ['META', item_id]
        #            or item_id, ['TST', num_table, num_col, alias1, field1, type, schema, table_name, table or cursor], oper, ['TST', num_table, num_col, alias2, field2, type, schema, table_name, table or cursor]
        self.__parsed_query = {"querytype": None, "select": [], "from": [], "where": [], "orderby": [], "groupby": [], "cursors": [],
                               "inner_where": [], "parsed_where": [], "parsed_inner_where": [], "grant": [], "revoke": [], "create": [], "drop": [], "insert": [], "update": []}
        self.__query = ''

    def parse_query(self, query:str) -> dict:
        self.__raz()
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
                        raise vExept(735)
                    self.__parsed_query["querytype"] = 'COMMIT'
                case 'ROLLBACK':
                    if pos < len(self.__query):
                        raise vExept(736)
                    self.__parsed_query["querytype"] = 'ROLLBACK'
        # print(self.__parsed_query)
        return self.__parsed_query

    def __parse_word(self, pos):
        __continue = True
        result = ''
        while __continue:
            if pos < len(self.__query):
                match self.__query[pos]:
                    case ' ':
                        pos += 1
                        if len(result) > 0:
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
                    case ';':
                        pos += 1
                        if pos < len(self.__query):
                            while pos < len(self.__query):
                                if self.__query[pos] != ' ':
                                    raise vExept(707)
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
                            raise vExept(700)
                    case _:
                        result += self.__query[pos]
                        pos += 1
            else:
                __continue = False
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
            raise vExept(704)
        return p_value[0:-1], pos

    def __parse_select(self, pos):
        word = ''
        # SELECT
        while word.upper() != 'FROM':
            word, pos = self.__parse_SEL_COL(pos)
        # FROM
        while (word.upper() not in self.__list_of_word(['FROM'])) and (pos < len(self.__query)):
            word, pos = self.__parse_FROM_OBJ(pos)
        while pos < len(self.__query) and (word.upper() not in self.__list_of_word(['INNER', 'LEFT', 'RIGHT'])):
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
        if (word.upper() == 'ORDER') and (pos < len(self.__query)):
            pass
        if (word.upper() == 'GROUP') and (pos < len(self.__query)):
            pass
        # parse selected columns
        for n in range(len(self.__parsed_query["select"])):
            fmt, al, cn, sh, tn, tc = self.__getColFromTable(self.__parsed_query["select"][n][3])
            self.__parsed_query["select"][n][0] = al
            self.__parsed_query["select"][n][1] = sh
            self.__parsed_query["select"][n][2] = tn
            self.__parsed_query["select"][n][3] = cn
            self.__parsed_query["select"][n][5] = fmt
            self.__parsed_query["select"][n][8] = tc
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

    def __parse_insert(self, pos):
        word, pos = self.__parse_word(pos)
        if word.upper() != 'INTO':
            raise vExept(729, word)
        word, pos = self.__parse_word(pos)
        s_t = word.upper().split('.')
        if len(s_t) == 1:
            self.__parsed_query['insert'] = [None, s_t[0], [], None, None]
        elif len(s_t) == 2:
            self.__parsed_query['insert'] = [s_t[0], s_t[1], [], None, None]
        else:
            raise vExept(730, word)
        word, pos = self.__parse_word(pos)
        if word == '(':
            not_ended = True
            memo = ''
            while not_ended:
                word, pos = self.__parse_word(pos)
                if word == ')':
                    if memo == '':
                        raise vExept(731, pos)
                    else:
                        self.__parsed_query['insert'][2].append(memo)
                        not_ended = False
                elif word == ',':
                    if memo == '':
                        raise vExept(731, pos)
                    else:
                        self.__parsed_query['insert'][2].append(memo)
                        memo = ''
                else:
                    if memo != '':
                        raise vExept(725, word)
                    else:
                        memo = word.upper()
            word, pos = self.__parse_word(pos)
        if word.upper() == 'VALUES':
            self.__parsed_query['insert'][3] = []
            word, pos = self.__parse_word(pos)
            if word != '(':
                raise vExept(732, word)
            not_ended = True
            memo = ''
            while not_ended:
                word, pos = self.__parse_word(pos)
                if word == ')':
                    if memo == '':
                        raise vExept(733, pos)
                    else:
                        self.__parsed_query['insert'][3].append(memo)
                        not_ended = False
                elif word == ',':
                    if memo == '':
                        raise vExept(733, pos)
                    else:
                        self.__parsed_query['insert'][3].append(memo)
                        memo = ''
                else:
                    if memo != '':
                        raise vExept(725, word)
                    else:
                        memo = word.upper()
            word, pos = self.__parse_word(pos)
            if word != '':
                raise vExept(719, word)
            if len(self.__parsed_query['insert'][3]) != len(self.__parsed_query['insert'][2]):
                raise vExept(734)
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
                raise vExept(709, word)
            word, pos = self.__parse_word(pos)
            if word != '(':
                raise vExept(710, c_name)
            c_query, pos = self.__parse_parenthesis(pos)
            # self.__parsed_query["from"].append([self.__get_cur_name(), None, c_name.upper(), 'CURSOR'])
            self.__parsed_query["cursors"].append([c_name.upper(), c_query])
            c_name, pos = self.__parse_word(pos)
        self.__parse_select(pos)

    def __parse_desc(self, pos):
        word, pos = self.__parse_word(pos)
        if pos < len( self.__query):
            raise vExept(708)
        a_t = word.upper().split('.')
        if len(a_t) == 1:
            self.__parsed_query["from"].append([self.__get_cur_name(), None, a_t[0], 'TABLE'])
        elif len(a_t) == 2:
            self.__parsed_query["from"].append([self.__get_cur_name(), a_t[0], a_t[1], 'TABLE'])
        else:
            raise vExept(209, word)
        word, pos = self.__parse_word(pos)
        if word != '':
            raise vExept(719, word)

    def __parse_grant(self, pos):
        word, pos = self.__parse_word(pos)
        match word.upper():
            case 'SELECT' | 'INSERT' | 'UPDATE' | 'DELETE':
                g_name = word.upper()
                word, pos = self.__parse_word(pos)
                if word.upper() != 'ON':
                    raise vExept(706, word)
                word, pos = self.__parse_word(pos)
                s_t = word.upper().split('.')
                if len(s_t) == 1:
                    g_type = 'SCHEMA'
                    g_table = s_t[0]
                elif len(s_t) == 2:
                    g_type = 'TABLE'
                    g_table = '{}.{}'.format(s_t[0], s_t[1])
                else:
                    raise vExept(711, word)
                word, pos = self.__parse_word(pos)
                if word.upper() != 'TO':
                    raise vExept(712, word)
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
                            raise vExept(712, word)
                        word, pos = self.__parse_word(pos)
                        g_user = word.lower()
                        g_admin, pos = self.__parse_GRANT_with_admin_option(pos)
                        self.__parsed_query["grant"].append([g_user, g_name, 'USER', None, g_admin])
                    case 'TABLE' | 'INDEX': # grant { create | drop } { table | index } on <SCHEMA> to <USERNAME> [ with admin option ]
                        g_type = word.upper()
                        word, pos = self.__parse_word(pos)
                        if word.upper() != 'ON':
                            raise vExept(706, word)
                        word, pos = self.__parse_word(pos)
                        s_t = word.upper().split('.')
                        g_table = s_t[0]
                        if len(s_t) != 1:
                            raise vExept(717, word)
                        if word.upper() != 'TO':
                            raise vExept(712, word)
                        word, pos = self.__parse_word(pos)
                        g_user = word.lower()
                        g_admin, pos = self.__parse_GRANT_with_admin_option(pos)
                        self.__parsed_query["grant"].append([g_user, g_name, g_type, g_table, g_admin])
                    case _:
                        raise vExept(716, word)
            case _:
                raise vExept(720, word)
        word, pos = self.__parse_word(pos)
        if word != '':
            raise vExept(719, word)

    def __parse_revoke(self, pos):
        word, pos = self.__parse_word(pos)
        match word.upper():
            case 'SELECT' | 'INSERT' | 'UPDATE' | 'DELETE':
                g_name = word.upper()
                word, pos = self.__parse_word(pos)
                if word.upper() != 'ON':
                    raise vExept(706, word)
                word, pos = self.__parse_word(pos)
                s_t = word.upper().split('.')
                if len(s_t) == 1:
                    g_type = 'SCHEMA'
                    g_table = s_t[0]
                elif len(s_t) == 2:
                    g_type = 'TABLE'
                    g_table = '{}.{}'.format(s_t[0], s_t[1])
                else:
                    raise vExept(711, word)
                word, pos = self.__parse_word(pos)
                if word.upper() != 'FROM':
                    raise vExept(718, word)
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
                            raise vExept(718, word)
                        word, pos = self.__parse_word(pos)
                        g_user = word.lower()
                        self.__parsed_query["revoke"].append([g_user, g_name, g_type, None])
                    case 'TABLE' | 'INDEX': # grant { create | drop } { table | index } on <SCHEMA> to <USERNAME> [ with admin option ]
                        g_type = word.upper()
                        word, pos = self.__parse_word(pos)
                        if word.upper() != 'ON':
                            raise vExept(706, word)
                        word, pos = self.__parse_word(pos)
                        s_t = word.upper().split('.')
                        g_table = s_t[0]
                        if len(s_t) != 1:
                            raise vExept(717, word)
                        if word.upper() != 'FROM':
                            raise vExept(718, word)
                        word, pos = self.__parse_word(pos)
                        g_user = word.lower()
                        self.__parsed_query["revoke"].append([g_user, g_name, g_type, g_table])
                    case _:
                        raise vExept(716, word)
            case _:
                raise vExept(720, word)
        word, pos = self.__parse_word(pos)
        if word != '':
            raise vExept(719, word)

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
                    raise vExept(722, word)
                if t_name in self.__list_of_word(except_this = []):
                    raise vExept(722, t_name)
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
                                raise vExept(723, c_col)
                            if c_type.lower() not in ['str', 'int', 'float', 'hex']:
                                raise vExept(724, c_type)
                            if word not in [',', ')']:
                                raise vExept(725, word)
                            c_cols.append(c_col)
                            c_cols.append(c_type)
                            c_col, c_type = None, None
                        self.__parsed_query["create"].append(['TABLE', t_owner, t_name, c_cols])
                    case _:
                        raise vExept(709, word)
            case 'USER':
                username, pos = self.__parse_word(pos)
                if (len(username.split('.')) > 1) or (len(username.split(' ')) > 1):
                    raise vExept(726, username)
                word, pos = self.__parse_word(pos)
                if word.upper() != 'IDENTIFIED':
                    raise vExept(727, word)
                word, pos = self.__parse_word(pos)
                if word.upper() != 'BY':
                    raise vExept(728, word)
                password, pos = self.__parse_word(pos)
                self.__parsed_query["create"].append(['USER', username, password])
            case _:
                raise vExept(721, word)
        word, pos = self.__parse_word(pos)
        if word != '':
            raise vExept(719, word)

    def __parse_update(self, pos):
        word, pos = self.__parse_word(pos)
        a_t = word.upper().split('.')
        if len(a_t) == 1:
            self.__parsed_query["from"].append([self.__get_cur_name(), None, a_t[0], 'TABLE'])
        elif len(a_t) == 2:
            self.__parsed_query["from"].append([self.__get_cur_name(), a_t[0], a_t[1], 'TABLE'])
        else:
            raise vExept(209, word)
        word, pos = self.__parse_word(pos)
        if word.upper() != 'SET':
            raise vExept(737, word)
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
                raise vExept(738, word)
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
            raise vExept(718, word)
        word, pos = self.__parse_word(pos)
        a_t = word.upper().split('.')
        if len(a_t) == 1:
            self.__parsed_query["from"].append([self.__get_cur_name(), None, a_t[0], 'TABLE'])
        elif len(a_t) == 2:
            self.__parsed_query["from"].append([self.__get_cur_name(), a_t[0], a_t[1], 'TABLE'])
        else:
            raise vExept(209, word)
        word, pos = self.__parse_word(pos)
        if pos < len(self.__query):
            if word.upper() == 'WHERE':
                word, pos = self.__parse_WHERE_CLAUSE(pos)
            else:
                raise vExept(738, word)
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
                    raise vExept(722, word)
            case 'USER':
                word, pos = self.__parse_word(pos)
                usr = word.upper().split('.')
                if len(word.split('.')) == 1:
                    self.__parsed_query["drop"].append(['USER', word.upper()])
                else:
                    raise vExept(726, word)
            case _:
                raise vExept(721, word)
        word, pos = self.__parse_word(pos)
        if word != '':
            raise vExept(719, word)

    def __parse_SEL_COL(self, pos):
        col, pos = self.__parse_word(pos)
        if col == ',':
            raise vExept(701, pos)
        word, pos = self.__parse_word(pos)
        if word.upper() in [',', 'FROM']:
            self.__parsed_query["select"].append([None, None, None, col, None, None, len(self.__parsed_query["select"]), None, None, []])
        else:
            self.__parsed_query["select"].append([None, None, None, col, word, None, len(self.__parsed_query["select"]), None, None, []])
            word, pos = self.__parse_word(pos)
            if word.upper() not in [',', 'FROM']:
                raise vExept(702, word)
        return word, pos

    def __parse_FROM_OBJ(self, pos):
        col, pos = self.__parse_word(pos)
        if col == ',':
            raise vExept(701, pos)
        if col == '(':
            cur_query, pos = self.__parse_parenthesis(pos)
            word, pos = self.__parse_word(pos)
            if (word.upper() == ',') or (word.upper() in self.__list_of_word(['NONE'])):
                raise vExept(703, word)
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
                    raise vExept(209, col)
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
                    raise vExept(209, col)
                word, pos = self.__parse_word(pos)
                if (word.upper() not in [',', '']) and (word.upper() not in self.__list_of_word(['NONE'])):
                    raise vExept(702, word)
        return word, pos

    def __parse_GRANT_with_admin_option(self, pos):
        if pos < len(self.__query):
            w, pos = self.__parse_word(pos)
            a, pos = self.__parse_word(pos)
            o, pos = self.__parse_word(pos)
            if w.upper() != 'WITH':
                raise vExept(713, w)
            if a.upper() != 'ADMIN':
                raise vExept(714, a)
            if o.upper() != 'OPTION':
                raise vExept(715, o)
            result = 'YES'
        else:
            result = 'NO'
        return result, pos

    def __parse_INNER_JOIN(self, pos):
        col, pos = self.__parse_word(pos)
        if col.upper() != 'JOIN':
            raise vExept(705, col)
        # par table/cursor
        col, pos = self.__parse_word(pos)
        if col == ',':
            raise vExept(701, pos)
        if col == '(':
            cur_query, pos = self.__parse_parenthesis(pos)
            word, pos = self.__parse_word(pos)
            if (word.upper() == ',') or (word.upper() in self.__list_of_word(['NONE'])):
                raise vExept(703, word)
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
                    raise vExept(209, col)
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
                    raise vExept(209, col)
                word, pos = self.__parse_word(pos)
                if word.upper() != 'ON':
                    raise vExept(706, word)
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
        while (word.upper() not in ['GROUP', 'ORDER']) and (pos < len(self.__query)):
            if word.upper() in ['(', ')', 'AND', 'OR']:
                self.__parsed_query["where"].append([word])
            elif m1 is None:
                m1 = word
            elif op is None:
                op = word
            else:
                self.__parsed_query["where"].append([m1, op, word])
                m1, op = None, None
            word, pos = self.__parse_word(pos)
        if m1 is not None:
            self.__parsed_query["where"].append([m1, op, word])
        self.__remove_parenthesis(_where = self.__parsed_query["where"])
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
        if fmt1 == 'COLUMN':
            tmp1=colin.upper().split('.')
            if len(tmp1) == 1:
                alias1 = None
                schema1 = None
                table_name1 = None
                col1 = tmp1[0]
                tab_cur1 = None
            elif len(tmp1) == 2:
                alias1 = tmp1[0]
                col1 = tmp1[1]
                found = False
                for t in self.__parsed_query["from"]:
                    if alias1 == t[0]:
                        found = True
                        schema1 = t[1]
                        table_name1 = t[2]
                        tab_cur1 = t[3]
                        break
                    if alias1 == t[2]:
                        found = True
                        alias1 = t[0]
                        schema1 = t[1]
                        table_name1 = t[2]
                        tab_cur1 = t[3]
                        break
                if not found:
                    raise vExept(1701, colin)
            elif len(tmp1) == 3:
                # from: table_alias, schema, table_name, TABLE or CURSOR
                schema1 = tmp1[0]
                table_name1 = tmp1[1]
                col1 = tmp1[2]
                found = False
                for t in self.__parsed_query["from"]:
                    if (schema1 == t[1]) and (table_name1 == t[2]):
                        found = True
                        alias1 = t[0]
                        tab_cur1 = t[3]
                        break
                if not found:
                    for t in self.__parsed_query["from"]:
                        if (t[1] is None) and (table_name1 == t[2]):
                            found = True
                            alias1 = t[0]
                            tab_cur1 = t[3]
                            break
                if not found:
                    raise vExept(1701, colin)
            else:
                raise vExept(1700, colin)
        else:
            alias1, col1, schema1, table_name1, tab_cur1 = None, colin, None, None, None
        return fmt1, alias1, col1, schema1, table_name1, tab_cur1
        
    def __compute_where(self, parsed_where, w_idx, lst_where:list, v_idx, oper, bracket):
        if w_idx < len(parsed_where):
            x : list = parsed_where[w_idx]
            if len(x) > 1:
                fmt1, alias1, col1, schema1, table_name1, tab_cur1 = self.__getColFromTable(x[0])
                fmt2, alias2, col2, schema2, table_name2, tab_cur2 = self.__getColFromTable(x[2])
                lst_where.append([v_idx, ['TST', None, None, alias1, col1, fmt1, schema1, table_name1, tab_cur1], x[1], ['TST', None, None, alias2, col2, fmt2, schema2, table_name2, tab_cur2]])
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

    def __get_item_format(self, varin):
        if self.__check_INT(varin):
            return 'INT'
        elif self.__check_FLOAT(varin):
            return 'FLOAT'
        elif self.__check_HEX(varin):
            return 'HEXA'
        elif self.__check_STR(varin):
            return 'STR'
        else:
            return 'COLUMN'

    def __remove_parenthesis(self, _where:list):
        for n in range(len(_where)-1, -1, -1):
            if (len(_where[n]) == 1) and (_where[n][0] == '(') and (n+2 < len(_where)) and (_where[n+2][0] == ')'):
                del _where[n+2]
                del _where[n]

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
        if ((varin[0] == '"') and (varin[-1] == '"')) or ((varin[0] == "'") and (varin[-1] == "'")):
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

    def __list_of_word(self, except_this:list):
        lst = ['SELECT', 'INSERT', 'UPDATE', 'DELETE', 'FROM', 'INNER', 'LEFT', 'RIGHT', 'WHERE', 'GROUP', 'ORDER', 'CREATE', 'DROP', 'TABLE', 'INDEX']
        for et in except_this:
            if et in lst:
                del lst[lst.index(et)]
        return lst

    def __get_cur_name(self):
        result = 'CUR_{}{}'.format(self.__intCurSeq, random.randint(1, 99999999))
        self.__intCurSeq += 1
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
