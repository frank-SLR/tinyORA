class vExcept(Exception):

    def __init__(self, errcode, message="Internal error"):
        self.errcode = errcode

        match errcode:
            # PATH, Files (0-99)
            case 12:
                self.message = f'Invalid PATH for DB_BASE_DIR ({message})'
            case 13:
                self.message = f'Invalid PATH for DB_SCHEMA_DIR ({message})'
            case 14:
                self.message = f'INTERNAL ERROR:: file for table ({message}) not found'
            case 15:
                self.message = 'Database not open'
            case 16:
                self.message = f'Unable to create directory ({message}) for new DB'
            case 17:
                self.message = f'Directory ({message}) for new DB already exists'
            case 23:
                self.message = f'Invalid PATH and file name for DB ({message})'
            case 24:
                self.message = f'Invalid PATH and file name for TABLE ({message})'
            # Users (100-199)
            case 109:
                self.message = 'Invalid username or password'
            # Tables (200-299)
            case 209:
                self.message = f'Invalid table name ({message})'
            case 210:
                self.message = f'Table ({message}) does not exists'
            case 211:
                self.message = f'Table is not unique in query ({message})'
            case 212:
                self.message = 'Table already exists'
            # Columns (300-399)
            case 310:
                self.message = 'Invalid columns number'
            case 311:
                self.message = f'Invalid column name ({message})'
            case 312:
                self.message = 'Invalid columns number in sub-query'
            case 313:
                self.message = f'Ambiguous column name ({message})'
            # SELECT clause (400-499)
            # WHERE clause (500-599)
            case 500:
                self.message = 'More than 1 column in sub query with IN statement'
            case 510:
                self.message = f'Invalid operator ({message})'
            # DB (600-679)
            case 600:
                self.message = 'Invalid database name'
            case 601:
                self.message = f'Database ({message}) already exists'
            case 602:
                self.message = f'Invalid parameters file ({message})'
            case 603:
                self.message = 'Parameters file is missing'
            # SERVER MANAGMENT (660-679)
            case 660:
                self.message = 'Invalid Manager password'
            # IP (680-699)
            case 680:
                self.message = f'Invalid client IP ({message})'
            # SYNTAX ERROR (700-899)
            case 700:
                self.message = 'Syntax error: quotes are not matching'
            case 701:
                self.message = f'Syntax error: found comma instead of argument in position {message}'
            case 702:
                self.message = f'Syntax error: found argument ({message}) instead of comma'
            case 703:
                self.message = f'Syntax error: found ({message}) instead of alias for sub-query'
            case 704:
                self.message = 'Syntax error: parenthesis are not matching'
            case 705:
                self.message = f'Syntax error: reserved word "JOIN" expected but "{message}" found'
            case 706:
                self.message = f'Syntax error: reserved word "ON" expected but "{message}" found'
            case 707:
                self.message = 'Syntax error: semicolon does not terminate query'
            case 708:
                self.message = 'Syntax error: DESCRIBE accepts only one table name as parameter'
            case 709:
                self.message = f'Syntax error: reserved word "AS" expected but "{message}" found'
            case 710:
                self.message = f'Syntax error: missing open parenthesis for sub query with alias "{message}"'
            case 711:
                self.message = f'Syntax error: bad object (schema or schema.table_name), found "{message}"'
            case 712:
                self.message = f'Syntax error: reserved word "TO" expected but "{message}" found'
            case 713:
                self.message = f'Syntax error: reserved word "WITH" expected but "{message}" found'
            case 714:
                self.message = f'Syntax error: reserved word "ADMIN" expected but "{message}" found'
            case 715:
                self.message = f'Syntax error: reserved word "OPTION" expected but "{message}" found'
            case 716:
                self.message = f'Syntax error: reserved word "TABLE" or "INDEX" or "USER" expected but "{message}" found'
            case 717:
                self.message = f'Syntax error: bad object, expect "schema name", found "{message}"'
            case 718:
                self.message = f'Syntax error: reserved word "FROM" expected but "{message}" found'
            case 719:
                self.message = f'Syntax error: end of query reached but "{message}" found'
            case 720:
                self.message = f'Syntax error: "{message}" is not a grant right'
            case 721:
                self.message = f'Syntax error: expect "TABLE" or "USER" found "{message}"'
            case 722:
                self.message = f'Syntax error: invalid table name "{message}"'
            case 723:
                self.message = f'Syntax error: invalid column name "{message}"'
            case 724:
                self.message = f'Syntax error: invalid column type "{message}"'
            case 725:
                self.message = f'Syntax error: expect comma or parenthesis but found "{message}"'
            case 726:
                self.message = f'Syntax error: invalid user name "{message}"'
            case 727:
                self.message = f'Syntax error: reserved word "IDENTIFIED" expected but "{message}" found'
            case 728:
                self.message = f'Syntax error: reserved word "BY" expected but "{message}" found'
            case 729:
                self.message = f'Syntax error: reserved word "INTO" expected but "{message}" found'
            case 730:
                self.message = f'Syntax error: table name expected but "{message}" found'
            case 731:
                self.message = f'Syntax error: missing column name at position {message}'
            case 732:
                self.message = f'Syntax error: parenthesys expexted but "{message}" found'
            case 733:
                self.message = f'Syntax error: missing value at position {message}'
            case 734:
                self.message = 'Syntax error: numbers of olomns and values does not mmatch'
            case 735:
                self.message = 'Syntax error: COMMIT does not support parameters'
            case 736:
                self.message = 'Syntax error: ROLLBACK does not support parameters'
            case 737:
                self.message = f'Syntax error: reserved word "SET" expected but "{message}" found'
            case 738:
                self.message = f'Syntax error: reserved word "WHERE" expected but "{message}" found'
            case 739:
                self.message = f'Syntax error: expect open parenthesis but found "{message}"'
            case 740:
                self.message = f'Syntax error: expect closing parenthesis but found "{message}"'
            case 741:
                self.message = f'Syntax error: reserved word "AND" expected but "{message}" found'
            case 742:
                self.message = f'Syntax error: reserved word "BY" expected but "{message}" found'
            case 743:
                self.message = f'Syntax error: reserved word "LEVEL" expected but "{message}" found'
            case 744:
                self.message = f'Syntax error: comparator expected (< or <=) but "{message}" found'
            case 745:
                self.message = f'Syntax error: integer expected but "{message}" found'
            case 746:
                self.message = f'Syntax error: symbol "|" expected but "{message}" found'
            case 747:
                self.message = 'Syntax error: query can not end with symbol "|"'
            case 748:
                self.message = f'Syntax error: symbol "||" can not be followed by "{message}"'
            case 749:
                self.message = 'Syntax error: symbol "||" can not be followed by "||"'
            case 750:
                self.message = 'Syntax error: DATETIME can not been concatened with "||"'
            case 751:
                self.message = f'Syntax error: expect BY after GROUP but "{message}" found'
            case 752:
                self.message = 'Syntax error: miss column(s) in GROUP BY section'
            case 753:
                self.message = f'Syntax error: column(s) "{message}" in GROUP BY statement is not present in SELECT statement'
            case 754:
                self.message = 'Syntax error: extra column(s) in GROUP BY section'
            case 755:
                self.message = f'Syntax error: expect BY after ORDER but "{message}" found'
            case 756:
                self.message = 'Syntax error: mathematical operations not allowed in ORDER BY statement'
            case 757:
                self.message = 'Syntax error: concatenation not allowed in ORDER BY statement'
            case 758:
                self.message = f'Syntax error: function ({message}) not allowed in ORDER BY statement'
            case 759:
                self.message = f'Syntax error: field in ORDER BY statement({message}) does not match fields in SELECT statement'
            case 760:
                self.message = f'Syntax error: ambiguous field in ORDER BY statement({message})'
            case 761:
                self.message = f'Syntax error: reserved word "OUTER" expected but "{message}" found'
            # INTERNAL ERROR (800-899)
            case 801:
                self.message = 'Internal error on column name in SELECT clause'
            case 802:
                self.message = f'Internal error on unknown function ID ({message})'
            case 803:
                self.message = f'Internal error on unknown list ID ({message})'
            case 804:
                self.message = f'Internal error on unknown maths ID ({message})'
            case 805:
                self.message = f'Internal error on unknown pipe ID ({message})'
            case 888:
                self.message = f'INTERNAL ERROR: {message}'
            case 899:
                self.message = f'INTERNAL ERROR: test labels does not match ({message})'
            # GRANT missing (900-999)
            case 900:
                self.message = 'It miss rights to define this new right'
            case 901:
                self.message = 'Unable to create object'
            case 902:
                self.message = 'Unable to drop object'
            # SESSION (1000-1099)
            case 1000:
                self.message = f'Invalid SESSION_ID ({message})'
            case 1001:
                self.message = 'No query submitted or previous result already fetched'
            # BIND (1100-1199)
            case 1100:
                self.message = f'Bind error: bind variable ({message}) in query is not supplied'
            # ~~~~~BLANK (1200-1699)
            # COLUMN format (1700-1799)
            case 1700:
                self.message = f'Invalid column format ({message})'
            case 1701:
                self.message = f'Invalid table name for column ({message})'
            # USER (1800-1899)
            case 1800:
                self.message = f'Username/Schema does not exist ({message})'
            case 1801:
                self.message = f'User already exists ({message})'
            # LOCKs (1900-1999)
            case 1900:
                self.message = f'Unable to catch lock on {message}'
            # CREATE table (2000-2099)
            # CURSOR (2100-2199)
            case 2100:
                self.message = f'Cursor ({message}) not found'
            case 2101:
                self.message = f'Duplicate name for cursor ({message})'
            # FORMAT (2200-2299)
            case 2200:
                self.message = f'Invalid format for value ({message})'
            case 2201:
                self.message = f'Invalid type ({message})'
            case 2202:
                self.message = f'Unable to determine type for {message}'
            # FUNCTION (2300-2499)
            case 2300:
                self.message = f'Bad arguments number for SUBSTR function: {message} supplied argument(s) but needs 3'
            case 2301:
                self.message = f'''First argument for SUBSTR function must have 'str' format' ({message})'''
            case 2302:
                self.message = f'''Second argument for SUBSTR function must have 'int' format' ({message})'''
            case 2303:
                self.message = f'''Third argument for SUBSTR function must have 'int' format' ({message})'''
            case 2304:
                self.message = f'''Function does not exist ({message})'''
            case 2305:
                self.message = f'Bad arguments number for TO_CHAR function: {message} supplied argument(s) but needs 2'
            case 2306:
                self.message = f'''First argument for TO_CHAR function must have 'datetime' format' ({message})'''
            case 2307:
                self.message = f'''Second argument for TO_CHAR function must have 'str' format' ({message})'''
            case 2308:
                self.message = f'''Incorrect number of arguments for DECODE function: must be even but {message} argument(s) provided'''
            case 2309:
                self.message = f'''Incorrect number of arguments for CHAR function: must be 1 but {message} argument(s) provided'''
            case 2310:
                self.message = f'''Incorrect parameter for CHAR function ({message})'''
            case 2311:
                self.message = f'''Incorrect number of arguments for ABS function: must be 1 but {message} argument(s) provided'''
            case 2312:
                self.message = f'''Incorrect parameter for ABS function ({message})'''
            case 2313:
                self.message = f'''Incorrect number of arguments for INSTR function: must be 2 or 3 or 4 but {message} argument(s) provided'''
            case 2314:
                self.message = f'''Incorrect number of arguments for NVL function: must be 2 but {message} argument(s) provided'''
            case 2315:
                self.message = f'''Incorrect number of arguments for NVL2 function: must be 3 but {message} argument(s) provided'''
            case 2316:
                self.message = f'''Incorrect number of arguments for LPAD function: must be 2 or 3 but {message} argument(s) provided'''
            case 2317:
                self.message = f'''Incorrect arguments for LPAD function: second parameter must be integer but {message} is supplied'''
            case 2318:
                self.message = f'''Incorrect number of arguments for RPAD function: must be 2 or 3 but {message} argument(s) provided'''
            case 2319:
                self.message = f'''Incorrect arguments for RPAD function: second parameter must be integer but {message} is supplied'''
            case 2320:
                self.message = f'''Incorrect number of arguments for LTRIM function: must be 1 or 2 but {message} argument(s) provided'''
            case 2321:
                self.message = f'''Incorrect number of arguments for RTRIM function: must be 1 or 2 but {message} argument(s) provided'''
            case 2322:
                self.message = f'''Incorrect number of arguments for LENGTH function: must be 1 but {message} argument(s) provided'''
            case 2323:
                self.message = f'''Incorrect number of arguments for {message} function: must be 1'''
            case 2324:
                self.message = f'''First argument for TRUNC function must have 'datetime' or 'float' format' ({message})'''
            case 2325:
                self.message = f'''Second (optional) argument for TRUNC function must have 'int' format' ({message})'''
            case 2326:
                self.message = f'''Second (optional) argument for TRUNC function must been lest or equal to 18' ({message})'''
            case 2327:
                self.message = f'Bad arguments number for TRUNC function: {message} supplied argument(s) but needs 1 or 2'
            case 2328:
                self.message = f'''First argument for ACOS function must have 'float' format' ({message})'''
            case 2329:
                self.message = f'''First argument for ASIN function must have 'float' format' ({message})'''
            case 2330:
                self.message = f'''First argument for ATAN function must have 'float' format' ({message})'''
            case 2331:
                self.message = f'''First argument for ATAN2 function must have 'float' format' ({message})'''
            case 2332:
                self.message = f'''Incorrect number of arguments for ATAN2 function: must be 2 but {message} argument(s) provided'''
            case 2333:
                self.message = f'''First argument for COS function must have 'float' format' ({message})'''
            case 2334:
                self.message = f'''First argument for SIN function must have 'float' format' ({message})'''
            case 2335:
                self.message = f'''First argument for TAN function must have 'float' format' ({message})'''
            case 2336:
                self.message = f'''First argument for EXP function must have 'float' format' ({message})'''
            case 2337:
                self.message = f'''First argument for LN function must have 'float' format' ({message})'''
            case 2338:
                self.message = f'''First argument for LOG function must have 'float' format' ({message})'''
            case 2339:
                self.message = f'''First argument for CEIL function must have 'float' or 'int' format' ({message})'''
            case 2340:
                self.message = f'''First argument for FLOOR function must have 'float' or 'int' format' ({message})'''
            case 2341:
                self.message = f'Bad arguments number for PI function: {message} supplied argument(s) but needs no one'
            case 2342:
                self.message = f'''First argument for COSH function must have 'float' format' ({message})'''
            case 2343:
                self.message = f'''First argument for SINH function must have 'float' format' ({message})'''
            case 2344:
                self.message = f'''First argument for TANH function must have 'float' format' ({message})'''
            case 2345:
                self.message = f'''First argument for MOD function must have 'int' format' ({message})'''
            case 2346:
                self.message = f'''Second argument for MOD function must have 'int' format' ({message})'''
            case 2347:
                self.message = f'''First argument for POWER function must have 'float' or 'int' format' ({message})'''
            case 2348:
                self.message = f'''Second argument for MOD function must have 'float' or 'int' format' ({message})'''
            case 2349:
                self.message = f'''Incorrect number of arguments for MOD function: must be 2 but {message} argument(s) provided'''
            case 2350:
                self.message = f'''Incorrect number of arguments for POWER function: must be 2 but {message} argument(s) provided'''
            case 2351:
                self.message = f'''Incorrect number of arguments for LOG function: must be 2 but {message} argument(s) provided'''
            case 2352:
                self.message = f'''Second argument for LOG function must have 'float' format' ({message})'''
            # MATHS (2500-2599)
            case 2500:
                self.message = '''Division by zero'''
            # ~~~~~BLANK (2600-9899)
            # HTML
            case 9900:
                self.message = f'TABLE parameter has an unexpected value ({message})'

        super().__init__(self.message)