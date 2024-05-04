class vExept(Exception):

    def __init__(self, errcode, message="Internal error"):
        self.errcode = errcode

        match errcode:
            # PATH, Files (0-99)
            case 12:
                self.message = 'Invalid PATH for DB_BASE_DIR ({})'.format(message)
            case 13:
                self.message = 'Invalid PATH for DB_SCHEMA_DIR ({})'.format(message)
            case 14:
                self.message = 'INTERNAL ERROR:: file for table ({}) not found'.format(message)
            case 15:
                self.message = 'Database not open'
            case 16:
                self.message = 'Unable to create directory ({}) for new DB'.format(message)
            case 23:
                self.message = 'Invalid PATH and file name for DB ({})'.format(message)
            case 24:
                self.message = 'Invalid PATH and file name for TABLE ({})'.format(message)
            # Users (100-199)
            case 109:
                self.message = 'Invalid username or password'
            # Tables (200-299)
            case 209:
                self.message = 'Invalid table name ({})'.format(message)
            case 210:
                self.message = 'Table ({}) does not exists'.format(message)
            case 211:
                self.message = 'Table is not unique in query ({})'.format(message)
            case 212:
                self.message = 'Table already exists'
            # Columns (300-399)
            case 310:
                self.message = 'Invalid columns number'
            case 311:
                self.message = 'Invalid column name ({})'.format(message)
            case 312:
                self.message = 'Invalid columns number in sub-query'
            case 313:
                self.message = 'Ambiguous column name ({})'.format(message)
            # SELECT clause (400-499)
            # WHERE clause (500-599)
            case 510:
                self.message = 'Invalid operator ({})'.format(message)
            # DB (600-679)
            case 600:
                self.message = 'Invalid database name'
            case 601:
                self.message = 'Database ({}) already exists'.format(message)
            case 602:
                self.message = 'Invalid parameters file ({})'.format(message)
            case 603:
                self.message = 'Parameters file is missing'.format(message)
            # SERVER MANAGMENT (660-679)
            case 660:
                self.message = 'Invalid Manager password'
            # IP (680-699)
            case 680:
                self.message = 'Invlid client IP ({})'.format(message)
            # SYNTAX ERROR (700-899)
            case 700:
                self.message = 'Syntax error: quotes are not matching'
            case 701:
                self.message = 'Syntax error: found comma instead of argument in position {}'.format(message)
            case 702:
                self.message = 'Syntax error: found argument ({}) instead of comma'.format(message)
            case 703:
                self.message = 'Syntax error: found ({}) instead of alias for sub-query'.format(message)
            case 704:
                self.message = 'Syntax error: parenthesis are not matching'
            case 705:
                self.message = 'Syntax error: reserved word "JOIN" expected but "{}" found'.format(message)
            case 706:
                self.message = 'Syntax error: reserved word "ON" expected but "{}" found'.format(message)
            case 707:
                self.message = 'Syntax error: semicolon does not terminate query'
            case 708:
                self.message = 'Syntax error: DESCRIBE accepts only one table name as parameter'
            case 709:
                self.message = 'Syntax error: reserved word "AS" expected but "{}" found'.format(message)
            case 710:
                self.message = 'Syntax error: missing open parenthesis for sub query with alias "{}"'.format(message)
            case 711:
                self.message = 'Syntax error: bad object (schema or schema.table_name), found "{}"'.format(message)
            case 712:
                self.message = 'Syntax error: reserved word "TO" expected but "{}" found'.format(message)
            case 713:
                self.message = 'Syntax error: reserved word "WITH" expected but "{}" found'.format(message)
            case 714:
                self.message = 'Syntax error: reserved word "ADMIN" expected but "{}" found'.format(message)
            case 715:
                self.message = 'Syntax error: reserved word "OPTION" expected but "{}" found'.format(message)
            case 716:
                self.message = 'Syntax error: reserved word "TABLE" or "INDEX" or "USER" expected but "{}" found'.format(message)
            case 717:
                self.message = 'Syntax error: bad object, expect "schema name", found "{}"'.format(message)
            case 718:
                self.message = 'Syntax error: reserved word "FROM" expected but "{}" found'.format(message)
            case 719:
                self.message = 'Syntax error: end of query reached but "{}" found'.format(message)
            case 720:
                self.message = 'Syntax error: "{}" is not a grant right'.format(message)
            case 721:
                self.message = 'Syntax error: expect "TABLE" or "USER" found "{}"'.format(message)
            case 722:
                self.message = 'Syntax error: invalid table name "{}"'.format(message)
            case 723:
                self.message = 'Syntax error: invalid column name "{}"'.format(message)
            case 724:
                self.message = 'Syntax error: invalid column type "{}"'.format(message)
            case 725:
                self.message = 'Syntax error: expect comma or parenthesis but found "{}"'.format(message)
            case 726:
                self.message = 'Syntax error: invalid user name "{}"'.format(message)
            case 727:
                self.message = 'Syntax error: reserved word "IDENTIFIED" expected but "{}" found'.format(message)
            case 728:
                self.message = 'Syntax error: reserved word "BY" expected but "{}" found'.format(message)
            case 729:
                self.message = 'Syntax error: reserved word "INTO" expected but "{}" found'.format(message)
            case 730:
                self.message = 'Syntax error: table name expected but "{}" found'.format(message)
            case 731:
                self.message = 'Syntax error: missing column name at position {}'.format(message)
            case 732:
                self.message = 'Syntax error: parenthesys expexted but "{}" found'.format(message)
            case 733:
                self.message = 'Syntax error: missing value at position {}'.format(message)
            case 734:
                self.message = 'Syntax error: numbers of olomns and values does not mmatch'
            case 735:
                self.message = 'Syntax error: COMMIT does not support parameters'
            case 736:
                self.message = 'Syntax error: ROLLBACK does not support parameters'
            case 737:
                self.message = 'Syntax error: reserved word "SET" expected but "{}" found'.format(message)
            case 738:
                self.message = 'Syntax error: reserved word "WHERE" expected but "{}" found'.format(message)
            # INTERNAL ERROR (800-899)
            case 801:
                self.message = 'Internal error on column name in SELECT clause'
            case 888:
                self.message = 'INTERNAL ERROR: {}'.format(message)
            case 899:
                self.message = 'INTERNAL ERROR: test labels does not match ({})'.format(message)
            # GRANT missing (900-999)
            case 900:
                self.message = 'It miss rights to define this new right'
            case 901:
                self.message = 'Unable to create object'
            case 902:
                self.message = 'Unable to drop object'
            # SESSION (1000-1099)
            case 1000:
                self.message = 'Invalid SESSION_ID ({})'.format(message)
            # ~~~~~BLANK (1100-1699)
            # COLUMN format (1700-1799)
            case 1700:
                self.message = 'Invalid column format ({})'.format(message)
            case 1701:
                self.message = 'Invalid table name for column ({})'.format(message)
            # USER (1800-1899)
            case 1800:
                self.message = 'Username/Schema does not exist ({})'.format(message)
            case 1801:
                self.message = 'User already exists ({})'.format(message)
            # ~~~~~BLANK (1900-1999)
            # CREATE table (2000-2099)
            # CURSOR (2100-2199)
            case 2100:
                self.message = 'Cursor ({}) not found'.format(message)
            case 2101:
                self.message = 'Duplicate name for cursor ({})'.format(message)
            # FORMAT (2200-2299)
            case 2200:
                self.message = 'Invalid format for value ({})'.format(message)
            # ~~~~~BLANK (2300-9999)

        super().__init__(self.message)