import random
from vExceptLib import vExcept
from jtinyDBLib import JSONtinyDB
from vCursorLib import vCursor


class vSession(object):
    def __init__(self, db: JSONtinyDB, username, password, mode=0):
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
        self.__mode = mode
        self.__updated_tables = []
        super().__init__()

    def cursor(self):
        return vCursor(
            username=self.__session_username,
            password=self.__password,
            db=self.db,
            updated_tables=self.__updated_tables,
            session=self,
        )

    def commit(self):
        __obj_to_commit = {"Tables": []}
        for tab_to_save in self.__updated_tables:
            self.db.UpdTableToDB(table_data=tab_to_save)
            __obj_to_commit["Tables"].append(
                [tab_to_save["schema"], tab_to_save["table_name"]]
            )
        self.db.save(obj_to_commit=__obj_to_commit)
        self.__updated_tables = []

    def rollback(self):
        self.__updated_tables = []
