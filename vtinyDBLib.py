from jtinyDBLib import JSONtinyDB
from vSessionLib import vSession


class vDB():
    def __init__(self, _db_base_dir, g_params):
        self.db = JSONtinyDB(_g_params=g_params, _db_base_dir=_db_base_dir)
        self.SYSDBA = 1
        self.SYSOPER = 2
        self.NORMAL = 0

    def connect(self, user, password, dns:str=""):
        session = vSession(db=self.db, username=user, password=password, mode=self.NORMAL)
        return session
