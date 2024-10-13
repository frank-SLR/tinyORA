from jtinyDBLib import JSONtinyDB
from vSessionLib import vSession


class vDB():
    def __init__(self, _db_base_dir, g_params):
        self.db = JSONtinyDB(_g_params=g_params, _db_base_dir=_db_base_dir)

    def create_session(self, username, password):
        session = vSession(db=self.db, username=username, password=password)
        return session
        
