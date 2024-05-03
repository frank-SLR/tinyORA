from jtinyDBLib import JSONtinyDB
from vSessionLib import vSession


class vDB():
    def __init__(self, _db_base_dir):
        self.db = JSONtinyDB(_db_base_dir)

    def create_session(self, username, password):
        session = vSession(db=self.db, username=username, password=password)
        return session
        
