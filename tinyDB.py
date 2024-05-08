# uvicorn tinyDB:app --reload --port 1521
# uvicorn tinyDB:app --reload --port 1521 --ssl-keyfile D:\Python\OpenSSL\key.pem --ssl-certfile D:\Python\OpenSSL\cert.pem --log-level warning

from vtinyDBLib import vDB
from jtinyDBLib import JSONtinyDB
from vExceptLib import vExept
from fastapi import FastAPI, Response, status, Request
from fastapi.responses import JSONResponse
import sys, os, ssl, json

class UnicornException(Exception):
    def __init__(self, err_code: int, message: str, status_code: int):
        self.message = message
        self.err_code = err_code
        self.status_code = status_code


# URL is <address>:<port>/tinyDB
app: FastAPI = FastAPI(root_path='/tinyDB')
# ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
# ssl_context.load_cert_chain('D:\\Python\\OpenSSL\\cert.pem', keyfile='D:\\Python\\OpenSSL\\key.pem')

# init internal variables
app.sessions = [] # [session_id, session, username, database, request.client.host]
app.dbs = []
app.parameters_file = "./parameters.json"

# load parameters file
if os.path.isfile(app.parameters_file):
    __id_db = open(app.parameters_file)
    app.__meta_cfg = json.load(__id_db)
    __id_db.close()
else:
    raise vExept(602, sys.argv[1])

# load and open DB(s)
for db in app.__meta_cfg["db_list"]:
    if os.path.exists(db["base_dir"]):
        app.dbs.append([db["name"], vDB(_db_base_dir=db["base_dir"], g_params=app.__meta_cfg["global_parameters"])])
    else:
        raise vExept(23, db["base_dir"])


# --------------------------------------------------------------------
# Exception
# --------------------------------------------------------------------
@app.exception_handler(UnicornException)
async def unicorn_exception_handler(request: Request, exc: UnicornException):
    return JSONResponse(
        status_code=exc.status_code,
        content={"err_message": exc.message, "err_code": exc.err_code},
    )

# --------------------------------------------------------------------
# Welcome
# --------------------------------------------------------------------
@app.get("/")
async def read_root(request: Request):
    """say hello

    Returns:
        str: welcome message
    """    
    return {"Welcome to tinyDB !! your IP is {}".format(request.client.host)}

# --------------------------------------------------------------------
# CONNECT
# --------------------------------------------------------------------
@app.get("/connect/db", status_code=status.HTTP_200_OK)
async def open_connection(database: str, username: str, password: str, request: Request) -> dict:
    """Open connection into "database" with "username" and "password"

    Args:
        database (str): Database name
        username (str): User name
        password (str): Password of user

    Raises:
        vExept: vDB exception {code, message}

    Returns:
        dict: dict{session_id, err_message{errcode, message}}
    """
    db = None
    err_message = {}
    try:
        for n in range(len(app.dbs)):
            if app.dbs[n][0] == database:
                db = app.dbs[n][1]
                break
        if db is None:
            raise vExept(600)
        else:
            try:
                session = await db.create_session(username=username, password=password)
                session_id = str(session.session_id)
                app.sessions.append([session_id, session, username, database, request.client.host])
            except vExept as e:
                Response.status_code = status.HTTP_401_UNAUTHORIZED
                session_id = None
                err_message = e
    except vExept as e:
        raise UnicornException(message=e.message, err_code=e.errcode, status_code = status.HTTP_503_SERVICE_UNAVAILABLE)
    return {"session_id": session_id, "err_message":err_message}

# --------------------------------------------------------------------
# DISCONNECT
# --------------------------------------------------------------------
@app.post("/disconnect/db", status_code=status.HTTP_200_OK)
def close_connection(session_id: str, request: Request) -> dict:
    """Close connection

    Args:
        session_id (str): Session ID
        request (Request): Information about client

    Raises:
        vExept: exception

    Returns:
        dict: dict{result, err_message{errcode, message}}
    """
    err_message = {}
    result = {}
    found_sess = False
    try:
        for n in range(len(app.sessions)):
            if str(app.sessions[n][0]) == str(session_id):
                found_sess = True
                break
        if not found_sess:
            raise vExept(1000, session_id)
        if app.sessions[n][4] != request.client.host:
            raise vExept(680, '{} / {}'.format(app.sessions[n][4], request.client.host))
        del app.sessions[n][1]
        del app.sessions[n]
    except vExept as e:
        raise UnicornException(message=e.message, err_code=e.errcode, status_code = status.HTTP_401_UNAUTHORIZED)
    return {"result":result, "err_message":err_message}


# --------------------------------------------------------------------
# QUERY
# --------------------------------------------------------------------
@app.post("/query", status_code=status.HTTP_200_OK)
async def post_query(query: str, session_id: str, request: Request) -> dict:
    """Submit SQL query

    Args:
        query (str): SQL query
        session_id (str): session ID
        request (Request): Informations about client

    Raises:
        vExept: vDB exception

    Returns:
        dict: dict{result, err_message{errcode, message}}
    """
    err_message = {}
    result = {}
    found_sess = False
    try:
        for n in range(len(app.sessions)):
            if str(app.sessions[n][0]) == str(session_id):
                found_sess = True
                break
        if not found_sess:
            raise vExept(1000, session_id)
        if app.sessions[n][4] != request.client.host:
            raise vExept(680, '{} / {}'.format(app.sessions[n][4], request.client.host))
        result = await app.sessions[n][1].submit_query(_query=query)
    except vExept as e:
        raise UnicornException(message=e.message, err_code=e.errcode, status_code = status.HTTP_400_BAD_REQUEST)
    return {"result":result, "err_message":err_message}


# --------------------------------------------------------------------
# CREATE DB
# --------------------------------------------------------------------
@app.post("/create", status_code=status.HTTP_201_CREATED)
async def create_db(mgrpassword: str, database: str, adminpwd: str) -> dict:
    """Create new DB

    Args:
        mgrpassword (str): Manager password
        database (str): Database name
        adminpwd (str): Password of database  ADMIN account

    Raises:
        vExept: vDB exception

    Returns:
        dict: dict{result, err_message{errcode, message}}
    """
    try:
        err_message = {}
        if mgrpassword != app.__meta_cfg["manager_password"]:
            raise vExept(660)
        if len(database.strip()) == 0:
            raise vExept(600)
        # remove space from database name
        dbn = ''
        for dbnmbr in database.strip().split(' '):
            if dbn == '':
                dbn = dbnmbr
            else:
                dbn += '_{}'.format(dbnmbr)
        # check database already exists
        for dbdesc in app.__meta_cfg["db_list"]:
            if dbdesc['name'] == dbn:
                raise vExept(601, dbn)
        
        db_base_dir = '{}/{}'.format(app.__meta_cfg["root_dir"], dbn)
        # create JSON DB on disk
        db = JSONtinyDB(None)
        db.create_db(_db_base_dir=db_base_dir, admin_password=adminpwd)
        del db
        # open DB
        db = vDB(db_base_dir)
        try:
            # append DB in catalog
            __id_db = open(app.parameters_file, mode='w')
            app.__meta_cfg["db_list"].append({"name": dbn, "base_dir": db_base_dir})
            json.dump(app.__meta_cfg, indent=4, fp=__id_db)
        finally:
            __id_db.close()
        app.dbs.append([dbn, db])
        result = 'Database {} successfuly created'.format(dbn)
    except vExept as e:
        raise UnicornException(message=e.message, err_code=e.errcode, status_code = status.HTTP_400_BAD_REQUEST)
    return {"result":result, "err_message":err_message}


# --------------------------------------------------------------------
# LIST TABLES
# --------------------------------------------------------------------
@app.get("/account/list_tables", status_code=status.HTTP_200_OK)
async def list_tables(session_id: str, request: Request):
    """List tables owned by user of session

    Args:
        session_id (str): Session ID
        request (Request): Information about client

    Raises:
        vExept: Vdb exception

    Returns:
        dict: dict{result, err_message{errcode, message}}
    """
    err_message = {}
    result = []
    found_sess = False
    try:
        for n in range(len(app.sessions)):
            if str(app.sessions[n][0]) == str(session_id):
                found_sess = True
                break
        if not found_sess:
            raise vExept(1000, session_id)
        if app.sessions[n][4] != request.client.host:
            raise vExept(680, '{} / {}'.format(app.sessions[n][4], request.client.host))
        result = await app.sessions[n][1].get_tables()
    except vExept as e:
        raise UnicornException(message=e.message, err_code=e.errcode, status_code = status.HTTP_400_BAD_REQUEST)
    return {"result":result, "err_message":err_message}


# --------------------------------------------------------------------
# LIST SESSIONS
# --------------------------------------------------------------------
@app.get("/admin/list_sessions", status_code=status.HTTP_200_OK)
async def list_sessions(mgrpassword: str):
    """List open sessions on databases

    Args:
        mgrpassword (str): Manager password

    Raises:
        vExept: Vdb exception

    Returns:
        dict: dict{result, err_message{errcode, message}}
    """
    result = {}
    err_message = {}
    try:
        if mgrpassword == app.__meta_cfg["manager_password"]:
            result["sessions"] = []
            for s in app.sessions:
                result["sessions"].append([s[0], s[2], s[3], s[4]])
        else:
            raise vExept(660)
    except vExept as e:
        raise UnicornException(message=e.message, err_code=e.errcode, status_code = status.HTTP_400_BAD_REQUEST)
    return {"result":result, "err_message":err_message}

# --------------------------------------------------------------------
# LIST DB
# --------------------------------------------------------------------
@app.get("/admin/list_databases", status_code=status.HTTP_200_OK)
async def list_dbs(mgrpassword: str):
    """List databases managed by server

    Args:
        mgrpassword (str): Manager password_

    Raises:
        vExept: vDB exception

    Returns:
        dict: dict{result, err_message{errcode, message}}
    """
    result = {}
    err_message = {}
    try:
        if mgrpassword == app.__meta_cfg["manager_password"]:
            result["databases"] = []
            for s in app.dbs:
                result["databases"].append([s[0]])
        else:
            raise vExept(660)
    except vExept as e:
        raise UnicornException(message=e.message, err_code=e.errcode, status_code = status.HTTP_400_BAD_REQUEST)
    return {"result":result, "err_message":err_message}
