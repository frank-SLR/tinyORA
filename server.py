# python server.py -s d:/Python/OpenSSL -p 1521 -l warning -d D:/Python/data/pfile.json
from vExceptLib import vExept
import argparse
import uvicorn
import os
from shutil import copyfile

if __name__ == "__main__":
    try:
        parser = argparse.ArgumentParser()
        parser.add_argument('-a', '--address', type=str, default='127.0.0.1', help="Listener address")
        parser.add_argument('-p', '--port', type=int, default=1521, help="Listener port")
        parser.add_argument('-s', '--ssl-path', type=str, default=None, help="Path for SSL files")
        parser.add_argument('-d', '--db-parameters-file', type=str, default=None, help="Parameters file for TinyDB")
        parser.add_argument('-l', '--log-level', type=str, default="warning",
                            choices=['critical', 'error', 'warning', 'info', 'debug', 'trace'],
                            help="Log level for server")
        args = parser.parse_args()
        if args.db_parameters_file is None:
            raise vExept(603)
        else:
            if os.path.isfile(args.db_parameters_file):
                copyfile(args.db_parameters_file, "./parameters.json")
            else:
                raise vExept(602, args.db_parameters_file)
        if args.ssl_path is None:
            uvicorn.run("tinyDB:app",
                        host=args.address,
                        port=args.port,
                        # workers=8,
                        reload=True,
                        log_level=args.log_level)
        else:
            uvicorn.run("tinyDB:app",
                        host=args.address,
                        port=args.port,
                        # workers=8,
                        reload=True,
                        ssl_keyfile="{}/key.pem".format(args.ssl_path),
                        ssl_certfile="{}/cert.pem".format(args.ssl_path),
                        log_level=args.log_level)
    except vExept as e:
        print("error code: {}".format(e.errcode))
        for s in e.message.split("\n"):
            print("  {}".format(s))
    finally:
        if os.path.isfile("./parameters.json"):
            copyfile("./parameters.json", args.db_parameters_file)
            os.remove("./parameters.json")
