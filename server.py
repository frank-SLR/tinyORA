# python server.py -s d:/Python/OpenSSL -p 1521 -l warning
import argparse
import uvicorn

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('-a', '--address', type=str, default='127.0.0.1', help="Listener address")
    parser.add_argument('-p', '--port', type=int, default=1521, help="Listener port")
    parser.add_argument('-s', '--ssl-path', type=str, default=None, help="Path for SSL files")
    parser.add_argument('-l', '--log-level', type=str, default="warning",
                        choices=['critical', 'error', 'warning', 'info', 'debug', 'trace'],
                        help="Log level for server")
    args = parser.parse_args()
    # try:
    if args.ssl_path is None:
        uvicorn.run("tinyDB:app",
                    host=args.address,
                    port=args.port,
                    reload=True,
                    log_level=args.log_level)
    else:
        uvicorn.run("tinyDB:app",
                    host=args.address,
                    port=args.port,
                    reload=True,
                    ssl_keyfile="{}/key.pem".format(args.ssl_path),
                    ssl_certfile="{}/cert.pem".format(args.ssl_path),
                    log_level=args.log_level)
    # except Exception as e:
    #     print('Internal error')
    #     print(e)
    # except UnboundLocalError as e:
    #     print('Internal error')
    #     print(e)
