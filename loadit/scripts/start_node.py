import os
import argparse
from multiprocessing import freeze_support


if __name__ == '__main__':
    freeze_support()
    parser = argparse.ArgumentParser(description='Start node server', epilog='You will be asked to login as an admin')
    parser.add_argument('server_address',
                        help='Server address of the central server (i.e. 192.168.0.154:8080)')
    parser.add_argument('--path', dest='root_path', metavar='ROOT_PATH', default=os.getcwd(),
                        help='folder containing the databases (by default is the current working directory)')
    parser.add_argument('--backup', dest='backup', action='store_const',
                        const=True, default=False,
                        help='activate backup mode. In backup mode, the node will perform a backup of all the databases present at the central server')
    parser.add_argument('--debug', dest='debug', action='store_const',
                        const=True, default=False,
                        help='activate debug mode')
    args = parser.parse_args()

    import loadit
    host, port = args.server_address.split(':')
    loadit.start_node((host, int(port)), args.root_path, args.backup, args.debug)
else:
    import loadit
