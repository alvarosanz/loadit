import os
import time
import argparse


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Perform a query', epilog='You will be asked to login')
    parser.add_argument('query_files', nargs='+',
                        help='JSON formatted file/s holding the query parameters')
    parser.add_argument('server_address',
                        default=None,
                        help='Server address of the central server (i.e. 192.168.0.154:8080)')

    args = parser.parse_args()

    import loadit
    client = loadit.Client()

    if args.server_address:
        client.connect(args.server_address)

    start_time = time.time()

    for i, file in enumerate(args.query_files):

        if args.server_address:
            database = client.load_remote_database(query['path'])
        else:
            database = client.load_database(query['path'])

        print(f"Performing query '{os.path.basename(file)}' ({i + 1} of {len(args.query_files)})...")
        database.query_from_file(file)

    print(f"{time.time() - start_time} seconds")
else:
    import loadit
