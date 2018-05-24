import os
import time
import argparse


parser = argparse.ArgumentParser(description='Perform a query', epilog='You will be asked to login')
parser.add_argument('query_files', nargs='+',
                    help='JSON formatted file/s holding the query parameters')

args = parser.parse_args()


import loadit


client = None

for i, file in enumerate(args.query_files):
    query = loadit.parse_query_file(file)

    if not client:
        client = loadit.Client((query['host'], query['port']))
        client.load(query['path'])
        start_time = time.time()

    print(f"Performing query '{os.path.basename(file)}' ({i + 1} of {len(args.query_files)}) ...")

    if not query['output_file']:
        print('WARNING: No output_file specified!')
        continue

    client.database.query(**query)

print(f"{time.time() - start_time} seconds")
