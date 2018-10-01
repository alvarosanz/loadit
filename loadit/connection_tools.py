import os
from io import BytesIO
import socket
import numpy as np
from loadit.misc import humansize
from loadit.read_results import tables_in_pch, ResultsTable
import logging


log = logging.getLogger()


def send_tables(connection, files, tables_specs):
    ignored_tables = set()

    for i, file in enumerate(files):
        log.info(f"Transferring file {i + 1} of {len(files)} ({humansize(os.path.getsize(file))}): '{os.path.basename(file)}'...")

        for table in tables_in_pch(file, tables_specs):

            if table.name not in tables_specs:

                if table.name not in ignored_tables:
                    log.warning("WARNING: '{}' is not supported!".format(table.name))
                    ignored_tables.add(table.name)

                continue

            f = BytesIO()
            np.save(f, table.data)
            table.data = None
            connection.send(table.__dict__)
            connection.send(f.getbuffer(), 'file')

    connection.send(b'END')


def recv_tables(connection):

    while True:
        data = connection.recv()

        if data == b'END':
            break

        table = ResultsTable(**data)
        table.data = np.load(connection.recv())
        yield table


def get_ip():
    """
    Get ip address of localhost.
    """
    s = socket.socket(type=socket.SOCK_DGRAM)
    try:
        # doesn't even have to be reachable
        s.connect(('10.255.255.255', 1))
        return s.getsockname()[0]
    except:
        return '127.0.0.1'
    finally:
        s.close()


def find_free_port():
    """
    Get an available TCP port.
    """
    s = socket.socket()
    s.bind(('localhost', 0))
    port = s.getsockname()[1]
    s.close()
    return port
