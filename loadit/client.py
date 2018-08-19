import os
import getpass
import json
import jwt
import time
import pyarrow as pa
from loadit.database import DatabaseHeader, Database, create_database, parse_query
from loadit.connection import Connection, get_private_key
from loadit.misc import get_hash, humansize
import logging


log = logging.getLogger()


class BaseClient(object):
    """
    Handles a remote connection.
    """

    def authenticate(self, connection, user=None, password=None):
        """
        Authenticate with the server (using a JSON Web Token).

        Parameters
        ----------
        connection : Connection
            Server connection instance.
        """

        if self._authentication:
            connection.send_secret(json.dumps({'authentication': self._authentication.decode()}).encode())
        else:
            from loadit.__init__ import __version__

            if not user:
                user = input('user: ')

            if not password:
                password = getpass.getpass('password: ')

            connection.send_secret(json.dumps({'user': user,
                                               'password': password,
                                               'request': 'authentication',
                                               'version': __version__}).encode())
            self._authentication = connection.recv_secret()

        connection.recv()

    def _request(self, is_redirected=False, **kwargs):
        """
        Request something to the server.
        """
        connection = Connection(self.server_address, private_key=self._private_key)

        try:
            # Authentication
            self.authenticate(connection, user=kwargs.pop('user', None), password=kwargs.pop('password', None))

            # Sending request
            if is_redirected:
                connection.send(msg={key: kwargs[key] for key in ('request_type', 'path')})
            else:
                connection.send(msg=kwargs)

            data = connection.recv()

            # Redirecting request (if necessary)
            if 'redirection_address' in data:

                for key in data:

                    if key != 'redirection_address':
                        kwargs[key] = data[key]

                connection.kill()
                connection.connect(tuple(data['redirection_address']))
                self.authenticate(connection)
                connection.send(msg=kwargs)
                data = connection.recv()

            if 'msg' in data and data['msg']:
                log.info(data['msg'])

            # Processing request
            if kwargs['request_type'] == 'sync_databases':

                while data['msg'] != 'Done!':
                    data = connection.recv()
                    log.info(data['msg'])

            elif kwargs['request_type'] == 'new_batch':
                connection.send_tables(kwargs['files'], data)
                data = connection.recv()
            elif kwargs['request_type'] == 'query':
                reader = pa.RecordBatchStreamReader(pa.BufferReader(connection.recv().getbuffer()))
                log.info('Done!')
                data['batch'] = reader.read_next_batch()
            elif kwargs['request_type'] == 'add_attachment':
                log.info(f"Transferring '{os.path.basename(kwargs['file'])}' ({humansize(os.path.getsize(kwargs['file']))})...")
                connection.send_file(kwargs['file'])
                data = connection.recv()
            elif kwargs['request_type'] == 'download_attachment':
                connection.recv_file(os.path.join(kwargs['output_path'], kwargs['name']))
                data = connection.recv()

            return data
        finally:
            connection.kill()


class DatabaseClient(BaseClient):
    """
    Handles a remote database.
    """

    def __init__(self, server_address, path, private_key, authentication, header=None):
        """
        Initialize a DatabaseClient instance.

        Parameters
        ----------
        server_address : (str, int)
            Server address (i.e. ('192.168.0.9', 8080)).
        path : str
            Database remote path.
        private_key : cryptography.hazmat.backends.openssl.ec._EllipticCurvePrivateKey
            Private key.
        authentication : bytes
            Server authentication (JSON Web Token).
        header : dict, optional
            Database header.
        """
        self.server_address = server_address
        self.path = path
        self._private_key = private_key
        self._authentication = authentication

        if header:
            self.header = DatabaseHeader(header=header)
        else:
            self._request(request_type='header')

    @property
    def read_only(self):
        return not jwt.decode(self._authentication, verify=False)['is_admin']

    def check(self):
        """
        Check database integrity.

        Returns
        -------
        list of str
            List of corrupted files.
        """
        return self._request(request_type='check')['corrupted_files']

    def add_attachment(self, file):
        """
        Add a new attachment (consisting on one or more files) to the database.

        Parameters
        ----------
        file : str
            Attachment file path.
        """
        self._request(request_type='add_attachment', file=file)

    def remove_attachment(self, name):
        """
        Remove an attachment.

        Parameters
        ----------
        name : str
            Attachment name.
        """
        self._request(request_type='remove_attachment', name=name)

    def download_attachment(self, name, path):
        """
        Download an attachment.

        Parameters
        ----------
        name : str
            Attachment name.
        path : str
            Output path.
        """
        self._request(request_type='download_attachment', name=name, output_path=path)

    def new_batch(self, files, batch_name, comment=''):
        """
        Append new batch to database. This operation is reversible.

        Parameters
        ----------
        files : list of str
            List of .pch files.
        batch_name : str
            Batch name.
        comment : str
            Batch comment.
        """
        self._request(request_type='new_batch', files=files, batch=batch_name, comment=comment)

    def restore(self, batch_name):
        """
        Restore database to a previous batch. This operation is not reversible.

        Parameters
        ----------
        batch_name : str
            Batch name.
        """
        restore_points = [batch[0] for batch in self.header.batches]

        if batch_name not in restore_points or batch_name == restore_points[-1]:
            raise ValueError(f"'{batch_name}' is not a valid restore point")

        self._request(request_type='restore_database', batch=batch_name)

    def query_from_file(self, file, double_precision=False):
        """
        Perform a query from a file.

        Parameters
        ----------
        file : str
            Query file.
        double_precision : bool, optional
            Whether to use single or double precision. By default single precision is used.

        Returns
        -------
        pyarrow.RecordBatch
            Data queried.
        """

        with open(file) as f:
            return self.query(**parse_query(json.load(f), True), double_precision=double_precision)

    def query(self, table=None, fields=None, LIDs=None, IDs=None, groups=None,
              geometry=None, sort_by_LID=True, double_precision=False, **kwargs):
        """
        Perform a query.

        Parameters
        ----------
        double_precision : bool, optional
            Whether to use single or double precision. By default single precision is used.

        Returns
        -------
        pyarrow.RecordBatch
            Data queried.
        """
        return self._request(request_type='query', table=table, fields=fields,
                             LIDs=LIDs, IDs=IDs, groups=groups,
                             geometry=geometry, sort_by_LID=sort_by_LID,
                             double_precision=double_precision)['batch']

    def _request(self, **kwargs):
        """
        Request something to the server.
        """
        kwargs['path'] = self.path
        data = super()._request(is_redirected=True, **kwargs)

        if data['header']:
            self.header = DatabaseHeader(header=data['header'])

        return data


class Client(BaseClient):
    """
    Handles a remote connection.
    """

    def __init__(self):
        self.server_address = None
        self._private_key = get_private_key()
        self._authentication = None

    def connect(self, server_address, user=None, password=None):
        host, port = server_address.split(':')
        self.server_address = (host, int(port))
        self._request(request_type='authentication', user=user, password=password)

    @property
    def session(self):
        return jwt.decode(self._authentication, verify=False)

    def info(self):

        if self._authentication:
            print(self._request(request_type='cluster_info').getvalue().decode())
        else:
            print('Not connected!')

    def load_database(self, database):
        return Database(database)

    def load_remote_database(self, database):
        return DatabaseClient(self.server_address, database, self._private_key, self._authentication)

    def create_database(self, database):
        return create_database(database)

    def create_remote_database(self, database):
        data = self._request(is_redirected=True, request_type='create_database', path=database)
        return DatabaseClient(self.server_address, database,
                              self._private_key, self._authentication, data['header'])

    def remove_remote_database(self, database):
        self._request(request_type='remove_database', path=database)

    @property
    def remote_databases(self):
        return list(self._request(request_type='list_databases'))

    @property
    def sessions(self):
        return self._request(request_type='list_sessions')['sessions']

    def add_session(self, user, password, is_admin=False, create_allowed=False, databases=None):
        self._request(request_type='add_session', session_hash=get_hash(f'{user}:{password}'),
                      user=user, is_admin=is_admin, create_allowed=create_allowed, databases=databases)

    def remove_session(self, user):
        self._request(request_type='remove_session', user=user)

    def sync_databases(self, nodes=None, databases=None):
        self._request(request_type='sync_databases', nodes=nodes, databases=databases)

    def shutdown(self, node=None):
        self._request(request_type='shutdown', node=node)
