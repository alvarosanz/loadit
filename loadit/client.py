import getpass
import json
import jwt
import time
import pyarrow as pa
from loadit.database import DatabaseHeader, parse_query_file
from loadit.connection import Connection, get_private_key
from loadit.misc import get_hash


class BaseClient(object):
    """
    Handles a remote connection.
    """

    def authenticate(self, connection):
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
            connection.send_secret(json.dumps({'user': input('user: '),
                                               'password': getpass.getpass('password: '),
                                               'request': 'authentication'}).encode())
            self._authentication = connection.recv_secret()

        connection.recv()

    def _request(self, **kwargs):
        """
        Request something to the server.
        """
        connection = Connection(self.server_address, private_key=self._private_key)

        try:
            # Authentication
            self.authenticate(connection)

            # Sending request
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

            # Processing request
            if kwargs['request_type'] == 'sync_databases':

                while data['msg'] != 'Done!':
                    data = connection.recv()
                    print(data['msg'])

            elif kwargs['request_type'] in ('create_database', 'append_to_database'):
                connection.send_tables(kwargs['files'], data)
                print(f"Assembling database ...")
                data = connection.recv()
            elif kwargs['request_type'] == 'query':
                print('Done!')
                print(data['msg'], end=' ')
                reader = pa.RecordBatchStreamReader(pa.BufferReader(connection.recv().getbuffer()))
                print('Done!')
                data['batch'] = reader.read_next_batch()

        finally:
            connection.kill()

        return data


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

    def check(self):
        """
        Check database integrity.
        """
        print(self._request(request_type='check')['msg'])

    def append(self, files, batch_name):
        """
        Append new results to database. This operation is reversible.

        Parameters
        ----------
        files : list of str
            List of .pch files.
        batch_name : str
            Batch name.
        """

        if isinstance(files, str):
            files = [files]

        print(self._request(request_type='append_to_database', files=files, batch=batch_name)['msg'])

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

        print(self._request(request_type='restore_database', batch=batch_name)['msg'])

    def query_from_file(self, file, double_precision=False, return_dataframe=True):
        """
        Perform a query from a file.

        Parameters
        ----------
        file : str
            Query file.
        double_precision : bool, optional
            Whether to use single or double precision. By default single precision is used.
        return_dataframe : bool, optional
            Whether to return a pandas dataframe or a pyarrow RecordBatch.

        Returns
        -------
        pandas.DataFrame or pyarrow.RecordBatch
            Data queried.
        """
        query = parse_query_file(file)
        output_file = query.pop('output_file')
        return self.query(**query, output_file=output_file,
                          double_precision=double_precision, return_dataframe=return_dataframe)

    def query(self, table=None, fields=None, LIDs=None, IDs=None, groups=None,
              geometry=None, weights=None, output_file=None,
              double_precision=False, return_dataframe=True, **kwargs):
        """
        Perform a query.

        Parameters
        ----------
        double_precision : bool, optional
            Whether to use single or double precision. By default single precision is used.
        return_dataframe : bool, optional
            Whether to return a pandas dataframe or a pyarrow RecordBatch.

        Returns
        -------
        pandas.DataFrame or pyarrow.RecordBatch
            Data queried.
        """
        start = time.time()
        print('Processing query ...', end=' ')
        batch = self._request(request_type='query', table=table, fields=fields,
                              LIDs=LIDs, IDs=IDs, groups=groups,
                              geometry=geometry, weights=weights,
                              double_precision=double_precision)['batch']

        if return_dataframe:
            df = batch.to_pandas()
            df.set_index(json.loads(batch.schema.metadata[b'index_columns'].decode()), inplace=True)
        else:
            return batch

        if output_file:
            print(f"Writing '{output_file}' ...", end=' ')
            df.to_csv(output_file)
            print('Done!')

        print('{:.1f} seconds'.format(time.time() - start))
        return df

    def _request(self, **kwargs):
        """
        Request something to the server.
        """
        kwargs['path'] = self.path
        data = super()._request(**kwargs)
        self.header = DatabaseHeader(header=data['header'])
        return data


class Client(BaseClient):
    """
    Handles a remote connection.
    """

    def __init__(self, server_address):
        self.server_address = server_address
        self.database = None
        self._private_key = get_private_key()
        self._authentication = None
        self._request(request_type='authentication')
        print('Login successful!')

    @property
    def session(self):
        return jwt.decode(self._authentication, verify=False)

    def info(self):
        print(self._request(request_type='cluster_info')['msg'])

    def load(self, database):
        self.database = DatabaseClient(self.server_address, database,
                                       self._private_key, self._authentication)

    def create_database(self, files, database):

        if isinstance(files, str):
            files = [files]

        data = self._request(request_type='create_database', files=files, path=database)
        print(data['msg'])
        self.database = DatabaseClient(self.server_address, database,
                                       self._private_key, self._authentication, data['header'])

    def remove_database(self, database):
        print(self._request(request_type='remove_database', path=database)['msg'])

    @property
    def databases(self):
        return list(self._request(request_type='list_databases'))

    @property
    def sessions(self):
        return self._request(request_type='list_sessions')['sessions']

    def add_session(self, user, password, is_admin=False, create_allowed=False, databases=None):
        print(self._request(request_type='add_session', session_hash=get_hash(f'{user}:{password}'),
                            user=user, is_admin=is_admin, create_allowed=create_allowed, databases=databases)['msg'])

    def remove_session(self, user):
        print(self._request(request_type='remove_session', user=user)['msg'])

    def sync_databases(self, nodes=None, databases=None):
        self._request(request_type='sync_databases', nodes=nodes, databases=databases)

    def shutdown(self, node=None):
        print(self._request(request_type='shutdown', node=node)['msg'])
