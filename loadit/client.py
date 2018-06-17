import getpass
import json
import jwt
import time
import pyarrow as pa
from loadit.database import DatabaseHeader, parse_query_file, check_aggregation_options, is_abs
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
            from loadit.__init__ import __version__
            connection.send_secret(json.dumps({'user': input('user: '),
                                               'password': getpass.getpass('password: '),
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
            self.authenticate(connection)

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
            self._set_assertions()
        else:
            self._request(request_type='header')

    def _set_assertions(self):
        self._assertions = {name: {'fields': {field for field, _ in table['columns'][2:]},
                                   'query_functions': set(table['query_functions']),
                                   'query_geometry': set(table['query_geometry']),
                                   'LIDs': set(table['LIDs']),
                                   'IDs': set(table['IDs'])} for name, table in
                            self.header.tables.items()}

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
        self._check_query(table, fields, LIDs, IDs, groups, geometry, weights)
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

    def _check_query(self, table, fields, LIDs, IDs, groups, geometry, weights):

        # table checking
        if table not in self._assertions:
            raise ValueError(f'Invalid table: {table}')

        # fields checking
        if fields:
            check_aggregation_options(fields, groups)
            basic_fields = {is_abs(field.split('-')[0])[0] for field in fields}
            invalid_fields = [field for field in basic_fields if
                              field not in self._assertions[table]['fields'] and
                              field not in self._assertions[table]['query_functions']]

            if invalid_fields:
                raise ValueError('Invalid field/s: {}'.format(', '.join(invalid_fields)))

        # LIDs checking
        if isinstance(LIDs, dict):
            new_LIDs = set()

            for new_LID, seq in LIDs.items():

                if seq:

                    if new_LID in self._assertions[table]['LIDs']:
                        raise ValueError(f'Combined LID already exists: {new_LID}')

                    new_LIDs.add(new_LID)

                    for coeff in seq[::2]:

                        if not type(coeff) is float:
                            raise TypeError(f'Coefficient must be a float: {LIDs[new_LID]}')

                    for LID in seq[1::2]:

                        if LID not in self._assertions[table]['LIDs'] and LID not in new_LIDs:
                            raise ValueError(f'Missing LID: {LID}')

                elif new_LID not in self._assertions[table]['LIDs']:
                    raise ValueError(f'Missing LID: {new_LID}')

        elif LIDs:
            missing_LIDs = {str(LID) for LID in LIDs if LID not in self._assertions[table]['LIDs']}

            if missing_LIDs:
                raise ValueError('Missing {}/s: {}'.format(self.header.tables[table]['columns'][0][0],
                                                          ', '.join(missing_LIDs)))

        # IDs and groups checking
        if groups:
            empty_groups = {group for group in groups if not groups[group]}

            if empty_groups:
                raise ValueError('Empty group/s: {}'.format(', '.join(empty_groups)))

            IDs2read = {ID for IDs in groups.values() for ID in IDs}
        else:
            IDs2read = IDs

        if IDs2read:
            missing_IDs = {str(ID) for ID in IDs2read if ID not in self._assertions[table]['IDs']}

            if missing_IDs:
                raise ValueError('Missing {}/s: {}'.format(self.header.tables[table]['columns'][1][0],
                                                          ', '.join(missing_IDs)))
        else:
            IDs2read = self._assertions[table]['IDs']

        # geometry checking
        if geometry:

            for geom_param in geometry:

                if geom_param not in self._assertions[table]['query_geometry']:
                    raise ValueError(f"Invalid geometric parameter: '{geom_param}'")

                missing_IDs = {str(ID) for ID in IDs2read if ID not in geometry[geom_param]}

                if missing_IDs:
                    raise ValueError("Missing {}/s in geometry inputs ('{}'): {}".format(self.header.tables[table]['columns'][1][0],
                                                                                         geom_param, ', '.join(missing_IDs)))

                for ID, value in geometry[geom_param].items():

                    if not type(value) is float:
                        raise TypeError('Geometry value must be a float!')

        # weights checking
        if weights:
            missing_IDs = {str(ID) for ID in IDs2read if ID not in weights}

            if missing_IDs:
                raise ValueError('Missing {}/s in weights inputs: {}'.format(self.header.tables[table]['columns'][1][0],
                                                                            ', '.join(missing_IDs)))

            for ID, value in weights.items():

                if not type(value) is float:
                    raise TypeError('weight value must be a float!')

    def _request(self, **kwargs):
        """
        Request something to the server.
        """
        kwargs['path'] = self.path
        data = super()._request(is_redirected=True, **kwargs)

        if data['header']:
            self.header = DatabaseHeader(header=data['header'])
            self._set_assertions()

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
        data = self._request(is_redirected=True, request_type='create_database', files=files, path=database)
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
