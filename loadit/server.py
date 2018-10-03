import jwt
import getpass
import os
import time
import shutil
from pathlib import Path
import json
import logging
import traceback
import socketserver
import ssl
import secrets
import threading
import pyarrow as pa
from multiprocessing import Process, cpu_count, Event, Manager, Lock
from loadit.resource_lock import ResourceLock
from loadit.database import Database, create_database, parse_query
from loadit.sessions import Sessions
from loadit.connection import Connection
from loadit.connection_tools import recv_tables, get_ip, find_free_port
from loadit.misc import humansize, get_hasher, hash_bytestr
import loadit.log as log


SERVER_PORT = 8080


class CentralQueryHandler(socketserver.BaseRequestHandler):

    def handle(self):
        connection = self.server.connection
        query = connection.recv()
        self.server.authorize(query)
        request_type = query['request_type']
        log_request = True

        # WORKER REQUESTS
        if request_type == 'add_worker':
            self.server.add_worker(tuple(query['worker_address']), query['databases'], query['backup'])
        elif request_type == 'remove_worker':
            self.server.remove_worker(tuple(query['worker_address']))
        elif request_type == 'acquire_worker':
            connection.send({'worker_address': self.server.acquire_worker(node=query['node'])})
        elif request_type == 'release_worker':

            if 'databases' in query:
                self.server.nodes[query['worker_address'][0]].databases = query['databases']

                if query['worker_address'][0] == self.server.server_address[0]:
                    self.server.databases = query['databases']

            self.server.release_worker(tuple(query['worker_address']))
        elif request_type == 'list_databases':
            connection.send(self.server.databases)

        # CLIENT REQUESTS
        elif request_type == 'authentication':
            connection.send({'msg': 'Logged in'})
        elif request_type == 'shutdown':

            if query['node']:
                self.server.shutdown_node(query['node'])
                connection.send({'msg': 'Node shutdown'})
            else:
                threading.Thread(target=self.server.shutdown).start()
                connection.send({'msg': 'Cluster shutdown'})

        elif request_type == 'cluster_info':
            connection.send(self.server.info().encode())
        elif request_type == 'add_session':
            self.server.sessions.add_session(query['user'], session_hash=query['session_hash'],
                                             is_admin=query['is_admin'],
                                             create_allowed=query['create_allowed'],
                                             databases=query['databases'])
            connection.send({'msg': "User '{}' added".format(query['user'])})
        elif request_type == 'remove_session':
            self.server.sessions.remove_session(query['user'])
            connection.send({'msg': "User '{}' removed".format(query['user'])})
        elif request_type == 'list_sessions':
            connection.send({'sessions': list(self.server.sessions.sessions.values())})
        elif request_type == 'sync_databases':
            self.server.sync_databases(query['nodes'], query['databases'], connection)
        else: # REDIRECTED REQUESTS
            log_request = False

            if request_type != 'create_database' and query['path'] not in self.server.databases:
                raise ValueError("Database '{}' not available!".format(query['path']))

            if  request_type in ('create_database', 'new_batch',
                                 'restore_database', 'remove_database',
                                 'add_attachment', 'remove_attachment'):
                node = self.server.server_address[0]
            else:
                node = None

            connection.send({'redirection_address': self.server.acquire_worker(node=node, database=query['path'])})

        if log_request:
            log_msg = "ip: {}, user: {}, request: {}, database: {}, in: {}, out: {}"

            if request_type == 'release_worker':

                if not query['is_error']:
                    self.server.log.info(log_msg.format(query['client_address'],
                                                        query['user'],
                                                        query['request'],
                                                        query['database'],
                                                        humansize(query['nbytes_in']),
                                                        humansize(query['nbytes_out'])))
            else:
                self.server.log.info(log_msg.format(self.request.getpeername()[0],
                                                    self.server.current_session['user'],
                                                    request_type,
                                                    None,
                                                    humansize(connection.nbytes_in),
                                                    humansize(connection.nbytes_out)))


class WorkerQueryHandler(socketserver.BaseRequestHandler):

    def handle(self):
        connection = self.server.connection
        self.server.log_handler = log.ConnectionHandler(connection)
        self.server.log_handler.setLevel(logging.INFO)
        self.server.log_handler.setFormatter(logging.Formatter('%(message)s'))
        self.server.log.addHandler(self.server.log_handler)
        query = connection.recv()
        self.server.authorize(query)
        request_type = query['request_type']

        if request_type == 'shutdown':
            self.server._shutdown_request = True
            threading.Thread(target=self.server.shutdown).start()
        elif request_type == 'list_databases':
            connection.send(self.server.databases._getvalue())
        elif request_type == 'sync_databases':
            self.server.sync_databases(query['nodes'], query['databases'], connection)
        elif request_type == 'recv_databases':
            self.server.recv_databases(connection)
        elif request_type == 'remove_database':
            self.server.current_database = query['path']

            with self.server.database_lock.acquire(query['path']):
                shutil.rmtree(os.path.join(self.server.root_path, query['path']))

            connection.send({'msg': "Database '{}' removed".format(query['path'])})
            del self.server.databases[query['path']]
        else:
            self.server.current_database = query['path']

            with self.server.database_lock.acquire(query['path'],
                                                   block=(request_type in ('create_database',
                                                                           'new_batch',
                                                                           'restore_database',
                                                                           'add_attachment',
                                                                           'remove_attachment'))):
                path = os.path.join(self.server.root_path, query['path'])

                if request_type == 'create_database':

                    if query['path'] in self.server.databases.keys():
                        raise FileExistsError(f"Database already exists at '{query['path']}'!")

                    db = create_database(path)
                else:
                    db = Database(path)

                if request_type == 'check':
                    connection.send({'corrupted_files': db.check(), 'header': None})
                    return
                elif request_type == 'query':
                    batch = db.query(**parse_query(query))
                elif request_type == 'new_batch':
                    connection.send(db._get_tables_specs())
                    db.new_batch(query['files'], query['batch'], query['comment'], table_generator=recv_tables(connection))
                elif request_type == 'restore_database':
                    db.restore(query['batch'])
                elif request_type == 'add_attachment':

                    if query['file'] in db.header.attachments:
                        raise FileExistsError(f"Already existing attachment!")

                    attachment_file = os.path.join(path, '.attachments', os.path.basename(query['file']))
                    connection.send(b'proceed')
                    connection.recv_file(attachment_file)
                    db.add_attachment(attachment_file, copy=False)
                elif request_type == 'remove_attachment':
                    db.remove_attachment(query['name'])
                elif request_type == 'download_attachment':

                    if query['name'] not in db.header.attachments:
                        raise FileNotFoundError(f"Attachment not found!")

                    attachment_file = os.path.join(path, '.attachments', query['name'])
                    connection.send({'msg': f"Downloading '{query['name']}' ({humansize(os.path.getsize(attachment_file))})..."})
                    connection.send_file(attachment_file)
                    self.server.log.info(f"Attachment '{query['name']}' downloaded")

                if self.server.current_session['database_modified']:
                    self.server.databases[query['path']] = get_database_hash(os.path.join(path, '##header.json'))

                if request_type in ('header', 'create_database',
                                    'new_batch', 'restore_database',
                                    'add_attachment', 'remove_attachment'):
                    header = db.header.__dict__
                else:
                    header = None

                db = None

            try:
                batch_message = get_batch_message(batch)
                connection.send({'msg': f"Transferring query results ({humansize(len(batch_message))})...", 'header': header})
                connection.send(batch_message, 'buffer')
            except NameError:
                connection.send({'header': header})


class DatabaseServer(socketserver.TCPServer):
    allow_reuse_address = True
    request_queue_size = 5

    def __init__(self, server_address, query_handler, root_path, certfile, debug=False):
        self.context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
        self.context.load_cert_chain(certfile)
        super().__init__(server_address, query_handler)
        self.root_path = root_path
        self.databases = None
        self.master_key = None
        self.current_session = None
        self._debug = debug
        self._done = Event()

    def wait(self):
        self._done.wait()
        self._done.clear()

    def server_activate(self):
        super().server_activate()
        self.socket = self.context.wrap_socket(self.socket, server_side=True)

    def serve_forever(self, *args, **kwargs):

        try:
            super().serve_forever(*args, **kwargs)
        finally:
            self.master_key = None

    def handle_error(self, request, client_address):
        self.current_session['is_error'] = True

        try:
            self.connection.send(traceback.format_exc(), 'exception')
        except BrokenPipeError:
            pass

    def refresh_databases(self):
        self.databases = get_local_databases(self.root_path)

    def verify_request(self, request, client_address):
        error_msg = 'Access denied!'

        try:
            self.connection = Connection(socket=request)
            data = self.connection.recv()

            if type(data) is dict:

                if 'password' in data: # User login

                    try:
                        self.current_session = self.sessions.get_session(data['user'], data['password'])
                    except KeyError:
                        error_msg = 'Wrong username or password!'
                        raise PermissionError()

                    from loadit.__init__ import __version__

                    if data['version'].split('.')[-1] != __version__.split('.')[-1]:
                        error_msg = f"Not supported version! Update to version: '{__version__}'"
                        raise PermissionError()

                    if data['request'] == 'master_key':

                        if not self.current_session['is_admin']:
                            error_msg = 'Not enough privileges!'
                            raise PermissionError()

                        self.connection.send(self.master_key)
                    else:
                        authentication = jwt.encode(self.current_session, self.master_key)
                        self.connection.send(authentication)

                else:
                    raise PermissionError()

            elif data == self.master_key: # master key
                self.current_session = {'is_admin': True}
            else: # JSON Web Token

                try:
                    self.current_session = jwt.decode(data, self.master_key)
                except Exception:
                    error_msg = 'Invalid token!'
                    raise PermissionError()

            return True

        except PermissionError:
            self.connection.send(error_msg, 'exception')
            return False
        except Exception:
            self.handle_error(request, client_address)
            return False

    def authorize(self, query):
        self.current_session['request_type'] = query['request_type']
        self.current_session['is_error'] = False

        if not self.current_session['is_admin']:

            if (query['request_type'] in ('shutdown', 'add_worker', 'remove_worker',
                                          'release_worker', 'acquire_worker',
                                          'sync_databases', 'recv_databases',
                                          'add_session', 'remove_session', 'list_sessions') or
                query['request_type'] == 'create_database' and not self.current_session['create_allowed'] or
                query['request_type'] in ('new_batch', 'restore_database', 'remove_database',
                                          'add_attachment', 'remove_attachment') and
                (not self.current_session['databases'] or query('path') not in self.current_session['databases'])):
                raise PermissionError('Not enough privileges!')

        if query['request_type'] in ('recv_databases', 'new_batch', 'restore_database',
                                     'create_database', 'remove_database',
                                     'add_attachment', 'remove_attachment'):
            self.current_session['database_modified'] = True
        else:
            self.current_session['database_modified'] = False
            
    def send(self, server_address, msg, recv=False):

        try:
            connection = Connection(server_address)
            connection.send(self.master_key)
            connection.recv()
            connection.send(msg)

            if recv:
                return connection.recv()

        finally:
            connection.kill()

    def request(self, server_address, msg):
        return self.send(server_address, msg, recv=True)


class CentralServer(DatabaseServer):

    def __init__(self, root_path, certfile, debug=False):
        super().__init__((get_ip(), SERVER_PORT), CentralQueryHandler, root_path, certfile, debug)
        self.certfile = certfile
        self.log = logging.getLogger('central_server')
        self.refresh_databases()
        self.sessions = None
        self.nodes = dict()

    def start(self, sessions_file=None):

        if sessions_file:
            password = getpass.getpass('password: ')
            self.sessions = Sessions(sessions_file)
        else:
            sessions_file = os.path.join(self.root_path, 'sessions.json')

            if os.path.exists(sessions_file):
                password = getpass.getpass('password: ')
                self.sessions = Sessions(sessions_file)
            else:

                while True:
                    password = getpass.getpass('password: ')
                    password_confirm = getpass.getpass('confirm password: ')

                    if password == password_confirm:
                        break
                    else:
                        print('Password does not match the confirm password. Please enter it again:')

                self.sessions = Sessions(sessions_file, password)

        manager = Manager()
        databases = manager.dict(self.databases)
        locked_databases = manager.dict()
        start_workers(self.server_address, self.root_path, self.certfile, manager, 'admin', password, databases, locked_databases,
                      n_workers=cpu_count() - 1, debug=self._debug)
        print('Address: {}:{}'.format(*self.server_address))
        log.disable_console()
        self.master_key = secrets.token_bytes()
        self.serve_forever()
        self.log.info('Cluster shutdown')

    def shutdown(self):

        for node in list(self.nodes):
            self.shutdown_node(node)

        self.wait()
        super().shutdown()

    def shutdown_node(self, node):

        for worker in list(self.nodes[node].workers):
            self.send(worker, {'request_type': 'shutdown'})

    def info(self):
        info = list()
        info.append(f"user: {self.current_session['user']}")

        if self.current_session['is_admin']:
            info.append(f"administrator privileges")
        elif self.current_session['create_allowed']:
            info.append(f"regular privileges; database creation allowed")
        else:
            info.append(f"regular privileges")

        info.append(f"address: {self.server_address}")
        info.append(f"\n{len(self.nodes)} nodes ({sum(len(node.workers) for node in self.nodes.values())} workers):")

        for node_address, node in self.nodes.items():
            info.append(f"  '{node_address}': {len(node.workers)} workers ({node.get_queue()} job/s in progress)")

            if node.backup:
                info[-1] += ' (backup mode)'

        if self.databases:
            info.append(f"\n{len(self.databases)} databases:")

            for database in self.databases:

                if not self.current_session['is_admin'] and (not self.current_session['databases'] or
                                                             database not in self.current_session['databases']):
                    info.append(f"  '{database}' [read-only]")
                else:
                    info.append(f"  '{database}'")

        return '\n'.join(info)

    def add_worker(self, worker, databases, backup):

        if worker[0] not in self.nodes:
            self.nodes[worker[0]] = Node([worker], databases, backup)
        else:
            self.nodes[worker[0]].workers[worker] = 0

    def remove_worker(self, worker):
        del self.nodes[worker[0]].workers[worker]

        if not self.nodes[worker[0]].workers:
            del self.nodes[worker[0]]

        if len(self.nodes) == 0:
            self._done.set()

    def acquire_worker(self, node=None, database=None):

        if not node:

            for node in sorted(self.nodes, key=lambda x: self.nodes[x].get_queue()):

                if (database in self.nodes[node].databases and
                    (not database in self.databases or
                     self.nodes[node].databases[database] == self.databases[database])):
                    break

        worker = self.nodes[node].get_worker()
        self.nodes[worker[0]].workers[worker] += 1
        return worker

    def release_worker(self, worker):
        self.nodes[worker[0]].workers[worker] -= 1

    def sync_databases(self, nodes, databases, connection):

        if not nodes:
            nodes = list(self.nodes)

        try:
            nodes.remove(self.server_address[0])
        except ValueError:
            pass

        nodes = {node: self.nodes[node].backup for node in nodes}

        if not nodes:
            raise ValueError('At least 2 nodes are required in order to sync them!')

        connection.send({'request_type': 'sync_databases',
                         'nodes': nodes, 'databases': databases,
                         'redirection_address': self.acquire_worker(node=self.server_address[0])})

    def shutdown_request(self, request):
        super().shutdown_request(request)
        self.current_session = None
        self.connection = None


class Node(object):

    def __init__(self, workers, databases, backup):
        self.workers = {worker: 0 for worker in workers}
        self.databases = databases
        self.backup = backup

    def get_worker(self):
        return sorted(self.workers, key= lambda x: self.workers[x])[0]

    def get_queue(self):
        return sum(queue for queue in self.workers.values())


class WorkerServer(DatabaseServer):

    def __init__(self, server_address, central_address, root_path, certfile,
                 databases, main_lock, database_lock, backup=False, debug=False):
        super().__init__(server_address, WorkerQueryHandler, root_path, certfile, debug)
        self.log = logging.getLogger()
        self.central = central_address
        self.databases = databases
        self.current_database = None
        self.main_lock = main_lock
        self.database_lock = database_lock
        self.backup = backup
        self._shutdown_request = False

    def start(self, user, password):

        try:
            connection = Connection(self.central)
            from loadit.__init__ import __version__
            connection.send({'user': user,
                             'password': password,
                             'request': 'master_key',
                             'version': __version__})
            self.master_key = connection.recv()
            connection.send({'request_type': 'add_worker',
                             'worker_address': self.server_address,
                             'databases': self.databases._getvalue(),
                             'backup': self.backup})
        finally:
            connection.kill()

        log.disable_console()
        self.serve_forever()

    def shutdown(self):
        self.send(self.central, {'request_type': 'remove_worker',
                                 'worker_address': self.server_address})
        super().shutdown()

    def shutdown_request(self, request):
        self.log.handlers.remove(self.log_handler)
        client_address = request.getpeername()[0]
        super().shutdown_request(request)

        if not self._shutdown_request:
            data = {'request_type': 'release_worker',
                    'worker_address': self.server_address,
                    'nbytes_in': self.connection.nbytes_in,
                    'nbytes_out': self.connection.nbytes_out,
                    'request': self.current_session['request_type'],
                    'client_address': client_address,
                    'user': self.current_session['user'],
                    'database': self.current_database,
                    'is_error': self.current_session['is_error']}

            if self.current_session['database_modified']:
                data['databases'] = self.databases._getvalue()

            self.send(self.central, data)

        self.current_database = None
        self.current_session = None
        self.connection = None

    def sync_databases(self, nodes, databases, client_connection):
        self.refresh_databases()

        if databases:
            update_only = False
            databases = {database: self.databases[database] for database in databases if
                         database in self.databases}
        else:
            update_only = True
            databases = self.databases

        for node, backup in nodes.items():
            worker = tuple(self.request(self.central, {'request_type': 'acquire_worker', 'node': node})[1]['worker_address'])
            client_connection.send({'msg': f"Syncing node '{node}'..."})

            try:
                connection = Connection(worker)
                connection.send(self.master_key)
                connection.recv()
                connection.send({'request_type': 'recv_databases'})
                remote_databases = connection.recv()

                for database in databases:

                    if (not update_only and (database not in remote_databases or
                                             databases[database] != remote_databases[database]) or
                        update_only and (not backup and database in remote_databases and databases[database] != remote_databases[database] or
                                         backup and (database not in remote_databases or databases[database] != remote_databases[database]))):

                        with self.database_lock.acquire(database, block=False):
                            database_path = Path(self.root_path) / database
                            files = [file for pattern in ('**/*header.*', '**/*.bin') for file in database_path.glob(pattern)]
                            connection.send({'database': database, 'msg': '',
                                             'files': [str(file.relative_to(database_path)) for file in files]})
                            client_connection.send({'msg': f"  Syncing database '{database}' ({len(files)} files; {humansize(sum(os.path.getsize(file) for file in files))})..."})

                            for file in files:
                                connection.send_file(file)
                                msg = connection.recv()

                connection.send({'msg': "Done!"})
            finally:
                connection.kill()

        client_connection.send({'msg': f"Done!"})

    def recv_databases(self, connection):
        self.refresh_databases()
        connection.send(self.databases._getvalue())
        data = connection.recv()

        while data['msg'] != 'Done!':

            with self.database_lock.acquire(data['database']):
                path = Path(self.root_path) / data['database']
                path_temp = path.parent / (path.name + '_TEMP')
                path_temp.mkdir()

                try:

                    for file in data['files']:
                        file = path_temp / file
                        file.parent.mkdir(exist_ok=True)
                        connection.recv_file(file)
                        connection.send(b'OK')

                    if os.path.exists(path):
                        shutil.rmtree(path)

                    path_temp.rename(path)
                except Exception as e:
                    shutil.rmtree(path_temp)
                    raise e

            data = connection.recv()

    def refresh_databases(self):

        with self.main_lock:
            self.databases.clear()
            self.databases.update(get_local_databases(self.root_path))
            return self.databases._getvalue()


def start_worker(server_address, central_address, root_path, certfile,
                 databases, main_lock, locks, locked_databases, user, password, backup, debug):
    import loadit.queries # Pre-load this heavy module
    database_lock = ResourceLock(main_lock, locks, locked_databases)
    worker = WorkerServer(server_address, central_address, root_path, certfile,
                          databases, main_lock, database_lock, backup, debug)
    worker.start(user, password)


def start_workers(central_address, root_path, certfile, manager, user, password, databases, locked_databases,
                  n_workers=None, backup=False, debug=False):

    if not n_workers:
        n_workers = cpu_count()

    main_lock = Lock()
    locks = [Lock() for lock in range(n_workers)]
    host = get_ip()
    workers = list()

    for i in range(n_workers):
        workers.append(Process(target=start_worker, args=((host, find_free_port()), central_address, root_path, certfile,
                                                          databases, main_lock, locks, locked_databases,
                                                          user, password, backup, debug)))
        workers[-1].start()

    return workers


def start_node(central_address, root_path, certfile, backup=False, debug=False):
    user = input('user: ')
    password = getpass.getpass('password: ')
    manager = Manager()
    databases = manager.dict(get_local_databases(root_path))
    locked_databases = manager.dict()
    workers = start_workers(central_address, root_path, certfile, manager, user, password, databases, locked_databases,
                            backup=backup, debug=debug)

    for worker in workers:
        worker.join()

    print('Node shutdown')


def get_local_databases(root_path):
    databases = dict()

    for header_file in Path(root_path).glob('**/##header.json'):
        database = str(header_file.parent.relative_to(root_path).as_posix())
        databases[database] = get_database_hash(header_file)

    return databases


def get_database_hash(header_file):

    with open(header_file, 'rb') as f:
        return hash_bytestr(f, get_hasher('sha256'))


def get_batch_message(batch):
    sink = pa.BufferOutputStream()
    writer = pa.RecordBatchStreamWriter(sink, batch.schema)
    writer.write_batch(batch)
    writer.close()
    return sink.get_result()
