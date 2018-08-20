import os
import base64
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.hazmat.primitives.kdf.hkdf import HKDF
from cryptography.fernet import Fernet
import socket
import json
import numpy as np
from io import BytesIO
from loadit.misc import humansize
from loadit.read_results import tables_in_pch, ResultsTable
import logging


log = logging.getLogger()


class Connection(object):

    def __init__(self, server_address=None, connection_socket=None,
                 private_key=None, header_size=15, buffer_size=4096):

        if server_address:
            self.connect(server_address)
        else:
            self.socket = connection_socket

        self.encryptor = None
        self.private_key = private_key
        self.header_size = header_size
        self.buffer_size = buffer_size
        self.pending_data = b''
        self.nbytes_in = 0
        self.nbytes_out = 0

    def connect(self, server_address):
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.connect(server_address)

    def kill(self):
        self.socket.close()
        self.encryptor = None

    def send(self, bytes=None, msg=None, debug_msg=None, info_msg=None, warning_msg=None, error_msg=None, critical_msg=None, exception=None):

        if bytes: # bytes message
            data_type = '0'
        elif msg: # json message
            data_type = '1'
            bytes = json.dumps(msg).encode()
        elif debug_msg: # debug log record
            data_type = '2'
            bytes = debug_msg.encode()
        elif info_msg: # info log record
            data_type = '3'
            bytes = info_msg.encode()
        elif warning_msg: # warning log record
            data_type = '4'
            bytes = warning_msg.encode()
        elif error_msg: # error log record
            data_type = '5'
            bytes = error_msg.encode()
        elif critical_msg: # critical log record
            data_type = '6'
            bytes = critical_msg.encode()
        elif exception: # exception
            data_type = '#'
            bytes = exception.encode()

        self.socket.send((str(len(bytes)).zfill(self.header_size - 1) + data_type).encode())
        self.socket.sendall(bytes)
        self.nbytes_out += self.header_size + len(bytes)

    def recv(self):

        while True:
            data = self._recv0()
            size = int(data[:self.header_size - 1].decode())
            data_type = data[self.header_size - 1:self.header_size].decode()
            buffer = BytesIO()
            buffer.write(data[self.header_size:])

            while buffer.tell() < size:
                buffer.write(self.socket.recv(self.buffer_size))

            self.nbytes_in += self.header_size + size
            buffer.seek(size)
            self.pending_data = buffer.read()
            buffer.seek(size)
            buffer.truncate()
            buffer.seek(0)

            if data_type == '0': # bytes message
                return buffer
            elif data_type == '1': # json message
                return json.loads(buffer.read())
            elif data_type == '2': # debug log record
                log.debug(buffer.read().decode())
            elif data_type == '3': # info log record
                log.info(buffer.read().decode())
            elif data_type == '4': # warning log record
                log.warning(buffer.read().decode())
            elif data_type == '5': # error log record
                log.error(buffer.read().decode())
            elif data_type == '6': # critical log record
                log.critical(buffer.read().decode())
            elif data_type == '#': # exception
                raise ConnectionError(buffer.read().decode())

    def _recv0(self):
        data = self.pending_data

        if not (self.pending_data and
                len(self.pending_data) > self.header_size and
                (len(self.pending_data) - self.header_size) == int(self.pending_data[:self.header_size - 1].decode())):
            data += self.socket.recv(self.buffer_size)

        self.pending_data = b''
        return data

    def send_tables(self, files, tables_specs):
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
                self.send(msg=table.__dict__)
                self.send(bytes=f.getbuffer())

        self.send(msg='END')

    def recv_tables(self):

        while True:
            data = self.recv()

            if data == 'END':
                break

            table = ResultsTable(**data)
            table.data = np.load(self.recv())
            yield table

    def send_file(self, file):
        size = os.path.getsize(file)
        self.socket.send(str(size).zfill(self.header_size).encode())

        with open(file, 'rb') as f:
            sended = 1

            while sended:
                sended = self.socket.send(f.read(self.buffer_size))
        
        self.nbytes_out += self.header_size + size
        self.recv()

    def recv_file(self, file):
        data = self._recv0()
        size = int(data[:self.header_size].decode())

        with open(file, 'wb') as f:
            f.write(data[self.header_size:])

            while f.tell() < size:
                f.write(self.socket.recv(self.buffer_size))

        self.nbytes_in += self.header_size + size
        self.send(b'OK')

    def send_secret(self, secret):

        if not self.encryptor:
            self.send(self.private_key.public_key().public_bytes(encoding=serialization.Encoding.PEM,
                                                                 format=serialization.PublicFormat.SubjectPublicKeyInfo))
            public_key_other = serialization.load_pem_public_key(self.recv().read(), backend=default_backend())
            self.encryptor = Fernet(self._get_key(public_key_other))

        self.send(self.encryptor.encrypt(secret))

    def recv_secret(self):

        if not self.encryptor:
            public_key_other = serialization.load_pem_public_key(self.recv().read(),
                                                                 backend=default_backend())
            self.send(self.private_key.public_key().public_bytes(encoding=serialization.Encoding.PEM,
                                                                 format=serialization.PublicFormat.SubjectPublicKeyInfo))
            self.encryptor = Fernet(self._get_key(public_key_other))

        return self.encryptor.decrypt(self.recv().read())

    def _get_key(self, public_key_other):
        shared_key = self.private_key.exchange(ec.ECDH(), public_key_other)
        return base64.urlsafe_b64encode(HKDF(algorithm=hashes.SHA256(),
                                             length=32,
                                             salt=None,
                                             info=b'handshake data',
                                             backend=default_backend()).derive(shared_key))


def get_private_key():
    return ec.generate_private_key(ec.SECP384R1(), default_backend())


def get_master_key():
    return Fernet.generate_key()


def get_ip():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        # doesn't even have to be reachable
        s.connect(('10.255.255.255', 1))
        return s.getsockname()[0]
    except:
        return '127.0.0.1'
    finally:
        s.close()


def find_free_port():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(('localhost', 0))
    port = s.getsockname()[1]
    s.close()
    return port
