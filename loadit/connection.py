import os
import socket as sock
import ssl
import json
from io import BytesIO
import logging


log = logging.getLogger()

# SSL default settings (intended to work with self-signed certificates)
SSL_CONTEXT = ssl.create_default_context()
SSL_CONTEXT.check_hostname = False
SSL_CONTEXT.verify_mode = ssl.VerifyMode.CERT_NONE


class Connection(object):
    """
    Handle a TCP/IP connection.
    """

    def __init__(self, peer_address=None, socket=None,
                 ssl_context=SSL_CONTEXT, buffer_size=4096):
        """
        Initialize a Connection instance.

        Parameters
        ----------
        peer_address : tuple of (str, int), optional
            Peer address (ip address, port number). It creates a new TCP/IP connection.
        socket : socket.socket, optional
            TCP/IP socket. It uses an already existing TCP/IP connection.
        ssl_context : ssl.SSLContext, optional
            SSL context settings.
        buffer_size : int, optional
            Socket buffer size.
        """
        self.ssl_context = ssl_context

        if peer_address:
            self.connect(peer_address)
        else:
            self.socket = socket

        self.buffer_size = buffer_size
        self.header_size = 8
        self.pending_data = b''
        self.nbytes_in = 0
        self.nbytes_out = 0
        self.waiting = False

    def connect(self, peer_address):
        """
        Create a new TCP/IP connection with a peer.

        Parameters
        ----------
        peer_address : tuple of (str, int)
            Peer address (ip address, port number). It creates a new TCP/IP connection.
        """
        self.socket = self.ssl_context.wrap_socket(sock.socket())
        self.socket.connect(peer_address)

    def kill(self):
        """
        Kill underlying TCP/IP connection.
        """
        self.socket.close()

    def wait(self):
        """
        Wait until peer confirmation.
        """

        if self.waiting:
            self.recv()

        self.waiting = True

    def awake_peer(self):
        """
        Send peer confirmation.
        """

        if not self.waiting and self.nbytes_in:
            self.send(b'OK')

        self.waiting = False

    def send(self, msg, msg_type='bytes'):
        """
        Send a message to peer.

        Parameters
        ----------
        msg : bytes, dict or str
            Message to be sended.
        msg_type : {'bytes', 'buffer', 'json',
                    'debug_log', 'info_log', 'warning_log',
                    'error_log', 'critical_log', 'exception'}, optional
            Message type. It can be raw bytes, a dict (encoded as json),
            a log entry or an exception descriptor (both of them of type str).
        """
        type_encoding = {'bytes': 'b', 'buffer': 'B', 'json': 'j',
                         'debug_log': 'd', 'info_log': 'i', 'warning_log': 'w',
                         'error_log': 'e', 'critical_log': 'c', 'exception': 'E'}

        if type(msg) is dict:
            self.wait()
            msg_type = 'json'
            bytes = json.dumps(msg).encode()
        elif msg_type in ('bytes', 'buffer'): # bytes message
            self.wait()
            bytes = msg
        elif msg_type in type_encoding:
            bytes = msg.encode()
        else:
            raise ValueError(f"Not supported message type: '{msg_type}'")

        self.socket.send((len(bytes).to_bytes(self.header_size - 1, 'little') + type_encoding[msg_type].encode()))
        self.socket.sendall(bytes)
        self.nbytes_out += self.header_size + len(bytes)

    def recv(self):
        """
        Receive a message from peer.

        Returns
        -------
        bytes, io.BytesIO or dict
        """
        self.awake_peer()

        while True:
            data = self._recv0()
            size = int.from_bytes(data[:self.header_size - 1], 'little')
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

            if data_type == 'b': # bytes message
                return buffer.getvalue()
            elif data_type == 'B': # buffer message
                return buffer
            elif data_type == 'j': # json message
                return json.loads(buffer.getvalue())
            elif data_type == 'd': # debug log record
                log.debug(buffer.getvalue().decode())
            elif data_type == 'i': # info log record
                log.info(buffer.getvalue().decode())
            elif data_type == 'w': # warning log record
                log.warning(buffer.getvalue().decode())
            elif data_type == 'e': # error log record
                log.error(buffer.getvalue().decode())
            elif data_type == 'c': # critical log record
                log.critical(buffer.getvalue().decode())
            elif data_type == 'E': # exception
                raise ConnectionError(buffer.getvalue().decode())

    def _recv0(self):
        data = self.pending_data

        if not (self.pending_data and
                len(self.pending_data) > self.header_size and
                (len(self.pending_data) - self.header_size) == int.from_bytes(self.pending_data[:self.header_size - 1], 'little')):
            data += self.socket.recv(self.buffer_size)

        self.pending_data = b''
        return data

    def send_file(self, file):
        """
        Send file to peer.

        Parameters
        ----------
        file : str
            File path.
        """
        self.wait()
        size = os.path.getsize(file)
        self.socket.send(size.to_bytes(self.header_size - 1, 'little'))

        with open(file, 'rb') as f:

            while True:
                bytes = f.read(self.buffer_size)

                if not bytes:
                    break
                
                self.socket.send(bytes)
                
        self.nbytes_out += self.header_size + size

    def recv_file(self, file):
        """
        Receive file from peer.

        Parameters
        ----------
        file : str
            File path.
        """
        self.awake_peer()
        data = self._recv0()
        size = int.from_bytes(data[:self.header_size], 'little')

        with open(file, 'wb') as f:
            f.write(data[self.header_size:])

            while f.tell() < size:
                f.write(self.socket.recv(self.buffer_size))

        self.nbytes_in += self.header_size + size
