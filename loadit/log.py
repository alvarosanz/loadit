import sys
import logging


def add_handler(handler, logger=None, format='%(asctime)s - %(message)s', time_format='%Y-%m-%d %H:%M:%S', level=logging.INFO):
    handler.setLevel(level)
    handler.setFormatter(logging.Formatter(format, time_format))
    logging.getLogger(logger).addHandler(handler)


# root logger
logging.getLogger().setLevel(logging.INFO)
console_handler = logging.StreamHandler(sys.stdout)
add_handler(console_handler, format='%(message)s')

# central_server logger
logging.getLogger('central_server').setLevel(logging.INFO)
add_handler(logging.FileHandler('server.log'), logger='central_server')


def enable_console():
    logging.getLogger().addHandler(console_handler)


def disable_console():
    logging.getLogger().handlers.remove(console_handler)


class ConnectionHandler(logging.StreamHandler):
    
    def __init__(self, connection):
        super().__init__()
        self.connection = connection
        
    def emit(self, record):

        if record.levelname == 'DEBUG':
            self.connection.send(debug_msg=record.getMessage())
        elif record.levelname == 'INFO':
            self.connection.send(info_msg=record.getMessage())
        elif record.levelname == 'WARNING':
            self.connection.send(warning_msg=record.getMessage())
        elif record.levelname == 'ERROR':
            self.connection.send(error_msg=record.getMessage())
        elif record.levelname == 'CRITICAL':
            self.connection.send(critical_msg=record.getMessage()) 


_is_file_handler = False


def add_file_handler(file=None):
    global _is_file_handler
    
    if not _is_file_handler:
        
        if not file:
            file = 'loadit.log'
            
        add_handler(logging.FileHandler(file))


def custom_logging(func):

    def wrapped(self, *args, **kwargs):
        
        try:
            logging.getLogger().addHandler(self.log)
            return func(self, *args, **kwargs)
        finally:
            logging.getLogger().handlers.remove(self.log)

    return wrapped