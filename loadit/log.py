import sys
import logging
from io import StringIO


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


class BufferLog(StringIO):
    
    def __init__(self):
        super().__init__()
        
    def pull(self):
        value = self.getvalue()
        self.clear()
        return value
    
    def clear(self):
        self.seek(0)
        self.truncate()


_logbuffer = None
_is_file_handler = False

    
def add_buffer_handler():
    global _logbuffer
    
    if _logbuffer is None:
        _logbuffer = BufferLog()
        add_handler(logging.StreamHandler(_logbuffer), time_format='%H:%M:%S')
        
    return _logbuffer


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