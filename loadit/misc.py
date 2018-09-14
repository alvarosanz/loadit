import os
import hashlib


suffixes = ['B', 'KB', 'MB', 'GB', 'TB', 'PB']

def humansize(nbytes):
    if nbytes == 0:
        return '0 B'

    i = 0

    while nbytes >= 1024 and i < len(suffixes) - 1:
        nbytes /= 1024.
        i += 1

    f = ('%.2f' % nbytes).rstrip('0').rstrip('.')
    return '%s %s' % (f, suffixes[i])


def hash_bytestr(file, hasher, blocksize=65536, ashexstr=True):

    for block in file_as_blockiter(file, blocksize):
        hasher.update(block)

    if ashexstr:
        return hasher.hexdigest()
    else:
        return hasher.digest()


def file_as_blockiter(file, blocksize):

    with file:
        block = file.read(blocksize)

        while len(block) > 0:
            yield block
            block = file.read(blocksize)


def get_hasher(hash_type):

    if hash_type == 'md5':
        return hashlib.md5()
    elif hash_type == 'sha1':
        return hashlib.sha1()
    elif hash_type == 'sha256':
        return hashlib.sha256()
    else:
        raise ValueError(f"Unsupported hash method: {hash_type}")


def get_hash(value):
    hasher = hashlib.sha256(value.encode())
    return hasher.hexdigest()


def get_resource_path(relative_path):
    """ Get absolute path to resource, works for dev and for PyInstaller """
    try:
        # PyInstaller creates a temp folder and stores path in _MEIPASS
        base_path = sys._MEIPASS
    except Exception:
        base_path = os.path.abspath('.')

    return os.path.join(base_path, relative_path)
