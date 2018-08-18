import os
import json
import datetime
import numpy as np
from loadit.read_results import tables_in_pch
from loadit.tables_specs import get_tables_specs
from loadit.misc import get_hasher, hash_bytestr, humansize
import logging


log = logging.getLogger()


def create_tables(database_path, files, headers, tables_specs=None,
                  table_generator=None):

    if not tables_specs:
        tables_specs = get_tables_specs()

    if not table_generator:

        def table_generator(files, tables_specs):

            for i, file in enumerate(files):
                log.info(f"Processing file {i + 1} of {len(files)} ({humansize(os.path.getsize(file))}): '{os.path.basename(file)}'...")

                for table in tables_in_pch(file, tables_specs):
                    yield table

        table_generator = table_generator(files, tables_specs)

    ignored_tables = set()

    try:

        for table in table_generator:

            if table.name not in tables_specs:

                if table.name not in ignored_tables:
                    log.warning("WARNING: '{}' is not supported!".format(table.name))
                    ignored_tables.add(table.name)

                continue

            if table.name not in headers:
                headers[table.name] = {
                    'name': table.name,
                    'path': os.path.join(database_path, table.name),
                    'columns': [(field, tables_specs[table.name]['dtypes'][field]) for field in
                                tables_specs[table.name]['columns']],
                    'batches': list(),
                    'LIDs': list(),
                    'IDs': None
                }
                open_table(headers[table.name], new_table=True)

            append_to_table(table, headers[table.name])
    finally:

        for header in headers.values():
            close_table(header)

    for header in headers.values():
        np.array(header['LIDs'], dtype=header['columns'][0][1]).tofile(os.path.join(header['path'], header['columns'][0][0] + '.bin'))
        header['IDs'].tofile(os.path.join(header['path'], header['columns'][1][0] + '.bin'))


def open_table(header, new_table=False):

    if not os.path.exists(header['path']):
        os.mkdir(header['path'])

    for field, dtype in header['columns'][2:]:
        file = os.path.join(header['path'], field + '.bin')

        if new_table:
            f = open(file, 'wb')
        else:
            f = open(file, 'rb+')
            f.seek(len(header['LIDs']) * len(header['IDs']) * np.dtype(dtype).itemsize)

        if 'files' not in header:
            header['files'] = dict()

        header['files'][field] = f


def append_to_table(table, header):
    LID_label = header['columns'][0][0]
    ID_label = header['columns'][1][0]

    LID = table.data[LID_label][0]

    if LID in header['LIDs']:
        log.warning("WARNING: Subcase already in the database! It will be skipped (LID: {}, table: '{}')".format(LID, header['name']))
        return False

    IDs = table.data[ID_label]

    if header['IDs'] is None:
        header['IDs'] = IDs

    if 'iIDs' not in header:
        header['iIDs'] = {ID: i for i, ID in enumerate(header['IDs'])}

    if np.array_equal(header['IDs'], IDs):

        for field, dtype in header['columns'][2:]:
            table.data[field].tofile(header['files'][field])

    else:
        indexes = {header['iIDs'][ID]: i for i, ID in enumerate(IDs) if ID in header['iIDs']}

        if len(indexes) < len(header['IDs']) or len(IDs) != len(header['IDs']):
            log.warning("WARNING: Inconsistent {}/s (LID: {}, table: '{}')".format(ID_label, LID, header['name']))

        for field, dtype in header['columns'][2:]:
            field_array = np.full(len(header['IDs']), np.nan, dtype=dtype)

            for index0, index1 in indexes.items():
                field_array[index0] = table.data[field][index1]

            field_array.tofile(header['files'][field])

    header['LIDs'].append(LID)
    return True


def close_table(header):

    try:

        for file in header['files'].values():
            file.close()

        del header['files']

    except KeyError:
        pass


def assembly_database(database_path, headers, batches, max_chunk_size=None,
                      hash_function='sha256', attachments=None):

    for name, header in headers.items():
        create_transpose(header, max_chunk_size)
        create_table_header(header, batches[-1][0], hash_function)

    create_database_header(database_path, headers, batches, hash_function, attachments)


def create_transpose(header, max_chunk_size):

    for field, dtype in header['columns'][2:]:
        field_file = os.path.join(header['path'], field + '.bin')
        n_LIDs = len(header['LIDs'])
        n_IDs = len(header['IDs'])
        field_array = np.memmap(field_file, dtype=dtype, shape=(n_LIDs, n_IDs), mode='r')
        n_IDs_per_chunk = int(max_chunk_size // (n_LIDs * np.dtype(dtype).itemsize))
        n_chunks = int(n_IDs // n_IDs_per_chunk)
        n_IDs_last_chunk = int(n_IDs % n_IDs_per_chunk)

        if n_IDs != n_IDs_per_chunk * n_chunks + n_IDs_last_chunk:
            raise ValueError(f"Inconsistency found! (table: '{header['name']}', field: '{field}')")

        chunks = list()

        if n_chunks:
            chunk = np.empty((n_IDs_per_chunk, n_LIDs), dtype)
            chunks += [(chunk, n_IDs_per_chunk)] * n_chunks

        if n_IDs_last_chunk:
            last_chunk = np.empty((n_IDs_last_chunk, n_LIDs), dtype)
            chunks.append((last_chunk, n_IDs_last_chunk))

        with open(field_file, 'ab') as f:
            i0 = 0
            i1 = 0

            for chunk, n_IDs_per_chunk in chunks:
                i1 += n_IDs_per_chunk
                chunk = field_array[:, i0:i1].T
                chunk.tofile(f)
                i0 += n_IDs_per_chunk


def create_table_header(header, batch_name, hash_function):
    # Set restore points
    if header['batches'] and header['batches'][-1][0] == batch_name:
        check = True
    else:
        check = False

    if not check:
        header['batches'].append([batch_name, len(header['LIDs']), dict()])

    for field, _ in header['columns']:

        with open(os.path.join(header['path'], field + '.bin'), 'rb') as f:

            if check:

                if header['batches'][-1][2][field + '.bin'] != hash_bytestr(f, get_hasher(hash_function)):
                    log.error(f"ERROR: '{os.path.join(header['path'], field + '.bin')} is corrupted!'")

            else:
                header['batches'][-1][2][field + '.bin'] = hash_bytestr(f, get_hasher(hash_function))

    # Write table header
    table_header = {
        'name': header['name'],
        'columns': header['columns'],
        'batches': header['batches']
    }

    with open(os.path.join(header['path'], '#header.json'), 'w') as f:
        json.dump(table_header, f)


def create_database_header(database_path, headers, batches, hash_function,
                           attachments=None, table_hashes=None):
    # Get table hashes
    if not table_hashes:
        table_hashes = dict()

        for table in headers:

            with open(os.path.join(database_path, table, '#header.json'), 'rb') as f:
                table_hashes[table] = hash_bytestr(f, get_hasher(hash_function))

    # Get hash of current batch
    if batches and batches[-1][1] is None:
        hasher = get_hasher(hash_function)

        for table_hash in table_hashes.values():
            hasher.update(table_hash.encode())

        batches[-1][1] = hasher.hexdigest()

    # Set datetime of current batch
    if batches and batches[-1][2] is None:
        batches[-1][2] = str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

    # Write database header
    from loadit.__init__ import __version__

    database_header = {
        'version': __version__,
        'hash_function': hash_function,
        'table_hashes': table_hashes,
        'batches': batches,
        'attachments': dict() if attachments is None else attachments
    }

    with open(os.path.join(database_path, '##header.json'), 'w') as f:
        json.dump(database_header, f)
