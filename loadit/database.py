import os
import re
from pathlib import Path
import csv
import json
import zlib
import binascii
import shutil
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
from loadit.table_data import TableData
from loadit.tables_specs import get_tables_specs
from loadit.database_creation import create_tables, assembly_database, open_table, create_database_header
from loadit.misc import humansize, get_hasher, hash_bytestr
import logging


log = logging.getLogger()


class DatabaseHeader(object):
    """
    Store database metadata.
    """

    def __init__(self, path=None, header=None):
        """
        Initialize a DatabaseHeader instance.

        Parameters
        ----------
        path : str, optional
            Database path.
        header : dict, optional
            Already constructed database header.
        """
        if header: # Load header from dict
            self.__dict__ = header
        else: # Load header from path

            # Load database header
            with open(os.path.join(path, '##header.json')) as f:
                self.__dict__ = json.load(f)

            # Check database version
            from loadit.__init__ import __version__

            if self.version.split('.')[-2] != __version__.split('.')[-2]:
                raise ValueError(f"Not supported version!")

            # Load tables headers
            from loadit.queries import query_functions, query_geometry
            self.name = os.path.basename(path)
            self.nbytes = 0
            self.tables = dict()

            for name in self.table_hashes:

                # Load table header
                with open(os.path.join(os.path.join(path, name), '#header.json')) as f:
                    self.tables[name] = json.load(f)

                self.tables[name]['nbytes'] = 0

                # Load LIDs & EIDs and calculate total size in bytes
                for i, (field_name, dtype) in enumerate(self.tables[name]['columns']):
                    file = os.path.join(path, name, field_name + '.bin')
                    self.tables[name]['nbytes'] += os.path.getsize(file)

                    if i == 0:
                        self.tables[name]['LIDs'] = np.fromfile(file, dtype=dtype).tolist()
                    elif i == 1:
                        self.tables[name]['IDs'] = np.fromfile(file, dtype=dtype).tolist()

                self.nbytes += self.tables[name]['nbytes']

                # Load query functions
                try:
                    self.tables[name]['query_functions'] = list(query_functions[name])
                except:
                    self.tables[name]['query_functions'] = list()

                # Load query geometry
                try:
                    self.tables[name]['query_geometry'] = list(query_geometry[name])
                except:
                    self.tables[name]['query_geometry'] = list()

    def get_query_header(self):
        return {'database': self.name, 'hash': self.batches[-1][1]}

    def info(self, print_to_screen=True, detailed=False):
        """
        Display database info.

        Parameters
        ----------
        print_to_screen : bool, optional
            Whether to print to screen or return an string instead.
        detailed : bool, optional
            Whether to show detailed info or not.

        Returns
        -------
        str, optional
            Database info.
        """
        info = list()

        # General info
        info.append(f"name: '{self.name}'")
        info.append(f'version: {self.version}')
        info.append(f'size: {humansize(self.nbytes)}'.format())
        info.append('')

        # Tables info
        if self.tables:

            if detailed:

                for table in self.tables.values():
                    ncols = len(table['columns'])
                    info.append(f"table: '{table['name']}'")
                    info.append(f"{table['columns'][0][0]}s: {len(table['LIDs'])}")
                    info.append(f"{table['columns'][1][0]}s: {len(table['IDs'])}")
                    info.append('   ' + ' '.join(['_' * 6 for i in range(ncols)]))
                    info.append('  |' + '|'.join([' ' * 6 for i in range(ncols)]) + '|')
                    info.append('  |' + '|'.join([field.center(6) for field, _ in table['columns']]) + '|')
                    info.append('  |' + '|'.join(['_' * 6 for i in range(ncols)]) + '|')
                    info.append('  |' + '|'.join([' ' * 6 for i in range(ncols)]) + '|')
                    info.append('  |' + '|'.join([dtype[1:].center(6) for _, dtype in table['columns']]) + '|')
                    info.append('  |' + '|'.join(['_' * 6 for i in range(ncols)]) + '|')

                    if table['query_functions']:
                        info.append("\nother fields: {}".format(', '.join(table['query_functions'])))

                    if table['query_geometry']:
                        info.append("geometry: {}".format(', '.join(table['query_geometry'])))

                    info.append('')
            else:
                info.append(f"{len(self.tables)} table/s:")

                for table in self.tables:
                    info.append(f"    '{table}'")

        # Batches info
        if self.batches:
            info.append('')
            info.append(f'{len(self.batches)} batches:')

            for i, (batch_name, batch_hash, batch_date, batch_files, batch_comment) in enumerate(self.batches):
                info.append(f"{str(i).rjust(4)} - '{batch_name}': {batch_date} [{batch_hash}]")

                if detailed:

                    if batch_comment:
                        info.append(f'\n        {batch_comment}')

                    info.append(f'\n        {len(batch_files)} file/s:')

                    for file in batch_files:
                        info.append(f'          {file}')

                    info.append('')

        # Attachments info
        if self.attachments:
            info.append('')
            info.append(f'{len(self.attachments)} attachment/s:')

            for attachment, (_, nbytes) in self.attachments.items():
                info.append(f"    {attachment} ({humansize(nbytes)})")

        # Summary
        info = '\n'.join(info)

        if print_to_screen:
            print(info)
        else:
            return info

    def get_batch_size(self, batch):
        size = 0

        for table in self.tables.values():
            i_LID0 = 0

            for batch_name, i_LID1, _ in table['batches']:
                n_LIDs = i_LID1 - i_LID0
                i_LID0 += n_LIDs

                if batch_name == batch:
                    n_EIDs = len(table['IDs'])
                    size += np.dtype(table['columns'][0][1]).itemsize * n_LIDs
                    size += np.dtype(table['columns'][1][1]).itemsize * n_EIDs

                    for field, dtype in table['columns'][2:]:
                        size += np.dtype(dtype).itemsize * n_LIDs * n_EIDs * 2

                    break

        return size

    def get_size(self, table, field=None):
        n_LIDs = len(self.tables[table]['LIDs'])
        n_EIDs = len(self.tables[table]['IDs'])
        size = 0

        for field_name, dtype in self.tables[table]['columns'][2:]:

            if not field or field_name == field:
                size += np.dtype(dtype).itemsize * n_LIDs * n_EIDs * 2

        return size


def create_database(database_path, overwrite=False):
    """
    Create a new database from .pch files.

    Parameters
    ----------
    database_path : str
        Database path.
    overwrite : bool, optional
        Whether to rewrite or not an already existing database.
    """
    Path(database_path).mkdir(parents=True, exist_ok=overwrite)
    (Path(database_path) / '.attachments').mkdir(exist_ok=overwrite)
    assembly_database(database_path, dict(), list())
    log.info(f"Database '{os.path.basename(database_path)}' created")
    database = Database(database_path)
    database.load()
    return database

class Database(object):
    """
    Handle a local database.
    """

    def __init__(self, path=None, max_memory=1e9):
        """
        Initialize a Database instance.

        Parameters
        ----------
        path : str, optional
            Database path.
        max_memory : int, optional
            Memory limit (in bytes).
        """
        self.path = path
        self.max_memory = int(max_memory)
        self.load()

    def load(self):
        """
        Load the database.
        """

        if self.path:
            # Load database header
            self.header = DatabaseHeader(self.path)

            # Load tables
            self.tables = dict()

            for name, header in self.header.tables.items():
                fields = [(field_name, dtype, os.path.join(self.path, name, field_name + '.bin')) for
                          field_name, dtype in header['columns'][2:]]
                self.tables[name] = TableData(fields, header['LIDs'], header['IDs'])

    def check(self):
        """
        Check database integrity.

        Returns
        -------
        list of str
            List of corrupted files.
        """
        log.info('Checking database integrity...')
        files_corrupted = list()

        # Check tables integrity
        for name, header in self.header.tables.items():

            # Check table fields integrity
            for filename, hash in header['batches'][-1][2].items():
                field_file = os.path.join(self.path, name, filename)

                with open(field_file, 'rb') as f:

                    if hash != hash_bytestr(f, get_hasher(self.header.hash_function)):
                        files_corrupted.append(field_file)

            # Check table header integrity
            header_file = os.path.join(self.path, name, '#header.json')

            with open(header_file, 'rb') as f:

                if self.header.table_hashes[header['name']] != hash_bytestr(f, get_hasher(self.header.hash_function)):
                    files_corrupted.append(header_file)

        # Check attachments
        for attachment in self.header.attachments:
            attachment_file = os.path.join(self.path, '.attachments', attachment)

            with open(attachment_file, 'rb') as f:

                if self.header.attachments[attachment][0] != hash_bytestr(f, get_hasher(self.header.hash_function)):
                    files_corrupted.append(attachment_file)

        # Summary
        if files_corrupted:
            info = list()

            for file in files_corrupted:
                info.append(f"'{Path(file).relative_to(self.path).as_posix()}' is corrupted!")

            log.error('\n'.join(info))
        else:
            log.info('Everything is OK!')

        return files_corrupted

    def _close(self):
        """
        Close tables.
        """

        for table in self.tables.values():
            table.close()

    def _get_tables_specs(self):
        """
        Get tables specifications.

        Returns
        -------
        dict
            Tables specifications.
        """
        tables_specs = get_tables_specs()

        for name, header in self.header.tables.items():
            tables_specs[name]['columns'] = [field for field, _ in header['columns']]
            tables_specs[name]['dtypes'] = {field: dtype for field, dtype in header['columns']}
            tables_specs[name]['pch_format'] = [[(field, tables_specs[name]['dtypes'][field] if
                                                  field in tables_specs[name]['dtypes'] else
                                                  dtype) for field, dtype in row] for row in
                                                tables_specs[name]['pch_format']]

        return tables_specs


    def _write_header(self):
        """
        Write database header file.
        """
        create_database_header(self.path, self.header.tables, self.header.batches, self.header.hash_function,
                               self.header.attachments, self.header.table_hashes)

    def add_attachment(self, file, copy=True):
        """
        Add a new attachment (consisting on one or more files) to the database.

        Parameters
        ----------
        file : str
            Attachment file path.
        copy : bool
            Whether to copy or not the file.
        """
        name = os.path.basename(file)

        if name in self.header.attachments:
            raise FileExistsError(f"Already existing attachment!")

        attachment_file = os.path.join(self.path, '.attachments', name)

        if copy:
            shutil.copyfile(file, attachment_file)
        else:
            os.rename(file, attachment_file)

        with open(attachment_file, 'rb') as f:
            self.header.attachments[name] = [hash_bytestr(f, get_hasher(self.header.hash_function)),
                                             os.path.getsize(attachment_file)]

        self._write_header()
        log.info(f"Attachment '{name}' added")

    def remove_attachment(self, name):
        """
        Remove an attachment.

        Parameters
        ----------
        name : str
            Attachment name.
        """

        if name not in self.header.attachments:
            raise FileNotFoundError(f"Attachment not found!")

        os.remove(os.path.join(self.path, '.attachments', name))
        del self.header.attachments[name]
        self._write_header()
        log.info(f"Attachment '{name}' removed")

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

        if name not in self.header.attachments:
            raise FileNotFoundError(f"Attachment not found!")

        shutil.copyfile(os.path.join(self.path, '.attachments', name),
                        os.path.join(path))
        log.info(f"Attachment '{name}' downloaded")

    def new_batch(self, files, batch_name, comment='', table_generator=None):
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
        table_generator : generator, optional
            A generator which yields tables.
        """

        if batch_name in {batch[0] for batch in self.header.batches}:
            raise ValueError(f"'{batch_name}' already exists!")

        self._close()

        for header in self.header.tables.values():
            header['path'] = os.path.join(self.path, header['name'])
            header['IDs'] = np.array(header['IDs'], dtype=header['columns'][1][1])
            open_table(header, new_table=False)

        try:
            create_tables(self.path, files, self.header.tables, self._get_tables_specs(),
                          table_generator=table_generator)
        except Exception as e: # Restore database if something unexpected happens
            self.load()
            self.restore(self.header.batches[-1][0])
            raise e

        log.info('Assembling database...')
        self.header.batches.append([batch_name, None, None, [os.path.basename(file) for file in files], comment])
        assembly_database(self.path, self.header.tables, self.header.batches,
                          self.max_memory, self.header.hash_function, self.header.attachments)
        self.load()
        log.info(f"Batch '{batch_name}' created")

    def restore(self, batch_name):
        """
        Restore database to a previous batch. This operation is not reversible.

        Parameters
        ----------
        batch_name : str
            Batch name.
        """
        restore_points = [batch[0] for batch in self.header.batches]

        if batch_name not in restore_points:
            raise ValueError(f"'{batch_name}' is not a valid restore point")

        log.info(f"Restoring database to '{batch_name}'...")
        self._close()
        batch_index = 0

        for name, header in self.header.tables.items():

            try:
                index = [batch_name for batch_name, _, _ in header['batches']].index(batch_name)
                batch_index = max(batch_index, index)
                header['batches'] = header['batches'][:index + 1]
                position = header['batches'][index][1]
                header['LIDs'] = header['LIDs'][:position]

                truncate_file(os.path.join(self.path, name, 'LID.bin'),
                              position * np.dtype(header['columns'][0][1]).itemsize)

                for field, dtype in header['columns'][2:]:
                    truncate_file(os.path.join(self.path, name, field + '.bin'),
                                  position * np.dtype(dtype).itemsize * len(header['IDs']))

                header['path'] = os.path.join(self.path, name)

            except ValueError:
                del self.tables[name]
                shutil.rmtree(os.path.join(self.path, name))

        batch_hash_old = self.header.batches[batch_index][1]
        assembly_database(self.path, {name: self.header.tables[name] for name in self.tables},
                          self.header.batches[:batch_index + 1], self.max_memory,
                          self.header.hash_function, self.header.attachments)
        self.load()

        if self.header.batches[-1][1] != batch_hash_old:
            raise ValueError('Database header is corrupted!')

        log.info(f"Database restored to '{batch_name}'")

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
            query = parse_query(json.load(f), True)

        record_batch =  self.query(**query, double_precision=double_precision)

        if query['output_file']:
            write_query(record_batch, query['output_file'])

        return record_batch

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
        from loadit.queries import query_functions
        log.info('Processing query...')

        try:
            query_functions = query_functions[table]
        except KeyError:
            query_functions = None

        if double_precision:
            float_dtype = np.float64
        else:
            float_dtype = np.float32

        if not fields:
            fields = self.tables[table].fields

        # Weigths
        if geometry and 'weights' in geometry:
            weights = geometry['weights']
        else:
            weights = None

        # Group data pre-processing
        if groups:
            IDs = sorted({ID for IDs in groups.values() for ID in IDs})
            iIDs = {ID: i for i, ID in enumerate(IDs)}
            indexes_by_group = {group: np.array([iIDs[ID] for ID in group_IDs], dtype=np.int64) for
                                group, group_IDs in groups.items()}

            if weights:
                weights_by_group = {group: np.array([weights[ID] for ID in group_IDs], dtype=np.int64) for
                                    group, group_IDs in groups.items()}

        # Requested LIDs & IDs
        LID_suffix = ': LID*' if LIDs else ': LID'
        LIDs_queried = self.tables[table]._LIDs if not LIDs else list(LIDs)
        IDs_queried = self.tables[table]._IDs if not IDs else IDs

        # Process LID combination data
        LID_combinations = None

        if isinstance(LIDs, dict):
            iLIDs = self.tables[table]._iLIDs
            LIDs2read = [LID for LID, seq in LIDs.items() if not seq]
            LIDs_requested = set(LIDs2read)
            LIDs2read += list({LID for seq in LIDs.values() for LID in seq[1::2] if
                                  LID not in LIDs_requested and LID in iLIDs})
            LIDs_combined_used = list({LID for seq in LIDs.values() for LID in seq[1::2] if
                                       LID not in iLIDs})
            LIDs_queried_index = {LID: i for i, LID in enumerate(LIDs2read + LIDs_combined_used)}
            LID_combinations = [(LIDs_queried_index[LID] if LID in LIDs_queried_index else None,
                                 np.array([LIDs_queried_index[LID] for LID in seq[1::2]], dtype=np.int64),
                                 np.array(seq[::2], dtype=float_dtype)) for LID, seq in LIDs.items()]
        else:
            LIDs2read = LIDs

        # Group data pre-processing
        if geometry:
            geometry = {parameter: np.array([geometry[parameter][ID] for ID in IDs_queried], dtype=float_dtype) for
                        parameter in geometry}

        # Memory pre-allocation
        mem_handler = MemoryHandler(self.max_memory, LID_suffix, fields, LIDs_queried, IDs_queried, groups, float_dtype,
                                    len(LIDs2read) + len(LIDs_combined_used) if LID_combinations else None)

        # Process batches
        for batch_index, batch_slice in enumerate(mem_handler.batches):
            # Process batch information
            read_fields = True

            if LID_combinations:

                if batch_index == 0:
                    LIDs2read_batch = LIDs2read
                else:
                    read_fields = False

                if batch_slice:
                    LID_combinations_batch = LID_combinations[batch_slice]
                else:
                    LID_combinations_batch = LID_combinations
            else:
                LID_combinations_batch = None

                if batch_slice:
                    LIDs2read_batch = LIDs_queried[batch_slice]
                else:
                    LIDs2read_batch = LIDs2read

            if batch_slice:
                LIDs_queried_batch = np.array(LIDs_queried[batch_slice], dtype=np.int64)
            else:
                LIDs_queried_batch = np.array(LIDs_queried, dtype=np.int64)

            # Process fields
            fields_processed = set()

            for field, level in mem_handler.field_seq:

                if field not in fields_processed:

                    if level == 0: # Load fields into memory
                        basic_field, is_absolute = is_abs(field)
                        process_field(field, basic_field, self.tables[table], query_functions, geometry,
                                      mem_handler, fields_processed, read_fields,
                                      batch_index, LIDs2read_batch, IDs ,LID_combinations_batch)
                    else: # Field aggregation
                        aggregation, is_absolute = is_abs(field.split('-')[-1])
                        array = mem_handler.get('-'.join(field.split('-')[:-1]), batch_index)
                        array_agg = mem_handler.get(field, batch_index)
                        basic_field = field

                        if level == 1: # 1st level

                            for j, group in enumerate(groups):
                                aggregate(array[:, indexes_by_group[group]],
                                          array_agg[:, j], aggregation, level,
                                          weights_by_group[group] if weights else None)

                        elif level == 2: # 2nd level
                            aggregate(array, array_agg, aggregation, level,
                                      LIDs_queried_batch, mem_handler.get(field + LID_suffix),
                                      use_previous_agg= batch_index > 0)

                    # Absolute value
                    if is_absolute:
                        np.abs(mem_handler.get(basic_field, batch_index), out=mem_handler.get(field, batch_index))

                    fields_processed.add(field)

        mem_handler.update()

        # RecordBatch creation
        from loadit.queries import set_index
        order = 'C' if sort_by_LID else 'F'

        if mem_handler.level == 0:
            index_names = [self.header.tables[table]['columns'][0][0],
                           self.header.tables[table]['columns'][1][0]]
            index = [LIDs_queried, IDs_queried]
            columns = mem_handler.fields[0]
            arrays = [pa.array(mem_handler.data0[i, :, :].ravel(order)) for i in range(len(fields))]
        elif mem_handler.level == 1:
            index_names = [self.header.tables[table]['columns'][0][0], 'Group']
            index = [LIDs_queried, list(groups)]
            columns = mem_handler.fields[1]
            arrays = [pa.array(mem_handler.data1[i, :, :].ravel(order)) for i in range(len(fields))]
        else:
            index_names = ['Group'] if groups else [self.header.tables[table]['columns'][1][0]]
            index = [list(groups) if groups else IDs_queried]
            columns = [field + suffix for field in mem_handler.fields[2] for suffix in ('', LID_suffix)]
            data = {field: mem_handler.get(field).ravel() for field in columns}
            arrays = [pa.array(data[field]) for field in data]

        log.info('Done!')
        query = {'table': table, 'fields': fields, 'LIDs': LIDs, 'IDs': IDs, 'groups': groups,
                 'geometry':geometry, 'sort_by_LID': sort_by_LID, 'double_precision': double_precision}
        return pa.RecordBatch.from_arrays(arrays, columns,
                                          metadata={b'index_names': json.dumps(index_names).encode(),
                                                    b'index': json.dumps(index).encode(),
                                                    b'sorted_by': b'0' if sort_by_LID else b'1',
                                                    b'header': json.dumps(self.header.get_query_header()).encode(),
                                                    b'query': zlib.compress(json.dumps(query).encode())})


def process_field(field, basic_field, table, query_functions, geometry,
                  mem_handler, fields_processed, read_fields,
                  batch_index, LIDs2read_batch, IDs ,LID_combinations_batch):

    if basic_field not in fields_processed:

        if basic_field not in mem_handler:
            mem_handler.add(basic_field)

        if basic_field in table: # Basic field

            if read_fields:
                table[basic_field].read(mem_handler.get(basic_field, batch_index, True), LIDs2read_batch, IDs)

            if LID_combinations_batch:
                combine_load_cases(mem_handler.get(basic_field, batch_index, True),
                                   LID_combinations_batch, mem_handler.get(basic_field, batch_index))

        elif basic_field in query_functions: # Derived field
            func, func_args = query_functions[basic_field]
            args = list()

            for arg in func_args:

                if geometry and arg in geometry:
                    args.append(geometry[arg])
                else:

                    if arg not in mem_handler:
                        mem_handler.add(arg)

                    if arg not in fields_processed:

                        if arg in table: # Basic field

                            if read_fields:
                                table[arg].read(mem_handler.get(arg, batch_index, True), LIDs2read_batch, IDs)

                            if LID_combinations_batch:
                                combine_load_cases(mem_handler.get(arg, batch_index, True),
                                                   LID_combinations_batch, mem_handler.get(arg, batch_index))
                        else: # Derived field
                            process_field(arg, arg, table, query_functions, geometry,
                                          mem_handler, fields_processed, read_fields,
                                          batch_index, LIDs2read_batch, IDs ,LID_combinations_batch)

                        fields_processed.add(arg)

                    args.append(mem_handler.get(arg, batch_index))

            func(*args, mem_handler.get(field, batch_index))
        else:
            raise ValueError(f"Unsupported output: '{basic_field}'")

        fields_processed.add(basic_field)


class MemoryHandler(object):
    """
    Handle memory management in queries.
    """

    def __init__(self, max_memory, LID_suffix, fields, LIDs, IDs, groups=None,
                 dtype=np.float32, n_basic_LIDs=None):
        """
        Initialize a MemoryHandler instance.

        Parameters
        ----------
        max_memory : int
            Memory limit (in bytes).
        LID_suffix : str
            LID column label suffix (i.e. ': LID').
        fields : list of str
            List of fields.
        LIDs : list of int
            List of LIDs.
        IDs : list of int
            List of IDs.
        groups : list of str, optional
            List of group names (for aggregated queries).
        dtype : {numpy.float32, numpy.float64}, optional
            Field dtype. By default single precision is used.
        n_basic_LIDs : int, optional
            Number of basic LIDs (either LIDs not combined or
            combined ones used later by other combinations) to be allocated.
            By default no basic load cases arrays are allocated.
        """

        # Check aggregation options
        self.level = check_aggregation_options(fields, groups)

        # Field processing
        self._arrays = dict()
        self.fields = {0: list(), 1: list(), 2: list()}

        for field in fields:

            for level, subfield in enumerate([field[:match.start()] for match in
                                              re.finditer('-', field)] + [field]):

                if not groups and level == 1:
                    level = 2

                if subfield not in self._arrays or level == self.level:
                    self.fields[level].append(subfield)

                if subfield not in self._arrays:
                    self._arrays[subfield] = list()

                    if level == 2:
                        self._arrays[subfield + LID_suffix] = list()

        self.field_seq = [(field, level) for level in self.fields for field in
                          self.fields[level] if LID_suffix not in field]

        # Batch processing (in case query doesn't fit in memory)
        size_per_LC = len(self.fields[0]) * len(IDs) * np.dtype(dtype).itemsize

        if size_per_LC * len(LIDs) > max_memory:

            if self.level < 2:
                raise MemoryError(f'Requested query exceeds max memory limit ({humansize(max_memory)})!')

            LIDs_per_batch = max_memory // size_per_LC
            self.batches = [slice(i * LIDs_per_batch, (i + 1) * LIDs_per_batch) for i in
                            range(len(LIDs) // LIDs_per_batch)]

            if len(LIDs) % LIDs_per_batch:
                self.batches.append(slice(self.batches[-1].stop,
                                          self.batches[-1].stop + len(LIDs) % LIDs_per_batch))

        else:
            LIDs_per_batch = len(LIDs)
            self.batches = [None]

        # Memory pre-allocation
        self.dtype = dtype
        self.shape = (LIDs_per_batch, len(IDs))
        self.data0 = np.empty((len(self.fields[0]), LIDs_per_batch, len(IDs)), dtype=dtype)

        for i, field in enumerate(self.fields[0]):
            self._arrays[field].append(self.data0[i, :, :])

        if self.level > 0:

            if groups:
                self.data1 = np.empty((len(self.fields[1]), LIDs_per_batch, len(groups)), dtype=dtype)

                for i, field in enumerate(self.fields[1]):
                    self._arrays[field].append(self.data1[i, :, :])
            else:
                groups = IDs

            if self.level == 2:
                self.data2 = np.empty((len(self.fields[2]), 1, len(groups)), dtype=dtype)
                self.LIDs2 = np.empty((len(self.fields[2]), 1, len(groups)), dtype=np.int64)

                for i, field in enumerate(self.fields[2]):
                    self._arrays[field].append(self.data2[i, :, :])
                    self._arrays[field + LID_suffix].append(self.LIDs2[i, :, :])

        # Memory pre-allocation: Basic load cases (used only when combining load cases)
        if n_basic_LIDs:
            self.shape_basic = (n_basic_LIDs, len(IDs))
            self._arrays_basic = {field: np.empty(self.shape_basic, dtype=dtype) for field in self.fields[0]}

    def add(self, field):
        """
        Allocate additional field arrays.

        Parameters
        ----------
        field : str
            Field name.
        """
        self._arrays[field] = [np.empty(self.shape, dtype=self.dtype)]

        try:
            self._arrays_basic[field] = np.empty(self.shape_basic, dtype=self.dtype)
        except AttributeError:
            pass

    def get(self, field, batch=None, basic_field=False):
        """
        Get a view array for the specified field.

        Parameters
        ----------
        field : str
            Field name.
        batch : int, optional
            Batch number. By default a view of the whole array is returned.
        basic_field : bool, optional
            Whether to get a view of the underlying basic field array or not.

        Returns
        -------
        numpy.array
            View array for the specified field.
        """

        if basic_field:

            try:
                return self._arrays_basic[field]
            except AttributeError:
                array = self._arrays[field][0]
        else:
            array = self._arrays[field][0]

        if self.batches[0] and not batch is None:
            return array[:self.batches[batch].stop - self.batches[batch].start, :]
        else:
            return array

    def update(self):
        """
        Copy data to all associated arrays (if any).
        """
        for field in self._arrays:

            for array in self._arrays[field][1:]:
                    array[:] = self._arrays[field][0]

    def __contains__(self, field):
        """
        Check if field is available.
        """
        return field in self._arrays


def check_aggregation_options(fields, groups):
    aggregations_level = None

    for field in fields:
        aggregations = field.split('-')[1:]
        level = len(aggregations)

        if not groups and level == 1:
            level = 2

        if aggregations_level is None:
            aggregations_level = level
        elif level != aggregations_level or level > 2:
            raise ValueError("All aggregations must be one-level (i.e. 'AVG') or two-level (i. e. 'AVG-MAX')")

        if level == 0 and groups:
            raise ValueError(f"A grouped query must be aggregated at least one time: '{field}'")

        if level == 2 and is_abs(aggregations[-1])[0] == 'AVG':
            raise ValueError(f"'AVG' aggregation cannot be applied to LIDs: '{field}'")

        for aggregation in aggregations:
            aggregation = is_abs(aggregation)[0]

            if aggregation not in ('AVG', 'MAX', 'MIN'):
                raise ValueError(f"Unsupported aggregation method: '{aggregation}'")

    return aggregations_level


def combine_load_cases(load_cases, LID_combinations, out):
    """
    Combine load cases.

    Parameters
    ----------
    load_cases : numpy.array
        Basic load cases array (either LIDs not combined or combined ones used later
        by other combinations).
    LID_combinations : list of [int, numpy.array, numpy.array]
        Each component of the list contains the following information:

            [index, indexes, coeffs]

        Where:
            index: Index of the basic combined load case (None for basic non-combined load cases).
            indexes: Indexes of each load case to combine.
            coeffs: Coefficients of each load case to combine.

    out : numpy.array
        Combined load case array.
    """
    from loadit.queries import combine

    for i, (index, indexes, coeffs) in enumerate(LID_combinations):

        if len(coeffs): # Combined load case
            combine(load_cases, indexes, coeffs, out[i, :])

            if index: # Store it in order to be used in the future
                load_cases[index, :] = out[i, :]

        else: # Pure load case (not required to combine)
            out[i, :] = load_cases[index, :]


def aggregate(array, array_agg, aggregation, level, LIDs=None, LIDs_agg=None, weights=None,
              use_previous_agg=False):
    """
    Aggregate array (AVG, MAX or MIN).

    Parameters
    ----------
    array : numpy.array
        Array to be aggregated (one row for each load case).
    array_agg : numpy.array
        Aggregated array.
    aggregation : str {'AVG', 'MAX', 'MIN'}
        Aggregation type.
    level : int {1, 2}
        Aggregation level.
    LIDs : numpy.array, optional
        Array of LIDs related to `array`.
    LIDs_agg : numpy.array, optional
        Array of critical LIDs (only for level = 2).
    weights : numpy.array, optional
        Array of averaging weights (only for aggregation = 'AVG' level = 1).
    use_previous_agg : bool, optional
        Whether to perform level-2 aggregations taking into account
        previous aggregations stored at `array_agg` and `LIDs_agg` or not.
    """
    from loadit.queries import max_load, min_load

    if aggregation == 'AVG':

        if level == 2:
            raise ValueError("'AVG' aggregation cannot be applied to LIDs!")
        else:
            array_agg[:] = np.average(array, 1, weights)

    elif aggregation == 'MAX':

        if level == 2:
            max_load(array, LIDs, use_previous_agg, array_agg, LIDs_agg)
        else:
            array_agg[:] = np.max(array, 1)

    elif aggregation == 'MIN':

        if level == 2:
            min_load(array, LIDs, use_previous_agg, array_agg, LIDs_agg)
        else:
            array_agg[:] = np.min(array, 1)
    else:
        raise ValueError(f"Unsupported aggregation method: '{aggregation}'")


def is_abs(field):
    """
    Check if field is absolute value.

    Parameters
    ----------
    field : str
        Field name.

    Returns
    -------
    (bool, str)
        Whether the field is absolute or not along with the basic field itself.
    """

    if field[:4] == 'ABS(' and field[-1] == ')':
        return field[4:-1], True
    else:
        return field, False


def get_dataframe(record_batch):
    """
    Get a pandas dataframe from query record_batch output.

    Parameters
    ----------
    record_batch : pyarrow.RecordBatch
        RecordBatch queried.

    Returns
    -------
    pandas.DataFrame
        DataFrame queried.
    """
    import pandas as pd
    from loadit.queries import set_index

    df = record_batch.to_pandas()
    index = json.loads(record_batch.schema.metadata[b'index'])
    index_names = json.loads(record_batch.schema.metadata[b'index_names'])

    if len(index) == 1:
        df.index = pd.Index(index[0], name=index_names[0])
    else:
        index0 = np.empty((len(index[0]), len(index[1])), dtype=np.int32)
        index1 = np.empty((len(index[0]), len(index[1])), dtype=np.int32)
        set_index(np.arange(len(index[0]), dtype=np.int32), np.arange(len(index[1]), dtype=np.int32), index0, index1)
        order = 'C' if record_batch.schema.metadata[b'sorted_by'] == b'0' else 'F'
        df.index = pd.MultiIndex(levels=index,
                                 labels=[index0.ravel(order), index1.ravel(order)],
                                 names=index_names)

    return df


def write_query(record_batch, output_file):
    """
    Write query output file (csv or parquet).

    Parameters
    ----------
    record_batch : pyarrow.RecordBatch
        RecordBatch queried.
    output_file : str
        Output file (*.csv, *.xlsx or *.parquet).
    """
    log.info(f"Writing '{output_file}'...")
    _, extension = os.path.splitext(output_file)

    if extension == '.csv':

        with open(output_file, 'w') as f:
            f.write(record_batch.schema.metadata[b'header'].decode() + '\n')
            get_dataframe(record_batch).to_csv(f, index=False)

    elif extension == '.xlsx':
        get_dataframe(record_batch).to_excel(output_file)
    elif extension == '.parquet':
        pq.write_table(pa.Table.from_batches([record_batch]), output_file, version='2.0')
    elif extension == '.db':
        import sqlite3

        with sqlite3.connect(output_file) as conn:
            get_dataframe(record_batch).to_sql(json.loads(zlib.decompress(record_batch.schema.metadata[b'query']))['table'], conn, index=False)

    log.info('Done!')


def truncate_file(file, offset):

    with open(file, 'rb+') as f:
        f.seek(offset)
        f.truncate()


def parse_query(query, parse_files=False):
    """
    Parse query dict.

    Parameters
    ----------
    query : dict
        Un-parsed query.
    parse_files : bool
        Parse additional param files (only when processing a query for the first time).

    Returns
    -------
    dict
        Parsed query.
    """

    if parse_files:

        if query['LIDs'] and isinstance(query['LIDs'], str):

            with open(query['LIDs']) as f:
                rows = list(csv.reader(f))

            if any(len(row) > 1 for row in rows):
                query['LIDs'] = {int(row[0]): [int(value) if i % 2 else float(value) for
                                            i, value in enumerate(row[1:])] for row in rows}
            else:
                query['LIDs'] = [int(row[0]) for row in rows]

        if query['IDs'] and isinstance(query['IDs'], str):

            with open(query['IDs']) as f:
                rows = list(csv.reader(f))

            query['IDs'] = [int(row[0]) for row in rows]

        if query['groups'] and isinstance(query['groups'], str):

            with open(query['groups']) as f:
                rows = list(csv.reader(f))

            query['groups'] = {row[0]: [int(ID) for ID in row[1:]] for row in rows}

        if query['geometry'] and isinstance(query['geometry'], str):

            with open(query['geometry']) as f:
                rows = list(csv.reader(f))

            query['geometry'] = {field: {int(row[0]): float(row[i + 1]) for row in rows} for i, field in
                                enumerate(rows[0][1:])}

    query = {key: value if value or type(value) is bool else None for key, value in query.items()}

    # Convert string dict keys to int keys
    for field in ('LIDs', 'geometry'):

        try:

            if query[field]:

                if field == 'geometry':

                    for geom_param in query[field]:
                        query[field][geom_param] = {int(key): value for key, value in query[field][geom_param].items()}

                else:
                    query[field] = {int(key): value for key, value in query[field].items()}

        except AttributeError:
            pass

    return query


def check_query(query, database_header):
    assertions = {name: {'fields': {field for field, _ in table['columns'][2:]},
                         'query_functions': set(table['query_functions']),
                         'query_geometry': {'weigths'} | set(table['query_geometry']),
                         'LIDs': set(table['LIDs']),
                         'IDs': set(table['IDs'])} for name, table in
                  database_header.tables.items()}

    # table checking
    if query['table'] not in assertions:
        raise ValueError('Invalid table: {}'.format(query['table']))

    # fields checking
    if query['fields']:
        check_aggregation_options(query['fields'], query['groups'])
        basic_fields = {is_abs(field.split('-')[0])[0] for field in query['fields']}
        invalid_fields = [field for field in basic_fields if
                            field not in assertions[query['table']]['fields'] and
                            field not in assertions[query['table']]['query_functions']]

        if invalid_fields:
            raise ValueError('Invalid field/s: {}'.format(', '.join(invalid_fields)))

    # LIDs checking
    if isinstance(query['LIDs'], dict):
        new_LIDs = set()

        for new_LID, seq in query['LIDs'].items():

            if seq:

                if new_LID in assertions[query['table']]['LIDs']:
                    raise ValueError(f'Combined LID already exists: {new_LID}')

                new_LIDs.add(new_LID)

                for coeff in seq[::2]:

                    if not type(coeff) is float:
                        raise TypeError('Coefficient must be a float: {}'.format(query['LIDs'][new_LID]))

                for LID in seq[1::2]:

                    if LID not in assertions[query['table']]['LIDs'] and LID not in new_LIDs:
                        raise ValueError(f'Missing LID: {LID}')

            elif new_LID not in assertions[query['table']]['LIDs']:
                raise ValueError(f'Missing LID: {new_LID}')

    elif query['LIDs']:
        missing_LIDs = {str(LID) for LID in query['LIDs'] if LID not in assertions[query['table']]['LIDs']}

        if missing_LIDs:
            raise ValueError('Missing {}/s: {}'.format(database_header.tables[query['table']]['columns'][0][0],
                                                       ', '.join(missing_LIDs)))

    # IDs and groups checking
    if query['groups']:
        empty_groups = {group for group in query['groups'] if not query['groups'][group]}

        if empty_groups:
            raise ValueError('Empty group/s: {}'.format(', '.join(empty_groups)))

        IDs2read = {ID for IDs in query['groups'].values() for ID in IDs}
    else:
        IDs2read = query['IDs']

    if IDs2read:
        missing_IDs = {str(ID) for ID in IDs2read if ID not in assertions[query['table']]['IDs']}

        if missing_IDs:
            raise ValueError('Missing {}/s: {}'.format(database_header.tables[query['table']]['columns'][1][0],
                                                       ', '.join(missing_IDs)))
    else:
        IDs2read = assertions[query['table']]['IDs']

    # geometry checking
    if query['geometry']:

        for geom_param in query['geometry']:

            if geom_param not in assertions[query['table']]['query_geometry']:
                raise ValueError(f"Invalid geometric parameter: '{geom_param}'")

            missing_IDs = {str(ID) for ID in IDs2read if ID not in query['geometry'][geom_param]}

            if missing_IDs:
                raise ValueError("Missing {}/s in geometry inputs ('{}'): {}".format(database_header.tables[query['table']]['columns'][1][0],
                                                                                     geom_param, ', '.join(missing_IDs)))

            for ID, value in query['geometry'][geom_param].items():

                if not type(value) is float:
                    raise TypeError('Geometry value must be a float!')
