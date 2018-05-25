import os
import re
from pathlib import Path
import csv
import json
import binascii
import shutil
import numpy as np
import pyarrow as pa
from numba import guvectorize
from loadit.table_data import TableData
from loadit.queries import query_functions
from loadit.tables_specs import get_tables_specs
from loadit.database_creation import create_tables, finalize_database, open_table, truncate_file
from loadit.misc import humansize, get_hasher, hash_bytestr


class DatabaseHeader(object):
    """
    Stores database metadata.
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

            # Load database header checksum
            with open(os.path.join(path, f'##header.{self.checksum_method}'), 'rb') as f:
                self.checksum = binascii.hexlify(f.read()).decode()

            self.nbytes = 0
            self.tables = dict()

            # Load tables headers
            for name in self.checksums:

                # Load table header
                with open(os.path.join(os.path.join(path, name), '#header.json')) as f:
                    self.tables[name] = json.load(f)

                # Load LIDs & EIDs and calculate total size in bytes
                for i, (field_name, dtype) in enumerate(self.tables[name]['columns']):
                    file = os.path.join(path, name, field_name + '.bin')
                    self.nbytes += os.path.getsize(file)

                    if i == 0:
                        self.tables[name]['LIDs'] = np.fromfile(file, dtype=dtype).tolist()
                    elif i == 1:
                        self.tables[name]['IDs'] = np.fromfile(file, dtype=dtype).tolist()

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

        # General database info
        if self.project:
            info.append(f'Project: {self.project}')

        info.append(f'Name: {self.name}')
        info.append(f'Version: {self.version}')
        info.append(f'Size: {humansize(self.nbytes)}'.format())
        info.append('')

        # Tables info
        for table in self.tables.values():
            ncols = len(table['columns'])
            info.append(f"Table: '{table['name']}' ({table['columns'][0][0]}: {len(table['LIDs'])}, {table['columns'][1][0]}: {len(table['IDs'])})")
            info.append('   ' + ' '.join(['_' * 6 for i in range(ncols)]))
            info.append('  |' + '|'.join([' ' * 6 for i in range(ncols)]) + '|')
            info.append('  |' + '|'.join([field.center(6) for field, _ in table['columns']]) + '|')
            info.append('  |' + '|'.join(['_' * 6 for i in range(ncols)]) + '|')
            info.append('  |' + '|'.join([' ' * 6 for i in range(ncols)]) + '|')
            info.append('  |' + '|'.join([dtype[1:].center(6) for _, dtype in table['columns']]) + '|')
            info.append('  |' + '|'.join(['_' * 6 for i in range(ncols)]) + '|')
            info.append('')

        # Restore points info
        info.append('Restore points:')

        for i, (batch_name, batch_date, batch_files) in enumerate(self.batches):
            info.append(f"  {i} - '{batch_name}': {batch_date}")

            if detailed:

                for file in batch_files:
                    info.append(f'        {file}')

                info.append('')

        # Summary
        info = '\n'.join(info)

        if print_to_screen:
            print(info)
        else:
            return info


class Database(object):
    """
    Handles a local database.
    """

    def __init__(self, path=None):
        """
        Initialize a Database instance.

        Parameters
        ----------
        path : str, optional
            Database path.
        """
        self.path = path
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

    def check(self, print_to_screen=True):
        """
        Check database integrity.

        Parameters
        ----------
        print_to_screen : bool, optional
            Whether to print to screen or return an string instead.

        Returns
        -------
        str, optional
            Database check integrity results.
        """
        files_corrupted = list()

        # Check tables integrity
        for name, header in self.header.tables.items():

            # Check table fields integrity
            for file, checksum in header['batches'][-1][2].items():

                with open(os.path.join(self.path, name, file), 'rb') as f:

                    if checksum != hash_bytestr(f, get_hasher(self.header.checksum_method)):
                        files_corrupted.append(file)

            # Check table header integrity
            header_file = os.path.join(self.path, name, '#header.json')

            with open(header_file, 'rb') as f:

                if self.header.checksums[header['name']] != hash_bytestr(f, get_hasher(self.header.checksum_method)):
                    files_corrupted.append(header_file)

        # Check database header integrity
        database_header_file = os.path.join(self.path, '##header.json')

        with open(database_header_file, 'rb') as f:

            if self.header.checksum != hash_bytestr(f, get_hasher(self.header.checksum_method)):
                files_corrupted.append(database_header_file)

        # Summary
        info = list()

        if files_corrupted:

            for file in files_corrupted:
                info.append(f"'{file.replace(self.path, '')}' is corrupted!")
        else:
            info.append('Everything is OK!')

        info = '\n'.join(info)

        if print_to_screen:
            print(info)
        else:
            return info

    def create(self, files, database_path, database_name, database_version,
               database_project=None, tables_specs=None, overwrite=False,
               table_generator=None, max_chunk_size=1e8):
        """
        Create a new database from .pch files.

        Parameters
        ----------
        files : list of str
            List of .pch files.
        database_path : str
            Database path.
        database_name : str
            Database name.
        database_version : str
            Database version.
        database_project : str, optional
            Database project.
        tables_specs : dict, optional
            Tables specifications. If not provided or None, default ones are used.
        overwrite : bool, optional
            Whether to rewrite or not an already existing database.
        table_generator : generator, optional
            A generator which yields tables.
        max_chunk_size : int, optional
            Maximum chunk size (in bytes) when dealing with database data files creation.
        """
        Path(database_path).mkdir(parents=True, exist_ok=overwrite)
        print('Creating database ...')
        self.path = database_path
        batches = [['Initial batch', None, [os.path.basename(file) for file in files]]]
        headers, load_cases_info = create_tables(self.path, files, tables_specs,
                                                 table_generator=table_generator)
        finalize_database(self.path, database_name, database_version, database_project,
                          headers, load_cases_info, batches, max_chunk_size)
        self.load()
        print('Database created succesfully!')

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

    def append(self, files, batch_name, table_generator=None, max_chunk_size=1e8):
        """
        Append new results to database. This operation is reversible.

        Parameters
        ----------
        files : list of str
            List of .pch files.
        batch_name : str
            Batch name.
        table_generator : generator, optional
            A generator which yields tables.
        max_chunk_size : int, optional
            Maximum chunk size (in bytes) when dealing with database data files creation.
        """

        if batch_name in {batch_name for batch_name, _, _ in self.header.batches}:
            raise ValueError(f"'{batch_name}' already exists!")

        print('Appending to database ...')

        self._close()

        for header in self.header.tables.values():
            header['path'] = os.path.join(self.path, header['name'])
            open_table(header, new_table=False)

        _, load_cases_info = create_tables(self.path, files, self._get_tables_specs(), self.header.tables,
                                           load_cases_info={name: dict() for name in self.tables},
                                           table_generator=table_generator)

        self.header.batches.append([batch_name, None, [os.path.basename(file) for file in files]])
        finalize_database(self.path, self.header.name, self.header.version, self.header.project,
                          self.header.tables, load_cases_info, self.header.batches, max_chunk_size, self.header.checksum_method)
        self.load()
        print('Database updated succesfully!')

    def restore(self, batch_name, max_chunk_size=1e8):
        """
        Restore database to a previous batch. This operation is not reversible.

        Parameters
        ----------
        batch_name : str
            Batch name.
        max_chunk_size : int, optional
            Maximum chunk size (in bytes) when dealing with database data files creation.
        """
        restore_points = [batch[0] for batch in self.header.batches]

        if batch_name not in restore_points or batch_name == restore_points[-1]:
            raise ValueError(f"'{batch_name}' is not a valid restore point")

        print(f"Restoring database to '{batch_name}' state ...")
        self._close()
        batch_index = 0

        for name, header in self.header.tables.items():

            try:
                index = [batch_name for batch_name, _, _ in header['batches']].index(batch_name)
                batch_index = max(batch_index, index)
                header['batches'] = header['batches'][:index + 1]
                position = header['batches'][index][1]

                for LID in header['LIDs'][position:]:
                    header['LOAD CASES INFO'].pop(LID, None)

                header['LIDs'] = header['LIDs'][:position]

                truncate_file(os.path.join(self.path, name, 'LID.bin'),
                              position * np.dtype(header['columns'][0][1]).itemsize)

                for field, dtype in header['columns'][2:]:
                    truncate_file(os.path.join(self.path, name, field + '.bin'),
                                  position * np.dtype(dtype).itemsize * len(header['IDs']))

                header['path'] = os.path.join(self.path, header['name'])

            except ValueError:
                del self.tables[name]
                shutil.rmtree(self.path, name)

        finalize_database(self.path, self.header.name, self.header.version, self.header.project,
                          {name: self.header.tables[name] for name in self.tables},
                          {name: dict() for name in self.tables},
                          self.header.batches[:batch_index + 1], max_chunk_size, self.header.checksum_method)
        self.load()
        print(f"Database restored to '{batch_name}' state succesfully!")

    def query_from_file(self, file, return_dataframe=True):
        """
        Perform a query from a file.

        Parameters
        ----------
        file : str
            Query file.
        return_dataframe : bool, optional
            Whether to return a pandas dataframe or a pyarrow RecordBatch.

        Returns
        -------
        pandas.DataFrame or pyarrow.RecordBatch
            Data queried.
        """
        return self.query(**parse_query_file(file), return_dataframe=return_dataframe)

    def query(self, table=None, fields=None, LIDs=None, IDs=None, groups=None,
              geometry=None, weights=None, return_dataframe=True, **kwargs):
        """
        Perform a query.

        Parameters
        ----------
        return_dataframe : bool, optional
            Whether to return a pandas dataframe or a pyarrow RecordBatch.

        Returns
        -------
        pandas.DataFrame or pyarrow.RecordBatch
            Data queried.
        """
        if not fields:
            fields = [[name, list()] for name in self.tables[table].names]

        # Group data pre-processing
        if groups:
            IDs = sorted({ID for IDs in groups.values() for ID in IDs})
            iIDs = {ID: i for i, ID in enumerate(IDs)}
            indexes_by_group = {group: np.array([iIDs[ID] for ID in group_IDs]) for
                                group, group_IDs in groups.items()}

            if weights:
                weights_by_group = {group: np.array([weights[ID] for ID in group_IDs]) for
                                    group, group_IDs in groups.items()}

        # Requested LIDs & IDs
        LIDs_queried = self.tables[table]._LIDs if LIDs is None else list(LIDs)
        IDs_queried = self.tables[table]._IDs if IDs is None else IDs

        # Group data pre-processing
        if geometry:
            geometry = {parameter: np.array([geometry[parameter][ID] for ID in IDs_queried]) for
                        parameter in geometry}

        # Memory pre-allocation
        mem_handler = MemoryHandler(fields, LIDs_queried, IDs_queried, groups)

        if mem_handler.level == 2:
            LIDs_queried_array = np.array(LIDs_queried)

        # Process fields
        for field, level in mem_handler.field_seq:

            if field not in mem_handler.completed_fields:

                if level == 0: # Load fields into memory
                    basic_field, is_absolute = is_abs(field)

                    if basic_field in self.tables[table]: # Basic field

                        if is_absolute and basic_field not in mem_handler:
                            mem_handler[basic_field] = self.tables[table][basic_field].read(LIDs, IDs)
                            mem_handler[field] = mem_handler[basic_field]
                        else:
                            self.tables[table][basic_field].read(LIDs, IDs, out=mem_handler[field])
                    else: # Derived field

                        if basic_field in query_functions[table]:
                            func, func_args = query_functions[table][basic_field]
                            args = list()

                            for arg in func_args:

                                if arg in self.tables[table]:

                                    if arg not in mem_handler:
                                        mem_handler[arg] = self.tables[table][arg].read(LIDs, IDs)
                                    elif arg not in mem_handler.completed_fields:
                                        self.tables[table][arg].read(LIDs, IDs, out=mem_handler[arg])
                                        mem_handler.update(arg)

                                    args.append(mem_handler[arg])
                                else:
                                    args.append(geometry[arg])

                            func(*args, mem_handler[field])
                        else:
                            raise ValueError(f"Unsupported output: '{basic_field}'")

                else: # Field aggregation
                    aggregation, is_absolute = is_abs(field.split('-')[-1])
                    array = mem_handler['-'.join(field.split('-')[:-1])]
                    array_agg = mem_handler[field]

                    if level == 1: # 1st level

                        for j, group in enumerate(groups):
                            aggregate(array[:, indexes_by_group[group]],
                                      array_agg[:, j], aggregation, level,
                                      weights_by_group[group] if weights else None)

                    elif level == 2: # 2nd level
                        aggregate(array, array_agg, aggregation, level,
                                  LIDs_queried_array, mem_handler[field + ': LID'])

                if is_absolute:
                    np.abs(mem_handler[field], out=mem_handler[field])

                mem_handler.update(field)

        # DataFrame creation
        if mem_handler.level == 0:
            index_names = [self.header.tables[table]['columns'][0][0],
                           self.header.tables[table]['columns'][1][0]]
            data = mem_handler.data0.reshape((len(fields), len(LIDs_queried) * len(IDs_queried))).T
            columns = mem_handler.fields[0]
        elif mem_handler.level == 1:
            index_names = [self.header.tables[table]['columns'][0][0], 'Group']
            data = mem_handler.data1.reshape((len(fields), len(LIDs_queried) * len(groups))).T
            columns = mem_handler.fields[1]
        else:

            if groups:
                index_names = ['Group']
            else:
                index_names = [self.header.tables[table]['columns'][1][0]]
                groups = IDs_queried

            columns = [field + suffix for field in mem_handler.fields[2] for suffix in ('', ': LID')]
            data = {field: mem_handler[field].ravel() for field in columns}

        if return_dataframe:
            import pandas as pd

            if mem_handler.level == 0:
                index = pd.MultiIndex.from_product([LIDs_queried, IDs_queried], names=index_names)
            elif mem_handler.level == 1:
                index = pd.MultiIndex.from_product([LIDs_queried, list(groups)], names=index_names)
            else:
                index = pd.Index(list(groups), name=index_names[0])

            return pd.DataFrame(data, columns=columns, index=index, copy=False)
        else:

            if mem_handler.level == 0:
                index0 = np.empty((len(LIDs_queried), len(IDs_queried)), dtype=np.int64)
                index1 = np.empty((len(LIDs_queried), len(IDs_queried)), dtype=np.int64)
                set_index(np.array(LIDs_queried), np.array(IDs_queried), index0, index1)
                arrays = [pa.array(index0.ravel()), pa.array(index1.ravel())]
                arrays += [pa.array(data[:, i]) for i in range(len(fields))]
            elif mem_handler.level == 1:
                index0 = np.empty((len(LIDs_queried), len(groups)), dtype=np.int64)
                index1 = np.empty((len(LIDs_queried), len(groups)), dtype=np.int64)
                set_index(np.array(LIDs_queried), np.arange(len(groups), dtype=np.int64), index0, index1)
                arrays = [pa.array(index0.ravel()),
                          pa.DictionaryArray.from_arrays(pa.array(index1.ravel()), pa.array(list(groups)))]
                arrays += [pa.array(data[:, i]) for i in range(len(fields))]
            else:
                index = np.arange(len(groups), dtype=np.int64)
                arrays = [pa.DictionaryArray.from_arrays(pa.array(index.ravel()), pa.array(list(groups)))]
                arrays += [pa.array(data[field]) for field in data]

            return pa.RecordBatch.from_arrays(arrays, index_names + columns,
                                              metadata={b'index_columns': json.dumps(index_names).encode()})


class MemoryHandler(object):
    """
    Handles memory management in queries.
    """

    def __init__(self, fields, LIDs, IDs, groups=None):
        """
        Initialize a MemoryHandler instance.

        Parameters
        ----------
        fields : list of str
            List of fields.
        LIDs : list of int
            List of LIDs.
        IDs : list of int
            List of IDs.
        groups : list of str, optional
            List of group names (for aggregated queries).
        """

        # Check aggregation options
        levels = {field.count('-') for field in fields}
        self.level = levels.pop()

        if not groups and self.level == 1:
            self.level = 2

        if levels or self.level > 2:
            raise ValueError("All aggregations must be one-level (i.e. 'AVG') or two-level (i. e. 'AVG-MAX')")

        if self.level == 0 and groups:
            raise ValueError('A grouped query must be aggregated at least one time!')

        # Field processing
        self._arrays = dict()
        self.fields = {0: list(), 1: list(), 2: list()}
        self.completed_fields = set()

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
                        self._arrays[subfield + ': LID'] = list()

        self.field_seq = [(field, level) for level in self.fields for field in
                          self.fields[level] if ': LID' not in field]

        # Memory pre-allocation
        self.data0 = np.empty((len(self.fields[0]), len(LIDs), len(IDs)), dtype=np.float64)

        for i, field in enumerate(self.fields[0]):
            self._arrays[field].append(self.data0[i, :, :])

        if self.level > 0:

            if groups:
                self.data1 = np.empty((len(self.fields[1]), len(LIDs), len(groups)), dtype=np.float64)

                for i, field in enumerate(self.fields[1]):
                    self._arrays[field].append(self.data1[i, :, :])
            else:
                groups = IDs

            if self.level == 2:
                self.data2 = np.empty((len(self.fields[2]), 1, len(groups)), dtype=np.float64)
                self.LIDs2 = np.empty((len(self.fields[2]), 1, len(groups)), dtype=np.int64)

                for i, field in enumerate(self.fields[2]):
                    self._arrays[field].append(self.data2[i, :, :])
                    self._arrays[field + ': LID'].append(self.LIDs2[i, :, :])

    def update(self, field):
        """
        Copy data from one array to the other associated arrays (if any).
        """

        for array in self._arrays[field][1:]:
                array[:] = self._arrays[field][0]

        self.completed_fields.add(field)

    def __getitem__(self, field):
        """
        Get a view array for the specified field.

        Parameters
        ----------
        field : str
            Field name.

        Returns
        -------
        numpy.array
            View array for the specified field.
        """
        return self._arrays[field][0]

    def __setitem__(self, field, value):
        """
        Copy data to the arrays associated with a field.

        Parameters
        ----------
        field : str
            Field name.
        value : numpy.array
            Field array.
        """

        try:

            for array in self._arrays[field]:
                array[:] = value

        except KeyError:
            self._arrays[field] = [value]

        self.completed_fields.add(field)

    def __contains__(self, field):
        """
        Check if field is available.
        """
        return field in self._arrays


def aggregate(array, array_agg, aggregation, level, LIDs=None, LIDs_agg=None, weights=None):
    """
    Aggregate array (AVG, MAX or MIN).

    Parameters
    ----------
    array : numpy.array
        Array to be aggregated.
    array_agg : numpy.array
        Aggregated array.
    aggregation : str {'AVG', 'MAX', 'MIN'}
        Aggregation type.
    level : int {1, 2}
        Aggregation level.
    LIDs : numpy.array, optional
        Array of LIDs related to 'array'.
    LIDs_agg : numpy.array, optional
        Array of critical LIDs (only for level = 2).
    weights : numpy.array, optional
        Array of averaging weights (only for aggregation = 'AVG' level = 1).
    """
    axis = 2 - level

    if aggregation == 'AVG':

        if axis == 0:
            raise ValueError("'AVG' aggregation cannot be applied to LIDs!")

        array_agg[:] = np.average(array, axis, weights)
    elif aggregation == 'MAX':

        if axis == 0:
            LIDs_agg[:] = LIDs[array.argmax(axis)]

        array_agg[:] = np.max(array, axis)
    elif aggregation == 'MIN':

        if axis == 0:
            LIDs_agg[:] = LIDs[array.argmin(axis)]

        array_agg[:] = np.min(array, axis)
    else:
        raise ValueError(f"Unsupported aggregation method: '{aggregation}'")


@guvectorize(['(int64[:], int64[:], int64[:, :], int64[:, :])'],
             '(n), (m) -> (n, m), (n, m)',
             target='cpu', nopython=True)
def set_index(index0, index1, out0, out1):

    for i in range(len(index0)):

        for j in range(len(index1)):
            out0[i, j] = index0[i]
            out1[i, j] = index1[j]


def is_abs(field_str):

    if field_str[:4] == 'ABS(' and field_str[-1] == ')':
        return field_str[4:-1], True
    else:
        return field_str, False


def parse_query_file(file):

    with open(file) as f:
        query = json.load(f)

    if query['LIDs'] and isinstance(query['LIDs'], str):

        with open(query['LIDs']) as f:
            rows = list(csv.reader(f))

        if any(len(row) > 1 for row in rows):
            query['LIDs'] = {int(row[0]): [[float(row[i]), int(row[i + 1])] for i in range(1, len(row), 2)] for row in rows}
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

    if query['weights'] and isinstance(query['weights'], str):

        with open(query['weights']) as f:
            query['weights'] = {int(row[0]): float(row[1]) for row in csv.reader(f)}

    return parse_query(query)


def parse_query(query):
    query = {key: value if value else None for key, value in query.items()}

    for field in ('LIDs', 'geometry', 'weights'):

        try:

            if query[field]:
                query[field] = {int(key): value for key, value in query[field].items()}

        except AttributeError:
            pass

    return query
