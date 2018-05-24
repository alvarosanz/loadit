import sys


def get_tables_specs():
    tables_specs = {
        'ELEMENT FORCES - ROD (1)': {
            'columns': ['LID', 'EID', 'FX', 'T',],
            'pch_format':[
                [('LID', 'i8'), ('EID', 'i8'), ('FX', 'f4'), ('T', 'f4'),],
            ],
        },
        'ELEMENT FORCES - BEAM (2)': {
            'columns': ['LID', 'EID', 'M1A', 'M2A', 'M1B', 'M2B', 'V1', 'V2', 'FX', 'T', 'WT',],
            'pch_format': [
                [('LID', 'i8'), ('EID', 'i8'), ('', ''), ('', ''), ('M1A', 'f4'), ('M2A', 'f4'), ('V1', 'f4'), ('V2', 'f4'), ('FX', 'f4'), ('T', 'f4'), ('WT', 'f4'),],
                [('', ''), ('', ''), ('', ''), ('', ''), ('', ''), ('', ''), ('', ''), ('', ''), ('', ''),],
                [('', ''), ('', ''), ('', ''), ('', ''), ('', ''), ('', ''), ('', ''), ('', ''), ('', ''),],
                [('', ''), ('', ''), ('', ''), ('', ''), ('', ''), ('', ''), ('', ''), ('', ''), ('', ''),],
                [('', ''), ('', ''), ('', ''), ('', ''), ('', ''), ('', ''), ('', ''), ('', ''), ('', ''),],
                [('', ''), ('', ''), ('', ''), ('', ''), ('', ''), ('', ''), ('', ''), ('', ''), ('', ''),],
                [('', ''), ('', ''), ('', ''), ('', ''), ('', ''), ('', ''), ('', ''), ('', ''), ('', ''),],
                [('', ''), ('', ''), ('', ''), ('', ''), ('', ''), ('', ''), ('', ''), ('', ''), ('', ''),],
                [('', ''), ('', ''), ('', ''), ('', ''), ('', ''), ('', ''), ('', ''), ('', ''), ('', ''),],
                [('', ''), ('', ''), ('', ''), ('', ''), ('', ''), ('', ''), ('', ''), ('', ''), ('', ''),],
                [('', ''), ('', ''), ('M1B', 'f4'), ('M2B', 'f4'), ('', ''), ('', ''), ('', ''), ('', ''), ('', ''),],
            ],
        },
        'ELEMENT FORCES - ELAS1 (11)': {
            'columns': ['LID', 'EID', 'F',],
            'pch_format':[
                [('LID', 'i8'), ('EID', 'i8'), ('F', 'f4'),],
            ],
        },
        'ELEMENT FORCES - ELAS2 (12)': {
            'columns': ['LID', 'EID', 'F',],
            'pch_format':[
                [('LID', 'i8'), ('EID', 'i8'), ('F', 'f4'),],
            ],
        },
        'ELEMENT FORCES - ELAS3 (13)': {
            'columns': ['LID', 'EID', 'F',],
            'pch_format':[
                [('LID', 'i8'), ('EID', 'i8'), ('F', 'f4'),],
            ],
        },
        'ELEMENT FORCES - ELAS4 (14)': {
            'columns': ['LID', 'EID', 'F',],
            'pch_format':[
                [('LID', 'i8'), ('EID', 'i8'), ('F', 'f4'),],
            ],
        },
        'ELEMENT FORCES - QUAD4 (33)': {
            'columns': ['LID', 'EID', 'NX', 'NY', 'NXY', 'MX', 'MY', 'MXY', 'QX', 'QY',],
            'pch_format': [
                [('LID', 'i8'), ('EID', 'i8'), ('NX', 'f4'), ('NY', 'f4'), ('NXY', 'f4'), ('MX', 'f4'), ('MY', 'f4'), ('MXY', 'f4'), ('QX', 'f4'), ('QY', 'f4'), ('', ''),],
            ],
        'ELEMENT FORCES - BAR (34)': {
            'columns': ['LID', 'EID', 'M1A', 'M2A', 'M1B', 'M2B', 'V1', 'V2', 'FX', 'T',],
            'pch_format':[
                [('LID', 'i8'), ('EID', 'i8'), ('M1A', 'f4'), ('M2A', 'f4'), ('M1B', 'f4'), ('M2B', 'f4'), ('V1', 'f4'), ('V2', 'f4'), ('FX', 'f4'), ('T', 'f4'), ('', ''),],
            ],
        },
        },
        'ELEMENT FORCES - TRIA3 (74)': {
            'columns': ['LID', 'EID', 'NX', 'NY', 'NXY', 'MX', 'MY', 'MXY', 'QX', 'QY',],
            'pch_format': [
                [('LID', 'i8'), ('EID', 'i8'), ('NX', 'f4'), ('NY', 'f4'), ('NXY', 'f4'), ('MX', 'f4'), ('MY', 'f4'), ('MXY', 'f4'), ('QX', 'f4'), ('QY', 'f4'), ('', ''),],
            ],
        },
        'ELEMENT FORCES - BARS (100)': {
            'columns': ['LID', 'EID', 'M1A', 'M2A', 'M1B', 'M2B', 'V1', 'V2', 'FX', 'T',],
            'pch_format':[
                [('LID', 'i8'), ('EID', 'i8'), ('', ''), ('M1A', 'f4'), ('M2A', 'f4'), ('V1', 'f4'), ('V2', 'f4'), ('FX', 'f4'), ('T', 'f4'), ('', ''), ('', ''),],
                [('', ''), ('M1B', 'f4'), ('M2B', 'f4'), ('', ''), ('', ''), ('', ''), ('', ''), ('', ''), ('', ''),],
            ],
        },
        'ELEMENT FORCES - BUSH (102)': {
            'columns': ['LID', 'EID', 'FX', 'FY', 'FZ', 'MX', 'MY', 'MZ',],
            'pch_format':[
                [('LID', 'i8'), ('EID', 'i8'), ('FX', 'f4'), ('FY', 'f4'), ('FZ', 'f4'), ('MX', 'f4'), ('MY', 'f4'), ('MZ', 'f4'),],
            ],
        },
    }

    for table_type in tables_specs:
        tables_specs[table_type]['dtypes'] = {name: ('<' if sys.byteorder == 'little' else '>') + dtype for
                                              row in tables_specs[table_type]['pch_format'] for name, dtype in row if name}

    return tables_specs
