import numpy as np
from loadit.field_data import FieldData


class TableData(object):

    def __init__(self, fields, LIDs, IDs):
        """
        Initialize a TableData instance.

        Parameters
        ----------
        fields : list of (str, np.dtype, str)
            List of tuples for each field. Each tuple contains the following info:
                (field name, field type, field file)

        LIDs : list of int
            List of LIDs.
        IDs : list of int
            List of IDs.
        """
        self._LIDs = LIDs
        self._IDs = IDs
        self._iLIDs = {LID: i for i, LID in enumerate(LIDs)}
        self._iIDs = {ID: i for i, ID in enumerate(IDs)}
        self._fields = {name: FieldData(name, dtype, file, LIDs, IDs, self._iLIDs, self._iIDs) for
                        name, dtype, file in fields}

    @property
    def LIDs(self):
        return np.array(self._LIDs)

    @property
    def IDs(self):
        return np.array(self._IDs)

    @property
    def fields(self):
        return list(self._fields)

    def __getitem__(self, field):
        return self._fields[field]

    def __contains__(self, field):
        return field in self._fields

    def close(self):
        """
        Close fields.
        """

        for field in self._fields.values():
            field.close()
