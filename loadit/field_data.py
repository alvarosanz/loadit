import numpy as np


class FieldData(object):

    def __init__(self, name, dtype, file, LIDs, IDs, iLIDs, iIDs):
        """
        Initialize a FieldData instance.

        Parameters
        ----------
        name : str
            Field name.
        dtype : numpy.dtype
            Field type.
        file : str
            File path.
        LIDs : list of int
            List of LIDs.
        IDs : list of int
            List of IDs.
        iLIDs : dict of int: int
            Dict of LID indexes.
        iIDs : dict of int: int
            Dict of ID indexes.
        """
        self.name = name
        self.dtype = dtype
        self.shape = (len(LIDs), len(IDs))
        self.file = file
        self._LIDs = LIDs
        self._IDs = IDs
        self._data_by_LID = None
        self._data_by_ID = None
        self._iLIDs = iLIDs
        self._iIDs = iIDs
        self._offset = len(LIDs) * len(IDs) * np.dtype(dtype).itemsize

    @property
    def LIDs(self):
        return np.array(self._LIDs, dtype=self._LIDs.dtype)

    @property
    def IDs(self):
        return np.array(self._IDs, dtype=self._IDs.dtype)

    def close(self):
        """
        Close mapped files.
        """
        self._data_by_LID = None
        self._data_by_ID = None

    def read(self, out, LIDs=None, IDs=None):
        """
        Returns requested field values.

        Parameters
        ----------
        LIDs : list of int or dict of int: [float, int, float, int,...], optional
            List of requested LIDs. If not provided or None, all LIDs are considered.
            Alternatively, a dict with the LID combinations can be specified as
            follows (d letter stands for derived load case):

                dLID0: [coeff0, LID0, coeff1, LID1, coeff2, LID2,...]
                dLID1: [coeff0, dLID0, coeff1, LID1, coeff2, LID2,...]
                LID2: []
                dLID3: [coeff0, dLID0, coeff1, dLID1, coeff2, LID2,...]

        IDs : list of int, optional
            List of requested IDs. If not provided or None, all IDs are considered.
        dtype : {numpy.float32, numpy.float64}, optional
            Field dtype. By default single precission is used.
        out : numpy.ndarray, optional
            A location into which the result is stored. If not provided or None, a freshly-allocated array is returned.

        Returns
        -------
        numpy.ndarray
            Field values requested.
        """

        # Request all items if not specified
        LIDs_queried = self._LIDs if LIDs is None else LIDs
        IDs_queried = self._IDs if IDs is None else IDs

        # Read fields mapped files
        if len(LIDs_queried) < len(IDs_queried): # Use LID-ordered mapped file (less disk seeks required)
            iIDs = slice(None) if IDs is None else np.array([self._iIDs[ID] for ID in IDs_queried])

            if self._data_by_LID is None: # Open file (if not already open)
                self._data_by_LID = np.memmap(self.file, dtype=self.dtype, shape=self.shape, mode='r')

            # Read data from mapped file
            for i, LID in enumerate(LIDs_queried):
                out[i, :] = self._data_by_LID[self._iLIDs[LID], :][iIDs]

        else: # Use ID-ordered mapped file (less disk seeks required)
            iLIDs = slice(None) if LIDs is None else np.array([self._iLIDs[LID] for LID in LIDs_queried])

            if self._data_by_ID is None: # Open file (if not already open)
                self._data_by_ID = np.memmap(self.file, dtype=self.dtype, shape=self.shape, mode='r',
                                             offset=self._offset, order='F')

            # Read data from mapped file
            for i, ID in enumerate(IDs_queried):
                out[:, i] = self._data_by_ID[:, self._iIDs[ID]][iLIDs]


