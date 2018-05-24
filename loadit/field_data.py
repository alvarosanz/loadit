import numpy as np
from numba import guvectorize


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
        return np.array(self._LIDs)

    @property
    def IDs(self):
        return np.array(self._IDs)

    def close(self):
        """
        Close mapped files.
        """
        self._data_by_LID = None
        self._data_by_ID = None

    def read(self, LIDs=None, IDs=None, out=None):
        """
        Returns requested field values.

        Parameters
        ----------
        LIDs : list of int or dict of int: [float, int, float, int, ...], optional
            List of requested LIDs. If not provided or None, all LIDs are considered.
            Alternatively, a dict with the LID combinations can be specified as
            follows (d letter stands for derived load case):

                dLID0: [coeff0, LID0, coeff1, LID1, coeff2, LID2, ...]
                dLID1: [coeff0, dLID0, coeff1, LID1, coeff2, LID2, ...]
                LID2: []
                dLID3: [coeff0, dLID0, coeff1, dLID1, coeff2, LID2, ...]

        IDs : list of int, optional
            List of requested IDs. If not provided or None, all IDs are considered.
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

        # Pre-allocate memory if necessary
        if out is None:
            out = np.empty((len(LIDs_queried), len(IDs_queried)), dtype=np.float64)

        # Process LID combination data
        is_combination = isinstance(LIDs, dict)

        if is_combination:
            LIDs_queried = [LID for LID, seq in LIDs.items() if not seq]
            LIDs_requested = set(LIDs_queried)
            LIDs_queried += list({LID for seq in LIDs.values() for _, LID in seq if
                                  LID not in LIDs_requested and LID in self._iLIDs})
            LIDs_combined_used = list({LID for seq in LIDs.values() for _, LID in seq if
                                       LID not in self._iLIDs})
            LIDs_queried_index = {LID: i for i, LID in enumerate(LIDs_queried + LIDs_combined_used)}
            LID_combinations = [(LIDs_queried_index[LID] if LID in LIDs_queried_index else None,
                                 np.array([LIDs_queried_index[LID] for _, LID in seq], dtype=np.int64),
                                 np.array([coeff for coeff, _ in seq], dtype=np.float64)) for LID, seq in LIDs.items()]
            array = np.empty((len(LIDs_queried) + len(LIDs_combined_used), len(IDs_queried)), dtype=np.float64)
        else:
            array = out

        # Read fields mapped files
        if len(LIDs_queried) < len(IDs_queried): # Use LID-ordered mapped file (less disk seeks required)
            iIDs = slice(None) if IDs is None else np.array([self._iIDs[ID] for ID in IDs_queried])

            if self._data_by_LID is None: # Open file (if not already open)
                self._data_by_LID = np.memmap(self.file, dtype=self.dtype, shape=self.shape, mode='r')

            # Read data from mapped file
            for i, LID in enumerate(LIDs_queried):
                array[i, :] = self._data_by_LID[self._iLIDs[LID], :][iIDs]

        else: # Use ID-ordered mapped file (less disk seeks required)
            iLIDs = slice(None) if LIDs is None else np.array([self._iLIDs[LID] for LID in LIDs_queried])

            if self._data_by_ID is None: # Open file (if not already open)
                self._data_by_ID = np.memmap(self.file, dtype=self.dtype, shape=self.shape, mode='r',
                                             offset=self._offset, order='C')

            # Read data from mapped file
            for i, ID in enumerate(IDs_queried):
                array[:len(LIDs_queried), i] = self._data_by_ID[:, self._iIDs[ID]][iLIDs]

        # Combine load cases
        if is_combination:

            for i, (index, indexes, coeffs) in enumerate(LID_combinations):

                if len(coeffs): # Combined load case
                    combine(array, indexes, coeffs, out[i, :])

                    if index: # Store it in order to be used in the future
                        array[index, :] = out[i, :]

                else: # Pure load case (not required to combine)
                    out[i, :] = array[index, :]

        return out

@guvectorize(['(double[:, :], int64[:], double[:], double[:])'],
             '(n, m), (l), (l) -> (m)',
             target='cpu', nopython=True)
def combine(array, indexes, coeffs, out):
    """
    Combine load cases.

    Parameters
    ----------
    array : numpy.ndarray
        Field values (not combined).
    indexes : numpy.ndarray
        Indexes of LIDs to combine.
    coeffs : numpy.ndarray
        Multiplication coefficients.
    out : numpy.ndarray
        Output argument. Combined field values.
    """

    for j in range(array.shape[1]):
        out[j] = 0

    for i in range(len(indexes)):
        index = indexes[i]
        coeff = coeffs[i]

        for j in range(array.shape[1]):
            out[j] += array[index, j] * coeff
