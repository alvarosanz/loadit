from numba import guvectorize
import numpy as np


np.seterr(invalid='ignore') # Ignore nan warnings


@guvectorize(['(int32[:], int32[:], int32[:, :], int32[:, :])'],
             '(n), (m) -> (n, m), (n, m)',
             target='cpu', nopython=True)
def set_index(index0, index1, out0, out1):
    """
    Set index columns as a combination of both.

    Parameters
    ----------
    index0 : numpy.array
        First index column non-combined (i.e. [10, 20, 30]).
    index1 : numpy.array
        Second index column non-combined (i.e. [1, 2]).
    out0 : numpy.array
        First index column combined (i.e. [10, 10, 20, 20, 30, 30]).
    out1 : numpy.array
        Second index column combined (i.e. [1, 2, 1, 2, 1, 2]).
    """

    for i in range(len(index0)):

        for j in range(len(index1)):
            out0[i, j] = index0[i]
            out1[i, j] = index1[j]


@guvectorize(['(float32[:, :], int64[:], boolean, float32[:], int64[:])',
              '(float64[:, :], int64[:], boolean, float64[:], int64[:])'],
             '(n, m), (n), () -> (m), (m)',
             target='cpu', nopython=True)
def max_load(array, LIDs, use_previous_agg, out, LIDs_out):
    """
    Get maximum load case for each item.

    Parameters
    ----------
    array : numpy.array
        Values array (one row for each load case).
    LIDs : numpy.array
        LIDs array.
    use_previous_agg : bool
        Whether to perform aggregation taking into account
        previous aggregation stored at `out` and `LIDs_out` or not.
    out : numpy.array
        Maximum values array.
    LIDs_out : numpy.array
        Maximum LIDs array.
    """

    for j in range(array.shape[1]):

        if not use_previous_agg or array[0, j] > out[j] or np.isnan(out[j]):
            out[j] = array[0, j]
            LIDs_out[j] = LIDs[0]

    for i in range(1, array.shape[0]):

        for j in range(array.shape[1]):

            if array[i, j] > out[j] or np.isnan(out[j]):
                out[j] = array[i, j]
                LIDs_out[j] = LIDs[i]


@guvectorize(['(float32[:, :], int64[:], boolean, float32[:], int64[:])',
              '(float64[:, :], int64[:], boolean, float64[:], int64[:])'],
             '(n, m), (n), () -> (m), (m)',
             target='cpu', nopython=True)
def min_load(array, LIDs, use_previous_agg, out, LIDs_out):
    """
    Get minimum load case for each item.

    Parameters
    ----------
    array : numpy.array
        Values array (one row for each load case).
    LIDs : numpy.array
        LIDs array.
    use_previous_agg : bool
        Whether to perform aggregation taking into account
        previous aggregation stored at `out` and `LIDs_out` or not.
    out : numpy.array
        Minimum values array.
    LIDs_out : numpy.array
        Minimum LIDs array.
    """

    for j in range(array.shape[1]):

        if not use_previous_agg or array[0, j] < out[j] or np.isnan(out[j]):
            out[j] = array[0, j]
            LIDs_out[j] = LIDs[0]

    for i in range(1, array.shape[0]):

        for j in range(array.shape[1]):

            if array[i, j] < out[j] or np.isnan(out[j]):
                out[j] = array[i, j]
                LIDs_out[j] = LIDs[i]


@guvectorize(['(float32[:, :], int64[:], float32[:], float32[:])',
              '(float64[:, :], int64[:], float64[:], float64[:])'],
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


@guvectorize(['(float32[:, :], float32[:, :], float32[:, :], float32[:, :])',
              '(float64[:, :], float64[:, :], float64[:, :], float64[:, :])'],
             '(n, m), (n, m), (n, m) -> (n, m)',
             target='cpu', nopython=True)
def von_mises_2D(sxx, syy, sxy, out):

    for i in range(out.shape[0]):

        for j in range(out.shape[1]):
            out[i, j] = (sxx[i, j] ** 2 + syy[i, j] ** 2 - sxx[i, j] * syy[i, j] + 3 * sxy[i, j] ** 2) ** 0.5


@guvectorize(['(float32[:, :], float32[:, :], float32[:, :], float32[:, :])',
              '(float64[:, :], float64[:, :], float64[:, :], float64[:, :])'],
             '(n, m), (n, m), (n, m) -> (n, m)',
             target='cpu', nopython=True)
def max_ppal_2D(sxx, syy, sxy, out):

    for i in range(out.shape[0]):

        for j in range(out.shape[1]):
            out[i, j] = (sxx[i, j] + syy[i, j]) / 2 + (((sxx[i, j] - syy[i, j]) / 2) ** 2 + sxy[i, j] ** 2) ** 0.5


@guvectorize(['(float32[:, :], float32[:, :], float32[:, :], float32[:, :])',
              '(float64[:, :], float64[:, :], float64[:, :], float64[:, :])'],
             '(n, m), (n, m), (n, m) -> (n, m)',
             target='cpu', nopython=True)
def min_ppal_2D(sxx, syy, sxy, out):

    for i in range(out.shape[0]):

        for j in range(out.shape[1]):
            out[i, j] = (sxx[i, j] + syy[i, j]) / 2 - (((sxx[i, j] - syy[i, j]) / 2) ** 2 + sxy[i, j] ** 2) ** 0.5


@guvectorize(['(float32[:, :], float32[:, :], float32[:, :], float32[:, :])',
              '(float64[:, :], float64[:, :], float64[:, :], float64[:, :])'],
             '(n, m), (n, m), (n, m) -> (n, m)',
             target='cpu', nopython=True)
def max_shear_2D(sxx, syy, sxy, out):

    for i in range(out.shape[0]):

        for j in range(out.shape[1]):
            out[i, j] = (((sxx[i, j] - syy[i, j]) / 2) ** 2 + sxy[i, j] ** 2) ** 0.5


@guvectorize(['(float32[:, :], float32[:], float32[:, :])',
              '(float64[:, :], float64[:], float64[:, :])'],
             '(n, m), (m) -> (n, m)',
             target='cpu', nopython=True)
def stress_2D(value, thickness, out):

    for i in range(out.shape[0]):

        for j in range(out.shape[1]):
            out[i, j] = value[i, j] / thickness[j]


query_functions = {
    'ELEMENT FORCES - QUAD4 (33)': {
        'VonMises': [von_mises_2D, ('NX', 'NY', 'NXY')],
        'MaxPpal': [max_ppal_2D, ('NX', 'NY', 'NXY')],
        'MinPpal': [min_ppal_2D, ('NX', 'NY', 'NXY')],
        'MaxShear': [max_shear_2D, ('NX', 'NY', 'NXY')],
        'sx': [stress_2D, ('NX', 'thickness')],
        'sy': [stress_2D, ('NY', 'thickness')],
        'sxy': [stress_2D, ('NXY', 'thickness')],
        'sVonMises': [von_mises_2D, ('sx', 'sy', 'sxy')],
        'sMaxPpal': [max_ppal_2D, ('sx', 'sy', 'sxy')],
        'sMinPpal': [min_ppal_2D, ('sx', 'sy', 'sxy')],
        'sMaxShear': [max_shear_2D, ('sx', 'sy', 'sxy')],
    },
    'ELEMENT FORCES - TRIA3 (74)': {
        'VonMises': [von_mises_2D, ('NX', 'NY', 'NXY')],
        'MaxPpal': [max_ppal_2D, ('NX', 'NY', 'NXY')],
        'MinPpal': [min_ppal_2D, ('NX', 'NY', 'NXY')],
        'MaxShear': [max_shear_2D, ('NX', 'NY', 'NXY')],
        'sx': [stress_2D, ('NX', 'thickness')],
        'sy': [stress_2D, ('NY', 'thickness')],
        'sxy': [stress_2D, ('NXY', 'thickness')],
        'sVonMises': [von_mises_2D, ('sx', 'sy', 'sxy')],
        'sMaxPpal': [max_ppal_2D, ('sx', 'sy', 'sxy')],
        'sMinPpal': [min_ppal_2D, ('sx', 'sy', 'sxy')],
        'sMaxShear': [max_shear_2D, ('sx', 'sy', 'sxy')],
    },
}

query_geometry = {
    'ELEMENT FORCES - QUAD4 (33)': {
        'thickness',
    },
    'ELEMENT FORCES - TRIA3 (74)': {
        'thickness',
    },
}
