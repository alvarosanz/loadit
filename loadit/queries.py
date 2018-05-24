from numba import guvectorize


@guvectorize(['(double[:, :], double[:, :], double[:, :], double[:, :])'],
             '(n, m), (n, m), (n, m) -> (n, m)',
             target='cpu', nopython=True)
def von_mises_2D(sxx, syy, sxy, out):

    for i in range(out.shape[0]):

        for j in range(out.shape[1]):
            out[i, j] = (sxx[i, j] ** 2 + syy[i, j] ** 2 - sxx[i, j] * syy[i, j] + 3 * sxy[i, j] ** 2) ** 0.5


@guvectorize(['(double[:, :], double[:, :], double[:, :], double[:, :])'],
             '(n, m), (n, m), (n, m) -> (n, m)',
             target='cpu', nopython=True)
def max_ppal_2D(sxx, syy, sxy, out):

    for i in range(out.shape[0]):

        for j in range(out.shape[1]):
            out[i, j] = (sxx[i, j] + syy[i, j]) / 2 + (((sxx[i, j] - syy[i, j]) / 2) ** 2 + sxy[i, j] ** 2) ** 0.5


@guvectorize(['(double[:, :], double[:, :], double[:, :], double[:, :])'],
             '(n, m), (n, m), (n, m) -> (n, m)',
             target='cpu', nopython=True)
def min_ppal_2D(sxx, syy, sxy, out):

    for i in range(out.shape[0]):

        for j in range(out.shape[1]):
            out[i, j] = (sxx[i, j] + syy[i, j]) / 2 - (((sxx[i, j] - syy[i, j]) / 2) ** 2 + sxy[i, j] ** 2) ** 0.5


@guvectorize(['(double[:, :], double[:, :], double[:, :], double[:, :])'],
             '(n, m), (n, m), (n, m) -> (n, m)',
             target='cpu', nopython=True)
def max_shear_2D(sxx, syy, sxy, out):

    for i in range(out.shape[0]):

        for j in range(out.shape[1]):
            out[i, j] = (((sxx[i, j] - syy[i, j]) / 2) ** 2 + sxy[i, j] ** 2) ** 0.5


@guvectorize(['(double[:, :], double[:], double[:, :])'],
             '(n, m), (m) -> (n, m)',
             target='cpu', nopython=True)
def stress_2D(value, thickness, out):

    for i in range(out.shape[0]):

        for j in range(out.shape[1]):
            out[i, j] = value[i, j] / thickness[j]


query_functions = {
    'ELEMENT FORCES - QUAD4 (33)': {
        'sx': [stress_2D, ('NX', 'THK')],
        'sy': [stress_2D, ('NY', 'THK')],
        'sxy': [stress_2D, ('NXY', 'THK')],
        'VonMises': [von_mises_2D, ('NX', 'NY', 'NXY')],
        'MaxPpal': [max_ppal_2D, ('NX', 'NY', 'NXY')],
        'MinPpal': [min_ppal_2D, ('NX', 'NY', 'NXY')],
        'MaxShear': [max_shear_2D, ('NX', 'NY', 'NXY')],
    },
    'ELEMENT FORCES - TRIA3 (74)': {
        'sx': [stress_2D, ('NX', 'THK')],
        'sy': [stress_2D, ('NY', 'THK')],
        'sxy': [stress_2D, ('NXY', 'THK')],
        'VonMises': [von_mises_2D, ('NX', 'NY', 'NXY')],
        'MaxPpal': [max_ppal_2D, ('NX', 'NY', 'NXY')],
        'MinPpal': [min_ppal_2D, ('NX', 'NY', 'NXY')],
        'MaxShear': [max_shear_2D, ('NX', 'NY', 'NXY')],
    },
}
