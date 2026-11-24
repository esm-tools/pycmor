"""Regression checks for masked XIOS overlap weights on the second ocean grid."""
from pathlib import Path
import sys
import unittest
from unittest.mock import patch

import numpy as np
from scipy.sparse import csr_matrix

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'rules'))
import ocean_spherical


class OceanSphericalTests(unittest.TestCase):
    def test_wet_overlap_is_renormalized_for_each_slice_without_extrapolation(self):
        # Three source cells overlap two target cells. The third target is land.
        weights = csr_matrix([[.25, .75, 0], [0, .5, .5], [0, 0, 0]])
        fields = np.array([[4., 8., np.nan], [np.nan, np.nan, 6.], [np.nan, np.nan, np.nan]])
        with patch.object(ocean_spherical, 'matrix', return_value=weights), \
                patch.object(ocean_spherical, 'LAT', np.zeros(1)), \
                patch.object(ocean_spherical, 'LON', np.zeros(3)):
            result = ocean_spherical.map_block(fields, 'weights')
        expected = [[7., 8., np.nan], [np.nan, 6., np.nan], [np.nan, np.nan, np.nan]]
        np.testing.assert_allclose(result[:, 0], expected, equal_nan=True)
