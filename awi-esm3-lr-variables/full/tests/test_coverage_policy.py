import unittest

from coverage_policy import regional_global_alternative


class RegionalPolicyTests(unittest.TestCase):
    def setUp(self):
        self.regional = 'ocnBgchem.chl.tavg-ols-hxy-sea.mon.30S-90S'
        self.global_ = 'ocnBgchem.chl.tavg-ols-hxy-sea.mon.glb'
        common = dict(units='kg m-3', standard_name='chlorophyll',
                      dimensions='longitude latitude time osurf',
                      cell_methods='area: mean where sea time: mean',
                      temporal_shape='time-intv')
        self.metadata = {
            self.regional: dict(common, processing_note=(
                'This is a regional subset. If producing the global equivalent, '
                'omit this regional South30 version.')),
            self.global_: dict(common),
        }
        self.candidates = {(self.global_, 2605): [dict(status='READY')]}

    def alternative(self):
        return regional_global_alternative(
            self.regional, 2605, self.metadata, self.candidates)

    def test_explicit_exact_global_alternative(self):
        self.assertEqual(self.alternative(), self.global_)

    def test_missing_global_input_not_covered(self):
        self.candidates[self.global_, 2605][0]['status'] = 'BLOCKED'
        self.assertIsNone(self.alternative())

    def test_other_year_not_covered(self):
        self.candidates = {(self.global_, 2606): [dict(status='READY')]}
        self.assertIsNone(self.alternative())

    def test_other_frequency_or_vertical_shape_not_equivalent(self):
        self.metadata[self.global_]['dimensions'] = 'longitude latitude olevel time'
        self.assertIsNone(self.alternative())

    def test_no_authorizing_note_not_covered(self):
        self.metadata[self.regional]['processing_note'] = 'A regional diagnostic.'
        self.assertIsNone(self.alternative())

    def test_missing_metadata_not_covered(self):
        del self.metadata[self.global_]
        self.assertIsNone(self.alternative())


if __name__ == '__main__':
    unittest.main()
