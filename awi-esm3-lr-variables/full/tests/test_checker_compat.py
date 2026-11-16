from types import SimpleNamespace
import unittest

import netCDF4
import numpy as np

from checker_compat import region_label_adapter


class Variable:
    name='sector'
    def __init__(self,values):self.values=values
    def __getitem__(self,key):return self.values[key]


class RegionCompatibilityTests(unittest.TestCase):
    def check(self,values):
        from compliance_checker.cf.cf_1_6 import CF1_6Check
        original=CF1_6Check.check_geographic_region
        checker=SimpleNamespace(section_titles={'6.1':'Region names'})
        dataset=SimpleNamespace(get_variables_by_attributes=lambda **attrs:[Variable(values)])
        return region_label_adapter(original)(checker,dataset)

    def test_character_matrix_validates_every_label_instead_of_crashing(self):
        labels=np.asarray(['atlantic_ocean','pacific_ocean'],dtype='S16')
        results=self.check(labels.view('S1').reshape(2,16))
        self.assertEqual(len(results),2)
        self.assertTrue(all(r.value==(1,1) for r in results))

    def test_invalid_label_still_fails_the_original_region_check(self):
        labels=np.asarray(['atlantic_ocean','invented_ocean'],dtype='S16')
        results=self.check(labels.view('S1').reshape(2,16))
        self.assertEqual([r.value for r in results],[(1,1),(0,1)])
        self.assertIn('invented_ocean',results[1].msgs[0])

    def test_variable_length_strings_are_checked_individually(self):
        results=self.check(np.asarray(['atlantic_ocean','pacific_ocean'],dtype=object))
        self.assertEqual(len(results),2)
        self.assertTrue(all(r.value==(1,1) for r in results))
