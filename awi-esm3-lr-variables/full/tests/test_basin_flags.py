"""Basin decoding follows flag semantics and never rewrites the source mask."""

import hashlib
from pathlib import Path
import tempfile
import unittest

import numpy as np
import xarray as xr

from pycmor.std_lib.basins import (
    BASIN_IDS,
    BASIN_NAMES,
    basin_flag_mapping,
    canonical_basin_ids,
)


FLAGS = np.array([1, 2, 3, 10, 11])
ARCTIC_FIRST = "atlantic_ocean pacific_ocean indian_ocean arctic_ocean southern_ocean"
SOUTHERN_FIRST = "atlantic_ocean pacific_ocean indian_ocean southern_ocean arctic_ocean"


class BasinFlagTests(unittest.TestCase):
    def test_internal_codes_are_explicitly_named(self):
        self.assertEqual(BASIN_NAMES, ("atlantic", "pacific", "indian", "arctic", "southern"))
        self.assertEqual(BASIN_IDS, (1, 2, 3, 10, 11))

    def test_both_polar_encodings_have_identical_canonical_result(self):
        # Each row represents the same physical basin order, despite opposite
        # Arctic/Southern source IDs. CORE3 uses 10=Southern and 11=Arctic.
        arctic_first = [11, 3, 2, 1, 10, 11]
        southern_first = [10, 3, 2, 1, 11, 10]
        expected = [11, 3, 2, 1, 10, 11]
        for values, meanings in ((arctic_first, ARCTIC_FIRST), (southern_first, SOUTHERN_FIRST)):
            with self.subTest(meanings=meanings):
                result = canonical_basin_ids(values, FLAGS, meanings)
                np.testing.assert_array_equal(result, expected)
                self.assertEqual(result.ndim, 1)
                self.assertTrue(np.issubdtype(result.dtype, np.integer))
        self.assertEqual(basin_flag_mapping(FLAGS, SOUTHERN_FIRST)[10], "southern")
        self.assertEqual(basin_flag_mapping(FLAGS, SOUTHERN_FIRST)[11], "arctic")

    def test_arbitrary_shuffled_flags_and_normalized_meanings(self):
        flags = np.array([400, -20, 150, 8, 99], dtype=np.int64)
        meanings = " SOUTHERN_OCEAN\tPacific\nAtlantic_ocean ARCTIC indian "
        self.assertEqual(basin_flag_mapping(flags, meanings), {
            400: "southern", -20: "pacific", 150: "atlantic", 8: "arctic", 99: "indian",
        })
        np.testing.assert_array_equal(
            canonical_basin_ids([150, -20, 99, 8, 400, 8], flags, meanings),
            [1, 2, 3, 10, 11, 10],
        )

    def test_integral_floats_and_subset_of_declared_basins_are_valid(self):
        np.testing.assert_array_equal(
            canonical_basin_ids([10.0, 11.0, 10.0], FLAGS.astype(float), SOUTHERN_FIRST),
            [11, 10, 11],
        )
        self.assertEqual(canonical_basin_ids([], FLAGS, ARCTIC_FIRST).size, 0)

    def test_large_source_ids_are_not_rounded_by_float_comparison(self):
        flags = np.array([2**53 + 1, 2, 3, 10, 11], dtype=np.uint64)
        np.testing.assert_array_equal(
            canonical_basin_ids(np.array([2**53 + 1, 10], dtype=np.uint64), flags, ARCTIC_FIRST),
            [1, 10],
        )
        with self.assertRaisesRegex(ValueError, "undeclared"):
            canonical_basin_ids(np.array([float(2**53)]), flags, ARCTIC_FIRST)

    def test_invalid_metadata_is_rejected(self):
        cases = [
            (None, ARCTIC_FIRST, "Missing"),
            (FLAGS, None, "flag_meanings"),
            (FLAGS, "", "flag_meanings"),
            (FLAGS, " \n\t", "flag_meanings"),
            (FLAGS, ["atlantic", "pacific", "indian", "arctic", "southern"], "flag_meanings"),
            (FLAGS, "atlantic pacific indian arctic", "matching lengths"),
            ([1, 2, 3, 10], ARCTIC_FIRST, "matching lengths"),
            ([1, 2, 3, 10], "atlantic pacific indian arctic", "five known basins"),
            ([1, 2, 3, 10, 10], ARCTIC_FIRST, "unique"),
            (FLAGS, "atlantic pacific indian arctic arctic_ocean", "unique"),
            (FLAGS, "atlantic pacific indian arctic antarctic", "five known basins"),
            ([1, 2, 3, 10, 11, 12], ARCTIC_FIRST + " mediterranean", "five known basins"),
            ([1, 2, 3, 10, np.nan], ARCTIC_FIRST, "finite"),
            ([1, 2, 3, 10, np.inf], ARCTIC_FIRST, "finite"),
            ([1, 2, 3, 10, -np.inf], ARCTIC_FIRST, "finite"),
            ([1, 2, 3, 10, 11.2], ARCTIC_FIRST, "integer"),
            (["1", "2", "3", "10", "11"], ARCTIC_FIRST, "numeric integer"),
            ("1 2 3 10 11", ARCTIC_FIRST, "one-dimensional"),
            ([[1, 2, 3, 10, 11]], ARCTIC_FIRST, "one-dimensional"),
            ([True, False, True, False, True], ARCTIC_FIRST, "numeric integer"),
            (np.array([1, 2, 3, 10, 11], dtype=complex), ARCTIC_FIRST, "numeric integer"),
            (np.ma.array(FLAGS, mask=[0, 0, 0, 0, 1]), ARCTIC_FIRST, "masked"),
        ]
        for flags, meanings, message in cases:
            with self.subTest(flags=flags, meanings=meanings):
                with self.assertRaisesRegex(ValueError, message):
                    basin_flag_mapping(flags, meanings)
                with self.assertRaises(ValueError):
                    canonical_basin_ids([1], flags, meanings)

    def test_invalid_node_values_are_rejected(self):
        cases = [
            (None, "Missing"),
            ([1, 0], "undeclared"),
            ([1, 12], "undeclared"),
            ([1, np.nan], "finite"),
            ([1, np.inf], "finite"),
            ([1, -np.inf], "finite"),
            ([1, 10.5], "integer"),
            (["1", "10"], "numeric integer"),
            (np.array([1, 10], dtype=complex), "numeric integer"),
            ([True, False], "numeric integer"),
            ([[1, 10]], "one-dimensional"),
            (1, "one-dimensional"),
            (np.ma.array([1, 10], mask=[0, 1]), "masked"),
        ]
        for values, message in cases:
            with self.subTest(values=values):
                with self.assertRaisesRegex(ValueError, message):
                    canonical_basin_ids(values, FLAGS, ARCTIC_FIRST)

    def test_source_arrays_and_metadata_are_never_mutated(self):
        values = np.array([10, 11, 10, 1], dtype=np.int16)
        flags = FLAGS.copy()
        original_values = values.copy()
        original_flags = flags.copy()
        values.flags.writeable = False
        flags.flags.writeable = False
        result = canonical_basin_ids(values, flags, SOUTHERN_FIRST)
        np.testing.assert_array_equal(result, [11, 10, 11, 1])
        self.assertFalse(np.shares_memory(values, result))
        self.assertFalse(np.shares_memory(flags, result))
        result[:] = -1
        np.testing.assert_array_equal(values, original_values)
        np.testing.assert_array_equal(flags, original_flags)

    def test_file_backed_decode_leaves_source_bytes_and_metadata_unchanged(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "basin_mask_CORE3.nc"
            source = xr.Dataset({"basin": ("node", [10, 3, 2, 1, 11])})
            source.basin.attrs.update(flag_values=FLAGS.copy(), flag_meanings=SOUTHERN_FIRST)
            source.to_netcdf(path)
            digest = hashlib.sha256(path.read_bytes()).hexdigest()
            mtime = path.stat().st_mtime_ns
            with xr.open_dataset(path) as dataset:
                basin = dataset.basin
                result = canonical_basin_ids(
                    basin.values, basin.attrs["flag_values"], basin.attrs["flag_meanings"],
                )
                np.testing.assert_array_equal(result, [11, 3, 2, 1, 10])
                np.testing.assert_array_equal(basin.values, [10, 3, 2, 1, 11])
                np.testing.assert_array_equal(basin.attrs["flag_values"], FLAGS)
                self.assertEqual(basin.attrs["flag_meanings"], SOUTHERN_FIRST)
            self.assertEqual(hashlib.sha256(path.read_bytes()).hexdigest(), digest)
            self.assertEqual(path.stat().st_mtime_ns, mtime)
            with xr.open_dataset(path) as dataset:
                np.testing.assert_array_equal(dataset.basin.values, [10, 3, 2, 1, 11])
                self.assertEqual(dataset.basin.attrs["flag_meanings"], SOUTHERN_FIRST)


if __name__ == "__main__":
    unittest.main()
