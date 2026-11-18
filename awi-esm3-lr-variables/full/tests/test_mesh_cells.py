"""FESOM node cells: median-dual polygons from the mesh triangulation, attached
to every node-grid product whatever route it took (XIOS stream, mesh nodes,
30S-90S subset)."""
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest

import numpy as np
import xarray as xr

from pycmor.std_lib.bounds import dual_cell_vertices
from pycmor.std_lib.files import _attach_mesh_node_cells, _ensure_lat_lon_bounds_and_external_vars, _mesh_node_cells


def patch(flip=()):
    """4 x 4 nodes one degree apart at the equator, two triangles per square."""
    lon, lat = (a.ravel().astype(float) for a in np.meshgrid(np.arange(4), np.arange(4)))
    tri = []
    for j in range(3):
        for i in range(3):
            a, b, c, d = 4 * j + i, 4 * j + i + 1, 4 * (j + 1) + i + 1, 4 * (j + 1) + i
            tri += [[a, b, c], [a, c, d]]
    tri = np.array(tri)
    tri[list(flip)] = tri[list(flip), ::-1]
    return lon, lat, tri


def shoelace(x, y):
    return 0.5 * (x * np.roll(y, -1, 1) - np.roll(x, -1, 1) * y).sum(1)


def distinct(row_lon, row_lat):
    return len({(round(a, 9), round(b, 9)) for a, b in zip(row_lon, row_lat)})


class DualCellTests(unittest.TestCase):
    def test_every_node_lies_in_its_counter_clockwise_cell(self):
        lon, lat, tri = patch()
        lon_b, lat_b = dual_cell_vertices(lon, lat, tri)
        self.assertTrue(np.all((lat_b.min(1) <= lat) & (lat <= lat_b.max(1))))
        self.assertTrue(np.all((lon_b.min(1) <= lon) & (lon <= lon_b.max(1))))
        self.assertTrue(np.all(shoelace(lon_b, lat_b) > 0))
        # The cells tile the patch (3 x 3 degrees, flat enough at the equator).
        self.assertAlmostEqual(shoelace(lon_b, lat_b).sum(), 9.0, places=3)

    def test_boundary_nodes_are_vertices_of_their_own_cell(self):
        lon, lat, tri = patch()
        lon_b, lat_b = dual_cell_vertices(lon, lat, tri)
        boundary = (lon % 3 == 0) | (lat % 3 == 0)
        on_ring = np.array([np.any((lon_b[i] == lon[i]) & (lat_b[i] == lat[i])) for i in range(lon.size)])
        np.testing.assert_array_equal(on_ring, boundary)
        # Interior node 5 has six triangles: 12 vertices, filling the vertex dim.
        self.assertEqual(lon_b.shape[1], 12)
        self.assertEqual(distinct(lon_b[5], lat_b[5]), 12)
        # Corner 3 is in one triangle: two edge midpoints, a centroid and the
        # node, then padded by repeating the node.
        self.assertEqual(distinct(lon_b[3], lat_b[3]), 4)
        self.assertTrue(np.all(lon_b[3, 3:] == lon[3]) and np.all(lat_b[3, 3:] == lat[3]))

    def test_triangle_orientation_does_not_matter(self):
        expected = dual_cell_vertices(*patch())
        got = dual_cell_vertices(*patch(flip=(0, 3, 7, 16)))
        for a, b in zip(got, expected):
            np.testing.assert_allclose(a, b, atol=1e-12)

    def test_a_cell_round_the_pole_reaches_the_pole(self):
        # Node 0 sits 0.03 degrees off the North Pole inside a ring of six
        # nodes at 89.9: its cell contains the pole, so its own latitude is
        # above every edge midpoint and centroid.
        ring = np.arange(6) * 60.0
        lon = np.concatenate([[10.0], ring])
        lat = np.concatenate([[89.97], np.full(6, 89.9)])
        tri = np.array([[0, 1 + k, 1 + (k + 1) % 6] for k in range(6)])
        lon_b, lat_b = dual_cell_vertices(lon, lat, tri)
        self.assertEqual(lat_b[0].max(), 90.0)
        self.assertTrue(lat_b[0].min() <= lat[0] <= lat_b[0].max())
        # The ring reaches the pole along a meridian and leaves along another,
        # neither of them the node's own.
        at = int(np.argmax(lat_b[0]))
        self.assertEqual(lon_b[0, at], lon_b[0, at - 1])
        self.assertNotAlmostEqual(lon_b[0, at - 1], 10.0)

    def test_dateline_cells_stay_on_one_side(self):
        lon, lat, tri = patch()
        lon = lon + 178.5  # columns at 178.5, 179.5, 180.5, 181.5
        lon_b, lat_b = dual_cell_vertices(np.where(lon >= 180, lon - 360, lon), lat, tri)
        self.assertTrue(np.all((lon_b >= -180) & (lon_b < 180)))
        unwrapped = (lon_b - lon[:, None] + 180) % 360 - 180
        self.assertTrue(np.all(np.abs(unwrapped) < 1.0))
        self.assertTrue(np.all((unwrapped.min(1) <= 0) & (unwrapped.max(1) >= 0)))


class Rule(SimpleNamespace):
    def get(self, key, default=None):
        return getattr(self, key, default)


class MeshAttachTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.lon, self.lat, tri = patch()
        self.lon = self.lon - 1.5  # straddle 0 degrees east like a real ocean
        # pyfesom2's mesh.nc: 1-based triangles, and lat_bnds/lon_bnds that
        # must not be used (here: every vertex at the node).
        n = self.lon.size
        mesh = xr.Dataset(
            {
                "lat_bnds": (("ncells", "vertices"), np.repeat(self.lat[:, None], 16, 1)),
                "lon_bnds": (("ncells", "vertices"), np.repeat(self.lon[:, None], 16, 1)),
                "triag_nodes": (("ntriags", "Three"), (tri + 1).astype("int32")),
            },
            coords={"lat": ("ncells", self.lat), "lon": ("ncells", self.lon)},
        )
        self.grid = Path(self.tmp.name) / "mesh.nc"
        mesh.to_netcdf(self.grid)
        self.lon_b, self.lat_b = dual_cell_vertices(self.lon, self.lat, tri)
        self.n = n

    def tearDown(self):
        self.tmp.cleanup()

    def rule(self):
        return Rule(grid_file=str(self.grid), compound_name="ocean.tos.tavg-u-hxy-sea.mon.glb", inputs=[])

    def xios(self, cells=None):
        """An XIOS stream: float32 node positions a little off the mesh's, and
        eight-vertex bounds collapsed to one point."""
        cells = np.arange(self.n) if cells is None else np.asarray(cells)
        lat = self.lat[cells].astype("float32")
        lat[0] += np.float32(0.003)
        lon = self.lon[cells].astype("float32")
        ds = xr.Dataset(
            {
                "tos": (("time", "nod2"), np.ones((2, cells.size), "float32")),
                "bounds_lat": (("nod2", "nvertex"), np.repeat(lat[:, None], 8, 1)),
                "bounds_lon": (("nod2", "nvertex"), np.repeat(lon[:, None], 8, 1)),
            },
            coords={
                "lat": ("nod2", lat, {"units": "degrees_north", "bounds": "bounds_lat"}),
                "lon": ("nod2", lon, {"units": "degrees_east", "bounds": "bounds_lon"}),
            },
        )
        return ds.set_coords(["bounds_lat", "bounds_lon"])

    def check(self, out, cells):
        self.assertEqual(out["latitude"].attrs["bounds"], "vertices_latitude")
        self.assertEqual(out["longitude"].attrs["bounds"], "vertices_longitude")
        self.assertEqual(out["vertices_latitude"].dims, ("nod2", "vertices"))
        self.assertNotIn("bounds_lat", out.variables)
        np.testing.assert_array_equal(out["latitude"].values, self.lat[cells])
        np.testing.assert_allclose(out["vertices_latitude"].values, self.lat_b[cells], atol=1e-12)
        # Longitudes come out in [0, 360] like CMIP7 asks; same places.
        vlon = out["vertices_longitude"].values
        self.assertTrue(np.all((vlon >= 0) & (vlon <= 360)))
        np.testing.assert_allclose((vlon - self.lon_b[cells] + 180) % 360 - 180, 0, atol=1e-9)

    def test_xios_stream_gets_the_mesh_cells(self):
        out = _ensure_lat_lon_bounds_and_external_vars(self.xios(), self.rule())
        self.check(out, np.arange(self.n))

    def test_region_subset_gets_the_same_cells(self):
        cells = [1, 5, 6, 10, 15]
        out = _ensure_lat_lon_bounds_and_external_vars(self.xios(cells), self.rule())
        self.check(out, np.array(cells))

    def test_stream_without_coordinates_gets_them_from_the_mesh(self):
        ds = xr.Dataset({"tos": (("time", "nod2"), np.ones((2, self.n), "float32"))})
        out = _ensure_lat_lon_bounds_and_external_vars(ds, self.rule())
        self.check(out, np.arange(self.n))

    def test_stored_polygons_that_are_right_are_kept_as_stored(self):
        # pyfesom2 starts each ring at another vertex; DARS2's are right
        # apart from that, and HR files must not change because of it.
        lon_b, lat_b = self.lon_b.copy(), self.lat_b.copy()
        k = 12  # node 5 is interior with six triangles
        lon_b[5, :k], lat_b[5, :k] = np.roll(lon_b[5, :k], 3), np.roll(lat_b[5, :k], 3)
        with xr.open_dataset(self.grid) as mesh:
            mesh = mesh.drop_vars(["lon_bnds", "lat_bnds"]).load()
        mesh["lon_bnds"] = (("ncells", "vertices"), np.repeat(self.lon[:, None], 12, 1))
        mesh["lat_bnds"] = (("ncells", "vertices"), np.repeat(self.lat[:, None], 12, 1))
        mesh["lon_bnds"][5] = lon_b[5]
        mesh["lat_bnds"][5] = lat_b[5]
        path = Path(self.tmp.name) / "rotated.nc"
        mesh.to_netcdf(path)
        lat, lon, got_lat_b, got_lon_b = _mesh_node_cells(str(path))
        np.testing.assert_array_equal(got_lon_b[5], lon_b[5])
        np.testing.assert_array_equal(got_lat_b[5], lat_b[5])
        others = np.arange(self.n) != 5
        np.testing.assert_array_equal(got_lat_b[others], self.lat_b[others])

    def test_other_grids_are_left_alone(self):
        ds = self.xios()
        ds = ds.assign_coords(lat=ds["lat"] + np.float32(0.5))
        out, attached = _attach_mesh_node_cells(ds, self.rule())
        self.assertFalse(attached)
        self.assertIn("bounds_lat", out.variables)
        out, attached = _attach_mesh_node_cells(self.xios(), Rule(grid_file=None))
        self.assertFalse(attached)


if __name__ == "__main__":
    unittest.main()
