"""rotate_vector_to_geographic: FESOM rotated-frame vectors to geographic east/north."""

import sys
from pathlib import Path

import dask.array as dsa
import numpy as np
import pytest
import xarray as xr

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO / "examples"))
import custom_steps as cs  # noqa: E402


class _Rule:
    def __init__(self, **kw):
        self._d = kw

    def get(self, key, default=None):
        return self._d.get(key, default)


@pytest.fixture(autouse=True)
def _fresh_caches():
    cs._load_secondary_mf_clear_cache()
    cs._MESH_POINTS_CACHE.clear()
    yield
    cs._load_secondary_mf_clear_cache()
    cs._MESH_POINTS_CACHE.clear()


# Reference: the Python port of FESOM's vector_r2g that reproduces FESOM's
# own io_r2g output to 6e-8 (hfx/rot.py), copied verbatim apart from layout.
def _ref_r2g_matrix(al=50.0, be=15.0, ga=-90.0):
    al, be, ga = np.deg2rad([al, be, ga])
    m = np.empty((3, 3))
    m[0, 0] = np.cos(ga) * np.cos(al) - np.sin(ga) * np.cos(be) * np.sin(al)
    m[0, 1] = np.cos(ga) * np.sin(al) + np.sin(ga) * np.cos(be) * np.cos(al)
    m[0, 2] = np.sin(ga) * np.sin(be)
    m[1, 0] = -np.sin(ga) * np.cos(al) - np.cos(ga) * np.cos(be) * np.sin(al)
    m[1, 1] = -np.sin(ga) * np.sin(al) + np.cos(ga) * np.cos(be) * np.cos(al)
    m[1, 2] = np.cos(ga) * np.sin(be)
    m[2, 0] = np.sin(be) * np.sin(al)
    m[2, 1] = -np.sin(be) * np.cos(al)
    m[2, 2] = np.cos(be)
    return m


_M = _ref_r2g_matrix()


def _ref_g2r(glon, glat):
    xg = np.cos(glat) * np.cos(glon)
    yg = np.cos(glat) * np.sin(glon)
    zg = np.sin(glat)
    xr_ = _M[0, 0] * xg + _M[0, 1] * yg + _M[0, 2] * zg
    yr_ = _M[1, 0] * xg + _M[1, 1] * yg + _M[1, 2] * zg
    zr_ = _M[2, 0] * xg + _M[2, 1] * yg + _M[2, 2] * zg
    return np.arctan2(yr_, xr_), np.arcsin(zr_)


def _ref_r2g(rlon, rlat):
    xr_ = np.cos(rlat) * np.cos(rlon)
    yr_ = np.cos(rlat) * np.sin(rlon)
    zr_ = np.sin(rlat)
    xg = _M[0, 0] * xr_ + _M[1, 0] * yr_ + _M[2, 0] * zr_
    yg = _M[0, 1] * xr_ + _M[1, 1] * yr_ + _M[2, 1] * zr_
    zg = _M[0, 2] * xr_ + _M[1, 2] * yr_ + _M[2, 2] * zr_
    return np.arctan2(yg, xg), np.arcsin(zg)


def _ref_r2g_coeffs(rlon, rlat):
    glon, glat = _ref_r2g(rlon, rlat)

    def apply(tlon, tlat):
        txg = -tlat * np.sin(rlat) * np.cos(rlon) - tlon * np.sin(rlon)
        tyg = -tlat * np.sin(rlat) * np.sin(rlon) + tlon * np.cos(rlon)
        tzg = tlat * np.cos(rlat)
        txr = _M[0, 0] * txg + _M[1, 0] * tyg + _M[2, 0] * tzg
        tyr = _M[0, 1] * txg + _M[1, 1] * tyg + _M[2, 1] * tzg
        tzr = _M[0, 2] * txg + _M[1, 2] * tyg + _M[2, 2] * tzg
        return (
            -np.sin(glon) * txr + np.cos(glon) * tyr,
            -np.sin(glat) * np.cos(glon) * txr - np.sin(glat) * np.sin(glon) * tyr + np.cos(glat) * tzr,
        )

    one, zero = np.ones_like(rlon), np.zeros_like(rlon)
    a11, a21 = apply(one, zero)
    a12, a22 = apply(zero, one)
    return a11, a12, a21, a22


def _random_points(n=500, seed=1):
    rng = np.random.default_rng(seed)
    lon = rng.uniform(-np.pi, np.pi, n)
    lat = np.arcsin(rng.uniform(-0.999, 0.999, n))
    return lon, lat


# ---------------------------------------------------------------- coefficients


def test_matches_rot_py_reference():
    glon, glat = _random_points()
    got = cs._fesom_vector_r2g_coeffs(glon, glat, (50.0, 15.0, -90.0))
    rlon, rlat = _ref_g2r(glon, glat)
    want = _ref_r2g_coeffs(rlon, rlat)
    for g, w in zip(got, want):
        np.testing.assert_allclose(g, w, atol=1e-12)


def test_rotation_about_the_polar_axis_leaves_vectors_alone():
    """beta=0 only spins the grid about the Earth's axis: east stays east."""
    glon, glat = _random_points()
    a11, a12, a21, a22 = cs._fesom_vector_r2g_coeffs(glon, glat, (37.0, 0.0, -20.0))
    np.testing.assert_allclose(a11, 1.0, atol=1e-12)
    np.testing.assert_allclose(a22, 1.0, atol=1e-12)
    np.testing.assert_allclose(a12, 0.0, atol=1e-12)
    np.testing.assert_allclose(a21, 0.0, atol=1e-12)


def test_rotated_east_vector_matches_finite_difference():
    """Known vector: a unit step along rotated east, mapped point by point to
    geographic coordinates, gives the direction the rotation must return."""
    glon, glat = _random_points(200, seed=3)
    keep = np.abs(glat) < np.deg2rad(85)  # away from the geographic poles
    glon, glat = glon[keep], glat[keep]
    rlon, rlat = _ref_g2r(glon, glat)
    keep = np.abs(rlat) < np.deg2rad(85)  # and from the rotated ones
    glon, glat, rlon, rlat = glon[keep], glat[keep], rlon[keep], rlat[keep]
    eps = 1e-6
    for dlon, dlat, col in ((eps / np.cos(rlat), 0.0, 0), (0.0, eps, 1)):
        g2lon, g2lat = _ref_r2g(rlon + dlon, rlat + dlat)
        de = np.angle(np.exp(1j * (g2lon - glon))) * np.cos(glat) / eps
        dn = (g2lat - glat) / eps
        a11, a12, a21, a22 = cs._fesom_vector_r2g_coeffs(glon, glat)
        e, n = (a11, a21) if col == 0 else (a12, a22)
        np.testing.assert_allclose(e, de, atol=1e-5)
        np.testing.assert_allclose(n, dn, atol=1e-5)


def test_round_trip_and_norm():
    glon, glat = _random_points()
    coeffs = cs._fesom_vector_r2g_coeffs(glon, glat)
    rng = np.random.default_rng(7)
    u, v = rng.normal(size=glon.size), rng.normal(size=glon.size)
    ug, vg = cs._rotate_pair(u, v, coeffs)
    np.testing.assert_allclose(np.hypot(ug, vg), np.hypot(u, v), rtol=1e-12)
    assert np.max(np.abs(ug - u)) > 0.1, "the default rotation must change directions somewhere"
    ub, vb = cs._rotate_pair(ug, vg, coeffs, inverse=True)
    np.testing.assert_allclose(ub, u, atol=1e-12)
    np.testing.assert_allclose(vb, v, atol=1e-12)


def test_spherical_centroid_across_the_dateline():
    lon = np.array([179.0, -179.0, 179.5, 10.0, 130.0, -110.0])
    lat = np.array([0.0, 0.0, 1.0, 89.0, 89.0, 89.0])
    tri = np.array([[0, 3], [1, 4], [2, 5]])
    clon, clat = cs._spherical_centroids(lon, lat, tri)
    assert abs(abs(np.rad2deg(clon[0])) - 180.0) < 0.5
    assert abs(np.rad2deg(clat[0]) - 1.0 / 3.0) < 0.01
    assert np.rad2deg(clat[1]) > 89.0  # around the pole, not the mean 89 deg


# ---------------------------------------------------------------- step


def _write_mesh(path):
    """Seven nodes, four triangles; one triangle straddles the dateline and one
    sits next to the geographic pole (not centred on it: east and north are
    undefined at the pole itself)."""
    lon = np.array([179.0, -179.0, 179.5, 10.0, 120.0, -100.0, -30.0])
    lat = np.array([0.0, 0.0, 1.0, 89.0, 89.0, 89.0, 60.0])
    tri = np.array([[1, 2, 3], [4, 5, 6], [4, 6, 7], [3, 4, 7]], dtype=np.int32)  # 1-based
    xr.Dataset(
        {
            "lon": (("ncells",), lon),
            "lat": (("ncells",), lat),
            "triag_nodes": (("ntriags", "Three"), tri),
        }
    ).to_netcdf(path)
    return lon, lat, tri - 1


def _write_component(path, name, hdim, n, nz, seed, lat=None, lon=None):
    rng = np.random.default_rng(seed)
    time = xr.date_range("1852-01-01", periods=3, freq="MS", use_cftime=True)
    shape = (3, n) if nz is None else (3, n, nz)
    dims = ("time", hdim) if nz is None else ("time", hdim, "nz")
    coords = {"time": time}
    if lat is not None:
        coords["lat"] = ((hdim,), lat)
        coords["lon"] = ((hdim,), lon)
    da = xr.DataArray(rng.normal(size=shape), dims=dims, coords=coords, name=name, attrs={"units": "m/s"})
    da.to_dataset().to_netcdf(path)


def _open(path, name):
    return xr.open_mfdataset([str(path)], use_cftime=True)[name]


def _rule(tmp_path, component, partner, mesh, **kw):
    return _Rule(
        vector_component=component,
        vector_partner_path=str(tmp_path),
        vector_partner_pattern=rf"{partner}\.fesom\.1852\.nc",
        vector_partner_variable=partner,
        grid_file=str(mesh),
        **kw,
    )


def test_noop_without_keys(tmp_path):
    da = xr.DataArray(np.ones((2, 3)), dims=("time", "nod2"))
    assert cs.rotate_vector_to_geographic(da, _Rule(grid_file="/nonexistent")) is da


@pytest.mark.parametrize("nz", [None, 4], ids=["2d", "3d"])
def test_nodes(tmp_path, nz):
    mesh = tmp_path / "mesh.nc"
    lon, lat, _ = _write_mesh(mesh)
    _write_component(tmp_path / "unod.fesom.1852.nc", "unod", "nod2", lon.size, nz, 1, lat, lon)
    _write_component(tmp_path / "vnod.fesom.1852.nc", "vnod", "nod2", lon.size, nz, 2, lat, lon)
    u = _open(tmp_path / "unod.fesom.1852.nc", "unod")
    v = _open(tmp_path / "vnod.fesom.1852.nc", "vnod")

    ug = cs.rotate_vector_to_geographic(u, _rule(tmp_path, "x", "vnod", mesh))
    vg = cs.rotate_vector_to_geographic(v, _rule(tmp_path, "y", "unod", mesh))

    for out, src in ((ug, u), (vg, v)):
        assert isinstance(out.data, dsa.Array), "must stay lazy"
        assert out.dims == src.dims and out.name == src.name and out.attrs == src.attrs
        assert out.dtype == src.dtype
        np.testing.assert_array_equal(out["lat"].values, src["lat"].values)

    rlon, rlat = _ref_g2r(np.deg2rad(lon), np.deg2rad(lat))
    a11, a12, a21, a22 = _ref_r2g_coeffs(rlon, rlat)
    bc = (slice(None), slice(None)) + ((None,) if nz else ())
    uv, vv = u.values, v.values
    np.testing.assert_allclose(ug.values, a11[bc[1:]] * uv + a12[bc[1:]] * vv, atol=1e-12)
    np.testing.assert_allclose(vg.values, a21[bc[1:]] * uv + a22[bc[1:]] * vv, atol=1e-12)
    np.testing.assert_allclose(np.hypot(ug.values, vg.values), np.hypot(uv, vv), rtol=1e-12)


def test_elements_use_spherical_centroids(tmp_path):
    mesh = tmp_path / "mesh.nc"
    lon, lat, tri = _write_mesh(mesh)
    ntri = tri.shape[0]
    # (time, elem, nz) as FESOM writes utemp; elem count differs from nodes
    _write_component(tmp_path / "utemp.fesom.1852.nc", "utemp", "elem", ntri, 3, 3)
    _write_component(tmp_path / "vtemp.fesom.1852.nc", "vtemp", "elem", ntri, 3, 4)
    u = _open(tmp_path / "utemp.fesom.1852.nc", "utemp")
    v = _open(tmp_path / "vtemp.fesom.1852.nc", "vtemp")

    ug = cs.rotate_vector_to_geographic(u, _rule(tmp_path, "x", "vtemp", mesh))
    assert isinstance(ug.data, dsa.Array) and ug.dims == ("time", "elem", "nz")

    xyz = np.stack(
        [
            np.cos(np.deg2rad(lat)) * np.cos(np.deg2rad(lon)),
            np.cos(np.deg2rad(lat)) * np.sin(np.deg2rad(lon)),
            np.sin(np.deg2rad(lat)),
        ]
    )
    c = xyz[:, tri].sum(axis=2)  # tri is (ntri, 3)
    c /= np.linalg.norm(c, axis=0)
    clon, clat = np.arctan2(c[1], c[0]), np.arcsin(c[2])
    rlon, rlat = _ref_g2r(clon, clat)
    a11, a12, _, _ = _ref_r2g_coeffs(rlon, rlat)
    want = a11[None, :, None] * u.values + a12[None, :, None] * v.values
    np.testing.assert_allclose(ug.values, want, atol=1e-12)


def test_regular_grid(tmp_path):
    lat = np.array([-60.0, 0.0, 70.0, 85.0])
    lon = np.array([-170.0, -40.0, 0.0, 100.0, 179.5])
    time = xr.date_range("1852-01-01", periods=2, freq="MS", use_cftime=True)
    rng = np.random.default_rng(5)
    for name in ("unod", "vnod"):
        xr.DataArray(
            rng.normal(size=(2, 3, lat.size, lon.size)),
            dims=("time", "nz", "lat", "lon"),
            coords={"time": time, "lat": lat, "lon": lon},
            name=name,
        ).to_dataset().to_netcdf(tmp_path / f"{name}.fesom.gr.1852.nc")
    u = _open(tmp_path / "unod.fesom.gr.1852.nc", "unod")
    v = _open(tmp_path / "vnod.fesom.gr.1852.nc", "vnod")
    rule = _Rule(
        vector_component="y",
        vector_partner_path=str(tmp_path),
        vector_partner_pattern=r"unod\.fesom\.gr\.1852\.nc",
        vector_partner_variable="unod",
    )
    vg = cs.rotate_vector_to_geographic(v, rule)
    glat, glon = np.meshgrid(np.deg2rad(lat), np.deg2rad(lon), indexing="ij")
    _, _, a21, a22 = _ref_r2g_coeffs(*_ref_g2r(glon, glat))
    np.testing.assert_allclose(vg.values, a21 * u.values + a22 * v.values, atol=1e-12)


def test_wrong_mesh_is_refused(tmp_path):
    mesh = tmp_path / "mesh.nc"
    lon, lat, _ = _write_mesh(mesh)
    _write_component(tmp_path / "uice.fesom.1852.nc", "uice", "nod2", lon.size, None, 1, lat + 5.0, lon)
    _write_component(tmp_path / "vice.fesom.1852.nc", "vice", "nod2", lon.size, None, 2, lat + 5.0, lon)
    u = _open(tmp_path / "uice.fesom.1852.nc", "uice")
    with pytest.raises(ValueError, match="wrong mesh"):
        cs.rotate_vector_to_geographic(u, _rule(tmp_path, "x", "vice", mesh))


def test_euler_angles_from_rule(tmp_path):
    """beta_euler=0 makes the rotation the identity: the step must read it."""
    mesh = tmp_path / "mesh.nc"
    lon, lat, _ = _write_mesh(mesh)
    _write_component(tmp_path / "uice.fesom.1852.nc", "uice", "nod2", lon.size, None, 1)
    _write_component(tmp_path / "vice.fesom.1852.nc", "vice", "nod2", lon.size, None, 2)
    u = _open(tmp_path / "uice.fesom.1852.nc", "uice")
    rule = _rule(tmp_path, "x", "vice", mesh, alpha_euler=10.0, beta_euler=0.0, gamma_euler=5.0)
    np.testing.assert_allclose(cs.rotate_vector_to_geographic(u, rule).values, u.values, atol=1e-12)
