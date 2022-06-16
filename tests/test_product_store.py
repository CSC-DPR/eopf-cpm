import os
import os.path
import shutil
from typing import Any, Optional
from unittest.mock import patch

import fsspec
import hypothesis.strategies as st
import numpy as np
import pytest
import xarray
import zarr
from fsspec.implementations.local import LocalFileSystem
from hypothesis import given
from pytest_lazyfixture import lazy_fixture

from eopf.exceptions import StoreNotOpenError
from eopf.exceptions.warnings import AlreadyOpen
from eopf.product.conveniences import init_product, open_store
from eopf.product.core import EOGroup, EOProduct, EOVariable
from eopf.product.store import (
    EONetCDFStore,
    EOProductStore,
    EOSafeStore,
    EOZarrStore,
    convert,
)
from eopf.product.store.cog import EOCogStore
from eopf.product.store.manifest import ManifestStore
from eopf.product.store.rasterio import EORasterIOAccessor
from eopf.product.store.wrappers import (
    FromAttributesToFlagValueAccessor,
    FromAttributesToVariableAccessor,
)

from .decoder import Netcdfdecoder
from .utils import (
    S3_CONFIG_FAKE,
    S3_CONFIG_REAL,
    assert_contain,
    assert_has_coords,
    couple_combinaison_from,
)

_FILES = {
    "netcdf": "test_ncdf_file_.nc",
    "netcdf0": "test_ncdf_read_file_.nc",
    "netcdf1": "test_ncdf_write_file_.nc",
    "json": "test_metadata_file_.json",
    "zarr": "test_zarr_files_.zarr",
    "zarr0": "test_zarr_read_files_.zarr",
    "zarr1": "test_zarr_write_files_.zarr",
    "cog": "test_cog.cog",
}


# needed because Netcdf4 have no convenience way to test without create a file ...
@pytest.fixture(autouse=True)
def cleanup_files():
    yield
    for file in _FILES.values():
        if os.path.isfile(file):
            try:
                os.remove(file)
            except PermissionError:
                pass
        if os.path.isdir(file):
            shutil.rmtree(file)


@pytest.fixture
def zarr_file(OUTPUT_DIR: str):
    file_name = f"file://{os.path.join(OUTPUT_DIR, _FILES['zarr'])}"
    dims = "_ARRAY_DIMENSIONS"

    root = zarr.open(file_name, mode="w")
    root.attrs["top_level"] = True
    root.create_group("coordinates")

    root["coordinates"].attrs["description"] = "coordinates Data Group"
    root["coordinates"].create_group("grid")
    root["coordinates"].create_group("tie_point")
    xarray.Dataset(
        {"radiance": (("rows", "columns"), np.zeros((2, 3))), "orphan": (("depths", "length"), np.zeros((2, 3)))},
    ).to_zarr(
        store=f"{file_name}/coordinates/grid",
        mode="a",
    )
    xarray.Dataset(
        {"radiance": (("rows", "columns"), np.zeros((2, 3))), "orphan": (("depths", "length"), np.zeros((2, 3)))},
    ).to_zarr(
        store=f"{file_name}/coordinates/tie_point",
        mode="a",
    )

    root.create_group("measurements")
    root["measurements"].attrs["description"] = "measurements Data Group"
    root["measurements"].create_group("geo_position")
    root["measurements"]["geo_position"].create_group("altitude")
    root["measurements"]["geo_position"].create_group("latitude")
    root["measurements"]["geo_position"].create_group("longitude")

    xarray.Dataset(
        {
            "polarian": xarray.DataArray([[12, 4], [3, 8]], attrs={dims: ["rows", "dim2"]}),
            "cartesian": xarray.DataArray([[5, -3], [-55, 66]], attrs={dims: ["rows", "dim2"]}),
        },
    ).to_zarr(store=f"{file_name}/measurements/geo_position/altitude", mode="a")
    xarray.Dataset(
        {
            "polarian": xarray.DataArray([[1, 2], [3, 4]], attrs={dims: ["rows", "dim2"]}),
            "cartesian": xarray.DataArray([[9, 7], [-12, 81]], attrs={dims: ["rows", "dim2"]}),
        },
    ).to_zarr(store=f"{file_name}/measurements/geo_position/latitude", mode="a")
    xarray.Dataset(
        {
            "polarian": xarray.DataArray([[6, 7, 8], [2, 1, -6]], attrs={dims: ["rows", "columns"]}),
            "cartesian": xarray.DataArray([[25, 0, 11], [-5, 72, 44]], attrs={dims: ["rows", "columns"]}),
        },
    ).to_zarr(store=f"{file_name}/measurements/geo_position/longitude", mode="a")
    zarr.consolidate_metadata(root.store)
    return file_name


@pytest.mark.unit
def test_load_product_from_zarr(dask_client_all, zarr_file: str):
    product = EOProduct("a_product", storage=zarr_file)
    with product.open(mode="r"):
        product.load()

    assert product.attrs["top_level"]
    assert product.attributes == product.attrs

    assert_contain(product, "coordinates", EOGroup)
    assert product["coordinates"].attrs["description"] == "coordinates Data Group"
    assert_contain(product.coordinates, "grid", EOGroup, "/coordinates/")
    assert_contain(product.coordinates.grid, "radiance", EOVariable, "/coordinates/grid/")
    assert_contain(product.coordinates.grid, "orphan", EOVariable, "/coordinates/grid/")
    assert_contain(product.coordinates, "tie_point", EOGroup, "/coordinates/")
    assert_contain(product.coordinates.tie_point, "radiance", EOVariable, "/coordinates/tie_point/")
    assert_contain(product.coordinates.tie_point, "orphan", EOVariable, "/coordinates/tie_point/")

    assert_contain(product, "measurements", EOGroup)
    assert product["measurements"].attrs["description"] == "measurements Data Group"
    assert_contain(product.measurements, "geo_position", EOGroup, "/measurements/")
    assert_contain(product.measurements.geo_position, "altitude", EOGroup, "/measurements/geo_position/")
    assert_contain(product.measurements.geo_position, "latitude", EOGroup, "/measurements/geo_position/")
    assert_contain(product.measurements.geo_position, "longitude", EOGroup, "/measurements/geo_position/")

    assert_contain(
        product.measurements.geo_position.altitude,
        "polarian",
        EOVariable,
        "/measurements/geo_position/altitude/",
    )
    coords = ["/coordinates/grid/radiance", "/coordinates/tie_point/radiance"]
    assert_has_coords(product.measurements.geo_position.altitude.polarian, coords)
    assert_contain(
        product.measurements.geo_position.altitude,
        "cartesian",
        EOVariable,
        "/measurements/geo_position/altitude/",
    )
    assert_has_coords(product.measurements.geo_position.altitude.cartesian, coords)

    assert_contain(
        product.measurements.geo_position.latitude,
        "polarian",
        EOVariable,
        "/measurements/geo_position/latitude/",
    )
    assert_has_coords(product.measurements.geo_position.latitude.polarian, coords)
    assert_contain(
        product.measurements.geo_position.latitude,
        "cartesian",
        EOVariable,
        "/measurements/geo_position/latitude/",
    )
    assert_has_coords(product.measurements.geo_position.latitude.cartesian, coords)

    assert_contain(
        product.measurements.geo_position.longitude,
        "polarian",
        EOVariable,
        "/measurements/geo_position/longitude/",
    )
    assert_has_coords(product.measurements.geo_position.longitude.polarian, coords)
    assert_contain(
        product.measurements.geo_position.longitude,
        "cartesian",
        EOVariable,
        "/measurements/geo_position/longitude/",
    )
    assert_has_coords(product.measurements.geo_position.longitude.cartesian, coords)

    with pytest.raises(KeyError):
        assert_contain(product, "measurements/group2", EOGroup)
    with pytest.raises(KeyError):
        assert_contain(product, "measurements/group1/variable_d", EOVariable)

    with (product.open(mode="r"), pytest.raises(TypeError)):
        product.store["an_utem"] = "A_Value"


@pytest.mark.unit
@pytest.mark.parametrize(
    "store, readable, writable, listable, erasable",
    [
        (EOZarrStore(zarr.MemoryStore()), True, True, True, True),
        (EONetCDFStore(_FILES["netcdf"]), True, True, True, True),
        (EORasterIOAccessor("a.jp2"), True, False, True, False),
    ],
)
def test_check_capabilities(store, readable, writable, listable, erasable):
    assert store.is_readable == readable
    assert store.is_writeable == writable
    assert store.is_listable == listable
    assert store.is_erasable == erasable


@pytest.mark.unit
@pytest.mark.parametrize(
    "store, decoder_type",
    [
        (EOZarrStore(zarr.MemoryStore()), zarr.open),
        (EONetCDFStore(_FILES["netcdf"]), Netcdfdecoder),
    ],
)
def test_write_stores(dask_client_all, store: EOProductStore, decoder_type: Any):
    with open_store(store, mode="w"):
        store["a_group"] = EOGroup()
        store.write_attrs("a_group", attrs={"description": "value"})
        store["a_group/a_variable"] = EOVariable(data=[])
        store["coordinates/a_coord"] = EOVariable(data=[1, 2, 3], dims=["a"])

    decoder = decoder_type(store.url, mode="r")
    assert dict(decoder["a_group"].attrs) == {"description": "value"}

    assert decoder["a_group"] is not None
    assert decoder["a_group/a_variable"] is not None


@pytest.mark.unit
@pytest.mark.parametrize(
    "store",
    [
        EOZarrStore(zarr.MemoryStore()),
        EONetCDFStore(_FILES["netcdf"]),
    ],
)
def test_read_stores(dask_client_all, store: EOProductStore):
    with open_store(store, mode="w"):
        store["a_group"] = EOGroup()
        store["a_group/a_variable"] = EOVariable(data=[])

    with open_store(store, mode="r"):
        assert isinstance(store["a_group"], EOGroup)
        assert isinstance(store["a_group/a_variable"], EOVariable)
        assert len(store) == 1
        assert "a_group" in [_ for _ in store]
        with pytest.raises(KeyError):
            store["invalid_key"]


@pytest.mark.unit
def test_abstract_store_cant_be_instantiate():
    with pytest.raises(TypeError):
        EOProductStore("not_instantiable")


@pytest.mark.unit
@pytest.mark.parametrize(
    "store",
    [
        EOZarrStore("a_product"),
        EONetCDFStore(_FILES["netcdf"]),
        EORasterIOAccessor("a.jp2"),
        FromAttributesToVariableAccessor(""),
        FromAttributesToFlagValueAccessor(""),
    ],
)
def test_store_must_be_open_read_method(store: EOProductStore):
    with pytest.raises(StoreNotOpenError):
        store["a_group"]

    with pytest.raises(StoreNotOpenError):
        store.is_group("a_group")

    with pytest.raises(StoreNotOpenError):
        store.is_variable("a_group")

    with pytest.raises(StoreNotOpenError):
        len(store)

    with pytest.raises(StoreNotOpenError):
        store.iter("a_group")

    with pytest.raises(StoreNotOpenError):
        for _ in store:
            continue


@pytest.mark.unit
@pytest.mark.parametrize(
    "store",
    [
        EOZarrStore("a_product"),
        EONetCDFStore(_FILES["netcdf"]),
    ],
)
def test_store_must_be_open_write_method(store):
    with pytest.raises(StoreNotOpenError):
        store["a_group"] = EOGroup(variables={})

    with pytest.raises(StoreNotOpenError):
        store.write_attrs("a_group", attrs={})


@pytest.mark.unit
@pytest.mark.parametrize(
    "store",
    [
        EOZarrStore(_FILES["zarr"]),
        EONetCDFStore(_FILES["netcdf"]),
    ],
)
def test_store_structure(dask_client_all, store: EOProductStore):
    with open_store(store, mode="w"):
        store["a_group"] = EOGroup()
        store["another_one"] = EOGroup()
        store["a_final_one"] = EOGroup()

        assert isinstance(store["a_group"], EOGroup)
        assert isinstance(store["another_one"], EOGroup)
        assert isinstance(store["a_final_one"], EOGroup)

        assert store.is_group("another_one")
        assert not store.is_variable("another_one")


@pytest.mark.unit
@pytest.mark.filterwarnings("error")
@pytest.mark.parametrize(
    "store, exceptions",
    [
        (EOZarrStore(zarr.MemoryStore()), (AlreadyOpen, StoreNotOpenError)),
        (EONetCDFStore(_FILES["netcdf"]), (AlreadyOpen, StoreNotOpenError)),
    ],
)
def test_open_close_already(store, exceptions):
    with open_store(store, mode="w"):
        with pytest.raises(exceptions[0]):
            store.open()
    with pytest.raises(exceptions[1]):
        store.close()


@pytest.mark.unit
@pytest.mark.parametrize(
    "store, formats, results",
    [
        (EOZarrStore(zarr.MemoryStore()), (".zarr", "", ".nc"), (True, True, False)),
        (EONetCDFStore(_FILES["netcdf"]), (".nc", "", ".zarr"), (True, False, False)),
        (ManifestStore(_FILES["json"]), (".nc", ".zarr", ""), (False, False, False)),
    ],
)
def test_guess_read_format(store, formats, results):
    assert len(formats) == len(results)
    for format, res in zip(formats, results):
        assert store.guess_can_read(f"file{format}") == res


@pytest.mark.unit
def test_mtd_store_must_be_open():
    """Given a manifest store, when accessing items inside it without previously opening it,
    the function must raise a StoreNotOpenError error.
    """
    store = ManifestStore(_FILES["json"])
    with pytest.raises(StoreNotOpenError):
        store["a_group"]

    with pytest.raises(StoreNotOpenError):
        for _ in store:
            continue


@pytest.mark.unit
def test_init_manifest_store():
    """Given a manifest store, with an XML file path as URL,
    the manifest's url must match the one given.
    """
    url = "/root/tmp"
    manifest = ManifestStore(url)
    assert manifest.url == url


@pytest.mark.unit
@pytest.mark.parametrize(
    "config, exception_type",
    [
        ({"mapping": {}}, TypeError),
        ({"namespaces": {}}, TypeError),
        ({"namespaces": {}, "mapping": {}}, FileNotFoundError),
    ],
)
def test_open_manifest_store(config: Optional[dict], exception_type: Exception):
    """Given a manifest store, without passing configuration parameters
    the function must raise a MissingConfigurationParameter error.
    """
    store = ManifestStore(_FILES["json"])
    with pytest.raises(exception_type):
        store.open(**config)


@pytest.mark.unit
def test_close_manifest_store():
    """Given a manifest store, when trying to close it while not previously opening it,
    the function must raise a StoreNotOpen error.
    """
    store = ManifestStore(_FILES["json"])
    with pytest.raises(StoreNotOpenError):
        store.close()


_FORMAT = {
    EOZarrStore: "zarr",
    EONetCDFStore: "netcdf",
}


@pytest.mark.unit
@given(
    st.sampled_from(couple_combinaison_from(elements=[EOZarrStore, EONetCDFStore])),
)
def test_convert(dask_client_all, read_write_stores):
    cls_read_store, cls_write_store = read_write_stores
    read_store = cls_read_store(_FILES[f"{_FORMAT[cls_read_store]}0"])
    write_store = cls_write_store(_FILES[f"{_FORMAT[cls_write_store]}1"])

    product = init_product("a_product", storage=read_store)
    with open_store(product, mode="w"):
        product.write()
    new_product = EOProduct("new_one", storage=convert(read_store, write_store))
    with open_store(new_product, mode="r"):
        new_product["measurements"]
        new_product["coordinates"]


@pytest.mark.unit
@pytest.mark.parametrize(
    "store_cls, format_file, params",
    [
        (EORasterIOAccessor, "tiff", {}),
        (EORasterIOAccessor, "jp2", {}),
    ],
)
def test_rasters(dask_client_all, store_cls: type[EORasterIOAccessor], format_file: str, params: dict[str, Any]):
    file_name = f"a_file.{format_file}"
    assert store_cls.guess_can_read(file_name)
    assert not store_cls.guess_can_read("false_format.false")
    raster = store_cls(file_name)

    with patch("rioxarray.open_rasterio") as mock_function:
        data_val = [[1, 2, 3], [3, 4, 5], [6, 7, 8]]
        coord_a = [1, 2, 4]
        coord_b = [14, 5, 7]
        mock_function.return_value = xarray.DataArray(
            data_val,
            coords={
                "a": coord_a,
                "b": coord_b,
            },
        )
        with open_store(raster, mode="r", **params):
            value = raster[""]
            assert isinstance(value, EOGroup)
            assert sum([1 for _ in raster]) == len(raster)

            assert isinstance(value["value"], EOVariable)
            assert np.array_equal(value["value"]._data, data_val)

            assert isinstance(raster["value"], EOVariable)
            assert np.array_equal(value["value"]._data, data_val)
            assert raster.is_variable("value")
            assert not raster.is_group("value")

            assert isinstance(raster["coordinates"], EOGroup)
            assert raster.is_group("coordinates")
            assert not raster.is_variable("coordinates")

            assert isinstance(raster["coordinates"]["a"], EOVariable)
            assert np.array_equal(raster["coordinates"]["a"]._data, coord_a)
            assert np.array_equal(raster["coordinates/a"]._data, coord_a)

            assert isinstance(raster["coordinates"]["b"], EOVariable)
            assert np.array_equal(raster["coordinates"]["b"]._data, coord_b)
            assert np.array_equal(raster["coordinates/b"]._data, coord_b)

            assert len([i for i in raster.iter("coordinates")]) == 2
            assert len([i for i in raster.iter("value")]) == 0

            not_existing_key = "not_existing_key"
            with pytest.raises(KeyError):
                raster[not_existing_key]
            assert not raster.is_group(not_existing_key)
            assert not raster.is_variable(not_existing_key)

        for return_val in [xarray.Dataset(data_vars={"a": xarray.DataArray()}), [xarray.Dataset()]]:
            mock_function.return_value = return_val
            with open_store(raster, mode="r", **params):
                with pytest.raises(NotImplementedError):
                    raster[""]
                with pytest.raises(NotImplementedError):
                    [i for i in raster.iter("")]
                with pytest.raises(NotImplementedError):
                    [i for i in raster.iter("not_implemeted")]

            with pytest.raises(StoreNotOpenError):
                raster.close()


@pytest.mark.unit
@pytest.mark.parametrize(
    "product, fakefilename, open_kwargs",
    [
        (EOProduct("", storage="s3://a_simple_zarr.zarr"), lazy_fixture("zarr_file"), S3_CONFIG_FAKE),
        (
            EOProduct("", storage="zip::s3://a_simple_zarr.zarr"),
            lazy_fixture("zarr_file"),
            dict(s3=S3_CONFIG_FAKE),
        ),
    ],
)
def test_zarr_open_on_different_fs(dask_client_all, product: EOProduct, fakefilename: str, open_kwargs: dict[str, Any]):
    with patch("dask.array.core.get_mapper") as mock_dask:
        with patch("fsspec.get_mapper") as mock_zarr:
            mock_dask.return_value = fsspec.FSMap(fakefilename, LocalFileSystem())
            mock_zarr.return_value = fsspec.FSMap(fakefilename, LocalFileSystem())
            with product.open(storage_options=open_kwargs):
                product.load()
            assert mock_zarr.call_count == 1
            mock_zarr.assert_called_with(product.store.url, **open_kwargs)
            assert mock_dask.call_count == 10


@pytest.mark.real_s3
@pytest.mark.unit
@pytest.mark.parametrize(
    "store, path, open_kwargs",
    [
        (EOZarrStore, "zip::s3://eopf/cpm/test_data/olci_zarr_test.zip", dict(s3=S3_CONFIG_REAL)),
        (EOZarrStore, "s3://eopf/cpm/test_data/olci_zarr_test.zarr/", S3_CONFIG_REAL),
        (EONetCDFStore, "s3://eopf/cpm/test_data/olci_netcdf_test.nc", S3_CONFIG_REAL),
        (
            EOSafeStore,
            "zip::s3://eopf/cpm/test_data/"
            + "S3A_OL_1_EFR____20200101T101517_20200101T101817_20200102T141102_0179_053_179_2520_LN1_O_NT_002.zip",
            dict(s3=S3_CONFIG_REAL),
        ),
    ],
)
def test_read_real_s3(dask_client_all, store: type, path: str, open_kwargs: dict[str, Any]):
    product = EOProduct("s3_test_product", storage=store(path))
    with product.open(storage_options=open_kwargs):
        product.load()


@pytest.mark.unit
@pytest.mark.parametrize(
    "w_store, w_path, w_kwargs",
    [
        (EOZarrStore, "s3://eopf/cpm/test_data/tmp/olci_zarr_test_cpy.zarr/", S3_CONFIG_REAL),
    ],
)
@pytest.mark.real_s3
def test_write_real_s3(dask_client_all, w_store: type, w_path: str, w_kwargs: dict[str, Any]):
    in_store = EOZarrStore("zip::s3://eopf/cpm/test_data/olci_zarr_test.zip")
    out_store = w_store(w_path)
    convert(in_store, out_store, dict(storage_options=dict(s3=S3_CONFIG_REAL)), dict(storage_options=w_kwargs))


@pytest.mark.unit
@pytest.mark.parametrize(
    "store_cls, format_file",
    [
        (EOCogStore, "jp2"),
    ],
)
def test_cog_store(store_cls: type[EOCogStore], format_file: str):
    assert store_cls.guess_can_read("some_file.cogs")
    assert not store_cls.guess_can_read("some_other_file.false")
    assert not store_cls.guess_can_read("some_other_file.cog")
    assert not store_cls.guess_can_read("some_other_file.nc")
    cog = store_cls(_FILES["cog"])
    with pytest.raises(ValueError):
        cog.open(mode="r+")

    data_val = [[1, 2, 3], [3, 4, 5], [6, 7, 8]]
    complex_data_val = [[[1, 2, 3], [3, 4, 5], [6, 7, 8]], [[1, 2, 3], [3, 4, 5], [6, 7, 8]]]
    coord_a = [1, 2, 4]
    coord_b = [5, 6, 7]

    with (
        patch("xarray.DataArray.rio.to_raster") as mock_raster,
        patch("eopf.product.store.EONetCDFStore.__setitem__") as mock_netcdf,
    ):
        with open_store(cog, mode="w"):
            cog["var1"] = EOVariable(data=xarray.DataArray(data_val, coords={"x": coord_a, "y": coord_b}))
            assert mock_raster.call_count == 1
            cog["group0"] = EOGroup(
                variables={"var2": EOVariable(data=xarray.DataArray(data_val, coords={"x": coord_a, "y": coord_b}))},
            )

            cog.write_attrs("", {"a": "b"})
            assert mock_raster.call_count == 2
            assert mock_netcdf.call_count == 0
            cog["var1"] = EOVariable(data=xarray.DataArray(complex_data_val))
            assert mock_raster.call_count == 2
            assert mock_netcdf.call_count == 1
            with pytest.raises(NotImplementedError):
                cog["anything"] = "something"

    with pytest.raises(StoreNotOpenError):
        cog[""]
    with pytest.raises(StoreNotOpenError):
        cog.close()
    with pytest.raises(StoreNotOpenError):
        len(cog)
    with pytest.raises(NotImplementedError):
        cog.open(mode="r")
        cog.write_attrs("", {})
