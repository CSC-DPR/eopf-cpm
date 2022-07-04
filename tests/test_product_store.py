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
from eopf.product.store.grib import EOGribAccessor
from eopf.product.store.manifest import ManifestStore
from eopf.product.store.rasterio import EORasterIOAccessor
from eopf.product.store.wrappers import (
    FromAttributesToFlagValueAccessor,
    FromAttributesToVariableAccessor,
)
from eopf.product.store.xml_accessors import XMLAnglesAccessor, XMLTPAccessor

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
    "store_cls",
    [
        EOZarrStore,
        EONetCDFStore,
        EORasterIOAccessor,
        EOSafeStore,
        EOCogStore,
        ManifestStore,
        EORasterIOAccessor,
        EOGribAccessor,
        XMLAnglesAccessor,
        XMLTPAccessor,
        FromAttributesToVariableAccessor,
        FromAttributesToFlagValueAccessor,
    ],
)
def test_store_must_be_open_read_method(store_cls):
    store = store_cls("a_product")
    with pytest.raises(StoreNotOpenError):
        store["a_group"]

    with pytest.raises(StoreNotOpenError):
        store.is_group("a_group")

    with pytest.raises(StoreNotOpenError):
        store.is_variable("a_group")

    with pytest.raises(StoreNotOpenError):
        len(store)

    with pytest.raises(StoreNotOpenError):
        set(store.iter("a_group"))

    with pytest.raises(StoreNotOpenError):
        for _ in store:
            continue

    with pytest.raises(StoreNotOpenError):
        store.close()


@pytest.mark.unit
@pytest.mark.parametrize(
    "store_cls",
    [
        EOZarrStore,
        EONetCDFStore,
        EOSafeStore,
        EOCogStore,
    ],
)
def test_store_must_be_open_write_method(store_cls):
    store = store_cls("a_product")
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
    "store, ok_formats",
    [
        (EOZarrStore, ("", ".zarr")),
        (EONetCDFStore, (".nc",)),
        (EOSafeStore, (".SAFE", ".SEN3")),
        (EOCogStore, (".cogs",)),
        (ManifestStore, ()),
        (EORasterIOAccessor, (".jp2", ".tiff")),
        (EOGribAccessor, (".grib",)),
        (XMLAnglesAccessor, ()),
        (XMLTPAccessor, ()),
    ],
)
def test_guess_read_format(store, ok_formats):
    all_guessed_formats = (
        "",
        ".cog",
        ".cogs",
        ".false",
        ".grib",
        ".jp2",
        ".json",
        ".nc",
        ".SAFE",
        ".SEN3",
        ".tiff",
        ".zarr",
    )
    for format in ok_formats:
        assert format in all_guessed_formats
    for format in all_guessed_formats:
        assert store.guess_can_read(f"file{format}") == (format in ok_formats)


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
def test_zarr_open_on_different_fs(client, product: EOProduct, fakefilename: str, open_kwargs: dict[str, Any]):
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
        (EOCogStore, "s3://eopf/cpm/test_data/OLCI_COG", S3_CONFIG_REAL),
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
    convert(
        in_store,
        out_store,
        dict(storage_options=dict(s3=S3_CONFIG_REAL)),
        dict(storage_options=w_kwargs, mode="a"),
    )


@pytest.mark.unit
@pytest.mark.parametrize(
    "store_cls, format_file",
    [
        (EOCogStore, "cog"),
    ],
)
def test_patch_cog_store(store_cls: type[EOCogStore], format_file: str):
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

    with pytest.raises(NotImplementedError):
        cog.open(mode="r")
        cog.write_attrs("", {})


@pytest.mark.real_s3
@pytest.mark.unit
@pytest.mark.parametrize(
    "store, path, storage_options",
    [
        (EOCogStore, "s3://eopf/cpm/test_data/OLCI_COG", S3_CONFIG_REAL),
    ],
)
def test_s3_reading_cog_store(dask_client_all, store: type, path: str, storage_options: dict[str, Any]):
    print(storage_options)
    cog_store = store(path)
    product = EOProduct("s3_test_product", storage=cog_store)
    with product.open(storage_options=storage_options):
        product.load()
        # Test getitem
        assert isinstance(product["conditions/geometry/altitude"], EOVariable)
        assert isinstance(product["conditions/geometry"], EOGroup)
        with pytest.raises(KeyError):
            product["invalid_key"]
        # Test iter
        expected_top_level_groups = ["conditions", "coordinates", "measurements", "quality"]
        actual_top_level_groups = [str(x) for x in cog_store.iter("")]
        assert expected_top_level_groups == actual_top_level_groups

        # Test len
        assert len(product) == len(expected_top_level_groups)


@pytest.mark.need_files
@pytest.mark.integration
@pytest.mark.parametrize(
    "store, legacy_product_path, write_target",
    [
        (
            EOCogStore,
            lazy_fixture("S3_OL_1_EFR"),
            "data/test_cog",
        ),
    ],
)
def test_convert_cog_store(store, legacy_product_path, write_target):
    safe_store = EOSafeStore(legacy_product_path)
    cog_store = store(write_target)
    # Convert legacy product stored in safe_store to cog_store
    convert(safe_store, cog_store)

    product = EOProduct("cog_product", storage=cog_store)
    with product.open():
        product.load()

    with pytest.raises(ValueError):
        cog_store.open(mode="incorrect_mode")

    # Test is group, is variable
    with product.open(mode="r"):
        assert cog_store.is_group("conditions/geometry")
        assert not cog_store.is_variable("conditions/geometry")
        assert cog_store.is_variable("conditions/geometry/altitude.cog")
        assert not cog_store.is_group("conditions/geometry/altitude.cog")

    # Test getitem
    # Try to get an item when mode is set to write
    with pytest.raises(NotImplementedError):
        cog_store.open(mode="w")
        data = cog_store[""]  # noqa

    # Test if returned output of getitem is EOGroup or EOVariable
    cog_store.open(mode="r")
    assert isinstance(cog_store[""], EOGroup)
    assert isinstance(cog_store["conditions/geometry/altitude"], EOVariable)

    # Try to get an item with an incorrect key
    with pytest.raises(KeyError):
        cog_store["some_incorrect_path"]

    # Test iter, iterate over top level and check results
    expected_top_level_groups = ["conditions", "measurements", "coordinates", "quality"]
    actual_top_level_groups = [group_name for group_name in cog_store.iter("")]
    assert set(expected_top_level_groups).issubset(actual_top_level_groups)
    # Test iter, iterate over an directory, and check variables
    expected_geometry_variables = ["saa", "oaa", "oza", "altitude", "sza"]
    actual_geometry_variables = [variable for variable in cog_store.iter("conditions/geometry")]
    assert set(expected_geometry_variables).issubset(actual_geometry_variables)
    # Try to iterate over an incorrect path
    with pytest.raises(FileNotFoundError):
        incorrect_variables = [idx for idx in cog_store.iter("some_incorrect_path")]  # noqa

    cog_store.close()
    # Test len, when store is opened in writing mode
    with pytest.raises(NotImplementedError):
        cog_store.open(mode="w")
        top_level_len = len(cog_store)  # noqa
        cog_store.close()

    cog_store.open(mode="r")
    assert len(cog_store) == len(expected_top_level_groups)
    cog_store.close()

    # Test setitem

    # Try to set an item when open mode is reading
    with pytest.raises(NotImplementedError):
        cog_store.open(mode="r")
        cog_store["name"] = EOVariable("name", data=[])
        cog_store.close()

    # Try to set an item when item type is not EOV, EOG, DataArray
    with pytest.raises(NotImplementedError):
        cog_store.open(mode="w")
        cog_store["name"] = "incorrect_data_type"

    # Set an empty EOGroup and verify on disk
    import os

    cog_store.open(mode="w")
    cog_store["empty_group"] = EOGroup("empty_group")
    assert os.path.isdir(f"{write_target}/empty_group")
    cog_store.close()

    cog_store.open(mode="r")
    assert cog_store.is_group("empty_group")
    cog_store.close()
    # Set EOV with data, open in write mode and use setitem to write them
    cog_store.open(mode="w")
    a_var, a_data = "a", EOVariable(data=[1, 2, 3])
    b_var, b_data = "b", EOVariable(data=[4, 5, 6])
    c_var, c_data = "c", EOVariable(data=[7, 8, 9])
    cog_store["full_group"] = EOGroup("full_group", variables={a_var: a_data, b_var: b_data, c_var: c_data})
    cog_store.close()
    # Reopen in reading mode and check if groups are correctly written using os.path
    cog_store.open(mode="r")
    assert os.path.isdir(f"{write_target}/full_group")
    assert cog_store.is_group("full_group")
    # Check if variables are written as netcdf files on disk, and stored EOVariable(s)
    for target in ["a", "b", "c"]:
        path = f"full_group/{target}"
        assert isinstance(cog_store[path], EOVariable)
        assert os.path.isfile(f"{write_target}/{path}.nc")
    # Check variables data using standard netcdf4 module, dataset["variable"][:] outputs variable dataarray
    from netCDF4 import Dataset

    var_a_ds = Dataset(f"{write_target}/full_group/a.nc")
    assert (var_a_ds[a_var][:] == a_data._data).all()
    var_b_ds = Dataset(f"{write_target}/full_group/b.nc")
    assert (var_b_ds[b_var][:] == b_data._data).all()
    var_c_ds = Dataset(f"{write_target}/full_group/c.nc")
    assert (var_c_ds[c_var][:] == c_data._data).all()
    cog_store.close()

    # $write_target should be removed
