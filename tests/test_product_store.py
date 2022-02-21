import tempfile
from typing import Any

import h5py
import pytest
import xarray
import zarr
from pyfakefs.fake_filesystem import FakeFilesystem

from eopf.exceptions import StoreNotOpenError
from eopf.product.core import EOGroup, EOProduct, EOVariable
from eopf.product.store import EOProductStore, EOZarrStore
from eopf.product.store.hdf5 import EOHDF5Store

from .utils import assert_contain


@pytest.fixture
def zarr_file(fs: FakeFilesystem):
    file_name = "file://test_attributes"
    dims = "_EOPF_DIMENSIONS"
    dims_paths = "_EOPF_DIMENSIONS_PATHS"

    root = zarr.open(file_name, mode="w")
    root.attrs["top_level"] = True
    root.create_group("coordinates")

    root["coordinates"].attrs["description"] = "coordinates Data Group"
    root["coordinates"].create_group("grid")
    root["coordinates"].create_group("tie_point")
    xarray.Dataset({"radiance": ["rows", "columns"], "orphan": ["depths", "length"]}).to_zarr(
        store=f"{file_name}/coordinates/grid",
        mode="a",
    )
    xarray.Dataset({"radiance": ["rows", "columns"], "orphan": ["depths", "length"]}).to_zarr(
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
            "polarian": xarray.DataArray([[12, 4], [3, 8]], attrs={dims: ["radiance"], dims_paths: ["grid"]}),
            "cartesian": xarray.DataArray([[5, -3], [-55, 66]], attrs={dims: ["orphan"], dims_paths: ["tie_point"]}),
        },
    ).to_zarr(store=f"{file_name}/measurements/geo_position/altitude", mode="a")
    xarray.Dataset(
        {
            "polarian": xarray.DataArray([[1, 2], [3, 4]], attrs={dims: ["radiance"], dims_paths: ["grid"]}),
            "cartesian": xarray.DataArray([[9, 7], [-12, 81]], attrs={dims: ["orphan"], dims_paths: ["tie_point"]}),
        },
    ).to_zarr(store=f"{file_name}/measurements/geo_position/latitude", mode="a")
    xarray.Dataset(
        {
            "polarian": xarray.DataArray([[6, 7], [2, 1]], attrs={dims: ["radiance"], dims_paths: ["tie_point"]}),
            "cartesian": xarray.DataArray([[25, 0], [-5, 72]], attrs={dims: ["orphan"], dims_paths: ["grid"]}),
        },
    ).to_zarr(store=f"{file_name}/measurements/geo_position/longitude", mode="a")
    return file_name


@pytest.mark.unit
def test_load_product_from_zarr(zarr_file: str, fs: FakeFilesystem):
    product = EOProduct("a_product", store_or_path_url=zarr_file)
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
    assert isinstance(product.measurements.geo_position.altitude.polarian.coordinates["grid/radiance"], EOVariable)
    assert_contain(
        product.measurements.geo_position.altitude,
        "cartesian",
        EOVariable,
        "/measurements/geo_position/altitude/",
    )
    assert isinstance(product.measurements.geo_position.altitude.cartesian.coordinates["tie_point/orphan"], EOVariable)

    assert_contain(
        product.measurements.geo_position.latitude,
        "polarian",
        EOVariable,
        "/measurements/geo_position/latitude/",
    )
    assert isinstance(product.measurements.geo_position.latitude.polarian.coordinates["grid/radiance"], EOVariable)
    assert_contain(
        product.measurements.geo_position.latitude,
        "cartesian",
        EOVariable,
        "/measurements/geo_position/latitude/",
    )
    assert isinstance(product.measurements.geo_position.latitude.cartesian.coordinates["tie_point/orphan"], EOVariable)

    assert_contain(
        product.measurements.geo_position.longitude,
        "polarian",
        EOVariable,
        "/measurements/geo_position/longitude/",
    )
    assert isinstance(
        product.measurements.geo_position.longitude.polarian.coordinates["tie_point/radiance"],
        EOVariable,
    )
    assert_contain(
        product.measurements.geo_position.longitude,
        "cartesian",
        EOVariable,
        "/measurements/geo_position/longitude/",
    )
    assert isinstance(product.measurements.geo_position.longitude.cartesian.coordinates["grid/orphan"], EOVariable)

    with pytest.raises(KeyError):
        assert_contain(product, "measurements/group2", EOGroup)
    with pytest.raises(KeyError):
        assert_contain(product, "measurements/group1/variable_d", EOVariable)

    with (product.open(mode="r"), pytest.raises(TypeError)):
        product.store["an_utem"] = "A_Value"


@pytest.mark.unit
@pytest.mark.parametrize(
    "store_and_decoder",
    [(EOZarrStore(zarr.MemoryStore()), zarr.open), (EOHDF5Store(tempfile.TemporaryFile()), h5py.File)],
)
def test_write_stores(fs: FakeFilesystem, store_and_decoder: tuple[EOProductStore, Any]):
    store, decoder_type = store_and_decoder

    store.open(mode="w")
    store["a_group"] = EOGroup()
    store.write_attrs("a_group", attrs={"description": "value"})
    store["a_group/a_variable"] = EOVariable(data=[])
    store.close()
    decoder = decoder_type(store.url, mode="r")

    assert dict(decoder["a_group"].attrs) == {"description": "value"}

    assert decoder["a_group"] is not None
    assert decoder["a_group/a_variable"] is not None


@pytest.mark.unit
@pytest.mark.parametrize("store", [EOZarrStore(zarr.MemoryStore()), EOHDF5Store(tempfile.TemporaryFile())])
def test_read_stores(fs: FakeFilesystem, store: EOProductStore):

    store.open(mode="w")
    store["a_group"] = EOGroup()
    store["a_group/a_variable"] = EOVariable(data=[])
    store.close()

    store.open(mode="r")
    assert isinstance(store["a_group"], EOGroup)
    assert isinstance(store["a_group/a_variable"], EOVariable)
    assert len(store) == 1
    assert "a_group" in [_ for _ in store]
    with pytest.raises(KeyError):
        store["invalid_key"]

    store.close()


@pytest.mark.unit
def test_abstract_store_cant_be_instantiate():
    with pytest.raises(TypeError):
        EOProductStore("not_instantiable")


@pytest.mark.unit
@pytest.mark.parametrize("store", [EOZarrStore("a_product"), EOHDF5Store(tempfile.TemporaryFile())])
def test_store_must_be_open(fs: FakeFilesystem, store: EOProductStore):

    with pytest.raises(StoreNotOpenError):
        store["a_group"]

    with pytest.raises(StoreNotOpenError):
        store["a_group"] = EOGroup()

    with pytest.raises(StoreNotOpenError):
        store["a_variable"] = EOVariable()

    with pytest.raises(StoreNotOpenError):
        store.is_group("a_group")

    with pytest.raises(StoreNotOpenError):
        store.is_variable("a_group")

    with pytest.raises(StoreNotOpenError):
        len(store)

    with pytest.raises(StoreNotOpenError):
        store.iter("a_group")

    with pytest.raises(StoreNotOpenError):
        store.write_attrs("a_group", attrs={})

    with pytest.raises(StoreNotOpenError):
        for i in store:
            continue


@pytest.mark.unit
@pytest.mark.parametrize("store", [EOZarrStore("a_product"), EOHDF5Store(tempfile.TemporaryFile())])
def test_store_structure(fs: FakeFilesystem, store: EOProductStore):
    store.open(mode="w")
    store["a_group"] = EOGroup()
    store["another_one"] = EOGroup()
    store["a_final_one"] = EOGroup()

    assert isinstance(store["a_group"], EOGroup)
    assert isinstance(store["another_one"], EOGroup)
    assert isinstance(store["a_final_one"], EOGroup)

    assert store.is_group("another_one")
    assert not store.is_variable("another_one")

    store.close()
