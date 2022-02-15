from unittest.mock import patch

import pytest
import xarray
import zarr
from pyfakefs.fake_filesystem import FakeFilesystem

from eopf.exceptions import StoreNotOpenError
from eopf.product.core import EOGroup, EOProduct, EOVariable
from eopf.product.store import EOProductStore, EOZarrStore

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
def test_load_product_from_zarr(zarr_file, fs: FakeFilesystem):
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
    with pytest.raises(NotImplementedError):
        product.store["an_utem"] = "A_Value"


@pytest.mark.unit
@pytest.mark.parametrize("_type", [EOZarrStore])
def test_write_stores(fs: FakeFilesystem, _type: type[EOProductStore]):
    store = _type("product_name")

    store.open(mode="w")
    store.add_group("a_group")
    with patch.object(xarray.Dataset, "to_zarr", return_value=None):
        store.add_variables("a_group", xarray.Dataset())
    store.update_attrs("a_group", attrs={"description": "value"})
    store.close()
    z = zarr.open("product_name", mode="r")
    assert fs.isfile("product_name") or fs.isdir("product_name/a_group") or fs.isfile("product_name/a_group")
    assert z["a_group"].attrs == {"description": "value"}

    store.open(mode="r+")
    assert "a_group" in store.listdir()
    assert "/product_name" == store.dir_path()
    store.delete_attr("a_group", "description")
    z = zarr.open("product_name", mode="r")
    assert z["a_group"].attrs == {}
    del store["a_group"]
    assert "a_group" not in store.listdir()
    store.close()


@pytest.mark.unit
@pytest.mark.parametrize("_type", [EOZarrStore])
def test_read_stores(fs: FakeFilesystem, _type: type[EOProductStore]):
    store = _type("a_product")

    store.open(mode="w")
    store.add_group("a_group")
    store.close()

    store.open(mode="r")
    assert store["a_group"] is not None
    assert store.map is None
    assert len(store) == 1
    assert "a_group" in [_ for _ in store]
    with pytest.raises(KeyError):
        store["invalid_key"]
    with pytest.raises(KeyError):
        store.get_data("invalid_key")
    store.close()


@pytest.mark.unit
def test_abstract_store_cant_be_instantiate():
    with pytest.raises(TypeError):
        EOProductStore("not_instantiable")


@pytest.mark.unit
@pytest.mark.parametrize("_type", [EOZarrStore])
def test_store_must_be_open(fs: FakeFilesystem, _type: type[EOProductStore]):
    store = _type("a_product")

    with pytest.raises(StoreNotOpenError):
        del store["a_group"]

    with pytest.raises(StoreNotOpenError):
        store["a_group"]

    with pytest.raises(StoreNotOpenError):
        store.get_data("a_group")

    with pytest.raises(StoreNotOpenError):
        store.add_group("a_group")

    with pytest.raises(StoreNotOpenError):
        store.add_variables("a_group", xarray.Dataset())

    with pytest.raises(StoreNotOpenError):
        store.is_group("a_group")

    with pytest.raises(StoreNotOpenError):
        store.is_variable("a_group")

    with pytest.raises(StoreNotOpenError):
        len(store)

    with pytest.raises(StoreNotOpenError):
        store.iter("a_group")

    with pytest.raises(StoreNotOpenError):
        store.update_attrs("a_group", attrs={})

    with pytest.raises(StoreNotOpenError):
        store.delete_attr("a_group", "attr")

    with pytest.raises(StoreNotOpenError):
        for i in store:
            continue

    for method in ["listdir", "rmdir", "clear", "getsize", "dir_path"]:
        with pytest.raises(StoreNotOpenError):
            getattr(store, method)()


@pytest.mark.unit
@pytest.mark.parametrize("_type", [EOZarrStore])
def test_store_structure(fs: FakeFilesystem, _type: type[EOProductStore]):
    store = _type("a_product")
    store.open(mode="w")
    store.add_group("a_group")
    store.add_group("another_one")
    store.add_group("a_final_one")

    assert store["a_group"] is not None

    assert isinstance(store.listdir(), list)
    assert isinstance(store.listdir("another_one"), list)

    assert isinstance(store.getsize(), int)
    assert isinstance(store.getsize("another_one"), int)

    assert isinstance(store.dir_path(), str)
    assert isinstance(store.dir_path("another_one"), str)

    assert store.is_group("another_one")
    assert not store.is_variable("another_one")

    assert store.rmdir("a_group") is None

    with pytest.raises(KeyError):
        store["a_group"]

    assert store.clear() is None

    with pytest.raises(KeyError):
        store["a_final_one"]
    with pytest.raises(KeyError):
        store["another_one"]

    store.close()
