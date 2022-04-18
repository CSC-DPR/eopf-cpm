import os
import os.path
import pathlib
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
from eopf.product.store import EONetCDFStore, EOProductStore, EOZarrStore, convert
from eopf.product.store.flags import EOFlagAccessor
from eopf.product.store.manifest import ManifestStore
from eopf.product.store.rasterio import EORasterIOAccessor

from .decoder import Netcdfdecoder
from .utils import (
    S3_CONFIG,
    assert_contain,
    assert_has_coords,
    assert_issubdict,
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
}


# needed because Netcdf4 have no convenience way to test without create a file ...
@pytest.fixture(autouse=True)
def cleanup_files():
    yield
    for file in _FILES.values():
        if os.path.isfile(file):
            os.remove(file)
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
    return file_name


@pytest.mark.unit
def test_load_product_from_zarr(zarr_file: str):
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
def test_write_stores(store: EOProductStore, decoder_type: Any):

    store.open(mode="w")
    store["a_group"] = EOGroup()
    store.write_attrs("a_group", attrs={"description": "value"})
    store["a_group/a_variable"] = EOVariable(data=[])
    store["coordinates/a_coord"] = EOVariable(data=[1, 2, 3], dims=["a"])
    store.close()

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
def test_read_stores(store: EOProductStore):
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
@pytest.mark.parametrize(
    "store",
    [
        EOZarrStore("a_product"),
        EONetCDFStore(_FILES["netcdf"]),
        EOFlagAccessor("a.jp2"),
        EORasterIOAccessor("a.jp2"),
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
def test_store_structure(store: EOProductStore):
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
    store.open(mode="w")
    with pytest.raises(exceptions[0]):
        store.open()
    store.close()
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


@pytest.mark.need_files
@pytest.mark.integration
def test_retrieve_from_manifest_store(S3_OLCI_L1_EFR: str, S3_OLCI_L1_MAPPING: str, tmp_path: pathlib.Path):
    """Tested on 24th of February on data coming from
    S3A_OL_1_EFR____20220116T092821_20220116T093121_20220117T134858_0179_081_036_2160_LN1_O_NT_002.SEN3
    Given a manifest XML file from a Legacy product and a mapping file,
    the CF and respectively OM_EOP attributes computed by the manifest store
    must match the ones expected.
    """
    import json

    fsmap = fsspec.get_mapper(S3_OLCI_L1_EFR)
    manifest_name = "xfdumanifest.xml"
    manifest_path = tmp_path / manifest_name
    for key in fsmap:
        if key.endswith(manifest_name):
            manifest_path.write_bytes(fsmap[key])

    manifest = ManifestStore(manifest_path)

    mapping_file = open(S3_OLCI_L1_MAPPING)
    map_olci = json.load(mapping_file)
    config = {"namespaces": map_olci["namespaces"], "mapping": map_olci["metadata_mapping"]}
    manifest.open(**config)
    eog = manifest[""]
    assert isinstance(eog, EOGroup)
    returned_cf = eog.attrs["CF"]
    returned_om_eop = eog.attrs["OM_EOP"]

    assert_issubdict(
        returned_cf,
        {
            "title": S3_OLCI_L1_EFR.split("/")[-1].replace(".zip", ".SEN3"),
            "institution": "European Space Agency, Land OLCI Processing and Archiving Centre [LN1]",
            "source": "Sentinel-3A OLCI Ocean Land Colour Instrument",
            "comment": "Operational",
            "references": "https://sentinels.copernicus.eu/web/sentinel/missions/sentinel-2, https://sentinels.copernicus.eu/web/sentinel/user-guides/sentinel-2-msi/processing-levels/level-1",  # noqa
            "Conventions": "CF-1.9",
        },
    ) and ("history" in returned_cf)

    phenomenon_time = returned_om_eop.get("phenomenonTime", {})
    import re
    from datetime import datetime

    assert all(datetime.strptime(phenomenon_time[p], "%Y-%m-%dT%H:%M:%S.%fZ") for p in ("beginPosition", "endPosition"))

    acq_parameter = returned_om_eop.get("procedure", {}).get("acquistionParameters", {})

    assert_issubdict(
        returned_om_eop.get("procedure", {}),
        {
            "platform": {"shortName": "Sentinel-3", "serialIdentifier": "A"},
            "instrument": {"shortName": "OLCI"},
            "sensor": {"sensorType": "OPTICAL", "operationalMode": "EO"},
        },
    )
    assert acq_parameter.get("orbitNumber").isnumeric() and acq_parameter.get("orbitDirection") == "descending"

    assert datetime.strptime(returned_om_eop.get("resultTime", {}).get("timePosition", ""), "%Y%m%dT%H%M%S")
    assert (
        re.match(
            r"POLYGON\(\((-?\d*\.\d* -?\d*\.\d*,?)*\)\)",
            returned_om_eop.get("featureOfInterest", {}).get("multiExtentOf", ""),
        )
        is not None
    )
    assert_issubdict(
        returned_om_eop,
        {
            "result": {
                "product": {
                    "fileName": "./Oa01_radiance.nc,./Oa02_radiance.nc,./Oa03_radiance.nc,./Oa04_radiance.nc,./Oa05_radiance.nc,./Oa06_radiance.nc,./Oa07_radiance.nc,./Oa08_radiance.nc,./Oa09_radiance.nc,./Oa10_radiance.nc,./Oa11_radiance.nc,./Oa12_radiance.nc,./Oa13_radiance.nc,./Oa14_radiance.nc,./Oa15_radiance.nc,./Oa16_radiance.nc,./Oa17_radiance.nc,./Oa18_radiance.nc,./Oa19_radiance.nc,./Oa20_radiance.nc,./Oa21_radiance.nc,./geo_coordinates.nc,./instrument_data.nc,./qualityFlags.nc,./removed_pixels.nc,./tie_geo_coordinates.nc,./tie_geometries.nc,./tie_meteo.nc,./time_coordinates.nc",  # noqa
                    "timeliness": "NT",
                },
            },
        },
    )

    metadata_property = returned_om_eop.get("metadataProperty", {})
    assert_issubdict(
        metadata_property,
        {
            "identifier": S3_OLCI_L1_EFR.split("/")[-1].replace(".zip", ".SEN3"),
            "acquisitionType": "Operational",
            "productType": "OL_1_EFR___",
            "status": "ARCHIVED",
            "productQualityStatus": "PASSED",
        },
    )
    assert len(metadata_property.get("productQualityDegradationTag", "")) > 0

    assert datetime.strptime(metadata_property.get("creationDate", ""), "%Y%m%dT%H%M%S")
    downlinked_to = metadata_property.get("downlinkedTo", {})
    assert datetime.strptime(downlinked_to.get("acquisitionDate", ""), "%Y-%m-%dT%H:%M:%S.%fZ")
    assert downlinked_to.get("acquisitionStation", "") == "CGS"

    processing_map = metadata_property.get("processing", {})
    assert processing_map["processorName"] == "PUG"
    assert processing_map["processingCenter"] == "Land OLCI Processing and Archiving Centre [LN1]"
    assert re.match(r"\d{1,2}\.\d{2}", processing_map["processorVersion"])
    assert datetime.strptime(processing_map["processingDate"], "%Y-%m-%dT%H:%M:%S.%f")


_FORMAT = {
    EOZarrStore: "zarr",
    EONetCDFStore: "netcdf",
}


@pytest.mark.unit
@given(
    st.sampled_from(couple_combinaison_from(elements=[EOZarrStore, EONetCDFStore])),
)
def test_convert(read_write_stores):

    cls_read_store, cls_write_store = read_write_stores
    read_store = cls_read_store(_FILES[f"{_FORMAT[cls_read_store]}0"])
    write_store = cls_write_store(_FILES[f"{_FORMAT[cls_write_store]}1"])

    product = init_product("a_product", store_or_path_url=read_store)
    with open_store(product, mode="w"):
        product.write()
    new_product = EOProduct("new_one", store_or_path_url=convert(read_store, write_store))
    with open_store(new_product, mode="r"):
        new_product["measurements"]
        new_product["coordinates"]


@pytest.mark.unit
@pytest.mark.parametrize(
    "store_cls, format_file, params",
    [
        (EORasterIOAccessor, "tiff", {}),
        (EORasterIOAccessor, "jp2", {}),
        (EOFlagAccessor, "jp2", {"flag_meanings": "a,b,c", "flag_values": "1,2,3"}),
    ],
)
def test_rasters(store_cls: type[EORasterIOAccessor], format_file: str, params: dict[str, Any]):
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
        raster.open(mode="r", **params)
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

        raster.close()

        for return_val in [xarray.Dataset(data_vars={"a": xarray.DataArray()}), [xarray.Dataset()]]:
            mock_function.return_value = return_val
            raster.open(mode="r", **params)
            with pytest.raises(NotImplementedError):
                raster[""]
            with pytest.raises(NotImplementedError):
                [i for i in raster.iter("")]
            with pytest.raises(NotImplementedError):
                [i for i in raster.iter("not_implemeted")]
            raster.close()
            with pytest.raises(StoreNotOpenError):
                raster.close()


@pytest.mark.unit
@pytest.mark.parametrize(
    "product, fakefilename, open_kwargs",
    [
        (EOProduct("", store_or_path_url="s3://a_simple_zarr.zarr"), lazy_fixture("zarr_file"), S3_CONFIG),
        (
            EOProduct("", store_or_path_url="zip::s3://a_simple_zarr.zarr"),
            lazy_fixture("zarr_file"),
            dict(s3=S3_CONFIG),
        ),
    ],
)
def test_zarr_open_on_different_fs(product: EOProduct, fakefilename: str, open_kwargs: dict[str, Any]):
    with patch("fsspec.get_mapper") as mock:
        mock.return_value = fsspec.FSMap(fakefilename, LocalFileSystem())
        with product.open(storage_options=open_kwargs):
            product.load()
        assert mock.call_count == 1
        mock.assert_called_with(product.store.url, **open_kwargs)
