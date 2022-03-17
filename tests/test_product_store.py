import os
import os.path
import shutil
from typing import Any, Optional

import hypothesis.strategies as st
import numpy as np
import pytest
import xarray
import zarr
from hypothesis import given
from pyfakefs.fake_filesystem import FakeFilesystem

from eopf.exceptions import MissingConfigurationParameter, StoreNotOpenError
from eopf.exceptions.warnings import AlreadyClose, AlreadyOpen
from eopf.product.conveniences import init_product, open_store
from eopf.product.core import EOGroup, EOProduct, EOVariable
from eopf.product.store import EONetCDFStore, EOProductStore, EOZarrStore, convert
from eopf.product.store.manifest import ManifestStore

from .decoder import Netcdfdecoder
from .utils import assert_contain, assert_has_coords, couple_combinaison_from

_FILES = {
    "netcdf": "test_ncdf_file_.nc",
    "netcdf0": "test_ncdf_read_file_.nc",
    "netcdf1": "test_ncdf_write_file_.nc",
    "json": "test_metadata_file_.json",
    "zarr": "test_zarr_files_.zarr",
    "zarr0": "test_zarr_read_files_.zarr",
    "zarr1": "test_zarr_write_files_.zarr",
}


@pytest.fixture
def zarr_file(fs: FakeFilesystem):
    file_name = f"file://{_FILES['zarr']}"
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
def test_write_stores(fs: FakeFilesystem, store: EOProductStore, decoder_type: Any):

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
@pytest.mark.parametrize(
    "store",
    [
        EOZarrStore("a_product"),
        EONetCDFStore(_FILES["netcdf"]),
    ],
)
def test_store_must_be_open(fs: FakeFilesystem, store: EOProductStore):

    with pytest.raises(StoreNotOpenError):
        store["a_group"]

    with pytest.raises(StoreNotOpenError):
        store["a_group"] = EOGroup(variables={})

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


# needed because Netcdf4 have no convenience way to test without create a file ...
@pytest.fixture(autouse=True)
def cleanup_files():
    yield
    for file in _FILES.values():
        if os.path.isfile(file):
            os.remove(file)
        if os.path.isdir(file):
            shutil.rmtree(file)


@pytest.mark.unit
@pytest.mark.filterwarnings("error")
@pytest.mark.parametrize(
    "store, exceptions",
    [
        (EOZarrStore(zarr.MemoryStore()), (AlreadyOpen, AlreadyClose)),
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
def test_mtd_store_must_be_open(fs: FakeFilesystem):
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
        (None, MissingConfigurationParameter),
        ([0, 1, 2], MissingConfigurationParameter),
        ({"metadata_mapping": {}}, KeyError),
        ({"namespaces": {}}, KeyError),
        ({"namespaces": {}, "metadata_mapping": {}}, FileNotFoundError),
    ],
)
def test_open_manifest_store(config: Optional[dict], exception_type: Exception):
    """Given a manifest store, without passing configuration parameters
    the function must raise a MissingConfigurationParameter error.
    """
    store = ManifestStore(_FILES["json"])
    with pytest.raises(exception_type):
        store.open(config=config)


@pytest.mark.unit
def test_close_manifest_store():
    """Given a manifest store, when trying to close it while not previously opening it,
    the function must raise a StoreNotOpen error.
    """
    store = ManifestStore(_FILES["json"])
    with pytest.raises(StoreNotOpenError):
        store.close()


@pytest.mark.usecase
def test_retrieve_from_manifest_store():
    """Tested on 24th of February on data coming from
    S3A_OL_1_EFR____20220116T092821_20220116T093121_20220117T134858_0179_081_036_2160_LN1_O_NT_002.SEN3
    Given a manifest XML file from a Legacy product and a mapping file,
    the CF and respectively OM_EOP attributes computed by the manifest store
    must match the ones expected.
    """
    import json
    from glob import glob

    olci_path = glob("data/S3A_OL_1*.SEN3")[0]
    manifest_path = os.path.join(olci_path, "xfdumanifest.xml")
    manifest = ManifestStore(manifest_path)

    mapping_file_path = glob("eopf/product/store/mapping/S3_OL_1_EFR_mapping.json")[0]
    mapping_file = open(mapping_file_path)
    map_olci = json.load(mapping_file)
    config = {"namespaces": map_olci["namespaces"], "metadata_mapping": map_olci["metadata_mapping"]}
    manifest.open(config=config)
    eog = manifest[""]
    assert isinstance(eog, EOGroup)
    returned_cf = eog.attrs["CF"]
    returned_om_eop = eog.attrs["OM_EOP"]

    assert returned_cf == {
        "title": "S3A_OL_1_EFR____20220116T092821_20220116T093121_20220117T134858_0179_081_036_2160_LN1_O_NT_002.SEN3",
        "history": "PUG 03.39 2022-01-17T13:53:47.648154",
        "institution": "European Space Agency, Land OLCI Processing and Archiving Centre [LN1]",
        "source": "Sentinel-3A OLCI Ocean Land Colour Instrument",
        "comment": "Operational",
        "references": "https://sentinels.copernicus.eu/web/sentinel/missions/sentinel-2, https://sentinels.copernicus.eu/web/sentinel/user-guides/sentinel-2-msi/processing-levels/level-1",  # noqa
        "Conventions": "CF-1.9",
    }
    assert returned_om_eop == {
        "phenomenonTime": {
            "beginPosition": "2022-01-16T09:28:21.493500Z",
            "endPosition": "2022-01-16T09:31:21.493500Z",
        },
        "resultTime": {"timePosition": "20220117T134858"},
        "procedure": {
            "platform": {"shortName": "Sentinel-3", "serialIdentifier": "A"},
            "instrument": {"shortName": "OLCI"},
            "sensor": {"sensorType": "OPTICAL", "operationalMode": "EO"},
            "acquistionParameters": {"orbitNumber": "30809", "orbitDirection": "descending"},
        },
        "featureOfInterest": {
            "multiExtentOf": "POLYGON((3.16201 41.9724,3.98942 41.8955,4.81448 41.8131,5.64024 41.7245,6.45761 41.6307,7.27659 41.5307,8.09209 41.425,8.90322 41.3139,9.71175 41.1967,10.5191 41.0736,11.3247 40.9497,12.1286 40.8147,12.9244 40.6778,13.7184 40.5298,14.5077 40.3796,15.2941 40.2212,16.0796 40.0608,16.8596 39.8945,17.6366 39.7226,18.4068 39.5462,19.4552 42.1435,20.5884 44.7363,21.8193 47.3178,23.1664 49.886,22.2555 50.0913,21.3387 50.2889,20.413 50.4794,19.4821 50.6611,18.5445 50.839,17.5972 51.0072,16.646 51.1702,15.6865 51.3203,14.7188 51.4656,13.7435 51.5978,12.7692 51.7261,11.7869 51.8466,10.8013 51.9584,9.80843 52.0622,8.81152 52.1576,7.80459 52.2451,6.80395 52.3232,5.79612 52.3931,4.78252 52.455,4.38612 49.8355,3.98361 47.2141,3.57524 44.5914,3.16201 41.9724))",  # noqa
        },
        "result": {
            "product": {
                "fileName": "./Oa01_radiance.nc,./Oa02_radiance.nc,./Oa03_radiance.nc,./Oa04_radiance.nc,./Oa05_radiance.nc,./Oa06_radiance.nc,./Oa07_radiance.nc,./Oa08_radiance.nc,./Oa09_radiance.nc,./Oa10_radiance.nc,./Oa11_radiance.nc,./Oa12_radiance.nc,./Oa13_radiance.nc,./Oa14_radiance.nc,./Oa15_radiance.nc,./Oa16_radiance.nc,./Oa17_radiance.nc,./Oa18_radiance.nc,./Oa19_radiance.nc,./Oa20_radiance.nc,./Oa21_radiance.nc,./geo_coordinates.nc,./instrument_data.nc,./qualityFlags.nc,./removed_pixels.nc,./tie_geo_coordinates.nc,./tie_geometries.nc,./tie_meteo.nc,./time_coordinates.nc",  # noqa
                "timeliness": "NT",
            },
        },
        "metadataProperty": {
            "identifier": "S3A_OL_1_EFR____20220116T092821_20220116T093121_20220117T134858_0179_081_036_2160_LN1_O_NT_002.SEN3",  # noqa
            "creationDate": "20220117T134858",
            "acquisitionType": "Operational",
            "productType": "OL_1_EFR___",
            "status": "ARCHIVED",
            "downlinkedTo": {"acquisitionStation": "CGS", "acquisitionDate": "2022-01-16T10:58:16.767081Z"},
            "productQualityStatus": "PASSED",
            "productQualityDegradationTag": "NON_NOMINAL_INPUT INPUT_GAPS",
            "processing": {
                "processingCenter": "Land OLCI Processing and Archiving Centre [LN1]",
                "processingDate": "2022-01-17T13:53:47.648154",
                "processorName": "PUG",
                "processorVersion": "03.39",
            },
        },
    }


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
