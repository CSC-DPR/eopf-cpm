import json
import os
import pathlib
from typing import Any
from unittest import mock

import fsspec
import numpy
import pytest
from numpy import testing
from pytest_lazyfixture import lazy_fixture

from eopf.product.conveniences import open_store
from eopf.product.core import EOGroup, EOVariable
from eopf.product.store.grib import EOGribAccessor
from eopf.product.store.wrappers import (
    FromAttributesToFlagValueAccessor,
    FromAttributesToVariableAccessor,
)
from eopf.product.store.xml_accessors import (
    XMLAnglesAccessor,
    XMLManifestAccessor,
    XMLTPAccessor,
)
from tests.test_eo_container import EmptyTestStore

EXPECTED_GRIB_MSL_ATTR = {
    "GRIB_paramId": 151,
    "GRIB_dataType": "fc",
    "GRIB_numberOfPoints": 81,
    "GRIB_typeOfLevel": "surface",
    "GRIB_stepUnits": 1,
    "GRIB_stepType": "instant",
    "GRIB_gridType": "regular_ll",
    "GRIB_NV": 0,
    "GRIB_Nx": 9,
    "GRIB_Ny": 9,
    "GRIB_cfName": "air_pressure_at_mean_sea_level",
    "GRIB_cfVarName": "msl",
    "GRIB_gridDefinitionDescription": "Latitude/Longitude Grid",
    "GRIB_iDirectionIncrementInDegrees": 0.126,
    "GRIB_iScansNegatively": 0,
    "GRIB_jDirectionIncrementInDegrees": 0.123,
    "GRIB_jPointsAreConsecutive": 0,
    "GRIB_jScansPositively": 0,
    "GRIB_latitudeOfFirstGridPointInDegrees": -9.042,
    "GRIB_latitudeOfLastGridPointInDegrees": -10.027,
    "GRIB_longitudeOfFirstGridPointInDegrees": 160.819,
    "GRIB_longitudeOfLastGridPointInDegrees": 161.826,
    "GRIB_missingValue": 9999,
    "GRIB_name": "Mean sea level pressure",
    "GRIB_shortName": "msl",
    "GRIB_totalNumber": 0,
    "GRIB_units": "Pa",
    "long_name": "Mean sea level pressure",
    "units": "Pa",
    "standard_name": "air_pressure_at_mean_sea_level",
    "_ARRAY_DIMENSIONS": ("latitude", "longitude"),
}

EXPECTED_GRIB_LON_ATTR = {
    "units": "degrees_east",
    "standard_name": "longitude",
    "long_name": "longitude",
    "_ARRAY_DIMENSIONS": ("longitude",),
}
EXPECTED_GRIB_LAT_ATTR = {
    "units": "degrees_north",
    "standard_name": "latitude",
    "long_name": "latitude",
    "stored_direction": "decreasing",
    "_ARRAY_DIMENSIONS": ("latitude",),
}


@pytest.mark.unit
@pytest.mark.parametrize(
    "eo_path, expected_set",
    [
        ("", {"msl", "tco3", "tcwv", "coordinates"}),
        ("/", {"msl", "tco3", "tcwv", "coordinates"}),
        ("coordinates", {"number", "surface", "valid_time", "time", "longitude", "latitude", "step"}),
    ],
)
def test_grib_store_iter(EMBEDED_TEST_DATA_FOLDER: str, eo_path: str, expected_set: set[str]):
    grib_store = EOGribAccessor(os.path.join(EMBEDED_TEST_DATA_FOLDER, "AUX_ECMWFT.grib"))
    grib_store.open(indexpath="")
    # test attributes

    # test iterator
    assert set(grib_store.iter(eo_path)) == expected_set
    grib_store.close()


@pytest.mark.unit
@pytest.mark.parametrize(
    "eo_path, shape, attrs, sampled_values",
    [
        ("msl", (9, 9), EXPECTED_GRIB_MSL_ATTR, {(4, 4): 100971.8125}),
        ("coordinates/longitude", (9,), EXPECTED_GRIB_LON_ATTR, {(4,): 161.3225}),
        ("coordinates/latitude", (9,), EXPECTED_GRIB_LAT_ATTR, {(4,): -9.534}),
    ],
)
def test_grib_store_get_item(
    EMBEDED_TEST_DATA_FOLDER: str,
    eo_path: str,
    shape: tuple[int, ...],
    attrs: dict[str, Any],
    sampled_values: dict[tuple[int, ...], float],
):
    grib_store = EOGribAccessor(os.path.join(EMBEDED_TEST_DATA_FOLDER, "AUX_ECMWFT.grib"))
    with open_store(grib_store, indexpath=""):
        assert grib_store[eo_path].shape == shape
        assert (
            dict(
                (key, value.strip() if isinstance(value, str) else value)
                for key, value in grib_store[eo_path].attrs.items()
            )
            == attrs
        )
        for key, value in sampled_values.items():
            testing.assert_allclose(grib_store[eo_path]._data.to_numpy()[key], value)


@pytest.mark.unit
def test_grib_exceptions(EMBEDED_TEST_DATA_FOLDER: str):
    grib_store = EOGribAccessor(os.path.join(EMBEDED_TEST_DATA_FOLDER, "AUX_ECMWFT.grib"))
    with open_store(grib_store, indexpath=""):
        with pytest.raises(KeyError):
            set(grib_store.iter("test"))
        with pytest.raises(KeyError):
            set(grib_store.iter("coordinates/test"))
        with pytest.raises(KeyError):
            grib_store["test"]
        with pytest.raises(KeyError):
            grib_store["coordinates/test"]


@pytest.mark.unit
@pytest.mark.parametrize("mapping", [lazy_fixture("S2_MSIL1C_MAPPING")])
@pytest.mark.parametrize(
    "array_size, expected_data, xpath",
    [
        (
            (23, 23),
            numpy.array([numpy.array(range(i * 23, (i + 1) * 23)) for i in range(23)]),
            "n1:Geometric_Info/Tile_Angles/Sun_Angles_Grid/Zenith/Values_List",
        ),
        (
            (23, 23),
            numpy.full((23, 23), 1.23, dtype=float),
            "n1:Geometric_Info/Tile_Angles/Sun_Angles_Grid/Azimuth/Values_List",
        ),
    ],
)
def test_xml_angles_accessor(EMBEDED_TEST_DATA_FOLDER, mapping, array_size, expected_data, xpath):
    # Create and open xml angles accessor
    with open(mapping) as mapping_file:
        map_config = json.load(mapping_file)
    config = {"namespace": map_config["xml_mapping"]["namespace"]}
    xml_accessor = XMLAnglesAccessor(os.path.join(EMBEDED_TEST_DATA_FOLDER, "MTD_TL.xml"))
    with open_store(xml_accessor, **config):
        # verify data shapes / data name / data type
        assert xml_accessor[xpath].data.shape == array_size
        assert isinstance(xml_accessor[xpath], EOVariable)
        # create an xarray with a user defined value
        # check if all data from an xpath match with user defined xarray
        assert numpy.all(xml_accessor[xpath].data == expected_data)


@pytest.mark.unit
@pytest.mark.parametrize("mapping", [lazy_fixture("S2_MSIL1C_MAPPING")])
@pytest.mark.parametrize("array_size", [(23,)])
@pytest.mark.parametrize("ul, dummy_array_factor, kind, reversed", [(300000, 1, "x", "y"), (4800000, -1, "y", "x")])
def test_xml_tiepoints_accessor(EMBEDED_TEST_DATA_FOLDER, mapping, ul, dummy_array_factor, kind, reversed, array_size):
    xpath = 'n1:Geometric_Info/Tile_Geocoding/Geoposition[@resolution="10"]/UL{}'
    # Compute a user defined tie points array with values similar with ones from XML.
    COL_STEP = 5000

    # Create XMLAccessors configuration
    with open(mapping) as mapping_file:
        map_config = json.load(mapping_file)

    config = {
        "namespace": map_config["xml_mapping"]["namespace"],
        "step_path": map_config["xml_mapping"]["xmltp"][f"step_{kind}"],
        "values_path": map_config["xml_mapping"]["xmltp"]["values"],
    }
    tp_accessor = XMLTPAccessor(os.path.join(EMBEDED_TEST_DATA_FOLDER, "MTD_TL.xml"))
    dummy_array = [
        ul + (dummy_array_factor * idx) * COL_STEP + (dummy_array_factor * COL_STEP / 2) for idx in range(array_size[0])
    ]

    # Create XMLAccessors
    tp_accessor = XMLTPAccessor(os.path.join(EMBEDED_TEST_DATA_FOLDER, "MTD_TL.xml"))
    with open_store(tp_accessor, **config):
        assert tp_accessor[xpath.format(kind.upper())].shape == array_size

        assert numpy.array_equal(dummy_array, tp_accessor[xpath.format(kind.upper())])

        # Verify incorrect path
        with pytest.raises(TypeError):
            tp_accessor["random_incorect_xpath"]


@pytest.mark.unit
@pytest.mark.parametrize(
    "store, kwargs",
    [
        (FromAttributesToVariableAccessor(""), {}),
        (FromAttributesToFlagValueAccessor(""), {"flag_values": [1, 2, 3], "flag_meanings": "23 1 5"}),
    ],
)
@pytest.mark.parametrize("attrs, attr_name, index", [({"a": [23]}, "a", 0), ({"a": [23], "B": [1, 5, 23]}, "B", 1)])
def test_fromattributestovarstore(
    attrs: dict,
    store: FromAttributesToVariableAccessor,
    attr_name: str,
    index: Any,
    kwargs: dict,
):
    with (
        mock.patch.object(EmptyTestStore, "__getitem__", return_value=EOGroup(attrs=attrs)),
        mock.patch.object(EmptyTestStore, "__iter__", return_value=iter(["value", "other"])),
        mock.patch.object(EmptyTestStore, "__len__", return_value=2),
    ):
        store.open(store_cls="tests.test_eo_container.EmptyTestStore", attr_name=attr_name, index=index, **kwargs)
        assert store._extract_data("value"), [attrs[attr_name][index]]

        assert len([i for i in store]) == len(store)
        assert len([i for i in store]) == len([i for i in store.iter("")])

    with pytest.raises(NotImplementedError):
        store.write_attrs("", {})


@pytest.mark.need_files
@pytest.mark.integration
@pytest.mark.parametrize(
    "path, mapping",
    [(lazy_fixture("S3_OL_1_EFR"), lazy_fixture("S3_OL_1_EFR_MAPPING"))],
)
def test_extended_xml_manifest_accessor(path: str, mapping: str, tmp_path: pathlib.Path):
    fsmap = fsspec.get_mapper(path)
    xmlfile = "xfdumanifest.xml"
    filepath = fsmap.fs.glob(f"*/{xmlfile}")[0]
    (tmp_path / xmlfile).write_bytes(fsmap[filepath])
    with open(mapping) as mapping_file:
        map_olci = json.load(mapping_file)
    config = {"namespaces": map_olci["namespaces"], "mapping": map_olci["stac_discovery"]}
    manifest_accessor = XMLManifestAccessor(tmp_path / xmlfile)
    with open_store(manifest_accessor, **config):
        eog = manifest_accessor[""]
        assert isinstance(eog, EOGroup)
        stac_discovery = eog.attrs
        expected_type = "Feature"
        expected_instruments = {
            "name": "Ocean Land Colour Instrument",
            "short_name": "OLCI",
            "mode": "Earth Observation",
            "identifier": "EO",
        }
        expected_FR_res = 270
        expected_bright_percent = 3.586159
        expected_links = [{"rel": "self", "href": "./.zattrs.json", "type": "application/json"}]
        assert stac_discovery["type_"] == expected_type
        assert stac_discovery["properties"]["eopf:instrument"] == expected_instruments
        assert isinstance(stac_discovery["properties"]["eopf:resolutions"]["FR"], int)
        assert stac_discovery["properties"]["eopf:resolutions"]["FR"] == expected_FR_res
        assert isinstance(
            stac_discovery["properties"]["eopf:product"]["pixel_classification"]["bright"]["percent"],
            float,
        )
        assert (
            stac_discovery["properties"]["eopf:product"]["pixel_classification"]["bright"]["percent"]
            == expected_bright_percent
        )
        assert stac_discovery["links"] == expected_links
    config = {"namespaces": map_olci["namespaces"], "mapping": map_olci["conditions_metadata"]}
    manifest_accessor = XMLManifestAccessor(tmp_path / xmlfile)
    with open_store(manifest_accessor, **config):
        eog = manifest_accessor[""]
        assert isinstance(eog, EOGroup)
        conditions_metadata = eog.attrs
        expected_bands_number = 21
        expected_band_7_name = "Oa07"
        expected_band_7_central_wavelength = 620.0
        expected_ephemeris = {
            "start": {
                "TAI": "2020-01-01T09:33:53.825486",
                "UTC": "2020-01-01T09:33:16.825486Z",
                "UT1": "2020-01-01T09:33:16.648568",
                "position": {"x": -7134399.613, "y": -838641.089, "z": -0.004},
                "velocity": {"x": -183.334412, "y": 1631.071145, "z": 7366.537065},
            },
            "stop": {
                "TAI": "2020-01-01T11:14:52.990229",
                "UTC": "2020-01-01T11:14:15.990229Z",
                "UT1": "2020-01-01T11:14:15.813289",
                "position": {"x": -6810629.029, "y": 2284455.415, "z": -0.001},
                "velocity": {"x": 529.833647, "y": 1553.517139, "z": 7366.523391},
            },
        }
        assert len(conditions_metadata["band_descriptions"]) == expected_bands_number
        assert (
            conditions_metadata["band_descriptions"][expected_band_7_name]["central_wavelength"]
            == expected_band_7_central_wavelength
        )
        assert isinstance(conditions_metadata["band_descriptions"][expected_band_7_name]["central_wavelength"], float)
        assert conditions_metadata["orbit_reference"]["ephemeris"] == expected_ephemeris
        assert conditions_metadata
        from datetime import datetime

        assert datetime.strptime(
            conditions_metadata["orbit_reference"]["ephemeris"]["start"]["TAI"],
            "%Y-%m-%dT%H:%M:%S.%f",
        )
