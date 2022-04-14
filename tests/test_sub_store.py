import json
import os
from glob import glob
from typing import Any

import numpy
import pytest
import xarray
from numpy import testing

from eopf.product.core import EOGroup, EOVariable
from eopf.product.store.grib import EOGribAccessor
from eopf.product.store.xml_accessors import (
    XMLAnglesAccessor,
    XMLManifestAccessor,
    XMLTPAccessor,
)

from .utils import PARENT_DATA_PATH, assert_issubdict

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
    grib_store.open(indexpath="")

    assert grib_store[eo_path].shape == shape
    assert grib_store[eo_path].attrs == attrs
    for key, value in sampled_values.items():
        testing.assert_allclose(grib_store[eo_path]._data.to_numpy()[key], value)
    grib_store.close()


@pytest.mark.unit
def test_grib_exceptions(EMBEDED_TEST_DATA_FOLDER: str):
    grib_store = EOGribAccessor(os.path.join(EMBEDED_TEST_DATA_FOLDER, "AUX_ECMWFT.grib"))
    grib_store.open(indexpath="")
    with pytest.raises(KeyError):
        set(grib_store.iter("test"))
    with pytest.raises(KeyError):
        set(grib_store.iter("coordinates/test"))
    with pytest.raises(KeyError):
        grib_store["test"]
    with pytest.raises(KeyError):
        grib_store["coordinates/test"]
    grib_store.close()


EXPECTED_XML_ATTR = {
    "array_size": (23, 23),
    "test_xpath_1": "n1:Geometric_Info/Tile_Angles/Sun_Angles_Grid/Zenith/Values_List",
    "test_xpath_2": "n1:Geometric_Info/Tile_Angles/Sun_Angles_Grid/Azimuth/Values_List",
    "test_xmltpy_path": 'n1:Geometric_Info/Tile_Geocoding/Geoposition[@resolution="10"]/ULY',
    "test_xmltpx_path": 'n1:Geometric_Info/Tile_Geocoding/Geoposition[@resolution="10"]/ULX',
    "tp_array_size": (23,),
}


@pytest.mark.unit
def test_xml_angles_accessor():
    # Create and open xml angles accessor
    mapping_file_path = glob("eopf/product/store/mapping/S2_MSIL1C_mapping.json")[0]
    mapping_file = open(mapping_file_path)
    map = json.load(mapping_file)
    config = {"namespaces": map["xml_mapping"]["namespace"]}
    xml_accessor = XMLAnglesAccessor(f"{PARENT_DATA_PATH}/tests/data/MTD_TL.xml")
    xml_accessor.open(**config)

    # verify data shapes / data name / data type
    assert xml_accessor[EXPECTED_XML_ATTR["test_xpath_1"]]._data.data.shape == EXPECTED_XML_ATTR["array_size"]
    assert isinstance(xml_accessor[EXPECTED_XML_ATTR["test_xpath_1"]], EOVariable)

    # create an xarray with a user defined value
    dummy_xarray = xarray.DataArray(data=numpy.full(EXPECTED_XML_ATTR["array_size"], 1.23, dtype=float))
    # check if all data from an xpath match with user defined xarray
    assert numpy.all(xml_accessor[EXPECTED_XML_ATTR["test_xpath_2"]]._data.data == dummy_xarray.data)
    # check if all items from other xpath does not match with user defined xarray
    assert not numpy.all(xml_accessor[EXPECTED_XML_ATTR["test_xpath_1"]]._data.data == dummy_xarray.data)


@pytest.mark.unit
def test_xml_tiepoints_accessor():
    # Compute a user defined tie points array with values similar with ones from XML.
    col_step = 5000
    ulx = 300000
    uly = 4800000

    dummy_x_array = [ulx + idx * col_step + col_step / 2 for idx in range(EXPECTED_XML_ATTR["tp_array_size"][0])]
    dummy_y_array = [uly - idx * col_step - col_step / 2 for idx in range(EXPECTED_XML_ATTR["tp_array_size"][0])]

    # Create XMLAccessors configuration
    mapping_file_path = glob("eopf/product/store/mapping/S2_MSIL1C_mapping.json")[0]
    mapping_file = open(mapping_file_path)
    map = json.load(mapping_file)

    config_x = {
        "namespaces": map["xml_mapping"]["namespace"],
        "xmltp_step": map["xml_mapping"]["xmltp"]["step_x"],
        "xmltp_values": map["xml_mapping"]["xmltp"]["values"],
    }
    config_y = {
        "namespaces": map["xml_mapping"]["namespace"],
        "xmltp_step": map["xml_mapping"]["xmltp"]["step_y"],
        "xmltp_values": map["xml_mapping"]["xmltp"]["values"],
    }
    # Create XMLAccessors
    tp_y_accessor = XMLTPAccessor(f"{PARENT_DATA_PATH}/tests/data/MTD_TL.xml")
    tp_y_accessor.open(**config_x)
    assert tp_y_accessor[EXPECTED_XML_ATTR["test_xmltpy_path"]]._data.shape == EXPECTED_XML_ATTR["tp_array_size"]

    tp_x_accessor = XMLTPAccessor(f"{PARENT_DATA_PATH}/tests/data/MTD_TL.xml")
    tp_x_accessor.open(**config_y)
    assert tp_x_accessor[EXPECTED_XML_ATTR["test_xmltpx_path"]]._data.shape == EXPECTED_XML_ATTR["tp_array_size"]

    assert all(dummy_x_array == tp_x_accessor[EXPECTED_XML_ATTR["test_xmltpx_path"]]._data)
    assert all(dummy_y_array == tp_y_accessor[EXPECTED_XML_ATTR["test_xmltpy_path"]]._data)
    assert not all(dummy_x_array == tp_y_accessor[EXPECTED_XML_ATTR["test_xmltpy_path"]]._data)
    assert not all(dummy_y_array == tp_x_accessor[EXPECTED_XML_ATTR["test_xmltpx_path"]]._data)
    # Verify incorrect path
    try:
        tp_x_accessor["random_incorect_xpath"]._data
    except TypeError:
        assert True
    try:
        tp_y_accessor["random_incorect_xpath"]._data
    except TypeError:
        assert True
    # Verify cross selection, accesing Y path with X-accessor
    try:
        tp_x_accessor[EXPECTED_XML_ATTR["test_xmltpy_path"]]
    except AttributeError:
        assert True
    try:
        tp_y_accessor[EXPECTED_XML_ATTR["test_xmltpx_path"]]
    except AttributeError:
        assert True


@pytest.mark.usecase
def test_xml_manifest_accessor():
    olci_path = glob(f"{PARENT_DATA_PATH}/data/S3A_OL_1*.SEN3")[0]
    manifest_path = os.path.join(olci_path, "xfdumanifest.xml")
    manifest_accessor = XMLManifestAccessor(manifest_path)
    mapping_file_path = glob("eopf/product/store/mapping/S3_OL_1_EFR_mapping.json")[0]
    mapping_file = open(mapping_file_path)
    map_olci = json.load(mapping_file)
    config = {"namespaces": map_olci["namespaces"], "metadata_mapping": map_olci["metadata_mapping"]}
    manifest_accessor.open(**config)
    eog = manifest_accessor[""]
    assert isinstance(eog, EOGroup)
    returned_cf = eog.attrs["CF"]
    returned_om_eop = eog.attrs["OM_EOP"]
    assert_issubdict(
        returned_cf,
        {
            "title": olci_path.replace(f"{PARENT_DATA_PATH}/data/", ""),
            "institution": "European Space Agency, Land OLCI Processing and Archiving Centre [LN1]",
            "source": "Sentinel-3A OLCI Ocean Land Colour Instrument",
            "comment": "Operational",
            "references": "https://sentinels.copernicus.eu/web/sentinel/missions/sentinel-2, "
            "https://sentinels.copernicus.eu/web/sentinel/user-guides/sentinel-2-msi/processing-levels/level-1",
            # noqa
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
                    "fileName": "./Oa01_radiance.nc,./Oa02_radiance.nc,./Oa03_radiance.nc,./Oa04_radiance.nc,"
                    "./Oa05_radiance.nc,./Oa06_radiance.nc,./Oa07_radiance.nc,./Oa08_radiance.nc,"
                    "./Oa09_radiance.nc,./Oa10_radiance.nc,./Oa11_radiance.nc,./Oa12_radiance.nc,"
                    "./Oa13_radiance.nc,./Oa14_radiance.nc,./Oa15_radiance.nc,./Oa16_radiance.nc,"
                    "./Oa17_radiance.nc,./Oa18_radiance.nc,./Oa19_radiance.nc,./Oa20_radiance.nc,"
                    "./Oa21_radiance.nc,./geo_coordinates.nc,./instrument_data.nc,./qualityFlags.nc,"
                    "./removed_pixels.nc,./tie_geo_coordinates.nc,./tie_geometries.nc,./tie_meteo.nc,"
                    "./time_coordinates.nc",
                    # noqa
                    "timeliness": "NT",
                },
            },
        },
    )

    metadata_property = returned_om_eop.get("metadataProperty", {})
    assert_issubdict(
        metadata_property,
        {
            "identifier": olci_path.replace(f"{PARENT_DATA_PATH}/data/", ""),  # noqa
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
