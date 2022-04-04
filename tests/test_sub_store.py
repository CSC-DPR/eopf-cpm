import os
from typing import Any

import pytest
from numpy import testing

from eopf.product.store.grib import EOGribAccessor

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
    grib_store.open()
    # test attributes

    # test iterator
    assert set(grib_store.iter(eo_path)) == expected_set


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
    shape: set[int, ...],
    attrs: dict[str, Any],
    sampled_values: dict[set[int, ...], float],
):
    grib_store = EOGribAccessor(os.path.join(EMBEDED_TEST_DATA_FOLDER, "AUX_ECMWFT.grib"))
    grib_store.open()

    assert grib_store[eo_path].shape == shape
    assert grib_store[eo_path].attrs == attrs
    for key, value in sampled_values.items():
        testing.assert_allclose(grib_store[eo_path]._data.to_numpy()[key], value)


@pytest.mark.unit
def test_grib_exceptions(EMBEDED_TEST_DATA_FOLDER: str):
    grib_store = EOGribAccessor(os.path.join(EMBEDED_TEST_DATA_FOLDER, "AUX_ECMWFT.grib"))
    grib_store.open()
    with pytest.raises(KeyError):
        set(grib_store.iter("test"))
    with pytest.raises(KeyError):
        set(grib_store.iter("coordinates/test"))
    with pytest.raises(KeyError):
        grib_store["test"]
    with pytest.raises(KeyError):
        grib_store["coordinates/test"]
