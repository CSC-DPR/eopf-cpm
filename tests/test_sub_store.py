import os

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
def test_grib_store(EMBEDED_TEST_DATA_FOLDER: str):
    grib_store = EOGribAccessor(os.path.join(EMBEDED_TEST_DATA_FOLDER, "AUX_ECMWFT.grib"))
    grib_store.open()
    # test attributes
    assert grib_store["msl"].attrs == EXPECTED_GRIB_MSL_ATTR
    assert grib_store["coordinates/longitude"].attrs == EXPECTED_GRIB_LON_ATTR
    assert grib_store["coordinates/latitude"].attrs == EXPECTED_GRIB_LAT_ATTR
    testing.assert_allclose(grib_store["msl"]._data.to_numpy()[4, 4], 100971.8125)
    assert grib_store["msl"].shape == (9, 9)
    testing.assert_allclose(grib_store["coordinates/longitude"]._data.to_numpy()[4], 161.3225)
    assert grib_store["coordinates/longitude"].shape == (9,)
    testing.assert_allclose(grib_store["coordinates/latitude"]._data.to_numpy()[4], -9.534)
    assert grib_store["coordinates/latitude"].shape == (9,)

    # test iterator
    assert set(grib_store) == {"msl", "tco3", "tcwv", "coordinates"}
    assert set(grib_store.iter("coordinates")) == {
        "number",
        "surface",
        "valid_time",
        "time",
        "longitude",
        "latitude",
        "step",
    }
