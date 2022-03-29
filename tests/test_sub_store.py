import datetime

import numpy
import pytest
import xarray
from numpy import testing

from eopf.product.core import EOVariable
from eopf.product.store.grib import EOGribAccessor
from eopf.product.store.xml import EOXmlAnglesAccesor

from .utils import PARENT_DATA_PATH

EXPECTED_GRIB_MSL_ATTR = {
    "globalDomain": "g",
    "GRIBEditionNumber": 1,
    "eps": 0,
    "offsetSection0": 0,
    "section0Length": 8,
    "totalLength": 270,
    "editionNumber": 1,
    "WMO": 0,
    "productionStatusOfProcessedData": 0,
    "section1Length": 52,
    "wrongPadding": 0,
    "table2Version": 128,
    "centre": "ecmf",
    "centreDescription": "European Centre for Medium-Range Weather Forecasts",
    "generatingProcessIdentifier": 128,
    "gridDefinition": 255,
    "indicatorOfParameter": 151,
    "parameterName": "Mean sea level pressure",
    "parameterUnits": "Pa",
    "indicatorOfTypeOfLevel": "sfc",
    "pressureUnits": "hPa",
    "typeOfLevelECMF": "surface",
    "typeOfLevel": "surface",
    "level": 0,
    "yearOfCentury": 20,
    "month": 2,
    "day": 2,
    "hour": 23,
    "minute": 38,
    "second": 0,
    "unitOfTimeRange": 1,
    "P1": 0,
    "P2": 0,
    "timeRangeIndicator": 0,
    "numberIncludedInAverage": 0,
    "numberMissingFromAveragesOrAccumulations": 0,
    "centuryOfReferenceTimeOfData": 21,
    "subCentre": 0,
    "paramIdECMF": "151",
    "paramId": 151,
    "cfNameECMF": "air_pressure_at_mean_sea_level",
    "cfName": "air_pressure_at_mean_sea_level",
    "cfVarNameECMF": "msl",
    "cfVarName": "msl",
    "unitsECMF": "Pa",
    "units": "Pa",
    "nameECMF": "Mean sea level pressure",
    "name": "Mean sea level pressure",
    "decimalScaleFactor": 0,
    "setLocalDefinition": 0,
    "optimizeScaleFactor": 0,
    "dataDate": 20200202,
    "year": 2020,
    "dataTime": 2338,
    "julianDay": 2458882.484722222,
    "stepUnits": 1,
    "stepType": "instant",
    "stepRange": "0",
    "startStep": 0,
    "endStep": 0,
    "marsParam": "151.128",
    "validityDate": 20200202,
    "validityTime": 2338,
    "deleteLocalDefinition": 0,
    "localUsePresent": 1,
    "localDefinitionNumber": 1,
    "GRIBEXSection1Problem": 0,
    "marsClass": "ei",
    "marsType": "fc",
    "marsStream": "oper",
    "experimentVersionNumber": "0001",
    "perturbationNumber": 0,
    "numberOfForecastsInEnsemble": 0,
    "grib2LocalSectionNumber": 1,
    "shortNameECMF": "msl",
    "shortName": "msl",
    "ifsParam": 151,
    "stepTypeForConversion": "unknown",
    "md5Section1": "72553241fa2349eabbc1732761eeaa1b",
    "md5Product": "498be06aeddb303f9bc70766b2151e27",
    "gridDescriptionSectionPresent": 1,
    "bitmapPresent": 0,
    "angleSubdivisions": 1000,
    "section2Length": 32,
    "radius": 6367470,
    "numberOfVerticalCoordinateValues": 0,
    "neitherPresent": 255,
    "pvlLocation": 255,
    "dataRepresentationType": 0,
    "gridDefinitionDescription": "Latitude/Longitude Grid",
    "gridDefinitionTemplateNumber": 0,
    "latitudeOfFirstGridPoint": -9042,
    "latitudeOfFirstGridPointInDegrees": -9.042,
    "longitudeOfFirstGridPoint": 160819,
    "longitudeOfFirstGridPointInDegrees": 160.819,
    "resolutionAndComponentFlags": 128,
    "ijDirectionIncrementGiven": 1,
    "earthIsOblate": 0,
    "resolutionAndComponentFlags3": 0,
    "resolutionAndComponentFlags4": 0,
    "uvRelativeToGrid": 0,
    "resolutionAndComponentFlags6": 0,
    "resolutionAndComponentFlags7": 0,
    "resolutionAndComponentFlags8": 0,
    "latitudeOfLastGridPoint": -10027,
    "latitudeOfLastGridPointInDegrees": -10.027,
    "longitudeOfLastGridPoint": 161826,
    "longitudeOfLastGridPointInDegrees": 161.826,
    "iDirectionIncrement": 126,
    "jDirectionIncrement": 123,
    "scanningMode": 0,
    "iScansNegatively": 0,
    "jScansPositively": 0,
    "jPointsAreConsecutive": 0,
    "alternativeRowScanning": 0,
    "iScansPositively": 1,
    "scanningMode4": 0,
    "scanningMode5": 0,
    "scanningMode6": 0,
    "scanningMode7": 0,
    "scanningMode8": 0,
    "jDirectionIncrementInDegrees": 0.123,
    "iDirectionIncrementInDegrees": 0.126,
    "numberOfDataPoints": 81,
    "numberOfValues": 81,
    "zeros": "",
    "PVPresent": 0,
    "PLPresent": 0,
    "deletePV": "1",
    "md5Section2": "c529d92e120f44b45953def45d350d6e",
    "lengthOfHeaders": 85,
    "md5Headers": "092132246d1d739edf2ff9f68810ff42",
    "missingValue": 9999,
    "tableReference": 0,
    "section4Length": 174,
    "halfByte": 8,
    "dataFlag": 8,
    "binaryScaleFactor": -8,
    "referenceValue": 100922.625,
    "referenceValueError": 0.0625,
    "sphericalHarmonics": 0,
    "complexPacking": 0,
    "integerPointValues": 0,
    "additionalFlagPresent": 0,
    "orderOfSPD": 2,
    "boustrophedonic": 0,
    "hideThis": 0,
    "packingType": "grid_simple",
    "bitsPerValue": 16,
    "constantFieldHalfByte": 8,
    "bitMapIndicator": 255,
    "numberOfCodedValues": 81,
    "packingError": 0.033203125,
    "unpackedError": 0.0078125,
    "maximum": 101128.3984375,
    "minimum": 100922.66015625,
    "average": 100965.48056520062,
    "numberOfMissing": 0,
    "standardDeviation": 38.74624908523734,
    "skewness": 2.204691574319274,
    "kurtosis": 5.268701908641333,
    "isConstant": 0.0,
    "dataLength": 20,
    "changeDecimalPrecision": 0,
    "decimalPrecision": 0,
    "bitsPerValueAndRepack": 16,
    "scaleValuesBy": 1.0,
    "offsetValuesBy": 0.0,
    "gridType": "regular_ll",
    "getNumberOfValues": 81,
    "md5Section4": "d05dead3ba069e053ba1f44d4fb69a31",
    "section5Length": 4,
    "analDate": datetime.datetime(2020, 2, 2, 23, 38),
    "validDate": datetime.datetime(2020, 2, 2, 23, 38),
    "_ARRAY_DIMENSIONS": [9, 9],
}
EXPECTED_GRIB_MSL_COORD_ATTR = dict(EXPECTED_GRIB_MSL_ATTR)
EXPECTED_GRIB_MSL_COORD_ATTR["_ARRAY_DIMENSIONS"] = [9]


@pytest.mark.usecase
def test_grib_store():
    grib_store = EOGribAccessor(f"{PARENT_DATA_PATH}/data/AUX_ECMWFT.grib")
    grib_store.open()
    # test attributes
    assert grib_store["msl"].attrs == EXPECTED_GRIB_MSL_ATTR
    assert grib_store["coordinates/msl_lon"].attrs == EXPECTED_GRIB_MSL_COORD_ATTR
    assert grib_store["coordinates/msl_lat"].attrs == EXPECTED_GRIB_MSL_COORD_ATTR
    testing.assert_allclose(grib_store["msl"]._data.to_numpy()[4, 4], 100971.8125)
    assert grib_store["msl"].shape == (9, 9)
    testing.assert_allclose(grib_store["coordinates/msl_lon"]._data.to_numpy()[4], 161.3225)
    assert grib_store["coordinates/msl_lon"].shape == (9,)
    testing.assert_allclose(grib_store["coordinates/msl_lat"]._data.to_numpy()[4], -9.534)
    assert grib_store["coordinates/msl_lat"].shape == (9,)

    # test iterator
    assert set(grib_store) == {"msl", "tco3", "tcwv", "coordinates"}
    assert set(grib_store.iter("coordinates")) == {"tco3_lat", "msl_lon", "tcwv_lat", "tco3_lon", "tcwv_lon", "msl_lat"}


EXPECTED_XML_ATTR = {
    "array_size": (23, 23),
    "test_xpath_1": "n1:Geometric_Info/Tile_Angles/Sun_Angles_Grid/Zenith/Values_List",
    "test_xpath_2": "n1:Geometric_Info/Tile_Angles/Sun_Angles_Grid/Azimuth/Values_List",
}


@pytest.mark.usecase
def test_xml_store():
    # Create and open xml angles accessor
    xml_store = EOXmlAnglesAccesor(f"{PARENT_DATA_PATH}/tests/data/MTD_TL.xml")
    xml_store.open()

    # verify data shapes / data name / data type
    assert xml_store[EXPECTED_XML_ATTR["test_xpath_1"]]._data.data.shape == EXPECTED_XML_ATTR["array_size"]
    assert xml_store[EXPECTED_XML_ATTR["test_xpath_1"]]._name == "sza"
    assert isinstance(xml_store[EXPECTED_XML_ATTR["test_xpath_1"]], EOVariable)

    # create an xarray with a user defined value
    dummy_xarray = xarray.DataArray(data=numpy.full(EXPECTED_XML_ATTR["array_size"], 1.23, dtype=float))
    # check if all data from an xpath match with user defined xarray
    assert numpy.all(xml_store[EXPECTED_XML_ATTR["test_xpath_2"]]._data.data == dummy_xarray.data)
    # check if all items from other xpath does not match with user defined xarray
    assert not numpy.all(xml_store[EXPECTED_XML_ATTR["test_xpath_1"]]._data.data == dummy_xarray.data)
