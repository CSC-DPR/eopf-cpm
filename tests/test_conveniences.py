import datetime
import os
import sys
from cmath import inf
from typing import Any

import dask
import hypothesis.extra.numpy as xps
import hypothesis.extra.pytz as xptz
import hypothesis.strategies as st
import numpy
import pytest
from hypothesis import HealthCheck, assume, given, settings

from eopf.product.utils import (
    apply_xpath,
    conv,
    convert_to_unix_time,
    is_date,
    parse_xml,
    reverse_conv,
    translate_structure,
)


@pytest.fixture
def tree(EMBEDED_TEST_DATA_FOLDER: str):
    snippet_path = os.path.join(EMBEDED_TEST_DATA_FOLDER, "snippet_xfdumanifest.xml")
    with open(snippet_path) as f:
        return parse_xml(f)


@st.composite
def value_with_type(draw, elements=st.integers(), expected_type=int, expected_container_type=None):
    if isinstance(expected_type, st.SearchStrategy):
        expected_type = draw(expected_type)

    if expected_container_type is not None:
        if isinstance(expected_container_type, st.SearchStrategy):
            expected_container_type = draw(expected_container_type)
        return (draw(elements), expected_type, expected_container_type)

    return (draw(elements), expected_type)


@st.composite
def numpy_value(draw, dtype_st=xps.scalar_dtypes(), allow_infinity=True, allow_nan=True):
    return draw(xps.from_dtype(draw(dtype_st), allow_infinity=allow_infinity, allow_nan=allow_nan))


@pytest.mark.unit
def test_parse_xml(tree):
    """Given an input xml,
    the output of the function must match the expected output"""
    result = ""
    display_namespaces = True
    for element in tree.iter():
        tag = element.tag
        result += f"{tag}\n"
        if display_namespaces:
            display_namespaces = False
            for key, value in element.nsmap.items():
                result += f"{key} : {value}\n"
        attributes = element.attrib
        for key, value in attributes.items():
            result += f"{key} : {value}\n"
        textual_content = element.text
        if textual_content and textual_content.strip():
            result += textual_content + "\n"
    file_path = os.path.join(os.path.abspath("tests/data"), "solutions.txt")
    with open(file_path, "r") as f:
        expected = f.read()
    assert result == expected


@pytest.mark.unit
def test_translate_structure(tree):
    """Given an input xml,
    the output of the function must match the expected output"""
    MAP = {
        "title": "concat('',metadataSection/metadataObject[@ID='generalProductInformation']/metadataWrap/xmlData/"
        "sentinel3:generalProductInformation/sentinel3:productName/text())",
        "Conventions": "'CF-1.9'",
    }
    NAMESPACES = {
        "xfdu": "urn:ccsds:schema:xfdu:1",
        "gml": "http://www.opengis.net/gml",
        "sentinel-safe": "http://www.esa.int/safe/sentinel/1.1",
        "sentinel3": "http://www.esa.int/safe/sentinel/sentinel-3/1.0",
        "olci": "http://www.esa.int/safe/sentinel/sentinel-3/olci/1.0",
    }
    result = translate_structure(MAP, tree, NAMESPACES)
    assert result == {
        "title": "S3A_OL_1_EFR____20220116T092821_20220116T093121_20220117T134858_0179_081_036_2160_LN1_O_NT_002.SEN3",
        "Conventions": "CF-1.9",
    }


@pytest.mark.unit
def test_apply_xpath(tree):
    """Given an input xml,
    the output of the function must match the expected output"""
    MAP = {
        "title": "concat('',metadataSection/metadataObject[@ID='generalProductInformation']/metadataWrap/xmlData/"
        "sentinel3:generalProductInformation/sentinel3:productName/text())",
        "Conventions": "'CF-1.9'",
    }
    NAMESPACES = {
        "xfdu": "urn:ccsds:schema:xfdu:1",
        "gml": "http://www.opengis.net/gml",
        "sentinel-safe": "http://www.esa.int/safe/sentinel/1.1",
        "sentinel3": "http://www.esa.int/safe/sentinel/sentinel-3/1.0",
        "olci": "http://www.esa.int/safe/sentinel/sentinel-3/olci/1.0",
    }
    result = {attr: apply_xpath(tree, MAP[attr], NAMESPACES) for attr in MAP}
    assert result == {
        "title": "S3A_OL_1_EFR____20220116T092821_20220116T093121_20220117T134858_0179_081_036_2160_LN1_O_NT_002.SEN3",
        "Conventions": "CF-1.9",
    }


@pytest.mark.unit
def test_is_date():
    string_date_1 = "2020-03-31T17:19:29.230522Z"  # Zulu time
    string_date_2 = "2020-03-31T17:19:29.230522GMT+3"  # GMT+3 Time
    string_date_3 = "some_random_string"
    dt_date = datetime.datetime(2020, 3, 31, 17, 19, 29, 230522)
    assert is_date(string_date_1)
    assert is_date(string_date_2)
    assert is_date(str(dt_date))
    assert not is_date(string_date_3)


@pytest.mark.unit
def test_convert_unix_time():
    import pytz

    # Define datetime-like string and verify if conversion match with datetime object and expected unix time. (MS)
    string_date = "2020-03-31T17:19:29.230522Z"
    dt_date = datetime.datetime(2020, 3, 31, 17, 19, 29, 230522, pytz.UTC)
    expected_unix_time = 1585675169230522

    assert convert_to_unix_time(string_date) == convert_to_unix_time(dt_date) == expected_unix_time

    # Define datetime-like string in Zulu Time Zone, and verify that it doesnt match with expected unix time
    string_date = "2020-03-31T17:19:29.230522GMT-3"
    assert convert_to_unix_time(string_date) != convert_to_unix_time(dt_date)
    assert convert_to_unix_time(string_date) != expected_unix_time

    #
    try:
        string_date = "a string that is not a valid date"
        convert_to_unix_time(string_date)
    except ValueError:
        assert True


@pytest.mark.unit
@given(
    value_and_types=st.one_of(
        value_with_type(
            st.lists(elements=st.floats(allow_infinity=False, allow_nan=False), unique=True, min_size=10),
            float,
            list,
        ),
        value_with_type(st.lists(elements=st.integers(), unique=True, min_size=10), int, list),
        value_with_type(st.lists(elements=st.booleans(), unique=True, min_size=2), int, list),
        value_with_type(st.sets(elements=st.floats(allow_infinity=False, allow_nan=False), min_size=10), float, set),
        value_with_type(st.sets(elements=st.integers(), min_size=10), int, set),
        value_with_type(st.sets(elements=st.booleans(), min_size=2), int, set),
        value_with_type(st.dictionaries(st.text(), st.integers(), min_size=10), int, dict),
        value_with_type(st.dictionaries(st.text(), st.booleans(), min_size=10), int, dict),
        value_with_type(
            st.dictionaries(st.text(), st.floats(allow_infinity=False, allow_nan=False), min_size=10),
            float,
            dict,
        ),
        value_with_type(xps.arrays(xps.floating_dtypes(), 10, unique=True), float, list),
        value_with_type(xps.arrays(xps.integer_dtypes(), 10, unique=True), int, list),
        value_with_type(xps.arrays(xps.boolean_dtypes(), 10, unique=True), int, list),
    ),
)
def test_conv_sequences(value_and_types: tuple[Any, type, type]):
    values, type_, container_type = value_and_types
    assume(inf not in values)
    converted_list = conv(values)
    assert isinstance(converted_list, container_type)
    # Check if size of converted value doesn't change
    assert len(converted_list) == len(values)
    # Check if type of each item from converted value is correct
    if isinstance(converted_list, dict):
        iterator = converted_list.values()
        original = values.values()
    else:
        iterator = converted_list
        original = values
    for converted_value, value in zip(sorted(iterator), sorted(original)):
        assert isinstance(converted_value, type_)
        conv_value = conv(value)
        # check if converted values are the same or both are nan
        assert converted_value == conv_value or (converted_value != converted_value and conv_value != conv_value)


@pytest.mark.unit
@pytest.mark.parametrize("EPSILON", [0.1])
@given(value=numpy_value(xps.floating_dtypes(), allow_infinity=False, allow_nan=False))
def test_epsilon_on_fp_conv(value, EPSILON):
    converted_value = conv(value)
    assert value - converted_value < EPSILON
    assert converted_value - value < EPSILON


@pytest.mark.unit
@given(
    value_and_type=st.one_of(
        value_with_type(
            elements=numpy_value(xps.floating_dtypes(), allow_infinity=False, allow_nan=False),
            expected_type=float,
        ),
        value_with_type(
            elements=numpy_value(xps.integer_dtypes(), allow_infinity=False, allow_nan=False),
            expected_type=int,
        ),
        value_with_type(
            elements=st.datetimes(timezones=xptz.timezones()),
            expected_type=int,
        ),
    ),
)
def test_conv(value_and_type):
    value, expected_type = value_and_type
    converted_value = conv(value)
    assert isinstance(converted_value, expected_type)


@pytest.mark.unit
@pytest.mark.parametrize(
    "sysmax, maxint",
    [
        (numpy.int64(sys.maxsize), numpy.int64(9223372036854775807)),
    ],
)
def test_maxint_conv(sysmax, maxint):
    # Robustness
    assert conv(sysmax) == maxint


@pytest.mark.unit
@given(
    value_and_types=st.one_of(
        value_with_type(
            st.integers(min_value=numpy.iinfo("int64").min, max_value=numpy.iinfo("int64").max),
            int,
            xps.integer_dtypes(endianness="=", sizes=(64,)),
        ),
        value_with_type(
            st.integers(min_value=numpy.iinfo("int32").min, max_value=numpy.iinfo("int32").max),
            int,
            xps.integer_dtypes(endianness="=", sizes=(32,)),
        ),
        value_with_type(
            st.integers(min_value=numpy.iinfo("int16").min, max_value=numpy.iinfo("int16").max),
            int,
            xps.integer_dtypes(endianness="=", sizes=(16,)),
        ),
        value_with_type(
            st.integers(min_value=numpy.iinfo("int8").min, max_value=numpy.iinfo("int8").max),
            int,
            xps.integer_dtypes(endianness="=", sizes=(8,)),
        ),
        value_with_type(st.floats(width=16), float, xps.floating_dtypes(endianness="=", sizes=(16,))),
        value_with_type(st.floats(width=32), float, xps.floating_dtypes(endianness="=", sizes=(32,))),
        value_with_type(st.floats(width=64), float, xps.floating_dtypes(endianness="=", sizes=(64,))),
    ),
)
def test_reverse_conv(value_and_types):
    value, current_type, data_type = value_and_types
    # verify if the current data type is as expected (int or float)
    assert isinstance(value, current_type)
    # convert value to given data type (int64, int32, float64 etc .. )
    converted_value = reverse_conv(data_type, value)
    # check if conversion is performed according to given data (int -> numpy.int64, float -> numpy.float64)
    assert numpy.issubdtype(type(converted_value), data_type)
    # check if converted data type is changed and not match with old one
    assert type(converted_value) != current_type


@pytest.mark.unit
@given(
    value_and_type=st.one_of(
        value_with_type(
            elements=numpy_value(xps.floating_dtypes(), allow_infinity=False, allow_nan=False),
            expected_type=float,
        ),
        value_with_type(
            elements=numpy_value(xps.integer_dtypes(), allow_infinity=False, allow_nan=False),
            expected_type=int,
        ),
        value_with_type(
            elements=st.datetimes(),
            expected_type=int,
        ),
    ),
)
@settings(suppress_health_check=(HealthCheck.function_scoped_fixture,))
def test_hypotesis_with_dask(dask_client_all, value_and_type):
    """Cf conftest comment. Function level fixture and hypothesis interaction is non trival."""
    # The scheduler coonfig is assigned when the distributed client is created
    try:
        assert dask.config.get("scheduler") == "dask.distributed"
        dask_distributed = True
    except KeyError:
        dask_distributed = False
    assert dask_distributed == (dask_client_all is not None)
