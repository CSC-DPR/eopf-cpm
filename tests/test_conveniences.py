import datetime
import os
import sys

import hypothesis.extra.numpy as et
import hypothesis.strategies as st
import numpy
import pytest

from eopf.product.utils import (
    apply_xpath,
    convert_to_unix_time,
    is_date,
    conv,
    parse_xml,
    reverse_conv,
    translate_structure,
)

from .utils import PARENT_DATA_PATH


@pytest.fixture
def tree(EMBEDED_TEST_DATA_FOLDER: str):
    snippet_path = os.path.join(EMBEDED_TEST_DATA_FOLDER, "snippet_xfdumanifest.xml")
    with open(snippet_path) as f:
        return parse_xml(f)


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


@pytest.mark.parametrize(
    "data_type, values, expected_type",
    [
        (
            dict,
            {
                key: numpy.float32(value)
                for key, value in zip(
                    st.lists(st.text(min_size=1), min_size=10, unique=True).example(),
                    st.lists(st.floats(-999, 999), unique=True, min_size=10).example(),
                )
            },
            float,
        ),
        (
            dict,
            {
                key: numpy.uint32(value)
                for key, value in zip(
                    st.lists(st.text(min_size=1), min_size=10, unique=True).example(),
                    st.lists(st.integers(-999, 999), unique=True, min_size=10).example(),
                )
            },
            int,
        ),
        (
            dict,
            {
                key: numpy.int32(value)
                for key, value in zip(
                    st.lists(st.text(min_size=1), min_size=10, unique=True).example(),
                    st.lists(st.integers(-999, 999), unique=True, min_size=10).example(),
                )
            },
            int,
        ),
    ],
)
def test_dict_conv(data_type, values, expected_type):
    converted_dict = conv(values)
    # Check if data type of converted value doesn't change
    assert isinstance(converted_dict, data_type)
    # Check if items of converted value doesn't change
    assert converted_dict.items() == values.items()
    for item_value in converted_dict:
        assert isinstance(converted_dict[item_value], expected_type)


@pytest.mark.unit
@pytest.mark.parametrize(
    "data_type, values, expected_type",
    [
        (
            list,
            [numpy.float64(item) for item in st.lists(st.floats(-999, 999), unique=True, min_size=10).example()],
            float,
        ),
        (
            list,
            [numpy.uint32(item) for item in st.lists(st.integers(-999, 999), unique=True, min_size=10).example()],
            int,
        ),
        (
            list,
            [numpy.int64(item) for item in st.lists(st.integers(-999, 999), unique=True, min_size=10).example()],
            int,
        ),
    ],
)
def test_list_conv(data_type, values, expected_type):
    converted_list = conv(values)
    # Check if data type of converted value doesn't change
    assert isinstance(converted_list, data_type)
    # Check if size of converted value doesn't change
    assert len(converted_list) == len(values)
    # Check if type of each item from converted value is correct
    for value in converted_list:
        assert isinstance(value, expected_type)


@pytest.mark.unit
@pytest.mark.parametrize(
    "data_type, values, expected_type",
    [
        (
            set,
            {numpy.float32(item) for item in st.lists(st.floats(-999, 999), unique=True, min_size=10).example()},
            float,
        ),
        (
            set,
            {numpy.int32(item) for item in st.lists(st.integers(-999, 999), unique=True, min_size=10).example()},
            int,
        ),
        (set, {numpy.uint32(item) for item in st.lists(st.floats(-999, 999), unique=True, min_size=10).example()}, int),
    ],
)
def test_set_conv(data_type, values, expected_type):
    converted_set = conv(values)
    # Check if data type of converted value doesn't change
    assert isinstance(converted_set, data_type)
    # Check if size of converted value doesn't change
    assert len(converted_set) == len(values)
    # Check if type of each item from converted value is correct
    for value in converted_set:
        assert isinstance(value, expected_type)


@pytest.mark.unit
@pytest.mark.parametrize(
    "data_type, values, expected_type",
    [
        (list, et.arrays(numpy.float64, 10, elements=st.floats(-999, 999), unique=True).example(), float),
        (list, et.arrays(numpy.int64, 3, elements=st.integers(-999, 999), unique=True).example(), int),
    ],
)
def test_np_conv(data_type, values, expected_type):
    converted_nparray = conv(values)
    assert isinstance(converted_nparray, data_type)
    assert numpy.all(converted_nparray == values)
    for value in converted_nparray:
        assert isinstance(value, expected_type)


@pytest.mark.unit
@pytest.mark.parametrize("tested_value, EPSILON", [(st.floats(-999, 999).example(), 0.1)])
def test_fp_conv(tested_value, EPSILON):
    # Floating points
    np_float_values = [numpy.float64(tested_value), numpy.float32(tested_value), numpy.float16(tested_value)]

    for value in np_float_values:
        converted_value = conv(value)
        assert isinstance(converted_value, float)
        assert value - converted_value < EPSILON
        assert converted_value - tested_value < EPSILON


@pytest.mark.unit
@pytest.mark.parametrize(
    "tested_value",
    [
        (st.integers(-999, 999).example()),
    ],
)
def test_int_conv(tested_value):
    # Integer - unsigned integer tests
    np_values = [
        numpy.int64(tested_value),
        numpy.int32(tested_value),
        numpy.int16(tested_value),
        # numpy.uint64(tested_value),
        # numpy.uint32(tested_value),
        # numpy.uint16(tested_value),
        # umpy.uint8(tested_value),
    ]

    for value in np_values:
        converted_value = conv(value)
        assert isinstance(converted_value, int)
        assert converted_value == value
        assert converted_value == tested_value


@pytest.mark.unit
@pytest.mark.parametrize(
    "tested_value",
    [
        (complex(st.integers(-999, 999).example(), st.integers(-999, 999).example())),
    ],
)
def test_complex_conv(tested_value):
    # Default type -> test that no conversion is performed
    converted_value = conv(tested_value)
    assert isinstance(tested_value, complex)
    assert isinstance(converted_value, complex)
    assert tested_value == converted_value


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
@pytest.mark.parametrize(
    "data_values, current_types, data_types",
    [
        (
            [
                st.integers(-999, 999).example(),
                st.integers(-999, 999).example(),
                st.integers(-999, 999).example(),
                st.integers(-999, 999).example(),
                st.integers(-999, 999).example(),
                st.integers(-999, 999).example(),
                st.floats(-999, 999).example(),
                st.floats(-999, 999).example(),
            ],
            [int, int, int, int, int, int, float, float],
            [
                numpy.int16,
                numpy.int32,
                numpy.int64,
                numpy.uint8,
                numpy.uint16,
                numpy.uint32,
                numpy.float64,
                numpy.float32,
            ],
        ),
    ],
)
def test_reverse_conv(data_values, current_types, data_types):
    for (idx, value), type_ in zip(enumerate(data_values), data_types):
        # verify if the current data type is as expected (int or float)
        assert type(value) == current_types[idx]
        # convert value to given data type (int64, int32, float64 etc .. )
        converted_value = reverse_conv(type_, value)
        # check if conversion is performed according to given data (int -> numpy.int64, float -> numpy.float64)
        assert isinstance(converted_value, type_)
        # check if converted data type is changed and not match with old one
        assert type(converted_value) != current_types[idx]
