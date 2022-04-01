import datetime
import os

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


def test_conv():
    # dict
    numpy_dict = {"int_value": numpy.int16(155), "float_value": numpy.float64(202.12), "uint_value": numpy.uint64(-5)}
    converted_dict = conv(numpy_dict)
    # Check items / converted type, every item type
    assert converted_dict.items() == numpy_dict.items()
    assert type(converted_dict) == dict
    assert type(converted_dict["int_value"]) == int
    assert type(converted_dict["float_value"]) == float
    assert type(converted_dict["uint_value"]) == int
    assert type(converted_dict["int_value"]) != numpy.int16

    # List / tuple / set
    numpy_list = [numpy.float32(101.255), numpy.int64(255), numpy.uint64(15)]
    converted_list = conv(numpy_list)
    # Check converted type, items types
    assert type(converted_list) == list
    assert type(converted_list[0]) == float
    assert type(converted_list[1]) == int
    assert type(converted_list[2]) == int
    assert type(converted_list[0]) != numpy.float32
    # Check converted type, every item type
    numpy_set = {numpy.float32(1.01), numpy.int64(2), numpy.uint64(3)}
    test_types = {float, int, int}
    converted_set = conv(numpy_set)
    assert type(converted_set) == set
    for value, type_ in zip(converted_set, test_types):
        assert type(value) == type_

    # Numpy ndarray

    numpy_ndarray = numpy.array([[1, 2, 3], [4, 2, 5]])
    expected_array = [[1, 2, 3], [4, 2, 5]]
    converted_ndarray = conv(numpy_ndarray)
    # Check converted type, items, array shapes/lengths
    assert type(converted_ndarray) == list
    assert expected_array == converted_ndarray
    assert numpy_ndarray.shape[0] == len(converted_ndarray)
    assert numpy_ndarray.shape[1] == len(converted_ndarray[0])
    assert numpy_ndarray.shape[1] == len(converted_ndarray[1])

    # Floating points
    EPSILON = 0.1  # to check why conversion precision is bad
    tested_value = 101.29393
    np_float_values = [numpy.float64(tested_value), numpy.float32(tested_value), numpy.float16(tested_value)]

    for value in np_float_values:
        converted_value = conv(value)
        assert type(value) != float
        assert type(converted_value) == float
        assert value - converted_value < EPSILON
        assert converted_value - tested_value < EPSILON

    # Integer - unsigned integer tests
    tested_value = 101
    np_values = [
        numpy.int64(tested_value),
        numpy.int32(tested_value),
        numpy.int16(tested_value),
        numpy.uint64(tested_value),
        numpy.uint32(tested_value),
        numpy.uint16(tested_value),
        numpy.uint8(tested_value),
    ]

    for value in np_values:
        converted_value = conv(value)
        assert type(converted_value) == int
        assert converted_value == value
        assert converted_value == tested_value

    # Default type -> test that no conversion is performed
    complex_number = complex(2, 3)
    converted_value = conv(complex_number)
    assert type(complex_number) == complex
    assert type(converted_value) == complex
    assert complex_number == converted_value

    # Robustness
    import sys

    max_int64 = numpy.int64(9223372036854775807)
    maximum_integer_value = sys.maxsize
    assert conv(max_int64) == maximum_integer_value


@pytest.mark.unit
def test_reverse_conv():
    data_values = [1, 2, 3, 4, 5, 6, 7.0494, 8.149]
    current_types = [int, int, int, int, int, int, float, float]
    data_types = [
        numpy.int16,
        numpy.int32,
        numpy.int64,
        numpy.uint8,
        numpy.uint16,
        numpy.uint32,
        numpy.float64,
        numpy.float32,
    ]

    for (idx, value), type_ in zip(enumerate(data_values), data_types):
        # verify if the current data type is as expected (int or float)
        assert type(value) == current_types[idx]
        # convert value to given data type (int64, int32, float64 etc .. )
        converted_value = reverse_conv(type_, value)
        # check if conversion is performed according to given data (int -> numpy.int64, float -> numpy.float64)
        assert type(converted_value) == type_
        # check if converted data type is changed and not match with old one
        assert type(converted_value) != current_types[idx]
