import datetime
import os

import pytest

from .utils import PARENT_DATA_PATH
from eopf.product.utils import (
    apply_xpath,
    convert_to_unix_time,
    is_date,
    parse_xml,
    translate_structure,
)


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
        "title": "concat('',metadataSection/metadataObject[@ID='generalProductInformation']/metadataWrap/xmlData/sentinel3:generalProductInformation/sentinel3:productName/text())",
        # noqa
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
        "title": "concat('',metadataSection/metadataObject[@ID='generalProductInformation']/metadataWrap/xmlData/sentinel3:generalProductInformation/sentinel3:productName/text())",
        # noqa
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
    dt_date = datetime.datetime(2020, 3, 31, 17, 19, 29, 230522)
    assert is_date(string_date_1)
    assert is_date(string_date_2)
    assert is_date(str(dt_date))


def test_convert_unix_time():
    # Define datetime-like string and verify if conversion match with datetime object and expected unix time. (MS)
    string_date = "2020-03-31T17:19:29.230522GMT-3"
    dt_date = datetime.datetime(2020, 3, 31, 17, 19, 29, 230522)
    expected_unix_time = 1585664369230522

    assert convert_to_unix_time(string_date) == convert_to_unix_time(dt_date) == expected_unix_time

    # Define datetime-like string in Zulu Time Zone, and verify that it doesnt match with expected unix time
    string_date = "2020-03-31T17:19:29.230522Z"
    assert convert_to_unix_time(string_date) != convert_to_unix_time(dt_date)
    assert convert_to_unix_time(string_date) != expected_unix_time
