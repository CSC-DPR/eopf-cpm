import os

import pytest

from eopf.product.conveniences import (
    apply_xpath,
    etree_to_dict,
    parse_xml,
    translate_structure,
)


@pytest.mark.unit
def test_parse_xml():
    tree = parse_xml(os.path.abspath("tests/data"), "*.xml")
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
def test_translate_structure():
    dom = parse_xml(os.path.abspath("tests/data"), "*.xml")
    MAP = {
        "title": "concat('',metadataSection/metadataObject[@ID='generalProductInformation']/metadataWrap/xmlData/sentinel3:generalProductInformation/sentinel3:productName/text())",  # noqa
        "Conventions": "'CF-1.9'",
    }
    NAMESPACES = {
        "xfdu": "urn:ccsds:schema:xfdu:1",
        "gml": "http://www.opengis.net/gml",
        "sentinel-safe": "http://www.esa.int/safe/sentinel/1.1",
        "sentinel3": "http://www.esa.int/safe/sentinel/sentinel-3/1.0",
        "olci": "http://www.esa.int/safe/sentinel/sentinel-3/olci/1.0",
    }
    result = {attr: translate_structure(MAP[attr], dom, NAMESPACES) for attr in MAP}
    assert result == {
        "title": "S3A_OL_1_EFR____20220116T092821_20220116T093121_20220117T134858_0179_081_036_2160_LN1_O_NT_002.SEN3",
        "Conventions": "CF-1.9",
    }


@pytest.mark.unit
def test_apply_xpath():
    dom = parse_xml(os.path.abspath("tests/data"), "*.xml")
    MAP = {
        "title": "concat('',metadataSection/metadataObject[@ID='generalProductInformation']/metadataWrap/xmlData/sentinel3:generalProductInformation/sentinel3:productName/text())",  # noqa
        "Conventions": "'CF-1.9'",
    }
    NAMESPACES = {
        "xfdu": "urn:ccsds:schema:xfdu:1",
        "gml": "http://www.opengis.net/gml",
        "sentinel-safe": "http://www.esa.int/safe/sentinel/1.1",
        "sentinel3": "http://www.esa.int/safe/sentinel/sentinel-3/1.0",
        "olci": "http://www.esa.int/safe/sentinel/sentinel-3/olci/1.0",
    }
    result = {attr: apply_xpath(dom, MAP[attr], NAMESPACES) for attr in MAP}
    assert result == {
        "title": "S3A_OL_1_EFR____20220116T092821_20220116T093121_20220117T134858_0179_081_036_2160_LN1_O_NT_002.SEN3",
        "Conventions": "CF-1.9",
    }


@pytest.mark.unit
def test_etree_to_dict():
    tree = parse_xml(os.path.abspath("tests/data"), "*.xml")
    root = tree.getroot()
    ddict = etree_to_dict(root[0])
    assert ddict == {
        "informationPackageMap": {
            "{urn:ccsds:schema:xfdu:1}contentUnit": {
                "{urn:ccsds:schema:xfdu:1}contentUnit": [
                    {
                        "dataObjectPointer": {"@dataObjectID": "Oa01_radianceData"},
                        "@ID": "Oa01_radianceUnit",
                        "@unitType": "Measurement Data Unit",
                        "@textInfo": "TOA radiance for OLCI acquisition band Oa01",
                        "@dmdID": "geoCoordinatesAnnotation timeCoordinatesAnnotation qualityFlagsAnnotation",
                    },
                    {
                        "dataObjectPointer": {"@dataObjectID": "Oa02_radianceData"},
                        "@ID": "Oa02_radianceUnit",
                        "@unitType": "Measurement Data Unit",
                        "@textInfo": "TOA radiance for OLCI acquisition band Oa02",
                        "@dmdID": "geoCoordinatesAnnotation timeCoordinatesAnnotation qualityFlagsAnnotation",
                    },
                ],
                "@ID": "packageUnit",
                "@unitType": "Information Package",
                "@textInfo": "SENTINEL-3 OLCI Level 1 Earth Observation Full Resolution Product",
                "@dmdID": "acquisitionPeriod platform measurementOrbitReference measurementQualityInformation processing measurementFrameSet generalProductInformation olciProductInformation",  # noqa
                "@pdiID": "processing",
            },
        },
    }
